// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

extern "C"
{
#include <postgres.h>

#include <access/xact.h>
#include <executor/spi.h>
#include <fmgr.h>
#include <miscadmin.h>
#include <postmaster/bgworker.h>
#include <storage/ipc.h>
#include <storage/latch.h>
#include <storage/lwlock.h>
#include <storage/procsignal.h>
#include <storage/shmem.h>
#include <utils/backend_status.h>
#include <utils/dsa.h>
#include <utils/guc.h>
#include <utils/snapmgr.h>
#include <utils/wait_event.h>
}

#undef Abs

#include <arrow/buffer.h>
#include <arrow/builder.h>
#include <arrow/flight/server_middleware.h>
#include <arrow/flight/sql/server.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/table_builder.h>
#include <arrow/util/base64.h>

#include <condition_variable>
#include <sstream>

#ifdef __GNUC__
#	define AFS_FUNC __PRETTY_FUNCTION__
#else
#	define AFS_FUNC __func__
#endif

#ifdef AFS_DEBUG
#	define P(...) ereport(DEBUG5, errmsg_internal(__VA_ARGS__))
#else
#	define P(...)
#endif

extern "C"
{
	PG_MODULE_MAGIC;

	extern PGDLLEXPORT void _PG_init(void);
	extern PGDLLEXPORT void afs_executor(Datum datum) pg_attribute_noreturn();
	extern PGDLLEXPORT void afs_server(Datum datum) pg_attribute_noreturn();
	extern PGDLLEXPORT void afs_main(Datum datum) pg_attribute_noreturn();
}

namespace {
static const char* LibraryName = "arrow_flight_sql";
static const char* SharedDataName = "arrow-flight-sql: shared data";
static const char* Tag = "arrow-flight-sql";

static const char* URIDefault = "grpc://127.0.0.1:15432";
static char* URI;

static const int SessionTimeoutDefault = 300;
static int SessionTimeout;

static const int MaxNRowsPerRecordBatchDefault = 1 * 1024 * 1024;
static int MaxNRowsPerRecordBatch;

static volatile sig_atomic_t GotSIGTERM = false;
void afs_sigterm(SIGNAL_ARGS)
{
	auto errnoSaved = errno;
	GotSIGTERM = true;
	SetLatch(MyLatch);
	errno = errnoSaved;
}

static volatile sig_atomic_t GotSIGHUP = false;
void afs_sighup(SIGNAL_ARGS)
{
	auto errnoSaved = errno;
	GotSIGHUP = true;
	SetLatch(MyLatch);
	errno = errnoSaved;
}

static volatile sig_atomic_t GotSIGUSR1 = false;
void afs_sigusr1(SIGNAL_ARGS)
{
	procsignal_sigusr1_handler(postgres_signal_arg);
	auto errnoSaved = errno;
	GotSIGUSR1 = true;
	SetLatch(MyLatch);
	errno = errnoSaved;
}

static shmem_request_hook_type PreviousShmemRequestHook = nullptr;
static const char* LWLockTrancheName = "arrow-flight-sql: lwlock tranche";
void
afs_shmem_request_hook(void)
{
	if (PreviousShmemRequestHook)
		PreviousShmemRequestHook();

	RequestNamedLWLockTranche(LWLockTrancheName, 1);
}

struct ConnectData {
	dsa_pointer databaseName;
	dsa_pointer userName;
	dsa_pointer password;
};

struct SharedRingBufferData {
	dsa_pointer pointer;
	size_t total;
	size_t head;
	size_t tail;
};

class SharedRingBuffer {
   public:
	static void initialize_data(SharedRingBufferData* data)
	{
		data->pointer = InvalidDsaPointer;
		data->total = 0;
		data->head = 0;
		data->tail = 0;
	}

	SharedRingBuffer(SharedRingBufferData* data, dsa_area* area)
		: data_(data), area_(area)
	{
	}

	void allocate(size_t total)
	{
		data_->pointer = dsa_allocate(area_, total);
		data_->total = total;
		data_->head = 0;
		data_->tail = 0;
	}

	void free()
	{
		dsa_free(area_, data_->pointer);
		initialize_data(data_);
	}

	size_t size() const
	{
		if (data_->head <= data_->tail)
		{
			return data_->tail - data_->head;
		}
		else
		{
			return (data_->total - data_->head) + data_->tail;
		}
	}

	size_t rest_size() const { return data_->total - size() - 1; }

	size_t write(const void* data, size_t n)
	{
		P("%s: %s: before: (%d:%d) %d", Tag, AFS_FUNC, data_->head, data_->tail, n);
		if (rest_size() == 0)
		{
			P("%s: %s: after: no space: (%d:%d) %d:0",
			  Tag,
			  AFS_FUNC,
			  data_->head,
			  data_->tail,
			  n);
			return 0;
		}

		size_t writtenSize = 0;
		auto output = address();
		if (data_->head <= data_->tail)
		{
			auto restSize = data_->total - data_->tail;
			if (data_->head == 0)
			{
				restSize--;
			}
			const auto firstHalfWriteSize = std::min(n, restSize);
			P("%s: %s: first half: (%d:%d) %d:%d",
			  Tag,
			  AFS_FUNC,
			  data_->head,
			  data_->tail,
			  n,
			  firstHalfWriteSize);
			memcpy(output + data_->tail, data, firstHalfWriteSize);
			data_->tail = (data_->tail + firstHalfWriteSize) % data_->total;
			n -= firstHalfWriteSize;
			writtenSize += firstHalfWriteSize;
		}
		if (n > 0 && rest_size() > 0)
		{
			const auto lastHalfWriteSize = std::min(n, data_->head - data_->tail - 1);
			P("%s: %s: last half: (%d:%d) %d:%d",
			  Tag,
			  AFS_FUNC,
			  data_->head,
			  data_->tail,
			  n,
			  lastHalfWriteSize);
			memcpy(output + data_->tail,
			       static_cast<const uint8_t*>(data) + writtenSize,
			       lastHalfWriteSize);
			data_->tail += lastHalfWriteSize;
			n -= lastHalfWriteSize;
			writtenSize += lastHalfWriteSize;
		}
		P("%s: %s: after: (%d:%d) %d:%d",
		  Tag,
		  AFS_FUNC,
		  data_->head,
		  data_->tail,
		  n,
		  writtenSize);
		return writtenSize;
	}

	size_t read(size_t n, void* output)
	{
		P("%s: %s: before: (%d:%d) %d", Tag, AFS_FUNC, data_->head, data_->tail, n);
		size_t readSize = 0;
		const auto input = address();
		if (data_->head > data_->tail)
		{
			const auto firstHalfReadSize = std::min(n, data_->total - data_->head);
			P("%s: %s: first half: (%d:%d) %d:%d",
			  Tag,
			  AFS_FUNC,
			  data_->head,
			  data_->tail,
			  n,
			  firstHalfReadSize);
			memcpy(output, input + data_->head, firstHalfReadSize);
			data_->head = (data_->head + firstHalfReadSize) % data_->total;
			n -= firstHalfReadSize;
			readSize += firstHalfReadSize;
		}
		if (n > 0 && data_->head != data_->tail)
		{
			const auto lastHalfReadSize = std::min(n, data_->tail - data_->head);
			P("%s: %s: last half: (%d:%d) %d:%d",
			  Tag,
			  AFS_FUNC,
			  data_->head,
			  data_->tail,
			  n,
			  lastHalfReadSize);
			memcpy(static_cast<uint8_t*>(output) + readSize,
			       input + data_->head,
			       lastHalfReadSize);
			data_->head += lastHalfReadSize;
			n -= lastHalfReadSize;
			readSize += lastHalfReadSize;
		}
		P("%s: %s: after: (%d:%d) %d:%d",
		  Tag,
		  AFS_FUNC,
		  data_->head,
		  data_->tail,
		  n,
		  readSize);
		return readSize;
	}

   private:
	SharedRingBufferData* data_;
	dsa_area* area_;

	uint8_t* address()
	{
		return static_cast<uint8_t*>(dsa_get_address(area_, data_->pointer));
	}
};

struct ExecuteData {
	dsa_pointer query;
	SharedRingBufferData bufferData;
};

struct SharedData {
	dsa_handle handle;
	pid_t executorPID;
	pid_t serverPID;
	pid_t mainPID;
	ConnectData connectData;
	ExecuteData executeData;
};

class Processor {
   public:
	Processor(const char* tag)
		: tag_(tag),
		  sharedData_(nullptr),
		  area_(nullptr),
		  lock_(),
		  mutex_(),
		  conditionVariable_()
	{
	}

	virtual ~Processor() { dsa_detach(area_); }

	const char* tag() { return tag_; }

	SharedRingBuffer create_shared_ring_buffer()
	{
		return SharedRingBuffer(&(sharedData_->executeData.bufferData), area_);
	}

	void lock_acquire(LWLockMode mode) { LWLockAcquire(lock_, LW_EXCLUSIVE); }

	void lock_release() { LWLockRelease(lock_); }

	void wait_executor_written(SharedRingBuffer* buffer)
	{
		if (ARROW_PREDICT_FALSE(sharedData_->executorPID == InvalidPid))
		{
			ereport(ERROR,
			        errcode(ERRCODE_INTERNAL_ERROR),
			        errmsg("%s: %s: executor isn't alive", Tag, tag_));
		}

		P("%s: %s: %s: kill executor: %d", Tag, tag_, AFS_FUNC, sharedData_->executorPID);
		kill(sharedData_->executorPID, SIGUSR1);
		auto size = buffer->size();
		std::unique_lock<std::mutex> lock(mutex_);
		conditionVariable_.wait(lock, [&] {
			P("%s: %s: %s: wait: write: %d:%d",
			  Tag,
			  tag_,
			  AFS_FUNC,
			  buffer->size(),
			  size);
			return buffer->size() != size;
		});
	}

	void wait_server_read(SharedRingBuffer* buffer)
	{
		if (ARROW_PREDICT_FALSE(sharedData_->serverPID == InvalidPid))
		{
			ereport(ERROR,
			        errcode(ERRCODE_INTERNAL_ERROR),
			        errmsg("%s: %s: server isn't alive", Tag, tag_));
		}

		P("%s: %s: %s: kill server: %d", Tag, tag_, AFS_FUNC, sharedData_->serverPID);
		kill(sharedData_->serverPID, SIGUSR1);
		auto restSize = buffer->rest_size();
		while (true)
		{
			int events = WL_LATCH_SET | WL_EXIT_ON_PM_DEATH;
			WaitLatch(MyLatch, events, -1, PG_WAIT_EXTENSION);
			if (GotSIGTERM)
			{
				break;
			}
			ResetLatch(MyLatch);

			if (GotSIGUSR1)
			{
				GotSIGUSR1 = false;
				P("%s: %s: %s: wait: read: %d:%d",
				  Tag,
				  tag_,
				  AFS_FUNC,
				  buffer->rest_size(),
				  restSize);
				if (buffer->rest_size() != restSize)
				{
					break;
				}
			}

			CHECK_FOR_INTERRUPTS();
		}
	}

	void signaled()
	{
		P("%s: %s: signaled: before", Tag, tag_);
		conditionVariable_.notify_all();
		P("%s: %s: signaled: after", Tag, tag_);
	}

   protected:
	const char* tag_;
	SharedData* sharedData_;
	dsa_area* area_;
	LWLock* lock_;
	std::mutex mutex_;
	std::condition_variable conditionVariable_;
};

class SharedRingBufferInputStream : public arrow::io::InputStream {
   public:
	SharedRingBufferInputStream(Processor* processor)
		: arrow::io::InputStream(), processor_(processor), position_(0), is_open_(true)
	{
	}

	arrow::Status Close() override
	{
		is_open_ = false;
		return arrow::Status::OK();
	}

	bool closed() const override { return !is_open_; }

	arrow::Result<int64_t> Tell() const override { return position_; }

	arrow::Result<int64_t> Read(int64_t nBytes, void* out) override
	{
		if (ARROW_PREDICT_FALSE(!is_open_))
		{
			return arrow::Status::IOError(std::string(Tag) + ": " + processor_->tag() +
			                              ": SharedRingBufferInputStream is closed");
		}
		auto buffer = std::move(processor_->create_shared_ring_buffer());
		size_t rest = static_cast<size_t>(nBytes);
		while (true)
		{
			processor_->lock_acquire(LW_EXCLUSIVE);
			auto readBytes = buffer.read(rest, out);
			processor_->lock_release();

			position_ += readBytes;
			rest -= readBytes;
			out = static_cast<uint8_t*>(out) + readBytes;
			if (ARROW_PREDICT_TRUE(rest == 0))
			{
				break;
			}

			processor_->wait_executor_written(&buffer);
		}
		return nBytes;
	}

	arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nBytes) override
	{
		ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nBytes));
		ARROW_ASSIGN_OR_RAISE(auto readBytes, Read(nBytes, buffer->mutable_data()));
		ARROW_RETURN_NOT_OK(buffer->Resize(readBytes, false));
		buffer->ZeroPadding();
		return std::move(buffer);
	}

   private:
	Processor* processor_;
	int64_t position_;
	bool is_open_;
};

class SharedRingBufferOutputStream : public arrow::io::OutputStream {
   public:
	SharedRingBufferOutputStream(Processor* processor)
		: arrow::io::OutputStream(), processor_(processor), position_(0), is_open_(true)
	{
	}

	arrow::Status Close() override
	{
		is_open_ = false;
		return arrow::Status::OK();
	}

	bool closed() const override { return !is_open_; }

	arrow::Result<int64_t> Tell() const override { return position_; }

	arrow::Status Write(const void* data, int64_t nBytes) override
	{
		if (ARROW_PREDICT_FALSE(!is_open_))
		{
			return arrow::Status::IOError(std::string(Tag) + ": " + processor_->tag() +
			                              ": SharedRingBufferOutputStream is closed");
		}
		if (ARROW_PREDICT_TRUE(nBytes > 0))
		{
			auto buffer = std::move(processor_->create_shared_ring_buffer());
			size_t rest = static_cast<size_t>(nBytes);
			while (true)
			{
				processor_->lock_acquire(LW_EXCLUSIVE);
				auto writtenSize = buffer.write(data, rest);
				processor_->lock_release();

				position_ += writtenSize;
				rest -= writtenSize;
				data = static_cast<const uint8_t*>(data) + writtenSize;

				if (ARROW_PREDICT_TRUE(rest == 0))
				{
					break;
				}

				processor_->wait_server_read(&buffer);
			}
		}
		return arrow::Status::OK();
	}

	using arrow::io::OutputStream::Write;

   private:
	Processor* processor_;
	int64_t position_;
	bool is_open_;
};

class WorkerProcessor : public Processor {
   public:
	explicit WorkerProcessor(const char* tag) : Processor(tag)
	{
		LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
		bool found;
		auto sharedData = static_cast<SharedData*>(
			ShmemInitStruct(SharedDataName, sizeof(SharedData), &found));
		if (!found)
		{
			LWLockRelease(AddinShmemInitLock);
			ereport(ERROR,
			        errcode(ERRCODE_INTERNAL_ERROR),
			        errmsg("%s: %s: shared data isn't created yet", Tag, tag_));
		}
		auto area = dsa_attach(sharedData->handle);
		lock_ = &(GetNamedLWLockTranche(LWLockTrancheName)[0].lock);
		LWLockRelease(AddinShmemInitLock);
		sharedData_ = sharedData;
		area_ = area;
	}
};

class Executor : public WorkerProcessor {
   public:
	explicit Executor() : WorkerProcessor("executor") {}

	void open()
	{
		pgstat_report_activity(STATE_RUNNING, (std::string(Tag) + ": opening").c_str());
		LWLockAcquire(lock_, LW_EXCLUSIVE);
		BackgroundWorkerInitializeConnection(
			static_cast<const char*>(
				dsa_get_address(area_, sharedData_->connectData.databaseName)),
			static_cast<const char*>(
				dsa_get_address(area_, sharedData_->connectData.userName)),
			0);
		unsetSharedString(sharedData_->connectData.databaseName);
		unsetSharedString(sharedData_->connectData.userName);
		unsetSharedString(sharedData_->connectData.password);
		{
			SharedRingBuffer buffer(&(sharedData_->executeData.bufferData), area_);
			// TODO: Customizable.
			buffer.allocate(1L * 1024L * 1024L);
		}
		LWLockRelease(lock_);
		StartTransactionCommand();
		SPI_connect();
		pgstat_report_activity(STATE_IDLE, NULL);
	}

	void close()
	{
		pgstat_report_activity(STATE_RUNNING, (std::string(Tag) + ": closing").c_str());
		SPI_finish();
		CommitTransactionCommand();
		LWLockAcquire(lock_, LW_EXCLUSIVE);
		{
			SharedRingBuffer buffer(&(sharedData_->executeData.bufferData), area_);
			buffer.free();
		}
		sharedData_->executorPID = InvalidPid;
		LWLockRelease(lock_);
		pgstat_report_activity(STATE_IDLE, NULL);
	}

	void signaled()
	{
		P("%s: %s: signaled: before: %d", Tag, tag_, sharedData_->executeData.query);
		P("signaled: before: %d", sharedData_->executeData.query);
		if (DsaPointerIsValid(sharedData_->executeData.query))
		{
			execute();
		}
		else
		{
			Processor::signaled();
		}
		P("%s: %s: signaled: after: %d", Tag, tag_, sharedData_->executeData.query);
	}

   private:
	void unsetSharedString(dsa_pointer& pointer)
	{
		if (!DsaPointerIsValid(pointer))
		{
			return;
		}
		dsa_free(area_, pointer);
		pointer = InvalidDsaPointer;
	}

	void execute()
	{
		pgstat_report_activity(STATE_RUNNING, (std::string(Tag) + ": executing").c_str());

		PushActiveSnapshot(GetTransactionSnapshot());

		LWLockAcquire(lock_, LW_EXCLUSIVE);
		auto query = static_cast<const char*>(
			dsa_get_address(area_, sharedData_->executeData.query));
		SetCurrentStatementStartTimestamp();
		P("%s: %s: execute: %s", Tag, tag_, query);
		auto result = SPI_execute(query, true, 0);
		dsa_free(area_, sharedData_->executeData.query);
		sharedData_->executeData.query = InvalidDsaPointer;
		LWLockRelease(lock_);

		if (result == SPI_OK_SELECT)
		{
			pgstat_report_activity(STATE_RUNNING,
			                       (std::string(Tag) + ": writing").c_str());
			auto status = write();
			if (!status.ok())
			{
				ereport(ERROR,
				        errcode(ERRCODE_INTERNAL_ERROR),
				        errmsg("%s: %s: failed to write", Tag, tag_));
			}
		}

		PopActiveSnapshot();

		if (sharedData_->serverPID != InvalidPid)
		{
			P("%s: %s: kill server: %s", Tag, tag_, sharedData_->serverPID);
			kill(sharedData_->serverPID, SIGUSR1);
		}

		pgstat_report_activity(STATE_IDLE, NULL);
	}

	arrow::Status write()
	{
		SharedRingBufferOutputStream output(this);
		std::vector<std::shared_ptr<arrow::Field>> fields;
		for (int i = 0; i < SPI_tuptable->tupdesc->natts; ++i)
		{
			auto attribute = TupleDescAttr(SPI_tuptable->tupdesc, i);
			std::shared_ptr<arrow::DataType> type;
			switch (attribute->atttypid)
			{
				case INT4OID:
					type = arrow::int32();
					break;
				default:
					return arrow::Status::NotImplemented("Unsupported PostgreSQL type: ",
					                                     attribute->atttypid);
			}
			fields.push_back(
				arrow::field(NameStr(attribute->attname), type, !attribute->attnotnull));
		}
		auto schema = arrow::schema(fields);
		ARROW_ASSIGN_OR_RAISE(
			auto builder,
			arrow::RecordBatchBuilder::Make(schema, arrow::default_memory_pool()));
		auto option = arrow::ipc::IpcWriteOptions::Defaults();
		option.emit_dictionary_deltas = true;

		// Write schema only stream format data to return only schema.
		ARROW_ASSIGN_OR_RAISE(auto writer,
		                      arrow::ipc::MakeStreamWriter(&output, schema, option));
		// Build an empty record batch to write schema.
		ARROW_ASSIGN_OR_RAISE(auto recordBatch, builder->Flush());
		P("%s: %s: write: schema: WriteRecordBatch", Tag, tag_);
		ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*recordBatch));
		P("%s: %s: write: schema: Close", Tag, tag_);
		ARROW_RETURN_NOT_OK(writer->Close());

		// Write another stream format data with record batches.
		ARROW_ASSIGN_OR_RAISE(writer,
		                      arrow::ipc::MakeStreamWriter(&output, schema, option));
		bool needLastFlush = false;
		for (uint64_t iTuple = 0; iTuple < SPI_processed; ++iTuple)
		{
			P("%s: %s: write: data: record batch: %d/%d",
			  Tag,
			  tag_,
			  iTuple,
			  SPI_processed);
			for (uint64_t iAttribute = 0; iAttribute < SPI_tuptable->tupdesc->natts;
			     ++iAttribute)
			{
				P("%s: %s: write: data: record batch: %d/%d: %d/%d",
				  Tag,
				  tag_,
				  iTuple,
				  SPI_processed,
				  iAttribute,
				  SPI_tuptable->tupdesc->natts);
				bool isNull;
				auto datum = SPI_getbinval(SPI_tuptable->vals[iTuple],
				                           SPI_tuptable->tupdesc,
				                           iAttribute + 1,
				                           &isNull);
				if (isNull)
				{
					auto arrayBuilder = builder->GetField(iAttribute);
					ARROW_RETURN_NOT_OK(arrayBuilder->AppendNull());
				}
				else
				{
					auto arrayBuilder =
						builder->GetFieldAs<arrow::Int32Builder>(iAttribute);
					ARROW_RETURN_NOT_OK(arrayBuilder->Append(DatumGetInt32(datum)));
				}
			}

			if (((iTuple + 1) % MaxNRowsPerRecordBatch) == 0)
			{
				ARROW_ASSIGN_OR_RAISE(recordBatch, builder->Flush());
				P("%s: %s: write: data: WriteRecordBatch: %d/%d",
				  Tag,
				  tag_,
				  iTuple,
				  SPI_processed);
				ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*recordBatch));
				needLastFlush = false;
			}
			else
			{
				needLastFlush = true;
			}
		}
		if (needLastFlush)
		{
			ARROW_ASSIGN_OR_RAISE(recordBatch, builder->Flush());
			P("%s: %s: write: data: WriteRecordBatch", Tag, tag_);
			ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*recordBatch));
		}
		P("%s: %s: write: data: Close", Tag, tag_);
		ARROW_RETURN_NOT_OK(writer->Close());
		return output.Close();
	}
};

class Proxy : public WorkerProcessor {
   public:
	explicit Proxy() : WorkerProcessor("proxy") {}

	arrow::Status connect(const std::string& databaseName,
	                      const std::string& userName,
	                      const std::string& password)
	{
		if (sharedData_->executorPID != InvalidPid)
		{
			return arrow::Status::OK();
		}
		LWLockAcquire(lock_, LW_EXCLUSIVE);
		setSharedString(sharedData_->connectData.databaseName, databaseName);
		setSharedString(sharedData_->connectData.userName, userName);
		setSharedString(sharedData_->connectData.password, password);
		LWLockRelease(lock_);
		kill(sharedData_->mainPID, SIGUSR1);
		{
			std::unique_lock<std::mutex> lock(mutex_);
			conditionVariable_.wait(
				lock, [&] { return sharedData_->executorPID != InvalidPid; });
		}
		return arrow::Status::OK();
	}

	arrow::Result<std::shared_ptr<arrow::Schema>> execute(const std::string& query)
	{
		LWLockAcquire(lock_, LW_EXCLUSIVE);
		setSharedString(sharedData_->executeData.query, query);
		LWLockRelease(lock_);
		if (sharedData_->executorPID != InvalidPid)
		{
			P("%s: %s: execute: kill executor: %d", Tag, tag_, sharedData_->executorPID);
			kill(sharedData_->executorPID, SIGUSR1);
		}
		P("%s: %s: execute: open", Tag, tag_);
		auto input = std::make_shared<SharedRingBufferInputStream>(this);
		// Read schema only stream format data.
		ARROW_ASSIGN_OR_RAISE(auto reader,
		                      arrow::ipc::RecordBatchStreamReader::Open(input));
		while (true)
		{
			std::shared_ptr<arrow::RecordBatch> recordBatch;
			P("%s: %s: execute: read next", Tag, tag_);
			ARROW_RETURN_NOT_OK(reader->ReadNext(&recordBatch));
			if (!recordBatch)
			{
				break;
			}
		}
		P("%s: %s: execute: schema", Tag, tag_);
		return reader->schema();
	}

	arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> read()
	{
		auto input = std::make_shared<SharedRingBufferInputStream>(this);
		// Read another stream format data with record batches.
		return arrow::ipc::RecordBatchStreamReader::Open(input);
	}

   private:
	void setSharedString(dsa_pointer& pointer, const std::string& input)
	{
		if (input.empty())
		{
			return;
		}
		pointer = dsa_allocate(area_, input.size() + 1);
		memcpy(dsa_get_address(area_, pointer), input.c_str(), input.size() + 1);
	}
};

class MainProcessor : public Processor {
   public:
	MainProcessor() : Processor("main")
	{
		LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
		bool found;
		auto sharedData = static_cast<SharedData*>(
			ShmemInitStruct(SharedDataName, sizeof(SharedData), &found));
		if (found)
		{
			LWLockRelease(AddinShmemInitLock);
			ereport(ERROR,
			        errcode(ERRCODE_INTERNAL_ERROR),
			        errmsg("%s: %s: shared data is already created", Tag, tag_));
		}
		auto area = dsa_create(LWLockNewTrancheId());
		sharedData->handle = dsa_get_handle(area);
		sharedData->executorPID = InvalidPid;
		sharedData->serverPID = InvalidPid;
		sharedData->mainPID = MyProcPid;
		sharedData->connectData.databaseName = InvalidDsaPointer;
		sharedData->connectData.userName = InvalidDsaPointer;
		sharedData->connectData.password = InvalidDsaPointer;
		SharedRingBuffer::initialize_data(&(sharedData->executeData.bufferData));
		lock_ = &(GetNamedLWLockTranche(LWLockTrancheName)[0].lock);
		LWLockRelease(AddinShmemInitLock);
		sharedData_ = sharedData;
		area_ = area;
	}

	BackgroundWorkerHandle* start_server()
	{
		BackgroundWorker worker = {0};
		snprintf(worker.bgw_name, BGW_MAXLEN, "%s: server", Tag);
		snprintf(worker.bgw_type, BGW_MAXLEN, "%s: server", Tag);
		worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
		worker.bgw_start_time = BgWorkerStart_ConsistentState;
		worker.bgw_restart_time = BGW_NEVER_RESTART;
		snprintf(worker.bgw_library_name, BGW_MAXLEN, "%s", LibraryName);
		snprintf(worker.bgw_function_name, BGW_MAXLEN, "afs_server");
		worker.bgw_main_arg = 0;
		worker.bgw_notify_pid = MyProcPid;
		BackgroundWorkerHandle* handle;
		if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		{
			ereport(ERROR,
			        errcode(ERRCODE_INTERNAL_ERROR),
			        errmsg("%s: %s: failed to start server", Tag, tag_));
		}
		WaitForBackgroundWorkerStartup(handle, &(sharedData_->serverPID));
		return handle;
	}

	void process_connect_request()
	{
		if (!DsaPointerIsValid(sharedData_->connectData.databaseName))
		{
			return;
		}

		BackgroundWorker worker = {0};
		// TODO: Add executor ID to bgw_name
		snprintf(worker.bgw_name, BGW_MAXLEN, "%s: executor", Tag);
		snprintf(worker.bgw_type, BGW_MAXLEN, "%s: executor", Tag);
		worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
		worker.bgw_start_time = BgWorkerStart_ConsistentState;
		worker.bgw_restart_time = BGW_NEVER_RESTART;
		snprintf(worker.bgw_library_name, BGW_MAXLEN, "%s", LibraryName);
		snprintf(worker.bgw_function_name, BGW_MAXLEN, "afs_executor");
		worker.bgw_main_arg = 0;
		worker.bgw_notify_pid = MyProcPid;
		BackgroundWorkerHandle* handle;
		if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		{
			ereport(ERROR,
			        errcode(ERRCODE_INTERNAL_ERROR),
			        errmsg("%s: %s: failed to start executor", Tag, tag_));
		}
		WaitForBackgroundWorkerStartup(handle, &(sharedData_->executorPID));
		kill(sharedData_->serverPID, SIGUSR1);
	}
};

class HeaderAuthServerMiddlewareFactory : public arrow::flight::ServerMiddlewareFactory {
   public:
	explicit HeaderAuthServerMiddlewareFactory(Proxy* proxy)
		: arrow::flight::ServerMiddlewareFactory(), proxy_(proxy)
	{
	}

	arrow::Status StartCall(const arrow::flight::CallInfo& info,
	                        const arrow::flight::CallHeaders& incoming_headers,
	                        std::shared_ptr<arrow::flight::ServerMiddleware>* middleware)
	{
		std::string databaseName("postgres");
		auto databaseHeader = incoming_headers.find("x-flight-sql-database");
		if (databaseHeader != incoming_headers.end())
		{
			databaseName = databaseHeader->second;
		}
		std::string userName("");
		std::string password("");
		auto authorizationHeader = incoming_headers.find("authorization");
		if (authorizationHeader != incoming_headers.end())
		{
			std::stringstream decodedStream(
				arrow::util::base64_decode(authorizationHeader->second));
			std::getline(decodedStream, userName, ':');
			std::getline(decodedStream, password);
		}
		auto status = proxy_->connect(databaseName, userName, password);
		if (status.ok())
		{
			return status;
		}
		else
		{
			return arrow::flight::MakeFlightError(
				arrow::flight::FlightStatusCode::Unauthenticated, status.ToString());
		}
	}

   private:
	Proxy* proxy_;
};

class FlightSQLServer : public arrow::flight::sql::FlightSqlServerBase {
   public:
	explicit FlightSQLServer(Proxy* proxy)
		: arrow::flight::sql::FlightSqlServerBase(), proxy_(proxy)
	{
	}

	~FlightSQLServer() override {}

	arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> GetFlightInfoStatement(
		const arrow::flight::ServerCallContext& context,
		const arrow::flight::sql::StatementQuery& command,
		const arrow::flight::FlightDescriptor& descriptor)
	{
		const auto& query = command.query;
		ARROW_ASSIGN_OR_RAISE(auto schema, proxy_->execute(query));
		ARROW_ASSIGN_OR_RAISE(auto ticket,
		                      arrow::flight::sql::CreateStatementQueryTicket(query));
		std::vector<arrow::flight::FlightEndpoint> endpoints{
			arrow::flight::FlightEndpoint{std::move(ticket), {}}};
		ARROW_ASSIGN_OR_RAISE(
			auto result,
			arrow::flight::FlightInfo::Make(*schema, descriptor, endpoints, -1, -1));
		return std::make_unique<arrow::flight::FlightInfo>(result);
	}

	arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> DoGetStatement(
		const arrow::flight::ServerCallContext& context,
		const arrow::flight::sql::StatementQueryTicket& command)
	{
		ARROW_ASSIGN_OR_RAISE(auto reader, proxy_->read());
		return std::make_unique<arrow::flight::RecordBatchStream>(reader);
	}

   private:
	Proxy* proxy_;
};

arrow::Status
afs_server_internal(Proxy* proxy)
{
	ARROW_ASSIGN_OR_RAISE(auto location, arrow::flight::Location::Parse(URI));
	arrow::flight::FlightServerOptions options(location);
	options.middleware.push_back(
		{"header-auth", std::make_shared<HeaderAuthServerMiddlewareFactory>(proxy)});
	FlightSQLServer flightSQLServer(proxy);
	ARROW_RETURN_NOT_OK(flightSQLServer.Init(options));

	while (!GotSIGTERM)
	{
		WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, -1, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		if (GotSIGHUP)
		{
			GotSIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		if (GotSIGUSR1)
		{
			GotSIGUSR1 = false;
			proxy->signaled();
		}

		CHECK_FOR_INTERRUPTS();
	}

	// TODO: Use before_shmem_exit()
	auto deadline = std::chrono::system_clock::now() + std::chrono::microseconds(10);
	return flightSQLServer.Shutdown(&deadline);
}

}  // namespace

extern "C" void
afs_executor(Datum arg)
{
	pqsignal(SIGTERM, afs_sigterm);
	pqsignal(SIGHUP, afs_sighup);
	pqsignal(SIGUSR1, afs_sigusr1);
	BackgroundWorkerUnblockSignals();

	{
		Executor executor;
		executor.open();
		while (!GotSIGTERM)
		{
			int events = WL_LATCH_SET | WL_EXIT_ON_PM_DEATH;
			const long timeout = SessionTimeout * 1000;
			if (timeout >= 0)
			{
				events |= WL_TIMEOUT;
			}
			auto conditions = WaitLatch(MyLatch, events, timeout, PG_WAIT_EXTENSION);

			if (conditions & WL_TIMEOUT)
			{
				break;
			}

			ResetLatch(MyLatch);

			if (GotSIGHUP)
			{
				GotSIGHUP = false;
				ProcessConfigFile(PGC_SIGHUP);
			}

			if (GotSIGUSR1)
			{
				GotSIGUSR1 = false;
				executor.signaled();
			}

			CHECK_FOR_INTERRUPTS();
		}
		// TODO: Use before_shmem_exit()
		executor.close();
	}

	proc_exit(0);
}

extern "C" void
afs_server(Datum arg)
{
	pqsignal(SIGTERM, afs_sigterm);
	pqsignal(SIGHUP, afs_sighup);
	pqsignal(SIGUSR1, afs_sigusr1);
	BackgroundWorkerUnblockSignals();

	{
		arrow::Status status;
		{
			Proxy proxy;
			status = afs_server_internal(&proxy);
		}
		if (!status.ok())
		{
			ereport(ERROR,
			        errcode(ERRCODE_INTERNAL_ERROR),
			        errmsg("%s: server: failed: %s", Tag, status.ToString().c_str()));
		}
	}

	proc_exit(0);
}

extern "C" void
afs_main(Datum arg)
{
	pqsignal(SIGTERM, afs_sigterm);
	pqsignal(SIGHUP, afs_sighup);
	pqsignal(SIGUSR1, afs_sigusr1);
	BackgroundWorkerUnblockSignals();

	{
		MainProcessor processor;
		auto serverHandle = processor.start_server();
		while (!GotSIGTERM)
		{
			WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, -1, PG_WAIT_EXTENSION);
			ResetLatch(MyLatch);

			if (GotSIGHUP)
			{
				GotSIGHUP = false;
				ProcessConfigFile(PGC_SIGHUP);
			}

			if (GotSIGUSR1)
			{
				GotSIGUSR1 = false;
				processor.process_connect_request();
			}

			CHECK_FOR_INTERRUPTS();
		}
		WaitForBackgroundWorkerShutdown(serverHandle);
	}

	proc_exit(0);
}

extern "C" void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
	{
		return;
	}

	DefineCustomStringVariable("arrow_flight_sql.uri",
	                           "Apache Arrow Flight SQL endpoint URI.",
	                           (std::string("default: ") + URIDefault).c_str(),
	                           &URI,
	                           URIDefault,
	                           PGC_POSTMASTER,
	                           0,
	                           NULL,
	                           NULL,
	                           NULL);

	DefineCustomIntVariable("arrow_flight_sql.session_timeout",
	                        "Maximum session duration in seconds.",
	                        "The default is 300 seconds. "
	                        "-1 means no timeout.",
	                        &SessionTimeout,
	                        SessionTimeoutDefault,
	                        -1,
	                        INT_MAX,
	                        PGC_USERSET,
	                        GUC_UNIT_S,
	                        NULL,
	                        NULL,
	                        NULL);

	DefineCustomIntVariable("arrow_flight_sql.max_n_rows_per_record_batch",
	                        "The maximum number of rows per record batch.",
	                        "The default is 1 * 1024 * 1024 rows.",
	                        &MaxNRowsPerRecordBatch,
	                        MaxNRowsPerRecordBatchDefault,
	                        1,
	                        INT_MAX,
	                        PGC_USERSET,
	                        0,
	                        NULL,
	                        NULL,
	                        NULL);

	PreviousShmemRequestHook = shmem_request_hook;
	shmem_request_hook = afs_shmem_request_hook;

	BackgroundWorker worker = {0};
	snprintf(worker.bgw_name, BGW_MAXLEN, "%s: main", Tag);
	snprintf(worker.bgw_type, BGW_MAXLEN, "%s: main", Tag);
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	snprintf(worker.bgw_library_name, BGW_MAXLEN, "%s", LibraryName);
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "afs_main");
	worker.bgw_main_arg = 0;
	worker.bgw_notify_pid = 0;
	RegisterBackgroundWorker(&worker);
}
