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
#include <lib/dshash.h>
#include <libpq/crypt.h>
#include <libpq/libpq-be.h>
#include <libpq/libpq.h>
#include <miscadmin.h>
#include <postmaster/bgworker.h>
#include <postmaster/postmaster.h>
#include <storage/ipc.h>
#include <storage/latch.h>
#include <storage/lwlock.h>
#include <storage/procsignal.h>
#include <storage/shmem.h>
#include <utils/backend_status.h>
#include <utils/builtins.h>
#include <utils/dsa.h>
#include <utils/guc.h>
#include <utils/memutils.h>
#include <utils/snapmgr.h>
#include <utils/timestamp.h>
#include <utils/wait_event.h>
}

#undef Abs

#if PG_VERSION_NUM >= 150000
#	define AFS_HAVE_SHMEM_REQUEST_HOOK
#endif

#include <arrow/buffer.h>
#include <arrow/builder.h>
#include <arrow/flight/server_middleware.h>
#include <arrow/flight/sql/server.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/table_builder.h>
#include <arrow/util/base64.h>
#include <arrow/util/string.h>

#include <cinttypes>
#include <condition_variable>
#include <fstream>
#include <iterator>
#include <map>
#include <random>
#include <sstream>
#include <type_traits>

#include <arpa/inet.h>

#ifdef __GNUC__
#	define AFS_FUNC __PRETTY_FUNCTION__
#else
#	define AFS_FUNC __func__
#endif

#ifdef _WIN32
#	define PRIsize "Iu"
#else
#	define PRIsize "zu"
#endif

// #define AFS_VERBOSE
#ifdef AFS_VERBOSE
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
void
afs_sigterm(SIGNAL_ARGS)
{
	auto errnoSaved = errno;
	GotSIGTERM = true;
	SetLatch(MyLatch);
	errno = errnoSaved;
}

static volatile sig_atomic_t GotSIGHUP = false;
void
afs_sighup(SIGNAL_ARGS)
{
	auto errnoSaved = errno;
	GotSIGHUP = true;
	SetLatch(MyLatch);
	errno = errnoSaved;
}

static volatile sig_atomic_t GotSIGUSR1 = false;
void
afs_sigusr1(SIGNAL_ARGS)
{
	procsignal_sigusr1_handler(postgres_signal_arg);
	auto errnoSaved = errno;
	GotSIGUSR1 = true;
	SetLatch(MyLatch);
	errno = errnoSaved;
}

#ifdef AFS_DEBUG
void
afs_sigsegv(SIGNAL_ARGS)
{
	ereport(FATAL, errcode(ERRCODE_INTERNAL_ERROR), errmsg("CRASHED!!!"), errbacktrace());
}
#endif

#ifdef AFS_HAVE_SHMEM_REQUEST_HOOK
static shmem_request_hook_type PreviousShmemRequestHook = nullptr;
#endif
static const char* LWLockTrancheName = "arrow-flight-sql: lwlock tranche";
void
afs_shmem_request_hook(void)
{
#ifdef AFS_HAVE_SHMEM_REQUEST_HOOK
	if (PreviousShmemRequestHook)
		PreviousShmemRequestHook();
#endif
	RequestNamedLWLockTranche(LWLockTrancheName, 1);
}

class ScopedMemoryContext {
   public:
	explicit ScopedMemoryContext(MemoryContext memoryContext)
		: memoryContext_(memoryContext), oldMemoryContext_(nullptr)
	{
		oldMemoryContext_ = MemoryContextSwitchTo(memoryContext_);
	}

	~ScopedMemoryContext()
	{
		MemoryContextSwitchTo(oldMemoryContext_);
		MemoryContextDelete(memoryContext_);
	}

   private:
	MemoryContext memoryContext_;
	MemoryContext oldMemoryContext_;
};

struct ScopedTransaction {
	ScopedTransaction() { StartTransactionCommand(); }
	~ScopedTransaction() { CommitTransactionCommand(); }
};

struct ScopedSnapshot {
	ScopedSnapshot() { PushActiveSnapshot(GetTransactionSnapshot()); }
	~ScopedSnapshot() { PopActiveSnapshot(); }
};

struct ScopedPlan {
	ScopedPlan(SPIPlanPtr plan) : plan_(plan) {}
	~ScopedPlan() { SPI_freeplan(plan_); }
	SPIPlanPtr plan_;
};

struct SharedRingBufferData {
	dsa_pointer pointer;
	size_t total;
	size_t head;
	size_t tail;
};

// Naive ring buffer implementation. We can improve this later.
//
// We can't call PostgreSQL API in this class.
class SharedRingBuffer {
   public:
	static void initialize_data(SharedRingBufferData* data)
	{
		data->pointer = InvalidDsaPointer;
		data->total = 0;
		data->head = 0;
		data->tail = 0;
	}

	static void allocate_data(SharedRingBufferData* data, dsa_area* area, size_t total)
	{
		data->pointer = dsa_allocate(area, total);
		data->total = total;
		data->head = 0;
		data->tail = 0;
	}

	static void free_data(SharedRingBufferData* data, dsa_area* area)
	{
		if (data->pointer != InvalidDsaPointer)
			dsa_free(area, data->pointer);
		initialize_data(data);
	}

	SharedRingBuffer(SharedRingBufferData* data, void* address, const char* tag)
		: data_(data), address_(static_cast<uint8_t*>(address)), tag_(tag)
	{
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

	// Need LWLockAcquire() by caller.
	size_t write(const void* data, size_t n)
	{
#ifdef AFS_VERBOSE
		const char* tag = "write";
#endif
		P("%s: %s: %s: before: (%" PRIsize ":%" PRIsize ") %" PRIsize,
		  Tag,
		  tag_,
		  tag,
		  data_->head,
		  data_->tail,
		  n);
		if (rest_size() == 0)
		{
			P("%s: %s: %s: after: no space: (%" PRIsize ":%" PRIsize ") %" PRIsize ":0",
			  Tag,
			  tag_,
			  tag,
			  data_->head,
			  data_->tail,
			  n);
			return 0;
		}

		size_t writtenSize = 0;
		auto output = address_;
		if (data_->head <= data_->tail)
		{
			auto restSize = data_->total - data_->tail;
			if (data_->head == 0)
			{
				restSize--;
			}
			const auto firstHalfWriteSize = std::min(n, restSize);
			P("%s: %s: %s: first half: (%" PRIsize ":%" PRIsize ") %" PRIsize
			  ":%" PRIsize,
			  Tag,
			  tag_,
			  tag,
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
			P("%s: %s: %s: last half: (%" PRIsize ":%" PRIsize ") %" PRIsize ":%" PRIsize,
			  Tag,
			  tag_,
			  tag,
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
		P("%s: %s: %s: after: (%" PRIsize ":%" PRIsize ") %" PRIsize ":%" PRIsize
		  " [%s] [%s]",
		  Tag,
		  tag_,
		  tag,
		  data_->head,
		  data_->tail,
		  n,
		  writtenSize,
		  arrow::HexEncode(reinterpret_cast<const uint8_t*>(data), writtenSize).c_str(),
		  arrow::HexEncode(reinterpret_cast<uint8_t*>(output), writtenSize).c_str());
		return writtenSize;
	}

	// Need LWLockAcquire() by caller.
	size_t read(size_t n, void* output)
	{
#ifdef AFS_VERBOSE
		const char* tag = "read";
#endif
		P("%s: %s: %s: before: (%" PRIsize ":%" PRIsize ") %" PRIsize,
		  Tag,
		  tag_,
		  tag,
		  data_->head,
		  data_->tail,
		  n);
		size_t readSize = 0;
		const auto input = address_;
		if (data_->head > data_->tail)
		{
			const auto firstHalfReadSize = std::min(n, data_->total - data_->head);
			P("%s: %s: %s: first half: (%" PRIsize ":%" PRIsize ") %" PRIsize
			  ":%" PRIsize,
			  Tag,
			  tag_,
			  tag,
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
			P("%s: %s: %s: last half: (%" PRIsize ":%" PRIsize ") %" PRIsize ":%" PRIsize,
			  Tag,
			  tag_,
			  tag,
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
		P("%s: %s: %s: after: (%" PRIsize ":%" PRIsize ") %" PRIsize ":%" PRIsize
		  " [%s] [%s]",
		  Tag,
		  tag_,
		  tag,
		  data_->head,
		  data_->tail,
		  n,
		  readSize,
		  arrow::HexEncode(reinterpret_cast<const uint8_t*>(output), readSize).c_str(),
		  arrow::HexEncode(reinterpret_cast<const uint8_t*>(input), readSize).c_str());
		return readSize;
	}

   private:
	SharedRingBufferData* data_;
	uint8_t* address_;
	const char* tag_;
};

enum class Action
{
	None,
	Select,
	Update,
	Prepare,
	ClosePreparedStatement,
	SetParameters,
	SelectPreparedStatement,
	UpdatePreparedStatement,
};

const char*
action_name(Action action)
{
	switch (action)
	{
		case Action::None:
			return "Action::None";
		case Action::Select:
			return "Action::Select";
		case Action::Update:
			return "Action::Update";
		case Action::Prepare:
			return "Action::Prepare";
		case Action::ClosePreparedStatement:
			return "Action::ClosePreparedStatement";
		case Action::SetParameters:
			return "Action::SetParameters";
		case Action::SelectPreparedStatement:
			return "Action::SelectPreparedStatement";
		case Action::UpdatePreparedStatement:
			return "Action::UpdatePreparedStatement";
		default:
			return "Action::Unknown";
	}
}

void
dsa_pointer_set_string(dsa_pointer& pointer, dsa_area* area, const std::string& input)
{
	if (DsaPointerIsValid(pointer))
	{
		dsa_free(area, pointer);
		pointer = InvalidDsaPointer;
	}
	if (input.empty())
	{
		return;
	}
	pointer = dsa_allocate(area, input.size() + 1);
	memcpy(dsa_get_address(area, pointer), input.c_str(), input.size() + 1);
}

// Session data used in only one process.
//
// They are based on data in SharedSessionData and process local data.
struct LocalSessionData {
	LocalSessionData()
		: id(0),
		  valid(false),
		  peerPID(InvalidPid),
		  bufferData(nullptr),
		  bufferAddress(nullptr),
		  errorMessage(std::nullopt)
	{
	}

	uint64_t id;
	bool valid;
	pid_t peerPID;
	SharedRingBufferData* bufferData;
	void* bufferAddress;
	std::optional<std::string> errorMessage;
};

// Session data shared with multiple processes. LWLockAcquire() with
// Processor's lock_ is needed when we change this data.
//
// This struct must have only data (don't add methods) to use with
// dshash.
struct SharedSessionData {
	uint64_t id;
	dsa_pointer errorMessage;
	pid_t executorPID;
	bool started;
	bool initialized;
	bool finished;
	dsa_pointer databaseName;
	dsa_pointer userName;
	dsa_pointer password;
	dsa_pointer clientAddress;
	Action action;
	dsa_pointer selectQuery;
	dsa_pointer updateQuery;
	int64_t nUpdatedRecords;
	dsa_pointer prepareQuery;
	dsa_pointer preparedStatementHandle;
	bool setParametersFinished;
	SharedRingBufferData bufferData;
};

void
shared_session_data_initialize(SharedSessionData* session,
                               dsa_area* area,
                               const std::string& databaseName,
                               const std::string& userName,
                               const std::string& password,
                               const std::string& clientAddress)
{
	session->errorMessage = InvalidDsaPointer;
	session->executorPID = InvalidPid;
	session->started = false;
	session->initialized = false;
	session->finished = false;
	dsa_pointer_set_string(session->databaseName, area, databaseName);
	dsa_pointer_set_string(session->userName, area, userName);
	dsa_pointer_set_string(session->password, area, password);
	dsa_pointer_set_string(session->clientAddress, area, clientAddress);
	session->action = Action::None;
	session->selectQuery = InvalidDsaPointer;
	session->updateQuery = InvalidDsaPointer;
	session->nUpdatedRecords = -1;
	session->prepareQuery = InvalidDsaPointer;
	session->preparedStatementHandle = InvalidDsaPointer;
	session->setParametersFinished = false;
	SharedRingBuffer::initialize_data(&(session->bufferData));
}

void
shared_session_data_finalize(SharedSessionData* session, dsa_area* area)
{
	if (DsaPointerIsValid(session->errorMessage))
		dsa_free(area, session->errorMessage);
	if (DsaPointerIsValid(session->databaseName))
		dsa_free(area, session->databaseName);
	if (DsaPointerIsValid(session->userName))
		dsa_free(area, session->userName);
	if (DsaPointerIsValid(session->password))
		dsa_free(area, session->password);
	if (DsaPointerIsValid(session->selectQuery))
		dsa_free(area, session->selectQuery);
	if (DsaPointerIsValid(session->updateQuery))
		dsa_free(area, session->updateQuery);
	if (DsaPointerIsValid(session->prepareQuery))
		dsa_free(area, session->prepareQuery);
	if (DsaPointerIsValid(session->preparedStatementHandle))
		dsa_free(area, session->preparedStatementHandle);
	SharedRingBuffer::free_data(&(session->bufferData), area);
}

class SharedSessionReleaser {
   public:
	explicit SharedSessionReleaser(dshash_table* sessions, SharedSessionData* data)
		: sessions_(sessions), data_(data)
	{
	}

	~SharedSessionReleaser() { dshash_release_lock(sessions_, data_); }

   private:
	dshash_table* sessions_;
	SharedSessionData* data_;
};

static dshash_parameters SessionsParams = {
	sizeof(uint64_t),
	sizeof(SharedSessionData),
	dshash_memcmp,
	dshash_memhash,
	0,  // Set later because this is determined dynamically.
};

struct SharedData {
	int trancheID;
	dsa_handle handle;
	int sessionsTrancheID;
	dshash_table_handle sessionsHandle;
	pid_t serverPID;
	pid_t mainPID;
};

class Processor {
   public:
	enum class WaitMode
	{
		Read,
		Written,
	};

	Processor(const char* tag) : tag_(tag), sharedData_(nullptr), area_(nullptr), lock_()
	{
	}

	virtual ~Processor()
	{
		if (area_)
		{
			dsa_detach(area_);
		}
	}

	const char* tag() { return tag_; }

	void lock_acquire() { LWLockAcquire(lock_, LW_EXCLUSIVE); }

	void lock_release() { LWLockRelease(lock_); }

	// Executor and Server must implement this.
	virtual arrow::Result<size_t> read(LocalSessionData* session,
	                                   SharedRingBuffer* buffer,
	                                   size_t n,
	                                   void* output)
	{
		return 0;
	}

	// Executor and Server must implement this.
	virtual arrow::Result<size_t> write(LocalSessionData* session,
	                                    SharedRingBuffer* buffer,
	                                    const void* data,
	                                    size_t n)
	{
		return 0;
	}

	arrow::Status wait(LocalSessionData* session, SharedRingBuffer* buffer, WaitMode mode)
	{
		const char* tag = (mode == WaitMode::Read) ? "wait read" : "wait written";
		auto peerPID = peer_pid(session);

		if (ARROW_PREDICT_FALSE(peerPID == InvalidPid))
		{
			return arrow::Status::IOError(
				Tag, ": ", tag_, ": ", tag, ": ", peer_name(), ": not alive");
		}

		P("%s: %s: %s: %s: kill: %d", Tag, tag_, tag, peer_name(), peerPID);
		kill(peerPID, SIGUSR1);

		return wait_internal(session, buffer, mode, tag);
	}

   protected:
	void set_shared_string(dsa_pointer& pointer, const std::string& input)
	{
		dsa_pointer_set_string(pointer, area_, input);
	}

	virtual pid_t peer_pid(LocalSessionData* session) { return InvalidPid; }

	virtual const char* peer_name() { return "unknown"; }

	virtual arrow::Status wait_internal(LocalSessionData* session,
	                                    SharedRingBuffer* buffer,
	                                    WaitMode mode,
	                                    const char* tag)
	{
		return arrow::Status::NotImplemented("wait_internal");
	}

	size_t get_waiting_buffer_size(SharedRingBuffer* buffer, WaitMode mode)
	{
		if (mode == WaitMode::Read)
		{
			return buffer->rest_size();
		}
		else
		{
			return buffer->size();
		}
	}

	const char* tag_;
	SharedData* sharedData_;
	dsa_area* area_;
	LWLock* lock_;
};

struct ProcessorLockGuard {
	ProcessorLockGuard(Processor* processor) : processor_(processor)
	{
		processor_->lock_acquire();
	}
	~ProcessorLockGuard() { processor_->lock_release(); }

   private:
	Processor* processor_;
};

class SharedRingBufferInputStream : public arrow::io::InputStream {
   public:
	SharedRingBufferInputStream(Processor* processor,
	                            std::shared_ptr<LocalSessionData> localSession)
		: arrow::io::InputStream(),
		  processor_(processor),
		  localSession_(std::move(localSession)),
		  position_(0),
		  is_open_(true)
	{
	}

	arrow::Status Close() override
	{
		is_open_ = false;
		return arrow::Status::OK();
	}

	bool closed() const override { return !is_open_; }

	arrow::Result<int64_t> Tell() const override { return position_; }

	arrow::Result<int64_t> Read(int64_t nBytes, void* out) override;

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
	std::shared_ptr<LocalSessionData> localSession_;
	int64_t position_;
	bool is_open_;
};

class SharedRingBufferOutputStream : public arrow::io::OutputStream {
   public:
	SharedRingBufferOutputStream(Processor* processor,
	                             std::shared_ptr<LocalSessionData> localSession)
		: arrow::io::OutputStream(),
		  processor_(processor),
		  localSession_(std::move(localSession)),
		  position_(0),
		  is_open_(true)
	{
	}

	arrow::Status Close() override
	{
		is_open_ = false;
		return arrow::Status::OK();
	}

	bool closed() const override { return !is_open_; }

	arrow::Result<int64_t> Tell() const override { return position_; }

	arrow::Status Write(const void* data, int64_t nBytes) override;

	using arrow::io::OutputStream::Write;

   private:
	Processor* processor_;
	std::shared_ptr<LocalSessionData> localSession_;
	int64_t position_;
	bool is_open_;
};

class WorkerProcessor : public Processor {
   public:
	explicit WorkerProcessor(const char* tag) : Processor(tag), sessions_(nullptr)
	{
		LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
		bool found;
		sharedData_ = static_cast<SharedData*>(
			ShmemInitStruct(SharedDataName, sizeof(sharedData_), &found));
		if (!found)
		{
			LWLockRelease(AddinShmemInitLock);
			ereport(ERROR,
			        errcode(ERRCODE_INTERNAL_ERROR),
			        errmsg("%s: %s: shared data isn't created yet", Tag, tag_));
		}
		area_ = dsa_attach(sharedData_->handle);
		SessionsParams.tranche_id = sharedData_->sessionsTrancheID;
		sessions_ =
			dshash_attach(area_, &SessionsParams, sharedData_->sessionsHandle, nullptr);
		lock_ = &(GetNamedLWLockTranche(LWLockTrancheName)[0].lock);
		LWLockRelease(AddinShmemInitLock);
	}

	~WorkerProcessor() override { dshash_detach(sessions_); }

   protected:
	dshash_table* sessions_;
};

class ArrowPGTypeConverter : public arrow::TypeVisitor {
   public:
	explicit ArrowPGTypeConverter() : oid_(InvalidOid) {}

	Oid oid() const { return oid_; }

	arrow::Status Visit(const arrow::Int8Type& type)
	{
		oid_ = INT2OID;
		return arrow::Status::OK();
	}

	arrow::Status Visit(const arrow::UInt8Type& type)
	{
		oid_ = INT2OID;
		return arrow::Status::OK();
	}

	arrow::Status Visit(const arrow::Int16Type& type)
	{
		oid_ = INT2OID;
		return arrow::Status::OK();
	}

	arrow::Status Visit(const arrow::UInt16Type& type)
	{
		oid_ = INT2OID;
		return arrow::Status::OK();
	}

	arrow::Status Visit(const arrow::Int32Type& type)
	{
		oid_ = INT4OID;
		return arrow::Status::OK();
	}

	arrow::Status Visit(const arrow::UInt32Type& type)
	{
		oid_ = INT4OID;
		return arrow::Status::OK();
	}

	arrow::Status Visit(const arrow::Int64Type& type)
	{
		oid_ = INT8OID;
		return arrow::Status::OK();
	}

	arrow::Status Visit(const arrow::UInt64Type& type)
	{
		oid_ = INT8OID;
		return arrow::Status::OK();
	}

	arrow::Status Visit(const arrow::FloatType& type)
	{
		oid_ = FLOAT4OID;
		return arrow::Status::OK();
	}

	arrow::Status Visit(const arrow::DoubleType& type)
	{
		oid_ = FLOAT8OID;
		return arrow::Status::OK();
	}

	arrow::Status Visit(const arrow::StringType& type)
	{
		oid_ = TEXTOID;
		return arrow::Status::OK();
	}

	arrow::Status Visit(const arrow::BinaryType& type)
	{
		oid_ = BYTEAOID;
		return arrow::Status::OK();
	}

	arrow::Status Visit(const arrow::TimestampType& type)
	{
		oid_ = TIMESTAMPOID;
		return arrow::Status::OK();
	}

   private:
	Oid oid_;
};

class ArrowPGValueConverter : public arrow::ArrayVisitor {
   public:
	explicit ArrowPGValueConverter(int64_t i_row, Datum& datum)
		: i_row_(i_row), datum_(datum)
	{
	}

	arrow::Status Visit(const arrow::Int8Array& array)
	{
		datum_ = Int8GetDatum(array.Value(i_row_));
		return arrow::Status::OK();
	}

	arrow::Status Visit(const arrow::UInt8Array& array)
	{
		datum_ = UInt8GetDatum(array.Value(i_row_));
		return arrow::Status::OK();
	}

	arrow::Status Visit(const arrow::Int16Array& array)
	{
		datum_ = Int16GetDatum(array.Value(i_row_));
		return arrow::Status::OK();
	}

	arrow::Status Visit(const arrow::UInt16Array& array)
	{
		datum_ = UInt16GetDatum(array.Value(i_row_));
		return arrow::Status::OK();
	}

	arrow::Status Visit(const arrow::Int32Array& array)
	{
		datum_ = Int32GetDatum(array.Value(i_row_));
		return arrow::Status::OK();
	}

	arrow::Status Visit(const arrow::UInt32Array& array)
	{
		datum_ = UInt32GetDatum(array.Value(i_row_));
		return arrow::Status::OK();
	}

	arrow::Status Visit(const arrow::Int64Array& array)
	{
		datum_ = Int64GetDatum(array.Value(i_row_));
		return arrow::Status::OK();
	}

	arrow::Status Visit(const arrow::UInt64Array& array)
	{
		datum_ = UInt64GetDatum(array.Value(i_row_));
		return arrow::Status::OK();
	}

	arrow::Status Visit(const arrow::FloatArray& array)
	{
		datum_ = Float4GetDatum(array.Value(i_row_));
		return arrow::Status::OK();
	}

	arrow::Status Visit(const arrow::DoubleArray& array)
	{
		datum_ = Float8GetDatum(array.Value(i_row_));
		return arrow::Status::OK();
	}

	arrow::Status Visit(const arrow::StringArray& array)
	{
		auto value = array.GetView(i_row_);
		datum_ = PointerGetDatum(cstring_to_text_with_len(value.data(), value.length()));
		return arrow::Status::OK();
	}

	arrow::Status Visit(const arrow::BinaryArray& array)
	{
		auto value = array.GetView(i_row_);
		datum_ = PointerGetDatum(cstring_to_text_with_len(value.data(), value.length()));
		return arrow::Status::OK();
	}

	arrow::Status Visit(const arrow::TimestampArray& array)
	{
		const auto unit =
			std::static_pointer_cast<arrow::TimestampType>(array.type())->unit();
		Timestamp value = 0;
		switch (unit)
		{
			case arrow::TimeUnit::SECOND:
				value += array.Value(i_row_) * 1000000;
				break;
			case arrow::TimeUnit::MILLI:
				value += array.Value(i_row_) * 1000;
				break;
			case arrow::TimeUnit::MICRO:
				value += array.Value(i_row_);
				break;
			case arrow::TimeUnit::NANO:
				value += array.Value(i_row_) / 1000;
				break;
			default:
				return arrow::Status::NotImplemented("Unsupported time unit: ", unit);
		}
		// Arrow uses UNIX epoch (1970-01-01) but PostgreSQL uses 2000-01-01.
		value -= (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * USECS_PER_DAY;
		datum_ = TimestampGetDatum(value);
		return arrow::Status::OK();
	}

   private:
	int64_t i_row_;
	Datum& datum_;
};

class ArrowArrayBuilderBase {
   public:
	static arrow::Result<std::unique_ptr<ArrowArrayBuilderBase>> make(
		Form_pg_attribute attribute, int iAttribute, arrow::ArrayBuilder* builder);

	static arrow::Result<std::shared_ptr<arrow::DataType>> arrow_type(
		Form_pg_attribute attribute);

	ArrowArrayBuilderBase(Form_pg_attribute attribute, int iAttribute)
		: attribute_(attribute), iAttribute_(iAttribute)
	{
	}

	virtual ~ArrowArrayBuilderBase() = default;

	arrow::Status build(uint64_t iTuple, uint64_t iTupleEnd)
	{
		if (attribute_->attnotnull)
		{
			return build_not_null(iTuple, iTupleEnd);
		}
		else
		{
			return build_may_null(iTuple, iTupleEnd);
		}
	}

   protected:
	Form_pg_attribute attribute_;
	int iAttribute_;

	virtual arrow::Status build_not_null(uint64_t iTuple, uint64_t iTupleEnd) = 0;
	virtual arrow::Status build_may_null(uint64_t iTuple, uint64_t iTupleEnd) = 0;
};

template <typename ArrowType, typename Enable = void>
class ArrowArrayBuilder;

template <typename ArrowType>
class ArrowArrayBuilder<ArrowType, arrow::enable_if_has_c_type<ArrowType>>
	: public ArrowArrayBuilderBase {
   public:
	ArrowArrayBuilder(Form_pg_attribute attribute,
	                  int iAttribute,
	                  arrow::ArrayBuilder* builder)
		: ArrowArrayBuilderBase(attribute, iAttribute),
		  builder_(static_cast<BuilderType*>(builder)),
		  values_(),
		  validBytes_()
	{
	}

   private:
	using CType = typename arrow::TypeTraits<ArrowType>::CType;
	using BuilderType = typename arrow::TypeTraits<ArrowType>::BuilderType;

	BuilderType* builder_;
	std::vector<CType> values_;
	std::vector<uint8_t> validBytes_;

	arrow::Status build_not_null(uint64_t iTuple, uint64_t iTupleEnd)
	{
		uint64_t n = iTupleEnd - iTuple;
		values_.resize(n);
		for (uint64_t i = 0; i < n; ++i)
		{
			bool isNull;
			auto datum = SPI_getbinval(SPI_tuptable->vals[iTuple + i],
			                           SPI_tuptable->tupdesc,
			                           iAttribute_ + 1,
			                           &isNull);
			values_[i] = convert_value<ArrowType>(datum);
		}
		return builder_->AppendValues(values_.data(), values_.size());
	}

	arrow::Status build_may_null(uint64_t iTuple, uint64_t iTupleEnd)
	{
		bool haveNull = false;
		uint64_t n = iTupleEnd - iTuple;
		values_.resize(n);
		validBytes_.resize(n);
		for (uint64_t i = 0; i < n; ++i)
		{
			bool isNull;
			auto datum = SPI_getbinval(SPI_tuptable->vals[iTuple + i],
			                           SPI_tuptable->tupdesc,
			                           iAttribute_ + 1,
			                           &isNull);
			if (isNull)
			{
				haveNull = true;
				validBytes_[i] = 0;
			}
			else
			{
				validBytes_[i] = 1;
				values_[i] = convert_value<ArrowType>(datum);
			}
		}
		if (haveNull)
		{
			return builder_->AppendValues(
				values_.data(), values_.size(), validBytes_.data());
		}
		else
		{
			return builder_->AppendValues(values_.data(), values_.size());
		}
	}

	template <typename TargetArrowType>
	std::enable_if_t<std::is_same_v<TargetArrowType, arrow::Int16Type>, int16_t>
	convert_value(Datum datum)
	{
		return DatumGetInt16(datum);
	}

	template <typename TargetArrowType>
	std::enable_if_t<std::is_same_v<TargetArrowType, arrow::Int32Type>, int32_t>
	convert_value(Datum datum)
	{
		return DatumGetInt32(datum);
	}

	template <typename TargetArrowType>
	std::enable_if_t<std::is_same_v<TargetArrowType, arrow::Int64Type>, int64_t>
	convert_value(Datum datum)
	{
		return DatumGetInt64(datum);
	}

	template <typename TargetArrowType>
	std::enable_if_t<std::is_same_v<TargetArrowType, arrow::FloatType>, float>
	convert_value(Datum datum)
	{
		return DatumGetFloat4(datum);
	}

	template <typename TargetArrowType>
	std::enable_if_t<std::is_same_v<TargetArrowType, arrow::DoubleType>, double>
	convert_value(Datum datum)
	{
		return DatumGetFloat8(datum);
	}

	template <typename TargetArrowType>
	std::enable_if_t<std::is_same_v<TargetArrowType, arrow::TimestampType>, int64_t>
	convert_value(Datum datum)
	{
		// Arrow uses UNIX epoch (1970-01-01) but PostgreSQL
		// uses 2000-01-01.
		return DatumGetTimestamp(datum) +
		       ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * USECS_PER_DAY);
	}
};

template <typename ArrowType>
class ArrowArrayBuilder<ArrowType, arrow::enable_if_base_binary<ArrowType>>
	: public ArrowArrayBuilderBase {
   public:
	ArrowArrayBuilder(Form_pg_attribute attribute,
	                  int iAttribute,
	                  arrow::ArrayBuilder* builder)
		: ArrowArrayBuilderBase(attribute, iAttribute),
		  builder_(static_cast<BuilderType*>(builder)),
		  values_(),
		  validBytes_()
	{
	}

   private:
	using BuilderType = typename arrow::TypeTraits<ArrowType>::BuilderType;

	BuilderType* builder_;
	std::vector<std::string> values_;
	std::vector<uint8_t> validBytes_;

	arrow::Status build_not_null(uint64_t iTuple, uint64_t iTupleEnd)
	{
		uint64_t n = iTupleEnd - iTuple;
		values_.resize(n);
		for (uint64_t i = 0; i < n; ++i)
		{
			bool isNull;
			auto datum = SPI_getbinval(SPI_tuptable->vals[iTuple + i],
			                           SPI_tuptable->tupdesc,
			                           iAttribute_ + 1,
			                           &isNull);
			values_[i] = std::string(VARDATA_ANY(datum), VARSIZE_ANY_EXHDR(datum));
		}
		return builder_->AppendValues(values_);
	}

	arrow::Status build_may_null(uint64_t iTuple, uint64_t iTupleEnd)
	{
		bool haveNull = false;
		uint64_t n = iTupleEnd - iTuple;
		values_.resize(n);
		validBytes_.resize(n);
		for (uint64_t i = 0; i < n; ++i)
		{
			bool isNull;
			auto datum = SPI_getbinval(SPI_tuptable->vals[iTuple + i],
			                           SPI_tuptable->tupdesc,
			                           iAttribute_ + 1,
			                           &isNull);
			if (isNull)
			{
				haveNull = true;
				validBytes_[i] = 0;
			}
			else
			{
				validBytes_[i] = 1;
				values_[i] = std::string(VARDATA_ANY(datum), VARSIZE_ANY_EXHDR(datum));
			}
		}
		if (haveNull)
		{
			return builder_->AppendValues(values_, validBytes_.data());
		}
		else
		{
			return builder_->AppendValues(values_);
		}
	}
};

arrow::Result<std::unique_ptr<ArrowArrayBuilderBase>>
ArrowArrayBuilderBase::make(Form_pg_attribute attribute,
                            int iAttribute,
                            arrow::ArrayBuilder* builder)
{
	switch (attribute->atttypid)
	{
		case INT2OID:
			return std::make_unique<ArrowArrayBuilder<arrow::Int16Type>>(
				attribute, iAttribute, builder);
		case INT4OID:
			return std::make_unique<ArrowArrayBuilder<arrow::Int32Type>>(
				attribute, iAttribute, builder);
		case INT8OID:
			return std::make_unique<ArrowArrayBuilder<arrow::Int64Type>>(
				attribute, iAttribute, builder);
		case FLOAT4OID:
			return std::make_unique<ArrowArrayBuilder<arrow::FloatType>>(
				attribute, iAttribute, builder);
		case FLOAT8OID:
			return std::make_unique<ArrowArrayBuilder<arrow::DoubleType>>(
				attribute, iAttribute, builder);
		case VARCHAROID:
		case TEXTOID:
			return std::make_unique<ArrowArrayBuilder<arrow::StringType>>(
				attribute, iAttribute, builder);
		case BYTEAOID:
			return std::make_unique<ArrowArrayBuilder<arrow::BinaryType>>(
				attribute, iAttribute, builder);
		case TIMESTAMPOID:
			return std::make_unique<ArrowArrayBuilder<arrow::TimestampType>>(
				attribute, iAttribute, builder);
		default:
			return arrow::Status::NotImplemented("Unsupported PostgreSQL type: ",
			                                     attribute->atttypid);
	}
}

arrow::Result<std::shared_ptr<arrow::DataType>>
ArrowArrayBuilderBase::arrow_type(Form_pg_attribute attribute)
{
	switch (attribute->atttypid)
	{
		case INT2OID:
			return arrow::int16();
		case INT4OID:
			return arrow::int32();
		case INT8OID:
			return arrow::int64();
		case FLOAT4OID:
			return arrow::float32();
		case FLOAT8OID:
			return arrow::float64();
		case VARCHAROID:
		case TEXTOID:
			return arrow::utf8();
		case BYTEAOID:
			return arrow::binary();
		case TIMESTAMPOID:
			return arrow::timestamp(arrow::TimeUnit::MICRO);
		default:
			return arrow::Status::NotImplemented("Unsupported PostgreSQL type: ",
			                                     attribute->atttypid);
	}
}

class PreparedStatement {
   public:
	explicit PreparedStatement(std::string query)
		: query_(std::move(query)), parameters_()
	{
	}

	~PreparedStatement() {}

	using WriteFunc = std::add_pointer<arrow::Status(void*)>::type;
	arrow::Status select(WriteFunc write, void* writeData)
	{
		if (parameters_.empty())
		{
			auto result = SPI_execute(query_.c_str(), false, 0);
			if (result <= 0)
			{
				return arrow::Status::Invalid("failed to run a query: ",
				                              SPI_result_code_string(result),
				                              ": ",
				                              query_);
			}
			return write(writeData);
		}

		for (const auto& recordBatch : parameters_)
		{
			SPIExecuteOptions options = {};
			std::vector<Oid> pgTypes;
			ARROW_RETURN_NOT_OK(prepare(options, pgTypes, recordBatch->schema()));
			auto plan = SPI_prepare(query_.c_str(), pgTypes.size(), pgTypes.data());
			ScopedPlan scopedPlan(plan);
			ARROW_RETURN_NOT_OK(
				execute(plan, recordBatch, options, [&]() { return write(writeData); }));
		}
		return arrow::Status::OK();
	}

	arrow::Status set_parameters(std::shared_ptr<SharedRingBufferInputStream>& input)
	{
		parameters_.clear();
		ARROW_ASSIGN_OR_RAISE(auto reader,
		                      arrow::ipc::RecordBatchStreamReader::Open(input));
		while (true)
		{
			std::shared_ptr<arrow::RecordBatch> recordBatch;
			ARROW_RETURN_NOT_OK(reader->ReadNext(&recordBatch));
			if (!recordBatch)
			{
				break;
			}
			parameters_.push_back(std::move(recordBatch));
		}
		return arrow::Status::OK();
	}

	arrow::Result<int64_t> update(std::shared_ptr<SharedRingBufferInputStream>& input)
	{
		ARROW_ASSIGN_OR_RAISE(auto reader,
		                      arrow::ipc::RecordBatchStreamReader::Open(input));
		SPIExecuteOptions options = {};
		std::vector<Oid> pgTypes;
		ARROW_RETURN_NOT_OK(prepare(options, pgTypes, reader->schema()));
		auto plan = SPI_prepare(query_.c_str(), pgTypes.size(), pgTypes.data());
		ScopedPlan scopedPlan(plan);

		int64_t nUpdatedRecords = 0;
		while (true)
		{
			std::shared_ptr<arrow::RecordBatch> recordBatch;
			ARROW_RETURN_NOT_OK(reader->ReadNext(&recordBatch));
			if (!recordBatch)
			{
				break;
			}
			ARROW_RETURN_NOT_OK(execute(plan, recordBatch, options, [&nUpdatedRecords]() {
				nUpdatedRecords += SPI_processed;
				return arrow::Status::OK();
			}));
		}
		return nUpdatedRecords;
	}

   private:
	arrow::Status prepare_pg_types(std::vector<Oid>& pgTypes,
	                               const std::shared_ptr<arrow::Schema>& schema)
	{
		ArrowPGTypeConverter converter;
		for (const auto& field : schema->fields())
		{
			ARROW_RETURN_NOT_OK(field->type()->Accept(&converter));
			pgTypes.push_back(converter.oid());
		}
		return arrow::Status::OK();
	}

	arrow::Status prepare(SPIExecuteOptions& options,
	                      std::vector<Oid>& pgTypes,
	                      const std::shared_ptr<arrow::Schema>& schema)
	{
		if (schema->num_fields() > 0)
		{
			options.params = makeParamList(schema->num_fields());
		}
		options.read_only = false;
		options.tcount = 0;
		ARROW_RETURN_NOT_OK(prepare_pg_types(pgTypes, schema));
		for (size_t i = 0; i < pgTypes.size(); ++i)
		{
			options.params->params[i].pflags = PARAM_FLAG_CONST;
			options.params->params[i].ptype = pgTypes[i];
		}
		return arrow::Status::OK();
	}

	template <typename OnSuccessFunc>
	arrow::Status execute(SPIPlanPtr plan,
	                      const std::shared_ptr<arrow::RecordBatch>& recordBatch,
	                      SPIExecuteOptions& options,
	                      OnSuccessFunc onSuccess)
	{
		const auto& columns = recordBatch->columns();
		for (int64_t i = 0; i < recordBatch->num_rows(); ++i)
		{
			ARROW_RETURN_NOT_OK(assign_parameters(recordBatch, i, columns, options));
			auto result = SPI_execute_plan_extended(plan, &options);
			if (result <= 0)
			{
				return arrow::Status::Invalid("failed to run a prepared statement: ",
				                              SPI_result_code_string(result),
				                              ": ",
				                              query_);
			}
			ARROW_RETURN_NOT_OK(onSuccess());
		}
		return arrow::Status::OK();
	}

	arrow::Status assign_parameters(
		const std::shared_ptr<arrow::RecordBatch>& recordBatch,
		int64_t i_row,
		const std::vector<std::shared_ptr<arrow::Array>>& columns,
		SPIExecuteOptions& options)
	{
		int64_t i_column = 0;
		for (const auto& column : columns)
		{
			auto param = &(options.params->params[i_column]);
			param->isnull = column->IsNull(i_row);
			if (!param->isnull)
			{
				ArrowPGValueConverter converter(i_row, param->value);
				ARROW_RETURN_NOT_OK(column->Accept(&converter));
			}
			++i_column;
		}
		return arrow::Status::OK();
	}

	std::string query_;
	std::vector<std::shared_ptr<arrow::RecordBatch>> parameters_;
};

class Executor : public WorkerProcessor {
   public:
	explicit Executor(uint64_t sessionID)
		: WorkerProcessor("executor"),
		  sessionID_(sessionID),
		  localSession_(std::make_shared<LocalSessionData>()),
		  needFinish_(false),
		  closed_(false),
		  nextPreparedStatementID_(1),
		  preparedStatements_()
	{
		localSession_->id = sessionID_;
		localSession_->peerPID = sharedData_->serverPID;
	}

	~Executor()
	{
		if (!closed_)
		{
			close();
		}
	}

	void open()
	{
		const char* tag = "open";
		// Use this when you want to attach a executor process.
		// sleep(5);
		pgstat_report_activity(STATE_RUNNING, (std::string(Tag) + ": opening").c_str());
		auto session = find_session();
		PG_TRY();
		{
			auto databaseName =
				static_cast<const char*>(dsa_get_address(area_, session->databaseName));
			auto userName =
				static_cast<const char*>(dsa_get_address(area_, session->userName));
			auto password =
				static_cast<const char*>(dsa_get_address(area_, session->password));
			auto clientAddress =
				static_cast<const char*>(dsa_get_address(area_, session->clientAddress));
			BackgroundWorkerInitializeConnection(databaseName, userName, 0);
			CurrentResourceOwner =
				ResourceOwnerCreate(nullptr, "arrow-flight-sql: Executor");
			if (check_password(session, databaseName, userName, password, clientAddress))
			{
				// TODO: Customizable.
				SharedRingBuffer::allocate_data(
					&(session->bufferData), area_, 1L * 1024L * 1024L);
			}
			session->initialized = true;
			localSession_->valid = true;
			localSession_->bufferData = &(session->bufferData);
			localSession_->bufferAddress =
				dsa_get_address(area_, session->bufferData.pointer);
			P("%s: %s: open: %" PRIu64, Tag, tag_, session->bufferData.pointer);
			dshash_release_lock(sessions_, session);
			signal_server(tag);
		}
		PG_CATCH();
		{
			set_error_message(session, "failed to connect", tag);
			session->initialized = true;
			dshash_release_lock(sessions_, session);
			signal_server(tag);
			PG_RE_THROW();
		}
		PG_END_TRY();
	}

	void close()
	{
		closed_ = true;
		pgstat_report_activity(STATE_RUNNING, (std::string(Tag) + ": closing").c_str());
		preparedStatements_.clear();
		if (needFinish_)
		{
			SPI_finish();
			needFinish_ = false;
		}
		if (CurrentResourceOwner)
		{
			auto resourceOwner = CurrentResourceOwner;
			CurrentResourceOwner = nullptr;
			ResourceOwnerRelease(
				resourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, true);
			ResourceOwnerRelease(resourceOwner, RESOURCE_RELEASE_LOCKS, false, true);
			ResourceOwnerRelease(
				resourceOwner, RESOURCE_RELEASE_AFTER_LOCKS, false, true);
			ResourceOwnerDelete(resourceOwner);
		}
		pgstat_report_activity(STATE_IDLE, nullptr);
	}

	arrow::Result<size_t> read(LocalSessionData* session,
	                           SharedRingBuffer* buffer,
	                           size_t n,
	                           void* output) override
	{
		ProcessorLockGuard lock(this);
		return buffer->read(n, output);
	}

	arrow::Result<size_t> write(LocalSessionData* session,
	                            SharedRingBuffer* buffer,
	                            const void* data,
	                            size_t n) override
	{
		ProcessorLockGuard lock(this);
		return buffer->write(data, n);
	}

	void signaled()
	{
		Action action;
		{
			auto session = find_session();
			SharedSessionReleaser sessionReleaser(sessions_, session);
			ProcessorLockGuard lock(this);
			action = session->action;
			session->action = Action::None;
		}
		P("%s: %s: signaled: before: %s", Tag, tag_, action_name(action));
		PG_TRY();
		{
			switch (action)
			{
				case Action::Select:
					select();
					break;
				case Action::Update:
					update();
					break;
				case Action::Prepare:
					prepare();
					break;
				case Action::ClosePreparedStatement:
					close_prepared_statement();
					break;
				case Action::SetParameters:
					set_parameters();
					break;
				case Action::SelectPreparedStatement:
					select_prepared_statement();
					break;
				case Action::UpdatePreparedStatement:
					update_prepared_statement();
					break;
				default:
					// TODO: Report an error
					break;
			}
		}
		PG_CATCH();
		{
			auto session = find_session();
			SharedSessionReleaser sessionReleaser(sessions_, session);
			if (!DsaPointerIsValid(session->errorMessage))
			{
				auto error = CopyErrorData();
				set_error_message(session,
				                  std::string("failed to run: ") + action_name(action) +
				                      ": " + error->message,
				                  "unexpected error");
				FreeErrorData(error);
			}
			pgstat_report_activity(STATE_IDLE, nullptr);
			PG_RE_THROW();
		}
		PG_END_TRY();
		pgstat_report_activity(STATE_IDLE, nullptr);
		P("%s: %s: signaled: after: %s", Tag, tag_, action_name(action));
	}

   protected:
	pid_t peer_pid(LocalSessionData* session) override { return sharedData_->serverPID; }

	const char* peer_name() override { return "server"; }

	arrow::Status wait_internal(LocalSessionData* sessio51n,
	                            SharedRingBuffer* buffer,
	                            WaitMode mode,
	                            const char* tag) override
	{
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
				P("%s: %s: %s: %s: wait: %" PRIsize,
				  Tag,
				  tag_,
				  tag,
				  peer_name(),
				  get_waiting_buffer_size(buffer, mode));
				if (get_waiting_buffer_size(buffer, mode) > 0)
				{
					break;
				}
			}

			// TODO: Convert PG error to arrow::Status.
			CHECK_FOR_INTERRUPTS();
		}
		return arrow::Status::OK();
	}

   private:
	SharedSessionData* find_session()
	{
		auto session =
			static_cast<SharedSessionData*>(dshash_find(sessions_, &sessionID_, false));
		if (!session)
		{
			ereport(ERROR,
			        errcode(ERRCODE_INTERNAL_ERROR),
			        errmsg("%s: %s: unknown session: %" PRIu64, Tag, tag_, sessionID_));
		}
		return session;
	}

	void signal_server(const char* tag)
	{
		if (sharedData_->serverPID == InvalidPid)
		{
			return;
		}
		P("%s: %s: %s: kill server: %d", Tag, tag_, tag, sharedData_->serverPID);
		kill(sharedData_->serverPID, SIGUSR1);
	}

	void set_error_message(SharedSessionData* session,
	                       const std::string& message,
	                       const char* tag)
	{
		if (DsaPointerIsValid(session->errorMessage))
		{
			return;
		}
		{
			ProcessorLockGuard lock(this);
			set_shared_string(session->errorMessage, message);
		}
		signal_server(tag);
	}

	bool check_password(SharedSessionData* session,
	                    const char* databaseName,
	                    const char* userName,
	                    const char* password,
	                    const char* clientAddress)
	{
		const char* tag = "check password";
		MemoryContext memoryContext =
			AllocSetContextCreate(CurrentMemoryContext,
		                          "arrow-flight-sql: Executor::check_password()",
		                          ALLOCSET_DEFAULT_SIZES);
		ScopedMemoryContext scopedMemoryContext(memoryContext);
		Port port = {};
		port.database_name = pstrdup(databaseName);
		port.user_name = pstrdup(userName);
		if (!fill_client_address(session, &port, clientAddress))
		{
			return false;
		}
		load_hba();
		hba_getauthmethod(&port);
		if (!port.hba)
		{
			set_error_message(session, "failed to get auth method", tag);
			return false;
		}
		switch (port.hba->auth_method)
		{
			case uaMD5:
				// TODO
				set_error_message(session, "MD5 auth method isn't supported yet", tag);
				return false;
			case uaSCRAM:
				// TODO
				set_error_message(session, "SCRAM auth method isn't supported yet", tag);
				return false;
			case uaPassword:
			{
				const char* logDetail = nullptr;
				auto shadowPassword = get_role_password(port.user_name, &logDetail);
				if (!shadowPassword)
				{
					set_error_message(session,
					                  std::string("failed to get password: ") + logDetail,
					                  tag);
					return false;
				}
				auto result = plain_crypt_verify(
					port.user_name, shadowPassword, password, &logDetail);
				if (result != STATUS_OK)
				{
					set_error_message(
						session,
						std::string("failed to verify password: ") + logDetail,
						tag);
					return false;
				}
				return true;
			}
			case uaTrust:
				return true;
			default:
				set_error_message(session,
				                  std::string("unsupported auth method: ") +
				                      hba_authname(port.hba->auth_method),
				                  tag);
				return false;
		}
	}

	bool fill_client_address(SharedSessionData* session,
	                         Port* port,
	                         const char* clientAddress)
	{
		const char* tag = "fill client address";
		// clientAddress: "ipv4:127.0.0.1:40468"
		// family: "ipv4"
		// host: "127.0.0.1"
		// port: "40468"
		std::stringstream clientAddressStream{std::string(clientAddress)};
		std::string clientFamily("");
		std::string clientHost("");
		std::string clientPort("");
		std::getline(clientAddressStream, clientFamily, ':');
		std::getline(clientAddressStream, clientHost, ':');
		std::getline(clientAddressStream, clientPort);
		if (!(clientFamily == "ipv4" || clientFamily == "ipv6"))
		{
			set_error_message(
				session,
				std::string("client family must be ipv4 or ipv6: ") + clientFamily,
				tag);
			return false;
		}
		auto clientPortStart = clientPort.c_str();
		char* clientPortEnd = nullptr;
		auto clientPortNumber = std::strtoul(clientPortStart, &clientPortEnd, 10);
		if (clientPortEnd[0] != '\0')
		{
			set_error_message(
				session, std::string("client port is invalid: ") + clientPort, tag);
			return false;
		}
		if (clientPortNumber == 0)
		{
			set_error_message(session, std::string("client port must not 0"), tag);
			return false;
		}
		if (clientPortNumber > 65535)
		{
			set_error_message(session,
			                  std::string("client port is too large: ") +
			                      std::to_string(clientPortNumber),
			                  tag);
			return false;
		}
		if (clientFamily == "ipv4")
		{
			auto raddr = reinterpret_cast<sockaddr_in*>(&(port->raddr.addr));
			port->raddr.salen = sizeof(sockaddr_in);
			raddr->sin_family = AF_INET;
			raddr->sin_port = htons(clientPortNumber);
			if (inet_pton(AF_INET, clientHost.c_str(), &(raddr->sin_addr)) == 0)
			{
				set_error_message(
					session,
					std::string("client IPv4 address is invalid: ") + clientHost,
					tag);
				return false;
			}
		}
		else if (clientFamily == "ipv6")
		{
			auto raddr = reinterpret_cast<sockaddr_in6*>(&(port->raddr.addr));
			port->raddr.salen = sizeof(sockaddr_in6);
			raddr->sin6_family = AF_INET6;
			raddr->sin6_port = htons(clientPortNumber);
			raddr->sin6_flowinfo = 0;
			if (inet_pton(AF_INET6, clientHost.c_str(), &(raddr->sin6_addr)) == 0)
			{
				set_error_message(
					session,
					std::string("client IPv6 address is invalid: ") + clientHost,
					tag);
				return false;
			}
			raddr->sin6_scope_id = 0;
		}
		return true;
	}

	void select()
	{
		const char* tag = "select";
		pgstat_report_activity(STATE_RUNNING, (std::string(Tag) + ": selecting").c_str());

		auto session = find_session();
		SharedSessionReleaser sessionReleaser(sessions_, session);
		if (!DsaPointerIsValid(session->selectQuery))
		{
			set_error_message(
				session,
				std::string(Tag) + ": " + tag_ + ": " + tag + ": query is missing",
				tag);
			return;
		}

		std::string query;
		{
			ProcessorLockGuard lock(this);
			query =
				static_cast<const char*>(dsa_get_address(area_, session->selectQuery));
			dsa_free(area_, session->selectQuery);
			session->selectQuery = InvalidDsaPointer;
		}
		P("%s: %s: %s: %s", Tag, tag_, tag, query.c_str());

		{
			ScopedTransaction scopedTransaction;
			ScopedSnapshot scopedSnapshot;

			SetCurrentStatementStartTimestamp();
			SPI_connect();
			needFinish_ = true;
			auto result = SPI_execute(query.c_str(), true, 0);
			if (result > 0)
			{
				pgstat_report_activity(
					STATE_RUNNING, (std::string(Tag) + ": " + tag + ": writing").c_str());
				auto status = write_record_batches(tag);
				if (!status.ok())
				{
					set_error_message(session,
					                  std::string(Tag) + ": " + tag_ + ": " + tag +
					                      ": failed to write: " + status.ToString(),
					                  tag);
				}
			}
			else
			{
				set_error_message(session,
				                  std::string(Tag) + ": " + tag_ + ": " + tag +
				                      ": failed to run a query: <" + query +
				                      ">: " + SPI_result_code_string(result),
				                  tag);
			}
			SPI_finish();
			needFinish_ = false;
			signal_server(tag);
		}
	}

	arrow::Status write_record_batches(const char* tag)
	{
		SharedRingBufferOutputStream output(this, localSession_);
		std::vector<std::shared_ptr<arrow::Field>> fields;
		for (int i = 0; i < SPI_tuptable->tupdesc->natts; ++i)
		{
			auto attribute = TupleDescAttr(SPI_tuptable->tupdesc, i);
			ARROW_ASSIGN_OR_RAISE(auto type,
			                      ArrowArrayBuilderBase::arrow_type(attribute));
			fields.push_back(arrow::field(
				NameStr(attribute->attname), std::move(type), !attribute->attnotnull));
		}
		auto schema = arrow::schema(fields);
		ARROW_ASSIGN_OR_RAISE(
			auto builder,
			arrow::RecordBatchBuilder::Make(schema, arrow::default_memory_pool()));
		std::vector<std::unique_ptr<ArrowArrayBuilderBase>> builders;
		for (int i = 0; i < SPI_tuptable->tupdesc->natts; ++i)
		{
			auto attribute = TupleDescAttr(SPI_tuptable->tupdesc, i);
			ARROW_ASSIGN_OR_RAISE(
				auto array_builder,
				ArrowArrayBuilderBase::make(attribute, i, builder->GetField(i)));
			builders.push_back(std::move(array_builder));
		}

		auto options = arrow::ipc::IpcWriteOptions::Defaults();
		options.emit_dictionary_deltas = true;

		// Write schema only stream format data to return only schema.
		ARROW_ASSIGN_OR_RAISE(auto writer,
		                      arrow::ipc::MakeStreamWriter(&output, schema, options));
		// Build an empty record batch to write schema.
		ARROW_ASSIGN_OR_RAISE(auto recordBatch, builder->Flush());
		P("%s: %s: %s: write: schema: WriteRecordBatch", Tag, tag_, tag);
		ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*recordBatch));
		P("%s: %s: %s: write: schema: Close", Tag, tag_, tag);
		ARROW_RETURN_NOT_OK(writer->Close());

		// Write another stream format data with record batches.
		ARROW_ASSIGN_OR_RAISE(writer,
		                      arrow::ipc::MakeStreamWriter(&output, schema, options));
		uint64_t iTuple = 0;
		for (; iTuple < SPI_processed; iTuple += MaxNRowsPerRecordBatch)
		{
			uint64_t iTupleEnd = iTuple + MaxNRowsPerRecordBatch;
			if (iTupleEnd >= SPI_processed)
			{
				iTupleEnd = SPI_processed;
			}
			P("%s: %s: %s: write: data: record batch: %" PRIu64 "/%" PRIu64,
			  Tag,
			  tag_,
			  tag,
			  iTuple,
			  iTupleEnd);
			for (int iAttribute = 0; iAttribute < SPI_tuptable->tupdesc->natts;
			     ++iAttribute)
			{
				P("%s: %s: %s: write: data: record batch: %" PRIu64 "/%" PRIu64 ": %d/%d",
				  Tag,
				  tag_,
				  tag,
				  iTuple,
				  iTupleEnd,
				  iAttribute,
				  SPI_tuptable->tupdesc->natts);
				ARROW_RETURN_NOT_OK(builders[iAttribute]->build(iTuple, iTupleEnd));
			}
			ARROW_ASSIGN_OR_RAISE(recordBatch, builder->Flush());
			P("%s: %s: %s: write: data: WriteRecordBatch: %" PRIu64 "/%" PRIu64,
			  Tag,
			  tag_,
			  tag,
			  iTuple,
			  iTupleEnd);
			ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*recordBatch));
		}

		if (iTuple < SPI_processed)
		{
			P("%s: %s: %s: write: data: record batch: last: %" PRIu64 "/%" PRIu64,
			  Tag,
			  tag_,
			  tag,
			  iTuple,
			  SPI_processed);
			for (int iAttribute = 0; iAttribute < SPI_tuptable->tupdesc->natts;
			     ++iAttribute)
			{
				P("%s: %s: %s: write: data: record batch: last: %" PRIu64 "/%" PRIu64
				  ": %d/%d",
				  Tag,
				  tag_,
				  tag,
				  iTuple,
				  SPI_processed,
				  iAttribute,
				  SPI_tuptable->tupdesc->natts);
				ARROW_RETURN_NOT_OK(builders[iAttribute]->build(iTuple, SPI_processed));
			}
			ARROW_ASSIGN_OR_RAISE(recordBatch, builder->Flush());
			P("%s: %s: %s: write: data: WriteRecordBatch: last: %" PRIu64 "/%" PRIu64,
			  Tag,
			  tag_,
			  tag,
			  iTuple,
			  SPI_processed);
			ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*recordBatch));
		}
		P("%s: %s: %s, write: data: Close", Tag, tag_, tag);
		ARROW_RETURN_NOT_OK(writer->Close());
		return output.Close();
	}

	void update()
	{
		const char* tag = "update";
		pgstat_report_activity(STATE_RUNNING, (std::string(Tag) + ": updating").c_str());

		auto session = find_session();
		SharedSessionReleaser sessionReleaser(sessions_, session);
		if (!DsaPointerIsValid(session->updateQuery))
		{
			set_error_message(
				session,
				std::string(Tag) + ": " + tag_ + ": " + tag + ": query is missing",
				tag);
			return;
		}

		std::string query;
		{
			ProcessorLockGuard lock(this);
			query =
				static_cast<const char*>(dsa_get_address(area_, session->updateQuery));
			dsa_free(area_, session->updateQuery);
			session->updateQuery = InvalidDsaPointer;
		}
		P("%s: %s: %s: %s", Tag, tag_, tag, query.c_str());

		{
			ScopedTransaction scopedTransaction;
			ScopedSnapshot scopedSnapshot;

			SetCurrentStatementStartTimestamp();
			SPI_connect();
			needFinish_ = true;
			auto result = SPI_execute(query.c_str(), false, 0);
			if (result > 0)
			{
				session->nUpdatedRecords = SPI_processed;
			}
			else
			{
				set_error_message(session,
				                  std::string(Tag) + ": " + tag_ + ": " + tag +
				                      ": failed to run a query: <" + query +
				                      ">: " + SPI_result_code_string(result),
				                  tag);
			}
			SPI_finish();
			needFinish_ = false;
			signal_server(tag);
		}
	}

	void prepare()
	{
		const char* tag = "prepare";
		pgstat_report_activity(STATE_RUNNING, (std::string(Tag) + ": preparing").c_str());

		auto session = find_session();
		SharedSessionReleaser sessionReleaser(sessions_, session);
		if (!DsaPointerIsValid(session->prepareQuery))
		{
			set_error_message(
				session,
				std::string(Tag) + ": " + tag_ + ": " + tag + ": query is missing",
				tag);
			return;
		}

		std::string query;
		{
			ProcessorLockGuard lock(this);
			query =
				static_cast<const char*>(dsa_get_address(area_, session->prepareQuery));
			dsa_free(area_, session->prepareQuery);
			session->prepareQuery = InvalidDsaPointer;
		}
		P("%s: %s: %s: %s", Tag, tag_, tag, query.c_str());
		std::string handle(std::to_string(nextPreparedStatementID_++));
		preparedStatements_.insert(
			std::make_pair(handle, PreparedStatement(std::move(query))));
		{
			ProcessorLockGuard lock(this);
			set_shared_string(session->preparedStatementHandle, handle);
		}
		signal_server(tag);
	}

	bool extract_handle(SharedSessionData* session, std::string& handle, const char* tag)
	{
		if (!DsaPointerIsValid(session->preparedStatementHandle))
		{
			set_error_message(
				session,
				std::string(Tag) + ": " + tag_ + ": " + tag + ": handle is missing",
				tag);
			return false;
		}

		{
			ProcessorLockGuard lock(this);
			handle = static_cast<const char*>(
				dsa_get_address(area_, session->preparedStatementHandle));
			dsa_free(area_, session->preparedStatementHandle);
			session->preparedStatementHandle = InvalidDsaPointer;
		}

		return true;
	}

	void close_prepared_statement()
	{
		const char* tag = "close prepared statement";

		pgstat_report_activity(STATE_RUNNING, (std::string(Tag) + ": " + tag).c_str());

		auto session = find_session();
		SharedSessionReleaser sessionReleaser(sessions_, session);
		std::string handle;
		if (!extract_handle(session, handle, tag))
		{
			return;
		}
		P("%s: %s: %s: %s", Tag, tag_, tag, handle.c_str());
		if (preparedStatements_.erase(handle) == 0)
		{
			set_error_message(session,
			                  std::string(Tag) + ": " + tag_ + ": " + tag +
			                      ": nonexistent handle: <" + handle + ">",
			                  tag);
		}
		signal_server(tag);
	}

	PreparedStatement* find_prepared_statement(SharedSessionData* session,
	                                           std::string& handle,
	                                           const char* tag)
	{
		if (!extract_handle(session, handle, tag))
		{
			return nullptr;
		}

		ProcessorLockGuard lock(this);
		auto it = preparedStatements_.find(handle);
		if (it == preparedStatements_.end())
		{
			set_error_message(session,
			                  std::string(Tag) + ": " + tag_ + ": " + tag +
			                      ": nonexistent handle: <" + handle + ">",
			                  tag);
			return nullptr;
		}
		else
		{
			return &(it->second);
		}
	}

	void set_parameters()
	{
		const char* tag = "set parameters";

		pgstat_report_activity(STATE_RUNNING,
		                       (std::string(Tag) + ": setting parameters").c_str());
		auto session = find_session();
		SharedSessionReleaser sessionReleaser(sessions_, session);
		std::string handle;
		auto preparedStatement = find_prepared_statement(session, handle, tag);
		P("%s: %s: %s: %s", Tag, tag_, tag, handle.c_str());

		if (!preparedStatement)
		{
			return;
		}

		auto input = std::make_shared<SharedRingBufferInputStream>(this, localSession_);
		auto status = preparedStatement->set_parameters(input);
		if (!status.ok())
		{
			set_error_message(session,
			                  std::string(Tag) + ": " + tag_ + ": " + tag +
			                      ": failed to set parameters: <" + handle +
			                      ">: " + status.ToString(),
			                  tag);
		}
		session->setParametersFinished = true;
		signal_server(tag);
	}

	void select_prepared_statement()
	{
		const char* tag = "select prepared statement";

		pgstat_report_activity(
			STATE_RUNNING, (std::string(Tag) + ": selecting prepared statement").c_str());

		auto session = find_session();
		SharedSessionReleaser sessionReleaser(sessions_, session);
		std::string handle;
		auto preparedStatement = find_prepared_statement(session, handle, tag);
		P("%s: %s: %s: %s", Tag, tag_, tag, handle.c_str());

		if (!preparedStatement)
		{
			return;
		}

		ScopedTransaction scopedTransaction;
		ScopedSnapshot scopedSnapshot;

		SetCurrentStatementStartTimestamp();
		SPI_connect();
		needFinish_ = true;
		struct Data {
			Executor* executor;
			const char* tag;
		} data = {this, tag};
		auto status = preparedStatement->select(
			[](void* data) {
				auto d = static_cast<Data*>(data);
				return d->executor->write_record_batches(d->tag);
			},
			&data);
		if (!status.ok())
		{
			set_error_message(session,
			                  std::string(Tag) + ": " + tag_ + ": " + tag +
			                      ": failed to select a prepared statement: <" + handle +
			                      ">: " + status.ToString(),
			                  tag);
		}
		SPI_finish();
		needFinish_ = false;
		signal_server(tag);
	}

	void update_prepared_statement()
	{
		const char* tag = "update prepared statement";

		pgstat_report_activity(
			STATE_RUNNING, (std::string(Tag) + ": updating prepared statement").c_str());

		auto session = find_session();
		SharedSessionReleaser sessionReleaser(sessions_, session);
		std::string handle;
		auto preparedStatement = find_prepared_statement(session, handle, tag);
		P("%s: %s: %s: %s", Tag, tag_, tag, handle.c_str());

		if (!preparedStatement)
		{
			return;
		}

		ScopedTransaction scopedTransaction;
		ScopedSnapshot scopedSnapshot;

		SetCurrentStatementStartTimestamp();
		SPI_connect();
		needFinish_ = true;
		auto input = std::make_shared<SharedRingBufferInputStream>(this, localSession_);
		auto n_updated_records_result = preparedStatement->update(input);
		if (n_updated_records_result.ok())
		{
			session->nUpdatedRecords = *n_updated_records_result;
		}
		else
		{
			set_error_message(session,
			                  std::string(Tag) + ": " + tag_ + ": " + tag +
			                      ": failed to update a prepared statement: <" + handle +
			                      ">: " + n_updated_records_result.status().ToString(),
			                  tag);
		}
		SPI_finish();
		needFinish_ = false;
		signal_server(tag);
	}

	uint64_t sessionID_;
	std::shared_ptr<LocalSessionData> localSession_;
	bool needFinish_;
	bool closed_;
	uint64_t nextPreparedStatementID_;
	std::map<std::string, PreparedStatement> preparedStatements_;
};

arrow::Status
SharedRingBufferOutputStream::Write(const void* data, int64_t nBytes)
{
	if (ARROW_PREDICT_FALSE(!is_open_))
	{
		return arrow::Status::IOError(std::string(Tag) + ": " + processor_->tag() +
		                              ": SharedRingBufferOutputStream is closed");
	}
	if (ARROW_PREDICT_TRUE(nBytes > 0))
	{
		SharedRingBuffer buffer(
			localSession_->bufferData, localSession_->bufferAddress, processor_->tag());
		size_t rest = static_cast<size_t>(nBytes);
		while (true)
		{
			ARROW_ASSIGN_OR_RAISE(
				auto writtenBytes,
				processor_->write(localSession_.get(), &buffer, data, rest));
			if (INTERRUPTS_PENDING_CONDITION())
			{
				return arrow::Status::IOError(std::string(Tag) + ": " +
				                              processor_->tag() + ": interrupted");
			}

			if (writtenBytes == 0)
			{
				ARROW_RETURN_NOT_OK(processor_->wait(
					localSession_.get(), &buffer, Processor::WaitMode::Read));
				if (INTERRUPTS_PENDING_CONDITION())
				{
					return arrow::Status::IOError(std::string(Tag) + ": " +
					                              processor_->tag() + ": interrupted");
				}
				continue;
			}

			position_ += writtenBytes;
			rest -= writtenBytes;
			data = static_cast<const uint8_t*>(data) + writtenBytes;

			if (ARROW_PREDICT_TRUE(rest == 0))
			{
				break;
			}
		}
	}
	return arrow::Status::OK();
}

// There is only one Proxy object in a PostgreSQL instance. The Proxy
// object communicates multiple "executor" processes.
//
// This class must be thread safe. We call PostgreSQL API only in
// signaled(). We can't call PostgreSQL API in other methods because
// they are called from gRPC threads not the main thread. PostgreSQL
// API isn't thread safe and must be called only in the main thread.
//
// If we need to call PostgreSQL API from gRPC threads, we need to
// create a request and process it in signaled().
class Proxy : public WorkerProcessor {
   public:
	explicit Proxy()
		: WorkerProcessor("proxy"),
		  mutex_(),
		  conditionVariable_(),
		  randomSeed_(),
		  randomEngine_(randomSeed_()),
		  localSessions_(),
		  connectRequests_(),
		  selectRequests_(),
		  updateRequests_(),
		  prepareRequests_(),
		  closePreparedStatementRequests_(),
		  setParametersRequests_(),
		  selectPreparedStatementRequests_(),
		  updatePreparedStatementRequests_(),
		  readRequests_(),
		  writeRequests_()
	{
	}

   private:
	std::mutex mutex_;
	std::condition_variable conditionVariable_;
	std::random_device randomSeed_;
	std::mt19937_64 randomEngine_;
	std::map<uint64_t, std::shared_ptr<LocalSessionData>> localSessions_;

   public:
	arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> open_reader(
		uint64_t sessionID)
	{
		std::shared_ptr<LocalSessionData> localSession;
		{
			std::lock_guard<std::mutex> lock(mutex_);
			auto it = localSessions_.find(sessionID);
			if (it == localSessions_.end())
			{
				return arrow::Status::Invalid("unknown session: ", sessionID);
			}
			localSession = it->second;
		}
		auto input =
			std::make_shared<SharedRingBufferInputStream>(this, std::move(localSession));
		// Read another stream format data with record batches.
		return arrow::ipc::RecordBatchStreamReader::Open(input);
	}

	// This is only called from PostgreSQL (main) thread. We can use
	// PostgreSQL API only in this method and methods called from this
	// method.
	void signaled()
	{
		process_connect_requests();
		process_select_requests();
		process_update_requests();
		process_prepare_requests();
		process_close_prepared_statement_requests();
		process_set_parameters_requests();
		process_select_prepared_statement_requests();
		process_update_prepared_statement_requests();
		process_read_requests();
		process_write_requests();
		sync_sessions();
		conditionVariable_.notify_all();
	}

	// Don't use PostgreSQL API.
	bool is_valid_session(uint64_t sessionID) { return find_session(sessionID).ok(); }

   private:
	// Don't use PostgreSQL API.
	arrow::Result<std::shared_ptr<LocalSessionData>> find_session(uint64_t id)
	{
		std::lock_guard<std::mutex> lock(mutex_);
		auto it = localSessions_.find(id);
		if (it == localSessions_.end())
		{
			return arrow::Status::Invalid("Unknown session: ", id);
		}
		return it->second;
	}

	// Don't use PostgreSQL API.
	arrow::Status check_local_session_error(LocalSessionData* localSession,
	                                        bool locked = false)
	{
		if (!localSession->errorMessage.has_value())
		{
			return arrow::Status::OK();
		}
		if (locked)
		{
			auto errorMessage = std::move(localSession->errorMessage.value());
			localSession->errorMessage = std::nullopt;
			return arrow::Status::Invalid(errorMessage);
		}
		else
		{
			std::lock_guard<std::mutex> lock(mutex_);
			if (!localSession->errorMessage.has_value())
			{
				return arrow::Status::OK();
			}
			auto errorMessage = std::move(localSession->errorMessage.value());
			localSession->errorMessage = std::nullopt;
			return arrow::Status::Invalid(errorMessage);
		}
	}

	// Don't use PostgreSQL API.
	arrow::Status check_local_session_error(
		const std::shared_ptr<LocalSessionData>& localSession)
	{
		return check_local_session_error(localSession.get());
	}

   private:
	struct ConnectRequest {
		ConnectRequest(uint64_t sessionID,
		               std::string databaseName,
		               std::string userName,
		               std::string password,
		               std::string clientAddress)
			: sessionID(sessionID),
			  databaseName(std::move(databaseName)),
			  userName(std::move(userName)),
			  password(std::move(password)),
			  clientAddress(std::move(clientAddress)),
			  processing(false),
			  finished(false)
		{
		}
		uint64_t sessionID;
		std::string databaseName;
		std::string userName;
		std::string password;
		std::string clientAddress;
		bool processing;
		bool finished;
	};

	std::list<std::shared_ptr<ConnectRequest>> connectRequests_;

	// Can use PostgreSQL API.
	void process_connect_requests()
	{
		std::lock_guard<std::mutex> lock(mutex_);
		bool haveRequest = false;
		for (auto it = connectRequests_.begin(); it != connectRequests_.end();)
		{
			auto& request = *it;
			ProcessorLockGuard lock(this);
			if (request->processing)
			{
				auto session = static_cast<SharedSessionData*>(
					dshash_find(sessions_, &(request->sessionID), false));
				if (!session)
				{
					++it;
					continue;
				}
				const auto initialized = session->initialized;
				if (initialized && !DsaPointerIsValid(session->errorMessage))
				{
					auto& localSession = localSessions_.find(request->sessionID)->second;
					localSession->valid = true;
					localSession->peerPID = session->executorPID;
					localSession->bufferData = &(session->bufferData);
					localSession->bufferAddress =
						dsa_get_address(area_, session->bufferData.pointer);
					P("%s: %s: connect: %" PRIu64,
					  Tag,
					  tag_,
					  session->bufferData.pointer);
				}
				dshash_release_lock(sessions_, session);
				if (!initialized)
				{
					++it;
					continue;
				}
				request->finished = true;
				it = connectRequests_.erase(it);
			}
			else
			{
				bool found;
				auto session = static_cast<SharedSessionData*>(
					dshash_find_or_insert(sessions_, &(request->sessionID), &found));
				if (found)
				{
					request->finished = true;
					auto& localSession = localSessions_.find(request->sessionID)->second;
					localSession->errorMessage = std::string("duplicated session ID: ") +
					                             std::to_string(request->sessionID);
					it = connectRequests_.erase(it);
				}
				else
				{
					request->processing = true;
					shared_session_data_initialize(session,
					                               area_,
					                               request->databaseName,
					                               request->userName,
					                               request->password,
					                               request->clientAddress);
					haveRequest = true;
					++it;
				}
				dshash_release_lock(sessions_, session);
			}
		}
		if (haveRequest)
		{
			kill(sharedData_->mainPID, SIGUSR1);
		}
	}

	// Don't use PostgreSQL API.
	uint64_t assign_session_id()
	{
		std::lock_guard<std::mutex> lock(mutex_);
		while (true)
		{
			auto id = randomEngine_();
			if (id == 0)
			{
				continue;
			}
			auto beforeSize = localSessions_.size();
			auto localSession = std::make_shared<LocalSessionData>();
			localSessions_.insert(std::make_pair(id, localSession));
			if (localSessions_.size() == beforeSize)
			{
				continue;
			}
			localSession->id = id;
			localSession->valid = false;
			return id;
		}
	}

   public:
	// Don't use PostgreSQL API.
	arrow::Result<uint64_t> connect(const std::string& databaseName,
	                                const std::string& userName,
	                                const std::string& password,
	                                const std::string& clientAddress)
	{
		auto id = assign_session_id();
		ARROW_ASSIGN_OR_RAISE(auto localSession, find_session(id));
		auto request = std::make_shared<ConnectRequest>(
			id, databaseName, userName, password, clientAddress);
		{
			std::lock_guard<std::mutex> lock(mutex_);
			connectRequests_.push_back(request);
		}
		kill(MyProcPid, SIGUSR1);
		{
			std::unique_lock<std::mutex> lock(mutex_);
			conditionVariable_.wait(lock, [&] {
				if (localSession->errorMessage.has_value())
				{
					return true;
				}
				if (INTERRUPTS_PENDING_CONDITION())
				{
					return true;
				}
				return request->finished;
			});
		}
		ARROW_RETURN_NOT_OK(check_local_session_error(localSession));
		if (INTERRUPTS_PENDING_CONDITION())
		{
			return arrow::Status::Invalid("interrupted");
		}
		return id;
	}

   private:
	struct SelectRequest {
		SelectRequest(std::shared_ptr<LocalSessionData> localSession, std::string query)
			: localSession(std::move(localSession)),
			  query(std::move(query)),
			  finished(false)
		{
		}
		std::shared_ptr<LocalSessionData> localSession;
		std::string query;
		bool finished;
	};

	std::list<std::shared_ptr<SelectRequest>> selectRequests_;

	// Can use PostgreSQL API.
	void process_select_requests()
	{
#ifdef AFS_VERBOSE
		const char* tag = "select";
#endif
		std::lock_guard<std::mutex> lock(mutex_);
		for (auto& request : selectRequests_)
		{
			request->finished = true;
			auto session = static_cast<SharedSessionData*>(
				dshash_find(sessions_, &(request->localSession->id), false));
			if (!session)
			{
				request->localSession->errorMessage =
					std::string("stolen session: ") +
					std::to_string(request->localSession->id);
				continue;
			}
			auto executorPID = session->executorPID;
			{
				SharedSessionReleaser sessionReleaser(sessions_, session);
				ProcessorLockGuard lock(this);
				set_shared_string(session->selectQuery, request->query);
				session->action = Action::Select;
			}
			P("%s: %s: %s: kill executor: %d", Tag, tag_, tag, executorPID);
			kill(executorPID, SIGUSR1);
		}
		selectRequests_.clear();
	}

	arrow::Result<std::shared_ptr<arrow::Schema>> read_schema(
		std::shared_ptr<LocalSessionData> localSession, const char* tag)
	{
		auto input =
			std::make_shared<SharedRingBufferInputStream>(this, std::move(localSession));

		// Read schema only stream format data.
		ARROW_ASSIGN_OR_RAISE(auto reader,
		                      arrow::ipc::RecordBatchStreamReader::Open(input));
		while (true)
		{
			std::shared_ptr<arrow::RecordBatch> recordBatch;
			P("%s: %s: %s: read next", Tag, tag_, tag);
			ARROW_RETURN_NOT_OK(reader->ReadNext(&recordBatch));
			if (!recordBatch)
			{
				break;
			}
		}
		return reader->schema();
	}

   public:
	arrow::Result<std::shared_ptr<arrow::Schema>> select(uint64_t sessionID,
	                                                     const std::string& query)
	{
		const char* tag = "select";
		ARROW_ASSIGN_OR_RAISE(auto localSession, find_session(sessionID));
		auto request = std::make_shared<SelectRequest>(localSession, query);
		{
			std::lock_guard<std::mutex> lock(mutex_);
			selectRequests_.push_back(request);
		}
		kill(MyProcPid, SIGUSR1);
		{
			std::unique_lock<std::mutex> lock(mutex_);
			conditionVariable_.wait(lock, [&] {
				if (localSession->errorMessage.has_value())
				{
					return true;
				}
				if (INTERRUPTS_PENDING_CONDITION())
				{
					return true;
				}
				return request->finished;
			});
		}
		ARROW_RETURN_NOT_OK(check_local_session_error(localSession));
		if (INTERRUPTS_PENDING_CONDITION())
		{
			return arrow::Status::Invalid("interrupted");
		}
		P("%s: %s: %s: open", Tag, tag_, tag);
		auto schema = read_schema(localSession, tag);
		P("%s: %s: %s: schema", Tag, tag_, tag);
		return schema;
	}

   private:
	struct UpdateRequest {
		UpdateRequest(std::shared_ptr<LocalSessionData> localSession, std::string query)
			: localSession(std::move(localSession)),
			  query(std::move(query)),
			  processing(false),
			  finished(false),
			  nUpdatedRecords(0)
		{
		}
		std::shared_ptr<LocalSessionData> localSession;
		std::string query;
		bool processing;
		bool finished;
		int64_t nUpdatedRecords;
	};

	std::list<std::shared_ptr<UpdateRequest>> updateRequests_;

	// Can use PostgreSQL API.
	void process_update_requests()
	{
#ifdef AFS_VERBOSE
		const char* tag = "update";
#endif
		std::lock_guard<std::mutex> lock(mutex_);
		for (auto it = updateRequests_.begin(); it != updateRequests_.end();)
		{
			auto& request = *it;
			auto session = static_cast<SharedSessionData*>(
				dshash_find(sessions_, &(request->localSession->id), false));
			if (!session)
			{
				request->finished = true;
				request->localSession->errorMessage =
					std::string("stolen session: ") +
					std::to_string(request->localSession->id);
				it = updateRequests_.erase(it);
				continue;
			}
			if (request->processing)
			{
				SharedSessionReleaser sessionReleaser(sessions_, session);
				if (DsaPointerIsValid(session->errorMessage))
				{
					request->processing = false;
					request->finished = true;
					it = updateRequests_.erase(it);
				}
				else if (session->nUpdatedRecords >= 0)
				{
					request->processing = false;
					request->finished = true;
					request->nUpdatedRecords = session->nUpdatedRecords;
					it = updateRequests_.erase(it);
				}
				else
				{
					++it;
				}
			}
			else
			{
				auto executorPID = session->executorPID;
				{
					SharedSessionReleaser sessionReleaser(sessions_, session);
					ProcessorLockGuard lock(this);
					set_shared_string(session->updateQuery, request->query);
					session->action = Action::Update;
					session->nUpdatedRecords = -1;
				}
				P("%s: %s: %s: kill executor: %d", Tag, tag_, tag, executorPID);
				kill(executorPID, SIGUSR1);
				request->processing = true;
				++it;
			}
		}
	}

   public:
	arrow::Result<int64_t> update(uint64_t sessionID, const std::string& query)
	{
#ifdef AFS_VERBOSE
		const char* tag = "update";
#endif
		ARROW_ASSIGN_OR_RAISE(auto localSession, find_session(sessionID));
		auto request = std::make_shared<UpdateRequest>(localSession, query);
		{
			std::lock_guard<std::mutex> lock(mutex_);
			updateRequests_.push_back(request);
		}
		kill(MyProcPid, SIGUSR1);
		{
			std::unique_lock<std::mutex> lock(mutex_);
			conditionVariable_.wait(lock, [&] {
				if (localSession->errorMessage.has_value())
				{
					return true;
				}
				if (INTERRUPTS_PENDING_CONDITION())
				{
					return true;
				}
				return request->finished;
			});
		}
		ARROW_RETURN_NOT_OK(check_local_session_error(localSession));
		if (INTERRUPTS_PENDING_CONDITION())
		{
			return arrow::Status::Invalid("interrupted");
		}
		P("%s: %s: %s: done: %" PRIu64, Tag, tag_, tag, request->nUpdatedRecords);
		return request->nUpdatedRecords;
	}

   private:
	struct PrepareRequest {
		PrepareRequest(std::shared_ptr<LocalSessionData> localSession, std::string query)
			: localSession(std::move(localSession)),
			  query(std::move(query)),
			  processing(false),
			  finished(false),
			  handle()
		{
		}
		std::shared_ptr<LocalSessionData> localSession;
		std::string query;
		bool processing;
		bool finished;
		std::string handle;
	};

	std::list<std::shared_ptr<PrepareRequest>> prepareRequests_;

	// Can use PostgreSQL API.
	void process_prepare_requests()
	{
#ifdef AFS_VERBOSE
		const char* tag = "prepare";
#endif
		std::lock_guard<std::mutex> lock(mutex_);
		for (auto it = prepareRequests_.begin(); it != prepareRequests_.end();)
		{
			auto& request = *it;
			auto session = static_cast<SharedSessionData*>(
				dshash_find(sessions_, &(request->localSession->id), false));
			if (!session)
			{
				request->finished = true;
				request->localSession->errorMessage =
					std::string("stolen session: ") +
					std::to_string(request->localSession->id);
				it = prepareRequests_.erase(it);
				continue;
			}
			if (request->processing)
			{
				SharedSessionReleaser sessionReleaser(sessions_, session);
				if (DsaPointerIsValid(session->errorMessage))
				{
					request->processing = false;
					request->finished = true;
					it = prepareRequests_.erase(it);
				}
				else if (DsaPointerIsValid(session->preparedStatementHandle))
				{
					request->processing = false;
					request->finished = true;
					request->handle = static_cast<const char*>(
						dsa_get_address(area_, session->preparedStatementHandle));
					it = prepareRequests_.erase(it);
				}
				else
				{
					++it;
				}
			}
			else
			{
				auto executorPID = session->executorPID;
				{
					SharedSessionReleaser sessionReleaser(sessions_, session);
					ProcessorLockGuard lock(this);
					set_shared_string(session->prepareQuery, request->query);
					session->action = Action::Prepare;
					session->nUpdatedRecords = -1;
					set_shared_string(session->preparedStatementHandle, std::string(""));
				}
				P("%s: %s: %s: kill executor: %d", Tag, tag_, tag, executorPID);
				kill(executorPID, SIGUSR1);
				request->processing = true;
				++it;
			}
		}
	}

   public:
	arrow::Result<arrow::flight::sql::ActionCreatePreparedStatementResult> prepare(
		uint64_t sessionID, const std::string& query)
	{
#ifdef AFS_VERBOSE
		const char* tag = "prepare";
#endif
		ARROW_ASSIGN_OR_RAISE(auto localSession, find_session(sessionID));
		auto request = std::make_shared<PrepareRequest>(localSession, query);
		{
			std::lock_guard<std::mutex> lock(mutex_);
			prepareRequests_.push_back(request);
		}
		kill(MyProcPid, SIGUSR1);
		{
			std::unique_lock<std::mutex> lock(mutex_);
			conditionVariable_.wait(lock, [&] {
				if (localSession->errorMessage.has_value())
				{
					return true;
				}
				if (INTERRUPTS_PENDING_CONDITION())
				{
					return true;
				}
				return request->finished;
			});
		}
		ARROW_RETURN_NOT_OK(check_local_session_error(localSession));
		if (INTERRUPTS_PENDING_CONDITION())
		{
			return arrow::Status::Invalid("interrupted");
		}
		arrow::flight::sql::ActionCreatePreparedStatementResult result = {
			nullptr,
			nullptr,
			std::move(request->handle),
		};
		P("%s: %s: %s: done", Tag, tag_, tag);
		return result;
	}

   private:
	struct ClosePreparedStatementRequest {
		ClosePreparedStatementRequest(std::shared_ptr<LocalSessionData> localSession,
		                              std::string handle)
			: localSession(std::move(localSession)),
			  handle(std::move(handle)),
			  processing(false),
			  finished(false)
		{
		}
		std::shared_ptr<LocalSessionData> localSession;
		std::string handle;
		bool processing;
		bool finished;
	};

	std::list<std::shared_ptr<ClosePreparedStatementRequest>>
		closePreparedStatementRequests_;

	// Can use PostgreSQL API.
	void process_close_prepared_statement_requests()
	{
#ifdef AFS_VERBOSE
		const char* tag = "close prepared statement";
#endif
		std::lock_guard<std::mutex> lock(mutex_);
		for (auto it = closePreparedStatementRequests_.begin();
		     it != closePreparedStatementRequests_.end();)
		{
			auto& request = *it;
			auto session = static_cast<SharedSessionData*>(
				dshash_find(sessions_, &(request->localSession->id), false));
			if (!session)
			{
				request->finished = true;
				request->localSession->errorMessage =
					std::string("stolen session: ") +
					std::to_string(request->localSession->id);
				it = closePreparedStatementRequests_.erase(it);
				continue;
			}
			if (request->processing)
			{
				SharedSessionReleaser sessionReleaser(sessions_, session);
				if (DsaPointerIsValid(session->errorMessage))
				{
					request->processing = false;
					request->finished = true;
					it = closePreparedStatementRequests_.erase(it);
				}
				else if (DsaPointerIsValid(session->preparedStatementHandle))
				{
					++it;
				}
				else
				{
					request->processing = false;
					request->finished = true;
					it = closePreparedStatementRequests_.erase(it);
				}
			}
			else
			{
				auto executorPID = session->executorPID;
				{
					SharedSessionReleaser sessionReleaser(sessions_, session);
					ProcessorLockGuard lock(this);
					set_shared_string(session->preparedStatementHandle, request->handle);
					session->action = Action::ClosePreparedStatement;
				}
				P("%s: %s: %s: kill executor: %d", Tag, tag_, tag, executorPID);
				kill(executorPID, SIGUSR1);
				request->processing = true;
				++it;
			}
		}
	}

   public:
	arrow::Status close_prepared_statement(uint64_t sessionID, const std::string& handle)
	{
#ifdef AFS_VERBOSE
		const char* tag = "close prepared statement";
#endif
		ARROW_ASSIGN_OR_RAISE(auto localSession, find_session(sessionID));
		auto request =
			std::make_shared<ClosePreparedStatementRequest>(localSession, handle);
		{
			std::lock_guard<std::mutex> lock(mutex_);
			closePreparedStatementRequests_.push_back(request);
		}
		kill(MyProcPid, SIGUSR1);
		{
			std::unique_lock<std::mutex> lock(mutex_);
			conditionVariable_.wait(lock, [&] {
				if (localSession->errorMessage.has_value())
				{
					return true;
				}
				if (INTERRUPTS_PENDING_CONDITION())
				{
					return true;
				}
				return request->finished;
			});
		}
		ARROW_RETURN_NOT_OK(check_local_session_error(localSession));
		if (INTERRUPTS_PENDING_CONDITION())
		{
			return arrow::Status::Invalid("interrupted");
		}
		P("%s: %s: %s: done", Tag, tag_, tag);
		return arrow::Status::OK();
	}

   private:
	struct SetParametersRequest {
		SetParametersRequest(std::shared_ptr<LocalSessionData> localSession,
		                     std::string handle)
			: localSession(std::move(localSession)),
			  handle(std::move(handle)),
			  processing(false),
			  finished(false)
		{
		}
		std::shared_ptr<LocalSessionData> localSession;
		std::string handle;
		bool processing;
		bool finished;
	};

	std::list<std::shared_ptr<SetParametersRequest>> setParametersRequests_;

	// Can use PostgreSQL API.
	void process_set_parameters_requests()
	{
#ifdef AFS_VERBOSE
		const char* tag = "set parameters";
#endif
		std::lock_guard<std::mutex> lock(mutex_);
		for (auto it = setParametersRequests_.begin();
		     it != setParametersRequests_.end();)
		{
			auto& request = *it;
			auto session = static_cast<SharedSessionData*>(
				dshash_find(sessions_, &(request->localSession->id), false));
			if (!session)
			{
				request->finished = true;
				request->localSession->errorMessage =
					std::string("stolen session: ") +
					std::to_string(request->localSession->id);
				it = setParametersRequests_.erase(it);
				continue;
			}
			if (request->processing)
			{
				SharedSessionReleaser sessionReleaser(sessions_, session);
				if (DsaPointerIsValid(session->errorMessage))
				{
					request->processing = false;
					request->finished = true;
					it = setParametersRequests_.erase(it);
				}
				else if (session->setParametersFinished)
				{
					request->processing = false;
					request->finished = true;
					it = setParametersRequests_.erase(it);
				}
				else
				{
					++it;
				}
			}
			else
			{
				auto executorPID = session->executorPID;
				{
					SharedSessionReleaser sessionReleaser(sessions_, session);
					ProcessorLockGuard lock(this);
					set_shared_string(session->preparedStatementHandle, request->handle);
					session->action = Action::SetParameters;
					session->setParametersFinished = false;
				}
				P("%s: %s: %s: kill executor: %d", Tag, tag_, tag, executorPID);
				kill(executorPID, SIGUSR1);
				request->processing = true;
				++it;
			}
		}
	}

   public:
	arrow::Status set_parameters(uint64_t sessionID,
	                             const std::string& handle,
	                             arrow::flight::FlightMessageReader* reader,
	                             arrow::flight::FlightMetadataWriter* writer)
	{
#ifdef AFS_VERBOSE
		const char* tag = "set parameters";
#endif
		ARROW_ASSIGN_OR_RAISE(auto localSession, find_session(sessionID));
		auto request = std::make_shared<SetParametersRequest>(localSession, handle);
		{
			std::lock_guard<std::mutex> lock(mutex_);
			setParametersRequests_.push_back(request);
		}
		kill(MyProcPid, SIGUSR1);
		auto executorPID = localSession->peerPID;
		{
			ARROW_ASSIGN_OR_RAISE(const auto& schema, reader->GetSchema());
			SharedRingBufferOutputStream output(this, localSession);
			auto options = arrow::ipc::IpcWriteOptions::Defaults();
			options.emit_dictionary_deltas = true;
			ARROW_ASSIGN_OR_RAISE(auto writer,
			                      arrow::ipc::MakeStreamWriter(&output, schema, options));
			while (true)
			{
				ARROW_ASSIGN_OR_RAISE(const auto& chunk, reader->Next());
				if (!chunk.data)
				{
					break;
				}
				ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*(chunk.data)));
			}
			ARROW_RETURN_NOT_OK(writer->Close());
			P("%s: %s: %s: kill executor: %d", Tag, tag_, tag, executorPID);
			kill(executorPID, SIGUSR1);
		}
		{
			std::unique_lock<std::mutex> lock(mutex_);
			conditionVariable_.wait(lock, [&] {
				if (localSession->errorMessage.has_value())
				{
					return true;
				}
				if (INTERRUPTS_PENDING_CONDITION())
				{
					return true;
				}
				return request->finished;
			});
		}
		ARROW_RETURN_NOT_OK(check_local_session_error(localSession));
		if (INTERRUPTS_PENDING_CONDITION())
		{
			return arrow::Status::Invalid("interrupted");
		}
		P("%s: %s: %s: done", Tag, tag_, tag);
		return arrow::Status::OK();
	}

   private:
	struct SelectPreparedStatementRequest {
		SelectPreparedStatementRequest(std::shared_ptr<LocalSessionData> localSession,
		                               std::string handle)
			: localSession(std::move(localSession)),
			  handle(std::move(handle)),
			  finished(false)
		{
		}
		std::shared_ptr<LocalSessionData> localSession;
		std::string handle;
		bool finished;
	};

	std::list<std::shared_ptr<SelectPreparedStatementRequest>>
		selectPreparedStatementRequests_;

	// Can use PostgreSQL API.
	void process_select_prepared_statement_requests()
	{
#ifdef AFS_VERBOSE
		const char* tag = "select prepared statement";
#endif
		std::lock_guard<std::mutex> lock(mutex_);
		for (auto& request : selectPreparedStatementRequests_)
		{
			request->finished = true;
			auto session = static_cast<SharedSessionData*>(
				dshash_find(sessions_, &(request->localSession->id), false));
			if (!session)
			{
				request->localSession->errorMessage =
					std::string("stolen session: ") +
					std::to_string(request->localSession->id);
				continue;
			}
			auto executorPID = session->executorPID;
			{
				SharedSessionReleaser sessionReleaser(sessions_, session);
				ProcessorLockGuard lock(this);
				set_shared_string(session->preparedStatementHandle, request->handle);
				session->action = Action::SelectPreparedStatement;
			}
			P("%s: %s: %s: kill executor: %d", Tag, tag_, tag, executorPID);
			kill(executorPID, SIGUSR1);
		}
		selectPreparedStatementRequests_.clear();
	}

   public:
	arrow::Result<std::shared_ptr<arrow::Schema>> select_prepared_statement(
		uint64_t sessionID, const std::string& handle)
	{
		const char* tag = "select prepared statement";
		ARROW_ASSIGN_OR_RAISE(auto localSession, find_session(sessionID));
		auto request =
			std::make_shared<SelectPreparedStatementRequest>(localSession, handle);
		{
			std::lock_guard<std::mutex> lock(mutex_);
			selectPreparedStatementRequests_.push_back(request);
		}
		kill(MyProcPid, SIGUSR1);
		{
			std::unique_lock<std::mutex> lock(mutex_);
			conditionVariable_.wait(lock, [&] {
				if (localSession->errorMessage.has_value())
				{
					return true;
				}
				if (INTERRUPTS_PENDING_CONDITION())
				{
					return true;
				}
				return request->finished;
			});
		}
		ARROW_RETURN_NOT_OK(check_local_session_error(localSession));
		if (INTERRUPTS_PENDING_CONDITION())
		{
			return arrow::Status::Invalid("interrupted");
		}
		P("%s: %s: %s: open", Tag, tag_, tag);
		auto schema = read_schema(localSession, tag);
		P("%s: %s: %s: schema", Tag, tag_, tag);
		return schema;
	}

   private:
	struct UpdatePreparedStatementRequest {
		UpdatePreparedStatementRequest(std::shared_ptr<LocalSessionData> localSession,
		                               std::string handle)
			: localSession(std::move(localSession)),
			  handle(std::move(handle)),
			  processing(false),
			  finished(false),
			  nUpdatedRecords(0)
		{
		}
		std::shared_ptr<LocalSessionData> localSession;
		std::string handle;
		bool processing;
		bool finished;
		int64_t nUpdatedRecords;
	};

	std::list<std::shared_ptr<UpdatePreparedStatementRequest>>
		updatePreparedStatementRequests_;

	// Can use PostgreSQL API.
	void process_update_prepared_statement_requests()
	{
#ifdef AFS_VERBOSE
		const char* tag = "update prepared statement";
#endif
		std::lock_guard<std::mutex> lock(mutex_);
		for (auto it = updatePreparedStatementRequests_.begin();
		     it != updatePreparedStatementRequests_.end();)
		{
			auto& request = *it;
			auto session = static_cast<SharedSessionData*>(
				dshash_find(sessions_, &(request->localSession->id), false));
			if (!session)
			{
				request->finished = true;
				request->localSession->errorMessage =
					std::string("stolen session: ") +
					std::to_string(request->localSession->id);
				it = updatePreparedStatementRequests_.erase(it);
				continue;
			}
			if (request->processing)
			{
				SharedSessionReleaser sessionReleaser(sessions_, session);
				if (DsaPointerIsValid(session->errorMessage))
				{
					request->processing = false;
					request->finished = true;
					it = updatePreparedStatementRequests_.erase(it);
				}
				else if (session->nUpdatedRecords >= 0)
				{
					request->processing = false;
					request->finished = true;
					request->nUpdatedRecords = session->nUpdatedRecords;
					it = updatePreparedStatementRequests_.erase(it);
				}
				else
				{
					++it;
				}
			}
			else
			{
				auto executorPID = session->executorPID;
				{
					SharedSessionReleaser sessionReleaser(sessions_, session);
					ProcessorLockGuard lock(this);
					set_shared_string(session->preparedStatementHandle, request->handle);
					session->action = Action::UpdatePreparedStatement;
					session->nUpdatedRecords = -1;
				}
				P("%s: %s: %s: kill executor: %d", Tag, tag_, tag, executorPID);
				kill(executorPID, SIGUSR1);
				request->processing = true;
				++it;
			}
		}
	}

   public:
	arrow::Result<int64_t> update_prepared_statement(
		uint64_t sessionID,
		const std::string& handle,
		arrow::flight::FlightMessageReader* reader)
	{
#ifdef AFS_VERBOSE
		const char* tag = "update prepared statement";
#endif
		ARROW_ASSIGN_OR_RAISE(auto localSession, find_session(sessionID));
		auto request =
			std::make_shared<UpdatePreparedStatementRequest>(localSession, handle);
		{
			std::lock_guard<std::mutex> lock(mutex_);
			updatePreparedStatementRequests_.push_back(request);
		}
		kill(MyProcPid, SIGUSR1);
		auto executorPID = localSession->peerPID;
		{
			ARROW_ASSIGN_OR_RAISE(const auto& schema, reader->GetSchema());
			SharedRingBufferOutputStream output(this, localSession);
			auto options = arrow::ipc::IpcWriteOptions::Defaults();
			options.emit_dictionary_deltas = true;
			ARROW_ASSIGN_OR_RAISE(auto writer,
			                      arrow::ipc::MakeStreamWriter(&output, schema, options));
			while (true)
			{
				ARROW_ASSIGN_OR_RAISE(const auto& chunk, reader->Next());
				if (!chunk.data)
				{
					break;
				}
				ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*(chunk.data)));
			}
			ARROW_RETURN_NOT_OK(writer->Close());
			P("%s: %s: %s: kill executor: %d", Tag, tag_, tag, executorPID);
			kill(executorPID, SIGUSR1);
		}
		{
			std::unique_lock<std::mutex> lock(mutex_);
			conditionVariable_.wait(lock, [&] {
				if (localSession->errorMessage.has_value())
				{
					return true;
				}
				if (INTERRUPTS_PENDING_CONDITION())
				{
					return true;
				}
				return request->finished;
			});
		}
		ARROW_RETURN_NOT_OK(check_local_session_error(localSession));
		if (INTERRUPTS_PENDING_CONDITION())
		{
			return arrow::Status::Invalid("interrupted");
		}
		P("%s: %s: %s: done: %" PRIu64, Tag, tag_, tag, request->nUpdatedRecords);
		return request->nUpdatedRecords;
	}

   private:
	struct ReadRequest {
		ReadRequest(LocalSessionData* localSession,
		            SharedRingBuffer* buffer,
		            size_t n,
		            void* output)
			: localSession(localSession),
			  buffer(buffer),
			  n(n),
			  output(output),
			  finished(false),
			  readBytes(0)
		{
		}
		LocalSessionData* localSession;
		SharedRingBuffer* buffer;
		size_t n;
		void* output;
		bool finished;
		size_t readBytes;
	};

	std::list<std::shared_ptr<ReadRequest>> readRequests_;

	// Can use PostgreSQL API.
	void process_read_requests()
	{
#ifdef AFS_VERBOSE
		const char* tag = "read";
#endif
		std::lock_guard<std::mutex> lock(mutex_);
		for (auto& request : readRequests_)
		{
			request->finished = true;
			{
				ProcessorLockGuard lock(this);
				request->readBytes = request->buffer->read(request->n, request->output);
			}
			P("%s: %s: %s: kill executor: %d",
			  Tag,
			  tag_,
			  tag,
			  request->localSession->peerPID);
			kill(request->localSession->peerPID, SIGUSR1);
		}
		readRequests_.clear();
	}

   public:
	// Don't use PostgreSQL API.
	arrow::Result<size_t> read(LocalSessionData* localSession,
	                           SharedRingBuffer* buffer,
	                           size_t n,
	                           void* output) override
	{
		auto request = std::make_shared<ReadRequest>(localSession, buffer, n, output);
		{
			std::lock_guard<std::mutex> lock(mutex_);
			readRequests_.push_back(request);
		}
		kill(MyProcPid, SIGUSR1);
		{
			std::unique_lock<std::mutex> lock(mutex_);
			conditionVariable_.wait(lock, [&] {
				if (localSession->errorMessage.has_value())
				{
					return true;
				}
				if (INTERRUPTS_PENDING_CONDITION())
				{
					return true;
				}
				return request->finished;
			});
		}
		ARROW_RETURN_NOT_OK(check_local_session_error(localSession));
		if (INTERRUPTS_PENDING_CONDITION())
		{
			return arrow::Status::IOError("interrupted");
		}
		return request->readBytes;
	}

   private:
	struct WriteRequest {
		WriteRequest(LocalSessionData* localSession,
		             SharedRingBuffer* buffer,
		             const void* data,
		             size_t n)
			: localSession(localSession),
			  buffer(buffer),
			  data(data),
			  n(n),
			  finished(false),
			  writtenBytes(0)
		{
		}
		LocalSessionData* localSession;
		SharedRingBuffer* buffer;
		const void* data;
		size_t n;
		bool finished;
		size_t writtenBytes;
	};

	std::list<std::shared_ptr<WriteRequest>> writeRequests_;

	// Can use PostgreSQL API.
	void process_write_requests()
	{
#ifdef AFS_VERBOSE
		const char* tag = "write";
#endif
		std::lock_guard<std::mutex> lock(mutex_);
		for (auto& request : writeRequests_)
		{
			auto session = static_cast<SharedSessionData*>(
				dshash_find(sessions_, &(request->localSession->id), false));
			if (!session)
			{
				request->finished = true;
				request->localSession->errorMessage =
					std::string("stolen session: ") +
					std::to_string(request->localSession->id);
				continue;
			}
			{
				SharedSessionReleaser sessionReleaser(sessions_, session);
				if (!DsaPointerIsValid(session->errorMessage))
				{
					ProcessorLockGuard lock(this);
					request->writtenBytes =
						request->buffer->write(request->data, request->n);
				}
				request->finished = true;
			}
			P("%s: %s: %s: kill executor: %d",
			  Tag,
			  tag_,
			  tag,
			  request->localSession->peerPID);
			kill(request->localSession->peerPID, SIGUSR1);
		}
		writeRequests_.clear();
	}

   public:
	// Don't use PostgreSQL API.
	arrow::Result<size_t> write(LocalSessionData* localSession,
	                            SharedRingBuffer* buffer,
	                            const void* data,
	                            size_t n) override
	{
		auto request = std::make_shared<WriteRequest>(localSession, buffer, data, n);
		{
			std::lock_guard<std::mutex> lock(mutex_);
			writeRequests_.push_back(request);
		}
		kill(MyProcPid, SIGUSR1);
		{
			std::unique_lock<std::mutex> lock(mutex_);
			conditionVariable_.wait(lock, [&] {
				if (localSession->errorMessage.has_value())
				{
					return true;
				}
				if (INTERRUPTS_PENDING_CONDITION())
				{
					return true;
				}
				return request->finished;
			});
		}
		ARROW_RETURN_NOT_OK(check_local_session_error(localSession));
		if (INTERRUPTS_PENDING_CONDITION())
		{
			return arrow::Status::IOError("interrupted");
		}
		return request->writtenBytes;
	}

   private:
	// Can use PostgreSQL API.
	void sync_sessions()
	{
		dshash_seq_status sessionsStatus;
		dshash_seq_init(&sessionsStatus, sessions_, false);
		while (true)
		{
			auto session =
				static_cast<SharedSessionData*>(dshash_seq_next(&sessionsStatus));
			if (!session)
			{
				break;
			}
			if (DsaPointerIsValid(session->errorMessage))
			{
				std::lock_guard<std::mutex> lock(mutex_);
				auto localSession = localSessions_.find(session->id)->second;
				if (localSession)
				{
					localSession->errorMessage = static_cast<const char*>(
						dsa_get_address(area_, session->errorMessage));
				}
				dsa_free(area_, session->errorMessage);
				session->errorMessage = InvalidDsaPointer;
			}
			if (session->finished)
			{
				{
					std::lock_guard<std::mutex> lock(mutex_);
					localSessions_.erase(localSessions_.find(session->id));
				}
				shared_session_data_finalize(session, area_);
				dshash_delete_current(&sessionsStatus);
			}
		}
		dshash_seq_term(&sessionsStatus);
	}

   protected:
	pid_t peer_pid(LocalSessionData* session) override { return session->peerPID; }

	const char* peer_name() override { return "executor"; }

	arrow::Status wait_internal(LocalSessionData* localSession,
	                            SharedRingBuffer* buffer,
	                            WaitMode mode,
	                            const char* tag) override
	{
		std::unique_lock<std::mutex> lock(mutex_);
		conditionVariable_.wait(lock, [&] {
			P("%s: %s: %s: %s: wait: %" PRIsize ": error: %s",
			  Tag,
			  tag_,
			  tag,
			  peer_name(),
			  get_waiting_buffer_size(buffer, mode),
			  localSession->errorMessage.has_value()
			      ? localSession->errorMessage.value().c_str()
			      : "");
			if (localSession->errorMessage.has_value())
			{
				return true;
			}
			if (INTERRUPTS_PENDING_CONDITION())
			{
				return true;
			}
			return get_waiting_buffer_size(buffer, mode) > 0;
		});
		ARROW_RETURN_NOT_OK(check_local_session_error(localSession, true));
		if (INTERRUPTS_PENDING_CONDITION())
		{
			return arrow::Status::Invalid(tag_, ": ", tag, ": ", "interrupted");
		}
		return arrow::Status::OK();
	}
};

arrow::Result<int64_t>
SharedRingBufferInputStream::Read(int64_t nBytes, void* out)
{
	if (ARROW_PREDICT_FALSE(!is_open_))
	{
		return arrow::Status::IOError(std::string(Tag) + ": " + processor_->tag() +
		                              ": SharedRingBufferInputStream is closed");
	}
	size_t rest = static_cast<size_t>(nBytes);
	SharedRingBuffer buffer(
		localSession_->bufferData, localSession_->bufferAddress, processor_->tag());
	while (true)
	{
		ARROW_ASSIGN_OR_RAISE(auto readBytes,
		                      processor_->read(localSession_.get(), &buffer, rest, out));
		if (INTERRUPTS_PENDING_CONDITION())
		{
			return arrow::Status::IOError(std::string(Tag) + ": " + processor_->tag() +
			                              ": interrupted");
		}

		if (readBytes == 0)
		{
			ARROW_RETURN_NOT_OK(processor_->wait(
				localSession_.get(), &buffer, Processor::WaitMode::Written));
			if (INTERRUPTS_PENDING_CONDITION())
			{
				return arrow::Status::IOError(std::string(Tag) + ": " +
				                              processor_->tag() + ": interrupted");
			}
			continue;
		}

		position_ += readBytes;
		rest -= readBytes;
		out = static_cast<uint8_t*>(out) + readBytes;
		if (ARROW_PREDICT_TRUE(rest == 0))
		{
			break;
		}
	}
	return nBytes;
}

class MainProcessor : public Processor {
   public:
	MainProcessor() : Processor("main"), sessions_(nullptr), executorHandles_()
	{
		LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
		bool found;
		sharedData_ = static_cast<SharedData*>(
			ShmemInitStruct(SharedDataName, sizeof(sharedData_), &found));
		if (found)
		{
			LWLockRelease(AddinShmemInitLock);
			ereport(ERROR,
			        errcode(ERRCODE_INTERNAL_ERROR),
			        errmsg("%s: %s: shared data is already created", Tag, tag_));
		}
		sharedData_->trancheID = LWLockNewTrancheId();
		sharedData_->sessionsTrancheID = LWLockNewTrancheId();
		area_ = dsa_create(sharedData_->trancheID);
		sharedData_->handle = dsa_get_handle(area_);
		SessionsParams.tranche_id = sharedData_->sessionsTrancheID;
		sessions_ = dshash_create(area_, &SessionsParams, nullptr);
		sharedData_->sessionsHandle = dshash_get_hash_table_handle(sessions_);
		sharedData_->serverPID = InvalidPid;
		sharedData_->mainPID = MyProcPid;
		lock_ = &(GetNamedLWLockTranche(LWLockTrancheName)[0].lock);
		LWLockRelease(AddinShmemInitLock);
	}

	~MainProcessor() override { dshash_destroy(sessions_); }

	BackgroundWorkerHandle* start_server()
	{
		BackgroundWorker worker = {};
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

	void signaled() { update_sessions(); }

   private:
	void update_sessions()
	{
		const char* tag = "update sessions";
		P("%s: %s: %s: start", Tag, tag_, tag);
		dshash_seq_status sessionsStatus;
		dshash_seq_init(&sessionsStatus, sessions_, false);
		while (true)
		{
			auto session =
				static_cast<SharedSessionData*>(dshash_seq_next(&sessionsStatus));
			if (!session)
			{
				break;
			}

			if (session->started)
			{
				if (!session->finished)
				{
					try
					{
						auto handle = executorHandles_.at(session->id);
						pid_t pid;
						if (GetBackgroundWorkerPid(handle, &pid) == BGWH_STOPPED)
						{
							// A finished session
							session->finished = true;
							executorHandles_.erase(session->id);
						}
					} catch (const std::exception& exception)
					{
						session->finished = true;
					}
				}
				continue;
			}

			// A new session request
			BackgroundWorker worker = {};
			snprintf(
				worker.bgw_name, BGW_MAXLEN, "%s: executor: %" PRIu64, Tag, session->id);
			snprintf(
				worker.bgw_type, BGW_MAXLEN, "%s: executor: %" PRIu64, Tag, session->id);
			worker.bgw_flags =
				BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
			worker.bgw_start_time = BgWorkerStart_ConsistentState;
			worker.bgw_restart_time = BGW_NEVER_RESTART;
			snprintf(worker.bgw_library_name, BGW_MAXLEN, "%s", LibraryName);
			snprintf(worker.bgw_function_name, BGW_MAXLEN, "afs_executor");
			worker.bgw_main_arg = Int64GetDatum(session->id);
			worker.bgw_notify_pid = MyProcPid;
			BackgroundWorkerHandle* handle;
			P("%s: %s: %s: register executor: %" PRIu64, Tag, tag_, tag, session->id);
			session->started = true;
			if (RegisterDynamicBackgroundWorker(&worker, &handle))
			{
				executorHandles_[session->id] = handle;
				WaitForBackgroundWorkerStartup(handle, &(session->executorPID));
				P("%s: %s: %s: started executor: %" PRIu64, Tag, tag_, tag, session->id);
			}
			else
			{
				P("%s: %s: %s: failed to start executor: %" PRIu64,
				  Tag,
				  tag_,
				  tag,
				  session->id);
				set_shared_string(
					session->errorMessage,
					std::string(Tag) + ": " + tag_ + ": " + tag +
						": failed to start executor: " + std::to_string(session->id));
			}
		}
		dshash_seq_term(&sessionsStatus);
		P("%s: %s: %s: end", Tag, tag_, tag);
		kill(sharedData_->serverPID, SIGUSR1);
	}

	dshash_table* sessions_;
	std::map<uint64_t, BackgroundWorkerHandle*> executorHandles_;
};

class HeaderAuthServerMiddleware : public arrow::flight::ServerMiddleware {
   public:
	explicit HeaderAuthServerMiddleware(uint64_t sessionID) : sessionID_(sessionID) {}

	void SendingHeaders(arrow::flight::AddCallHeaders* outgoing_headers) override
	{
		outgoing_headers->AddHeader("authorization",
		                            std::string("Bearer ") + std::to_string(sessionID_));
	}

	void CallCompleted(const arrow::Status& status) override {}

	std::string name() const override { return "HeaderAuthServerMiddleware"; }

	uint64_t session_id() { return sessionID_; }

   private:
	uint64_t sessionID_;
};

class HeaderAuthServerMiddlewareFactory : public arrow::flight::ServerMiddlewareFactory {
   public:
	explicit HeaderAuthServerMiddlewareFactory(Proxy* proxy)
		: arrow::flight::ServerMiddlewareFactory(), proxy_(proxy)
	{
	}

	arrow::Status StartCall(
		const arrow::flight::CallInfo& info,
#if ARROW_VERSION_MAJOR >= 13
		const arrow::flight::ServerCallContext& context,
#else
		const arrow::flight::CallHeaders& incoming_headers,
#endif
		std::shared_ptr<arrow::flight::ServerMiddleware>* middleware) override
	{
		std::string databaseName("postgres");
#if ARROW_VERSION_MAJOR >= 13
		const auto& incomingHeaders = context.incoming_headers();
#else
		const auto& incomingHeaders = incoming_headers;
#endif
		auto databaseHeader = incomingHeaders.find("x-flight-sql-database");
		if (databaseHeader != incomingHeaders.end())
		{
			databaseName = databaseHeader->second;
		}
		auto authorizationHeader = incomingHeaders.find("authorization");
		if (authorizationHeader == incomingHeaders.end())
		{
			return arrow::flight::MakeFlightError(
				arrow::flight::FlightStatusCode::Unauthenticated,
				"No authorization header");
		}
		auto value = authorizationHeader->second;
		std::stringstream valueStream{std::string(value)};
		std::string type("");
		std::getline(valueStream, type, ' ');
		if (type == "Basic")
		{
			std::stringstream decodedStream(
				arrow::util::base64_decode(value.substr(valueStream.tellg())));
			std::string userName("");
			std::string password("");
			std::getline(decodedStream, userName, ':');
			std::getline(decodedStream, password);
#if ARROW_VERSION_MAJOR >= 13
			const auto& clientAddress = context.peer();
#else
			// 192.0.0.1 is one of reserved IPv4 addresses for documentation.
			std::string clientAddress("ipv4:192.0.2.1:2929");
#endif
			auto sessionIDResult =
				proxy_->connect(databaseName, userName, password, clientAddress);
			if (!sessionIDResult.status().ok())
			{
				return arrow::flight::MakeFlightError(
					arrow::flight::FlightStatusCode::Unauthenticated,
					sessionIDResult.status().ToString());
			}
			auto sessionID = *sessionIDResult;
			*middleware = std::make_shared<HeaderAuthServerMiddleware>(sessionID);
			return arrow::Status::OK();
		}
		else if (type == "Bearer")
		{
			std::string sessionIDString(value.substr(valueStream.tellg()));
			if (sessionIDString.size() == 0)
			{
				return arrow::flight::MakeFlightError(
					arrow::flight::FlightStatusCode::Unauthorized,
					std::string("invalid Bearer token"));
			}
			auto start = sessionIDString.c_str();
			char* end = nullptr;
			uint64_t sessionID = std::strtoull(start, &end, 10);
			if (end[0] != '\0')
			{
				return arrow::flight::MakeFlightError(
					arrow::flight::FlightStatusCode::Unauthorized,
					std::string("invalid Bearer token"));
			}
			if (!proxy_->is_valid_session(sessionID))
			{
				return arrow::flight::MakeFlightError(
					arrow::flight::FlightStatusCode::Unauthorized,
					std::string("invalid Bearer token"));
			}
			*middleware = std::make_shared<HeaderAuthServerMiddleware>(sessionID);
			return arrow::Status::OK();
		}
		else
		{
			return arrow::flight::MakeFlightError(
				arrow::flight::FlightStatusCode::Unauthenticated,
				std::string("authorization header must use Basic or Bearer: <") + type +
					std::string(">"));
		}
	}

   private:
	Proxy* proxy_;
};

// Flight SQL server doesn't communicate an "executor" process
// directly. The Proxy object communicates an "executor" process.
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
		const arrow::flight::FlightDescriptor& descriptor) override
	{
		ARROW_ASSIGN_OR_RAISE(auto sessionID, session_id(context));
		const auto& query = command.query;
		ARROW_ASSIGN_OR_RAISE(auto schema, proxy_->select(sessionID, query));
		ARROW_ASSIGN_OR_RAISE(auto ticket,
		                      arrow::flight::sql::CreateStatementQueryTicket(query));
		std::vector<arrow::flight::FlightEndpoint> endpoints{
			arrow::flight::FlightEndpoint{arrow::flight::Ticket{std::move(ticket)}, {}}};
		ARROW_ASSIGN_OR_RAISE(
			auto result,
			arrow::flight::FlightInfo::Make(*schema, descriptor, endpoints, -1, -1));
		return std::make_unique<arrow::flight::FlightInfo>(result);
	}

	arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> DoGetStatement(
		const arrow::flight::ServerCallContext& context,
		const arrow::flight::sql::StatementQueryTicket& command) override
	{
		ARROW_ASSIGN_OR_RAISE(auto sessionID, session_id(context));
		ARROW_ASSIGN_OR_RAISE(auto reader, proxy_->open_reader(sessionID));
		return std::make_unique<arrow::flight::RecordBatchStream>(reader);
	}

	arrow::Result<int64_t> DoPutCommandStatementUpdate(
		const arrow::flight::ServerCallContext& context,
		const arrow::flight::sql::StatementUpdate& command) override
	{
		ARROW_ASSIGN_OR_RAISE(auto sessionID, session_id(context));
		const auto& query = command.query;
		return proxy_->update(sessionID, query);
	}

	arrow::Result<arrow::flight::sql::ActionCreatePreparedStatementResult>
	CreatePreparedStatement(
		const arrow::flight::ServerCallContext& context,
		const arrow::flight::sql::ActionCreatePreparedStatementRequest& request) override
	{
		ARROW_ASSIGN_OR_RAISE(auto sessionID, session_id(context));
		const auto& query = request.query;
		return proxy_->prepare(sessionID, query);
	}

	arrow::Status ClosePreparedStatement(
		const arrow::flight::ServerCallContext& context,
		const arrow::flight::sql::ActionClosePreparedStatementRequest& request) override
	{
		ARROW_ASSIGN_OR_RAISE(auto sessionID, session_id(context));
		const auto& handle = request.prepared_statement_handle;
		return proxy_->close_prepared_statement(sessionID, handle);
	}

	arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>>
	GetFlightInfoPreparedStatement(
		const arrow::flight::ServerCallContext& context,
		const arrow::flight::sql::PreparedStatementQuery& command,
		const arrow::flight::FlightDescriptor& descriptor) override
	{
		ARROW_ASSIGN_OR_RAISE(auto sessionID, session_id(context));
		const auto& handle = command.prepared_statement_handle;
		ARROW_ASSIGN_OR_RAISE(auto schema,
		                      proxy_->select_prepared_statement(sessionID, handle));
		ARROW_ASSIGN_OR_RAISE(auto ticket,
		                      arrow::flight::sql::CreateStatementQueryTicket(handle));
		std::vector<arrow::flight::FlightEndpoint> endpoints{
			arrow::flight::FlightEndpoint{arrow::flight::Ticket{std::move(ticket)}, {}}};
		ARROW_ASSIGN_OR_RAISE(
			auto result,
			arrow::flight::FlightInfo::Make(*schema, descriptor, endpoints, -1, -1));
		return std::make_unique<arrow::flight::FlightInfo>(result);
	}

	arrow::Status DoPutPreparedStatementQuery(
		const arrow::flight::ServerCallContext& context,
		const arrow::flight::sql::PreparedStatementQuery& command,
		arrow::flight::FlightMessageReader* reader,
		arrow::flight::FlightMetadataWriter* writer) override
	{
		ARROW_ASSIGN_OR_RAISE(auto sessionID, session_id(context));
		const auto& handle = command.prepared_statement_handle;
		return proxy_->set_parameters(sessionID, handle, reader, writer);
	}

	arrow::Result<int64_t> DoPutPreparedStatementUpdate(
		const arrow::flight::ServerCallContext& context,
		const arrow::flight::sql::PreparedStatementUpdate& command,
		arrow::flight::FlightMessageReader* reader) override
	{
		ARROW_ASSIGN_OR_RAISE(auto sessionID, session_id(context));
		const auto& handle = command.prepared_statement_handle;
		return proxy_->update_prepared_statement(sessionID, handle, reader);
	}

   private:
	arrow::Result<uint64_t> session_id(const arrow::flight::ServerCallContext& context)
	{
		auto middleware = reinterpret_cast<HeaderAuthServerMiddleware*>(
			context.GetMiddleware("header-auth"));
		if (!middleware)
		{
			return arrow::flight::MakeFlightError(
				arrow::flight::FlightStatusCode::Unauthenticated, "no authorization");
		}
		return middleware->session_id();
	}

	Proxy* proxy_;
};

arrow::Status
afs_server_internal(Proxy* proxy)
{
	ARROW_ASSIGN_OR_RAISE(auto location, arrow::flight::Location::Parse(URI));
	arrow::flight::FlightServerOptions options(location);
	if (EnableSSL)
	{
		arrow::flight::CertKeyPair certificate;
		if (ssl_cert_file)
		{
			std::ifstream input(ssl_cert_file);
			if (input)
			{
				certificate.pem_cert =
					std::string(std::istreambuf_iterator<char>{input}, {});
			}
		}
		if (ssl_key_file)
		{
			std::ifstream input(ssl_key_file);
			if (input)
			{
				certificate.pem_key =
					std::string(std::istreambuf_iterator<char>{input}, {});
			}
		}
		if (!certificate.pem_cert.empty() && !certificate.pem_key.empty())
		{
			options.tls_certificates.push_back(std::move(certificate));
		}
	}
	options.auth_handler = std::make_unique<arrow::flight::NoOpAuthHandler>();
	options.middleware.push_back(
		{"header-auth", std::make_shared<HeaderAuthServerMiddlewareFactory>(proxy)});
	FlightSQLServer flightSQLServer(proxy);
	ARROW_RETURN_NOT_OK(flightSQLServer.Init(options));

	ereport(LOG, (errmsg("listening on %s for Apache Arrow Flight SQL", URI)));

	while (!GotSIGTERM)
	{
		WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, -1, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		if (GotSIGHUP)
		{
			GotSIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		while (GotSIGUSR1)
		{
			// SIGUSR1 is emitted by:
			// * The "main" process notifies a new "executor" process is spawned
			// * The "main" process notifies a "executor" process is finished
			//   (may be crashed)
			// * The "executor" process requests a read/write
			// * The "executor" process notifies a request is finished
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

static void
afs_executor_before_shmem_exit(int code, Datum arg)
{
	// TODO: This doesn't work. We need to improve
	// BackgroundWorkerInitializeConnection() failed case.
	auto executor = reinterpret_cast<Executor*>(DatumGetPointer(arg));
	executor->close();
	delete executor;
}

extern "C" void
afs_executor(Datum arg)
{
	pqsignal(SIGTERM, afs_sigterm);
	pqsignal(SIGHUP, afs_sighup);
	pqsignal(SIGUSR1, afs_sigusr1);
#ifdef AFS_DEBUG
	pqsignal(SIGSEGV, afs_sigsegv);
#endif
	BackgroundWorkerUnblockSignals();

	auto executor = new Executor(DatumGetInt64(arg));
	before_shmem_exit(afs_executor_before_shmem_exit, PointerGetDatum(executor));
	executor->open();

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

		while (GotSIGUSR1)
		{
			// SIGUSR1 is emitted by:
			// * The "server" process requests a new request
			GotSIGUSR1 = false;
			executor->signaled();
		}

		CHECK_FOR_INTERRUPTS();
	}
	executor->close();

	proc_exit(0);
}

extern "C" void
afs_server(Datum arg)
{
	pqsignal(SIGTERM, afs_sigterm);
	pqsignal(SIGHUP, afs_sighup);
	pqsignal(SIGUSR1, afs_sigusr1);
#ifdef AFS_DEBUG
	pqsignal(SIGSEGV, afs_sigsegv);
#endif
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

			while (GotSIGUSR1)
			{
				// SIGUSR1 is emitted by:
				// * The "server" process is exited (may be crashed)
				// * The "executor" process is exited (may be crashed)
				// * The "server" process requests a new session
				GotSIGUSR1 = false;
				processor.signaled();
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

#ifdef AFS_HAVE_SHMEM_REQUEST_HOOK
	PreviousShmemRequestHook = shmem_request_hook;
	shmem_request_hook = afs_shmem_request_hook;
#else
	afs_shmem_request_hook();
#endif

	BackgroundWorker worker = {};
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
