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
#include <utils/memutils.h>
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

#include <cinttypes>
#include <condition_variable>
#include <random>
#include <sstream>

#include <arpa/inet.h>

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

	static void free_data(SharedRingBufferData* data, dsa_area* area)
	{
		if (data->pointer != InvalidDsaPointer)
			dsa_free(area, data->pointer);
		initialize_data(data);
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

	void free() { free_data(data_, area_); }

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

struct SessionData {
	uint64_t id;
	dsa_pointer errorMessage;
	pid_t executorPID;
	bool initialized;
	dsa_pointer databaseName;
	dsa_pointer userName;
	dsa_pointer password;
	dsa_pointer clientAddress;
	dsa_pointer query;
	SharedRingBufferData bufferData;
};

class SessionReleaser {
   public:
	explicit SessionReleaser(dshash_table* sessions, SessionData* data)
		: sessions_(sessions), data_(data)
	{
	}

	~SessionReleaser() { dshash_release_lock(sessions_, data_); }

   private:
	dshash_table* sessions_;
	SessionData* data_;
};

static dshash_parameters SessionsParams = {
	sizeof(uint32_t),
	sizeof(SessionData),
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
	Processor(const char* tag)
		: tag_(tag),
		  sharedData_(nullptr),
		  area_(nullptr),
		  lock_(),
		  mutex_(),
		  conditionVariable_()
	{
	}

	virtual ~Processor()
	{
		if (area_)
			dsa_detach(area_);
	}

	const char* tag() { return tag_; }

	void lock_acquire(LWLockMode mode) { LWLockAcquire(lock_, LW_EXCLUSIVE); }

	void lock_release() { LWLockRelease(lock_); }

	void signaled()
	{
		P("%s: %s: signaled: before", Tag, tag_);
		conditionVariable_.notify_all();
		P("%s: %s: signaled: after", Tag, tag_);
	}

   protected:
	void set_shared_string(dsa_pointer& pointer, const std::string& input)
	{
		if (DsaPointerIsValid(pointer))
		{
			dsa_free(area_, pointer);
			pointer = InvalidDsaPointer;
		}
		if (input.empty())
		{
			return;
		}
		pointer = dsa_allocate(area_, input.size() + 1);
		memcpy(dsa_get_address(area_, pointer), input.c_str(), input.size() + 1);
	}

	const char* tag_;
	SharedData* sharedData_;
	dsa_area* area_;
	LWLock* lock_;
	std::mutex mutex_;
	std::condition_variable conditionVariable_;
};

class Proxy;
class SharedRingBufferInputStream : public arrow::io::InputStream {
   public:
	SharedRingBufferInputStream(Proxy* proxy, SessionData* session)
		: arrow::io::InputStream(),
		  proxy_(proxy),
		  session_(session),
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
	Proxy* proxy_;
	SessionData* session_;
	int64_t position_;
	bool is_open_;
};

class Executor;
class SharedRingBufferOutputStream : public arrow::io::OutputStream {
   public:
	SharedRingBufferOutputStream(Executor* executor)
		: arrow::io::OutputStream(), executor_(executor), position_(0), is_open_(true)
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
	Executor* executor_;
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
	void delete_session(SessionData* session)
	{
		if (DsaPointerIsValid(session->errorMessage))
			dsa_free(area_, session->errorMessage);
		if (DsaPointerIsValid(session->databaseName))
			dsa_free(area_, session->databaseName);
		if (DsaPointerIsValid(session->userName))
			dsa_free(area_, session->userName);
		if (DsaPointerIsValid(session->password))
			dsa_free(area_, session->password);
		if (DsaPointerIsValid(session->query))
			dsa_free(area_, session->query);
		SharedRingBuffer::free_data(&(session->bufferData), area_);
		dshash_delete_entry(sessions_, session);
	}

   protected:
	dshash_table* sessions_;
};

class Executor : public WorkerProcessor {
   public:
	explicit Executor(uint64_t sessionID)
		: WorkerProcessor("executor"),
		  sessionID_(sessionID),
		  session_(nullptr),
		  connected_(false),
		  closed_(false)
	{
	}

	~Executor()
	{
		if (!closed_)
		{
			close_internal(false);
		}
	}

	void open()
	{
		// pg_usleep(5000000);
		// pg_usleep(5000000);
		pgstat_report_activity(STATE_RUNNING, (std::string(Tag) + ": opening").c_str());
		session_ = static_cast<SessionData*>(dshash_find(sessions_, &sessionID_, false));
		auto databaseName =
			static_cast<const char*>(dsa_get_address(area_, session_->databaseName));
		auto userName =
			static_cast<const char*>(dsa_get_address(area_, session_->userName));
		auto password =
			static_cast<const char*>(dsa_get_address(area_, session_->password));
		auto clientAddress =
			static_cast<const char*>(dsa_get_address(area_, session_->clientAddress));
		BackgroundWorkerInitializeConnection(databaseName, userName, 0);
		CurrentResourceOwner = ResourceOwnerCreate(nullptr, "arrow-flight-sql: Executor");
		if (!check_password(databaseName, userName, password, clientAddress))
		{
			session_->initialized = true;
			P("%s: %s: %s: kill server: %d", Tag, tag_, AFS_FUNC, sharedData_->serverPID);
			kill(sharedData_->serverPID, SIGUSR1);
			return;
		}
		{
			SharedRingBuffer buffer(&(session_->bufferData), area_);
			// TODO: Customizable.
			buffer.allocate(1L * 1024L * 1024L);
		}
		StartTransactionCommand();
		SPI_connect();
		pgstat_report_activity(STATE_IDLE, NULL);
		session_->initialized = true;
		connected_ = true;
		P("%s: %s: %s: kill server: %d", Tag, tag_, AFS_FUNC, sharedData_->serverPID);
		kill(sharedData_->serverPID, SIGUSR1);
	}

	void close() { close_internal(true); }

	SharedRingBuffer create_shared_ring_buffer()
	{
		return SharedRingBuffer(&(session_->bufferData), area_);
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
		P("%s: %s: signaled: before: %d", Tag, tag_, session_->query);
		P("signaled: before: %d", session_->query);
		if (DsaPointerIsValid(session_->query))
		{
			execute();
		}
		else
		{
			Processor::signaled();
		}
		P("%s: %s: signaled: after: %d", Tag, tag_, session_->query);
	}

   private:
	void close_internal(bool unlockSession)
	{
		closed_ = true;
		pgstat_report_activity(STATE_RUNNING, (std::string(Tag) + ": closing").c_str());
		if (connected_)
		{
			SPI_finish();
			CommitTransactionCommand();
			{
				SharedRingBuffer buffer(&(session_->bufferData), area_);
				buffer.free();
			}
			delete_session(session_);
		}
		else
		{
			if (!DsaPointerIsValid(session_->errorMessage))
			{
				set_shared_string(session_->errorMessage, "failed to connect");
			}
			session_->initialized = true;
			if (unlockSession)
			{
				dshash_release_lock(sessions_, session_);
			}
			P("%s: %s: %s: kill server: %d", Tag, tag_, AFS_FUNC, sharedData_->serverPID);
			kill(sharedData_->serverPID, SIGUSR1);
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
		pgstat_report_activity(STATE_IDLE, NULL);
	}

	bool check_password(const char* databaseName,
	                    const char* userName,
	                    const char* password,
	                    const char* clientAddress)
	{
		MemoryContext memoryContext =
			AllocSetContextCreate(CurrentMemoryContext,
		                          "arrow-flight-sql: Executor::check_password()",
		                          ALLOCSET_DEFAULT_SIZES);
		ScopedMemoryContext scopedMemoryContext(memoryContext);
		Port port = {};
		port.database_name = pstrdup(databaseName);
		port.user_name = pstrdup(userName);
		if (!fill_client_address(&port, clientAddress))
		{
			return false;
		}
		load_hba();
		hba_getauthmethod(&port);
		if (!port.hba)
		{
			set_shared_string(session_->errorMessage, "failed to get auth method");
			return false;
		}
		switch (port.hba->auth_method)
		{
			case uaMD5:
				// TODO
				set_shared_string(session_->errorMessage,
				                  "MD5 auth method isn't supported yet");
				return false;
			case uaSCRAM:
				// TODO
				set_shared_string(session_->errorMessage,
				                  "SCRAM auth method isn't supported yet");
				return false;
			case uaPassword:
			{
				const char* logDetail = nullptr;
				auto shadowPassword = get_role_password(port.user_name, &logDetail);
				if (!shadowPassword)
				{
					set_shared_string(
						session_->errorMessage,
						std::string("failed to get password: ") + logDetail);
					return false;
				}
				auto result = plain_crypt_verify(
					port.user_name, shadowPassword, password, &logDetail);
				if (result != STATUS_OK)
				{
					set_shared_string(
						session_->errorMessage,
						std::string("failed to verify password: ") + logDetail);
					return false;
				}
				return true;
			}
			case uaTrust:
				return true;
			default:
				set_shared_string(session_->errorMessage,
				                  std::string("unsupported auth method: ") +
				                      hba_authname(port.hba->auth_method));
				return false;
		}
	}

	bool fill_client_address(Port* port, const char* clientAddress)
	{
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
			set_shared_string(
				session_->errorMessage,
				std::string("client family must be ipv4 or ipv6: ") + clientFamily);
			return false;
		}
		auto clientPortStart = clientPort.c_str();
		char* clientPortEnd = nullptr;
		auto clientPortNumber = std::strtoul(clientPortStart, &clientPortEnd, 10);
		if (clientPortEnd[0] != '\0')
		{
			set_shared_string(session_->errorMessage,
			                  std::string("client port is invalid: ") + clientPort);
			return false;
		}
		if (clientPortNumber == 0)
		{
			set_shared_string(session_->errorMessage,
			                  std::string("client port must not 0"));
			return false;
		}
		if (clientPortNumber > 65535)
		{
			set_shared_string(session_->errorMessage,
			                  std::string("client port is too large: ") +
			                      std::to_string(clientPortNumber));
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
				set_shared_string(
					session_->errorMessage,
					std::string("client IPv4 address is invalid: ") + clientHost);
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
				set_shared_string(
					session_->errorMessage,
					std::string("client IPv6 address is invalid: ") + clientHost);
				return false;
			}
			raddr->sin6_scope_id = 0;
		}
		return true;
	}

	void execute()
	{
		pgstat_report_activity(STATE_RUNNING, (std::string(Tag) + ": executing").c_str());

		PushActiveSnapshot(GetTransactionSnapshot());

		LWLockAcquire(lock_, LW_EXCLUSIVE);
		std::string query(
			static_cast<const char*>(dsa_get_address(area_, session_->query)));
		dsa_free(area_, session_->query);
		session_->query = InvalidDsaPointer;
		SetCurrentStatementStartTimestamp();
		P("%s: %s: execute: %s", Tag, tag_, query.c_str());
		auto result = SPI_execute(query.c_str(), true, 0);
		LWLockRelease(lock_);

		if (result == SPI_OK_SELECT)
		{
			pgstat_report_activity(STATE_RUNNING,
			                       (std::string(Tag) + ": writing").c_str());
			auto status = write();
			if (!status.ok())
			{
				set_shared_string(session_->errorMessage, status.ToString());
			}
		}
		else
		{
			set_shared_string(session_->errorMessage,
			                  std::string(Tag) + ": " + tag_ +
			                      ": failed to run a query: <" + query +
			                      ">: " + SPI_result_code_string(result));
		}

		PopActiveSnapshot();

		if (sharedData_->serverPID != InvalidPid)
		{
			P("%s: %s: kill server: %d", Tag, tag_, sharedData_->serverPID);
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

	uint64_t sessionID_;
	SessionData* session_;
	bool connected_;
	bool closed_;
};

arrow::Status
SharedRingBufferOutputStream::Write(const void* data, int64_t nBytes)
{
	if (ARROW_PREDICT_FALSE(!is_open_))
	{
		return arrow::Status::IOError(std::string(Tag) + ": " + executor_->tag() +
		                              ": SharedRingBufferOutputStream is closed");
	}
	if (ARROW_PREDICT_TRUE(nBytes > 0))
	{
		auto buffer = std::move(executor_->create_shared_ring_buffer());
		size_t rest = static_cast<size_t>(nBytes);
		while (true)
		{
			executor_->lock_acquire(LW_EXCLUSIVE);
			auto writtenSize = buffer.write(data, rest);
			executor_->lock_release();

			position_ += writtenSize;
			rest -= writtenSize;
			data = static_cast<const uint8_t*>(data) + writtenSize;

			if (ARROW_PREDICT_TRUE(rest == 0))
			{
				break;
			}

			executor_->wait_server_read(&buffer);
		}
	}
	return arrow::Status::OK();
}

class Proxy : public WorkerProcessor {
   public:
	explicit Proxy()
		: WorkerProcessor("proxy"), randomSeed_(), randomEngine_(randomSeed_())
	{
	}

	SharedRingBuffer create_shared_ring_buffer(SessionData* session)
	{
		return SharedRingBuffer(&(session->bufferData), area_);
	}

	void wait_executor_written(SessionData* session, SharedRingBuffer* buffer)
	{
		if (ARROW_PREDICT_FALSE(session->executorPID == InvalidPid))
		{
			ereport(ERROR,
			        errcode(ERRCODE_INTERNAL_ERROR),
			        errmsg("%s: %s: executor isn't alive", Tag, tag_));
		}

		P("%s: %s: %s: kill executor: %d", Tag, tag_, AFS_FUNC, session->executorPID);
		kill(session->executorPID, SIGUSR1);
		auto size = buffer->size();
		std::unique_lock<std::mutex> lock(mutex_);
		conditionVariable_.wait(lock, [&] {
			P("%s: %s: %s: wait: write: %d:%d",
			  Tag,
			  tag_,
			  AFS_FUNC,
			  buffer->size(),
			  size);
			if (INTERRUPTS_PENDING_CONDITION())
			{
				return true;
			}
			return buffer->size() != size;
		});
	}

	arrow::Result<uint64_t> connect(const std::string& databaseName,
	                                const std::string& userName,
	                                const std::string& password,
	                                const std::string& clientAddress)
	{
		auto session = create_session(databaseName, userName, password, clientAddress);
		auto id = session->id;
		dshash_release_lock(sessions_, session);
		kill(sharedData_->mainPID, SIGUSR1);
		{
			std::unique_lock<std::mutex> lock(mutex_);
			conditionVariable_.wait(lock, [&] {
				if (INTERRUPTS_PENDING_CONDITION())
				{
					return true;
				}
				session = static_cast<SessionData*>(dshash_find(sessions_, &id, false));
				if (!session)
				{
					return true;
				}
				const auto initialized = session->initialized;
				dshash_release_lock(sessions_, session);
				return initialized;
			});
		}
		session = static_cast<SessionData*>(dshash_find(sessions_, &id, false));
		if (!session)
		{
			return arrow::Status::Invalid("session is stale: ", id);
		}
		SessionReleaser sessionReleaser(sessions_, session);
		if (DsaPointerIsValid(session->errorMessage))
		{
			return report_session_error(session);
		}
		if (INTERRUPTS_PENDING_CONDITION())
		{
			return arrow::Status::Invalid("interrupted");
		}
		return id;
	}

	bool is_valid_session(uint64_t sessionID)
	{
		auto session = find_session(sessionID);
		if (session)
		{
			dshash_release_lock(sessions_, session);
			return true;
		}
		else
		{
			return false;
		}
	}

	arrow::Result<std::shared_ptr<arrow::Schema>> execute(uint64_t sessionID,
	                                                      const std::string& query)
	{
		auto session = find_session(sessionID);
		SessionReleaser sessionReleaser(sessions_, session);
		set_shared_string(session->query, query);
		if (session->executorPID != InvalidPid)
		{
			P("%s: %s: execute: kill executor: %d", Tag, tag_, session->executorPID);
			kill(session->executorPID, SIGUSR1);
		}
		{
			auto buffer = std::move(create_shared_ring_buffer(session));
			std::unique_lock<std::mutex> lock(mutex_);
			conditionVariable_.wait(lock, [&] {
				P("%s: %s: %s: wait: execute", Tag, tag_, AFS_FUNC);
				return DsaPointerIsValid(session->errorMessage) || buffer.size() > 0;
			});
		}
		if (DsaPointerIsValid(session->errorMessage))
		{
			return report_session_error(session);
		}
		P("%s: %s: execute: open", Tag, tag_);
		auto input = std::make_shared<SharedRingBufferInputStream>(this, session);
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

	arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> read(uint64_t sessionID)
	{
		auto session = find_session(sessionID);
		SessionReleaser sessionReleaser(sessions_, session);
		auto input = std::make_shared<SharedRingBufferInputStream>(this, session);
		// Read another stream format data with record batches.
		return arrow::ipc::RecordBatchStreamReader::Open(input);
	}

   private:
	SessionData* create_session(const std::string& databaseName,
	                            const std::string& userName,
	                            const std::string& password,
	                            const std::string& clientAddress)
	{
		LWLockAcquire(lock_, LW_EXCLUSIVE);
		uint64_t id = 0;
		SessionData* session = nullptr;
		do
		{
			id = randomEngine_();
			if (id == 0)
			{
				continue;
			}
			bool found = false;
			session =
				static_cast<SessionData*>(dshash_find_or_insert(sessions_, &id, &found));
			if (!found)
			{
				break;
			}
		} while (true);
		session->errorMessage = InvalidDsaPointer;
		session->executorPID = InvalidPid;
		session->initialized = false;
		set_shared_string(session->databaseName, databaseName);
		set_shared_string(session->userName, userName);
		set_shared_string(session->password, password);
		set_shared_string(session->clientAddress, clientAddress);
		session->query = InvalidDsaPointer;
		SharedRingBuffer::initialize_data(&(session->bufferData));
		LWLockRelease(lock_);
		return session;
	}

	SessionData* find_session(uint64_t sessionID)
	{
		return static_cast<SessionData*>(dshash_find(sessions_, &sessionID, false));
	}

	arrow::Status report_session_error(SessionData* session)
	{
		auto status = arrow::Status::Invalid(
			static_cast<const char*>(dsa_get_address(area_, session->errorMessage)));
		P("%s: %s: %s: kill SIGTERM executor: %d",
		  Tag,
		  tag_,
		  AFS_FUNC,
		  session->executorPID);
		kill(session->executorPID, SIGTERM);
		return status;
	}

	std::random_device randomSeed_;
	std::mt19937_64 randomEngine_;
};

arrow::Result<int64_t>
SharedRingBufferInputStream::Read(int64_t nBytes, void* out)
{
	if (ARROW_PREDICT_FALSE(!is_open_))
	{
		return arrow::Status::IOError(std::string(Tag) + ": " + proxy_->tag() +
		                              ": SharedRingBufferInputStream is closed");
	}
	auto buffer = std::move(proxy_->create_shared_ring_buffer(session_));
	size_t rest = static_cast<size_t>(nBytes);
	while (true)
	{
		proxy_->lock_acquire(LW_EXCLUSIVE);
		auto readBytes = buffer.read(rest, out);
		proxy_->lock_release();

		position_ += readBytes;
		rest -= readBytes;
		out = static_cast<uint8_t*>(out) + readBytes;
		if (ARROW_PREDICT_TRUE(rest == 0))
		{
			break;
		}

		proxy_->wait_executor_written(session_, &buffer);
		if (INTERRUPTS_PENDING_CONDITION())
		{
			return arrow::Status::IOError(std::string(Tag) + ": " + proxy_->tag() +
			                              ": interrupted");
		}
	}
	return nBytes;
}

class MainProcessor : public Processor {
   public:
	MainProcessor() : Processor("main"), sessions_(nullptr)
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

	void process_connect_requests()
	{
		dshash_seq_status sessionsStatus;
		dshash_seq_init(&sessionsStatus, sessions_, false);
		SessionData* session;
		while ((session = static_cast<SessionData*>(dshash_seq_next(&sessionsStatus))))
		{
			if (session->initialized)
			{
				continue;
			}

			BackgroundWorker worker = {0};
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
			if (RegisterDynamicBackgroundWorker(&worker, &handle))
			{
				WaitForBackgroundWorkerStartup(handle, &(session->executorPID));
			}
			else
			{
				set_shared_string(
					session->errorMessage,
					std::string(Tag) + ": " + tag_ +
						": failed to start executor: " + std::to_string(session->id));
			}
		}
		dshash_seq_term(&sessionsStatus);
		kill(sharedData_->serverPID, SIGUSR1);
	}

   private:
	dshash_table* sessions_;
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
		ARROW_ASSIGN_OR_RAISE(auto sessionID, session_id(context));
		const auto& query = command.query;
		ARROW_ASSIGN_OR_RAISE(auto schema, proxy_->execute(sessionID, query));
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
		ARROW_ASSIGN_OR_RAISE(auto sessionID, session_id(context));
		ARROW_ASSIGN_OR_RAISE(auto reader, proxy_->read(sessionID));
		return std::make_unique<arrow::flight::RecordBatchStream>(reader);
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
	options.auth_handler = std::make_unique<arrow::flight::NoOpAuthHandler>();
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

		if (GotSIGUSR1)
		{
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
				processor.process_connect_requests();
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
