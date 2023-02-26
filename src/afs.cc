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

static volatile sig_atomic_t GotSIGTERM = false;
void afs_sigterm(SIGNAL_ARGS)
{
	auto errnoSaved = errno;
	GotSIGTERM = true;
	SetLatch(MyLatch);
	errno = errnoSaved;
}

static volatile sig_atomic_t GotSIGUSR1 = false;
void afs_sigusr1(SIGNAL_ARGS)
{
	procsignal_sigusr1_handler(postgres_signal_arg);
	GotSIGUSR1 = true;
	auto errnoSaved = errno;
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

struct Buffer {
	dsa_pointer data;
	size_t total;
	size_t used;
};

struct ExecuteData {
	dsa_pointer query;
	Buffer buffer;
};

struct SharedData {
	dsa_handle handle;
	LWLock* lock;
	pid_t executorPID;
	pid_t serverPID;
	pid_t mainPID;
	ConnectData connectData;
	ExecuteData executeData;
};

class Processor {
   public:
	Processor(const char* tag) : tag_(tag), sharedData_(nullptr), area_(nullptr) {}

	virtual ~Processor() { dsa_detach(area_); }

   protected:
	const char* tag_;
	SharedData* sharedData_;
	dsa_area* area_;
	LWLock* lock_;
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
			elog(ERROR, "%s: %s: shared data isn't created yet", Tag, tag_);
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
		// TODO: Customizable.
		sharedData_->executeData.buffer.total = 1L * 1024L;
		sharedData_->executeData.buffer.data =
			dsa_allocate(area_, sharedData_->executeData.buffer.total);
		sharedData_->executeData.buffer.used = 0;
		LWLockRelease(lock_);
		StartTransactionCommand();
		SPI_connect();
		PushActiveSnapshot(GetTransactionSnapshot());
		pgstat_report_activity(STATE_IDLE, NULL);
	}

	void close()
	{
		pgstat_report_activity(STATE_RUNNING, (std::string(Tag) + ": closing").c_str());
		PopActiveSnapshot();
		SPI_finish();
		CommitTransactionCommand();
		LWLockAcquire(lock_, LW_EXCLUSIVE);
		dsa_free(area_, sharedData_->executeData.buffer.data);
		sharedData_->executeData.buffer.data = InvalidDsaPointer;
		sharedData_->executeData.buffer.total = 0;
		sharedData_->executeData.buffer.used = 0;
		sharedData_->executorPID = InvalidPid;
		LWLockRelease(lock_);
		pgstat_report_activity(STATE_IDLE, NULL);
	}

	void execute()
	{
		pgstat_report_activity(STATE_RUNNING, (std::string(Tag) + ": executing").c_str());
		LWLockAcquire(lock_, LW_EXCLUSIVE);
		auto query = static_cast<const char*>(
			dsa_get_address(area_, sharedData_->executeData.query));
		SetCurrentStatementStartTimestamp();
		auto result = SPI_execute(query, true, 0);
		dsa_free(area_, sharedData_->executeData.query);
		sharedData_->executeData.query = InvalidDsaPointer;
		if (result == SPI_OK_SELECT)
		{
			auto bufferResult = write();
			if (bufferResult.ok())
			{
				auto buffer = *bufferResult;
				auto output =
					dsa_get_address(area_, sharedData_->executeData.buffer.data);
				memcpy(output, buffer->data(), buffer->size());
				sharedData_->executeData.buffer.used = buffer->size();
			}
		}
		LWLockRelease(lock_);
		if (sharedData_->serverPID != InvalidPid)
		{
			kill(sharedData_->serverPID, SIGUSR1);
		}
		pgstat_report_activity(STATE_IDLE, NULL);
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

	arrow::Result<std::shared_ptr<arrow::Buffer>> write()
	{
		ARROW_ASSIGN_OR_RAISE(auto output, arrow::io::BufferOutputStream::Create());
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
		auto option = arrow::ipc::IpcWriteOptions::Defaults();
		option.emit_dictionary_deltas = true;
		ARROW_ASSIGN_OR_RAISE(auto writer,
		                      arrow::ipc::MakeStreamWriter(output, schema, option));
		ARROW_ASSIGN_OR_RAISE(
			auto builder,
			arrow::RecordBatchBuilder::Make(schema, arrow::default_memory_pool()));
		for (uint64_t iTuple = 0; iTuple < SPI_processed; ++iTuple)
		{
			for (uint64_t iAttribute = 0; iAttribute < SPI_tuptable->tupdesc->natts;
			     ++iAttribute)
			{
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
		}
		ARROW_ASSIGN_OR_RAISE(auto recordBatch, builder->Flush());
		ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*recordBatch));
		ARROW_RETURN_NOT_OK(writer->Close());
		return output->Finish();
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
		std::unique_lock<std::mutex> lock(mutex_);
		condition_variable_.wait(lock,
		                         [&] { return sharedData_->executorPID != InvalidPid; });
		return arrow::Status::OK();
	}

	arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> execute(
		const std::string& query)
	{
		LWLockAcquire(lock_, LW_EXCLUSIVE);
		setSharedString(sharedData_->executeData.query, query);
		LWLockRelease(lock_);
		if (sharedData_->executorPID != InvalidPid)
		{
			kill(sharedData_->executorPID, SIGUSR1);
		}
		std::unique_lock<std::mutex> lock(mutex_);
		condition_variable_.wait(
			lock, [&] { return sharedData_->executeData.buffer.used != 0; });
		return read();
	}

	arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> read()
	{
		auto input = std::make_shared<arrow::io::BufferReader>(
			static_cast<const uint8_t*>(
				dsa_get_address(area_, sharedData_->executeData.buffer.data)),
			sharedData_->executeData.buffer.used);
		return arrow::ipc::RecordBatchStreamReader::Open(input);
	}

	void signaled()
	{
		std::lock_guard<std::mutex> lock(mutex_);
		condition_variable_.notify_all();
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

	std::mutex mutex_;
	std::condition_variable condition_variable_;
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
			elog(ERROR, "%s: %s: shared data is already created", Tag, tag_);
		}
		auto area = dsa_create(LWLockNewTrancheId());
		sharedData->handle = dsa_get_handle(area);
		sharedData->executorPID = InvalidPid;
		sharedData->serverPID = InvalidPid;
		sharedData->mainPID = MyProcPid;
		sharedData->connectData.databaseName = InvalidDsaPointer;
		sharedData->connectData.userName = InvalidDsaPointer;
		sharedData->connectData.password = InvalidDsaPointer;
		sharedData->executeData.buffer.data = InvalidDsaPointer;
		sharedData->executeData.buffer.total = 0;
		sharedData->executeData.buffer.used = 0;
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
			elog(ERROR, "%s: %s: failed to start server", Tag, tag_);
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
			elog(ERROR, "%s: %s: failed to start executor", Tag, tag_);
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
		ARROW_ASSIGN_OR_RAISE(auto reader, proxy_->execute(query));
		auto schema = reader->schema();
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

		if (GotSIGUSR1)
		{
			GotSIGUSR1 = false;
			proxy->signaled();
		}

		CHECK_FOR_INTERRUPTS();
	}

	auto deadline = std::chrono::system_clock::now() + std::chrono::microseconds(10);
	return flightSQLServer.Shutdown(&deadline);
}

}  // namespace

extern "C" void
afs_executor(Datum arg)
{
	pqsignal(SIGTERM, afs_sigterm);
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
			int conditions = WaitLatch(MyLatch, events, timeout, PG_WAIT_EXTENSION);

			if (conditions & WL_TIMEOUT)
			{
				break;
			}

			ResetLatch(MyLatch);

			if (GotSIGUSR1)
			{
				GotSIGUSR1 = false;
				executor.execute();
			}

			CHECK_FOR_INTERRUPTS();
		}
		executor.close();
	}

	proc_exit(0);
}

extern "C" void
afs_server(Datum arg)
{
	pqsignal(SIGTERM, afs_sigterm);
	pqsignal(SIGUSR1, afs_sigusr1);
	BackgroundWorkerUnblockSignals();

	{
		Proxy proxy;
		auto status = afs_server_internal(&proxy);
		if (!status.ok())
		{
			elog(ERROR, "%s: server: failed: %s", Tag, status.ToString().c_str());
		}
	}

	proc_exit(0);
}

extern "C" void
afs_main(Datum arg)
{
	pqsignal(SIGTERM, afs_sigterm);
	pqsignal(SIGUSR1, afs_sigusr1);
	BackgroundWorkerUnblockSignals();

	{
		MainProcessor processor;
		auto serverHandle = processor.start_server();
		while (!GotSIGTERM)
		{
			WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, -1, PG_WAIT_EXTENSION);
			ResetLatch(MyLatch);

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
	                           PGC_USERSET,
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
	                        PGC_SIGHUP,
	                        GUC_UNIT_S,
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
