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

#include <arrow/flight/sql/server.h>

#include <condition_variable>

extern "C"
{
	PG_MODULE_MAGIC;

	extern PGDLLEXPORT void _PG_init(void);
	extern PGDLLEXPORT void afs_executor(Datum datum) pg_attribute_noreturn();
	extern PGDLLEXPORT void afs_server(Datum datum) pg_attribute_noreturn();
	extern PGDLLEXPORT void afs_main(Datum datum) pg_attribute_noreturn();
}

namespace {
static const char* Tag = "arrow-flight-sql";
static const char* URIDefault = "grpc://127.0.0.1:15432";
static char* URI;
static volatile sig_atomic_t GotSIGTERM = false;
static volatile sig_atomic_t GotSIGUSR1 = false;
static const char* LibraryName = "arrow_flight_sql";
static const char* SharedDataName = "arrow-flight-sql: shared data";

void afs_sigterm(SIGNAL_ARGS)
{
	auto errnoSaved = errno;
	GotSIGTERM = true;
	SetLatch(MyLatch);
	errno = errnoSaved;
}

void afs_sigusr1(SIGNAL_ARGS)
{
	procsignal_sigusr1_handler(postgres_signal_arg);
	GotSIGUSR1 = true;
	auto errnoSaved = errno;
	SetLatch(MyLatch);
	errno = errnoSaved;
}

struct SharedData {
	dsa_handle handle;
	pid_t executorPID;
	pid_t serverPID;
	pid_t mainPID;
	Oid databaseOID;
};

class Processor {
   public:
	Processor(const char* tag) : tag_(tag), sharedData_(nullptr), area_(nullptr) {}

	virtual ~Processor() { dsa_detach(area_); }

   protected:
	const char* tag_;
	SharedData* sharedData_;
	dsa_area* area_;
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
		BackgroundWorkerInitializeConnectionByOid(
			sharedData_->databaseOID, InvalidOid, 0);
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
		pgstat_report_activity(STATE_IDLE, NULL);
	}

	void execute() {}
};

class Proxy : public WorkerProcessor {
   public:
	explicit Proxy() : WorkerProcessor("proxy") {}

	void connect()
	{
		kill(sharedData_->mainPID, SIGUSR1);
		std::unique_lock<std::mutex> lock(mutex_);
		condition_variable_.wait(lock,
		                         [&] { return sharedData_->executorPID != InvalidPid; });
	}

	void signaled()
	{
		std::lock_guard<std::mutex> lock(mutex_);
		condition_variable_.notify_all();
	}

   private:
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
		LWLockRelease(AddinShmemInitLock);
		sharedData_ = sharedData;
		area_ = area;
	}

	BackgroundWorkerHandle* start_server()
	{
		BackgroundWorker worker = {0};
		snprintf(worker.bgw_name, BGW_MAXLEN, "%s: server", Tag);
		snprintf(worker.bgw_type, BGW_MAXLEN, Tag);
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
		BackgroundWorker worker = {0};
		snprintf(worker.bgw_name, BGW_MAXLEN, "%s: executor", Tag);
		snprintf(worker.bgw_type, BGW_MAXLEN, Tag);
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

class AuthHandler : public arrow::flight::ServerAuthHandler {
   public:
	explicit AuthHandler(Proxy* proxy) : arrow::flight::ServerAuthHandler(), proxy_(proxy)
	{
	}

	~AuthHandler() override {}

	arrow::Status Authenticate(arrow::flight::ServerAuthSender* outgoing,
	                           arrow::flight::ServerAuthReader* incoming) override
	{
		proxy_->connect();
		return arrow::Status::OK();
	}

	arrow::Status IsValid(const std::string& token, std::string* peer_identity) override
	{
		*peer_identity = "postgres";
		return arrow::Status::OK();
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

   private:
	Proxy* proxy_;
};

arrow::Status
afs_server_internal(Proxy* proxy)
{
	ARROW_ASSIGN_OR_RAISE(auto location, arrow::flight::Location::Parse(URI));
	arrow::flight::FlightServerOptions options(location);
	options.auth_handler = std::make_shared<AuthHandler>(proxy);
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
			WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, -1, PG_WAIT_EXTENSION);
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

	BackgroundWorker worker = {0};
	snprintf(worker.bgw_name, BGW_MAXLEN, "%s: main", Tag);
	snprintf(worker.bgw_type, BGW_MAXLEN, Tag);
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	snprintf(worker.bgw_library_name, BGW_MAXLEN, "%s", LibraryName);
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "afs_main");
	worker.bgw_main_arg = 0;
	worker.bgw_notify_pid = 0;
	RegisterBackgroundWorker(&worker);
}
