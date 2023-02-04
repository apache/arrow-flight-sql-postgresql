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

#include <fmgr.h>
#include <miscadmin.h>
#include <postmaster/bgworker.h>
#include <storage/ipc.h>
#include <storage/latch.h>
#include <utils/guc.h>
#include <utils/wait_event.h>
}

#include <arrow/flight/sql/server.h>

extern "C"
{
	PG_MODULE_MAGIC;

	extern PGDLLEXPORT void _PG_init(void);
	extern PGDLLEXPORT void afs_listener(Datum datum) pg_attribute_noreturn();
}

#define TAG "arrow-flight-sql"
#define AFSURIDefault "grpc://127.0.0.1:15432"

namespace {
static char* AFSURI;
static volatile sig_atomic_t AFSGotSIGTERM = false;
static const char* AFSLibraryName = "arrow_flight_sql";

void afs_sigterm(SIGNAL_ARGS)
{
	auto save_errno = errno;

	AFSGotSIGTERM = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

class PostgreSQLFlightSqlServer : public arrow::flight::sql::FlightSqlServerBase {
   public:
	PostgreSQLFlightSqlServer() : arrow::flight::sql::FlightSqlServerBase() {}
	~PostgreSQLFlightSqlServer() override {}
};

arrow::Status
afs_listen_internal(void)
{
	ARROW_ASSIGN_OR_RAISE(auto location, arrow::flight::Location::Parse(AFSURI));
	arrow::flight::FlightServerOptions options(location);
	PostgreSQLFlightSqlServer server;
	ARROW_RETURN_NOT_OK(server.Init(options));

	while (!AFSGotSIGTERM)
	{
		WaitLatch(MyLatch,
		          WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
		          0,
		          PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		CHECK_FOR_INTERRUPTS();
	}

	return server.Shutdown();
}

}  // namespace

extern "C" void
afs_listener(Datum arg)
{
	pqsignal(SIGTERM, afs_sigterm);
	BackgroundWorkerUnblockSignals();

	auto status = afs_listen_internal();
	if (!status.ok())
	{
		elog(ERROR, "%s: listener: failed: %s", status.ToString().c_str());
	}

	proc_exit(1);
}

extern "C" void
_PG_init(void)
{
	BackgroundWorker worker = {0};

	if (!process_shared_preload_libraries_in_progress)
	{
		return;
	}

	DefineCustomStringVariable("arrow_flight_sql.uri",
	                           "Apache Arrow Flight SQL endpoint URI.",
	                           "default: " AFSURIDefault,
	                           &AFSURI,
	                           AFSURIDefault,
	                           PGC_USERSET,
	                           0,
	                           NULL,
	                           NULL,
	                           NULL);

	snprintf(worker.bgw_name, BGW_MAXLEN, TAG ": listener");
	snprintf(worker.bgw_type, BGW_MAXLEN, TAG);
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	snprintf(worker.bgw_library_name, BGW_MAXLEN, "%s", AFSLibraryName);
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "afs_listener");
	worker.bgw_main_arg = 0;
	worker.bgw_notify_pid = 0;

	RegisterBackgroundWorker(&worker);
}
