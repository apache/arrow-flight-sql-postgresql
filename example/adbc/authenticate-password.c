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

// Start authentication
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <adbc.h>

#ifndef ADBC_ERROR_INIT
#	define ADBC_ERROR_INIT \
		{                   \
			0               \
		}
#endif

static AdbcStatusCode
database_init(struct AdbcDatabase* database, struct AdbcError* error)
{
	AdbcStatusCode code;
	code = AdbcDatabaseSetOption(database, "driver", "adbc_driver_flightsql", error);
	if (code != ADBC_STATUS_OK)
	{
		return code;
	}
	const char* uri = getenv("PGFLIGHTSQLURI");
	char uri_buffer[4096];
	if (!uri)
	{
		const char* host = getenv("PGHOST");
		if (!host)
		{
			host = "localhost";
		}
		const char* sslmode = getenv("PGSSLMODE");
		if (sslmode &&
		    ((strcmp(sslmode, "require") == 0) || (strcmp(sslmode, "verify-ca") == 0) ||
		     (strcmp(sslmode, "verify-full") == 0)))
		{
			snprintf(uri_buffer, sizeof(uri_buffer), "grpc+tls://%s:15432", host);
		}
		else
		{
			snprintf(uri_buffer, sizeof(uri_buffer), "grpc://%s:15432", host);
		}
		uri = uri_buffer;
	}
	code = AdbcDatabaseSetOption(database, "uri", uri, error);
	if (code != ADBC_STATUS_OK)
	{
		return code;
	}
	const char* sslrootcert = getenv("PGSSLROOTCERT");
	char sslrootcert_buffer[4096];
	if (!sslrootcert)
	{
		const char* home = getenv("HOME");
		if (home)
		{
			snprintf(sslrootcert_buffer,
			         sizeof(sslrootcert_buffer),
			         "%s/.postgresql/root.crt",
			         home);
			sslrootcert = sslrootcert_buffer;
		}
	}
	if (sslrootcert)
	{
		FILE* input = fopen(sslrootcert, "r");
		if (input)
		{
			char sslrootcert_data[40960];
			size_t read_size =
				fread(sslrootcert_data, 1, sizeof(sslrootcert_data), input);
			fclose(input);
			if (read_size < sizeof(sslrootcert_data))
			{
				code =
					AdbcDatabaseSetOption(database,
				                          "adbc.flight.sql.client_option.tls_root_certs",
				                          sslrootcert_data,
				                          error);
				if (code != ADBC_STATUS_OK)
				{
					return code;
				}
			}
		}
	}
	const char* user = getenv("PGUSER");
	if (!user)
	{
		user = getenv("USER");
	}
	if (user)
	{
		code = AdbcDatabaseSetOption(database, "username", user, error);
		if (code != ADBC_STATUS_OK)
		{
			return code;
		}
	}
	const char* password = getenv("PGPASSWORD");
	if (password)
	{
		code = AdbcDatabaseSetOption(database, "password", password, error);
		if (code != ADBC_STATUS_OK)
		{
			return code;
		}
	}
	const char* database_name = getenv("PGDATABASE");
	if (!database_name)
	{
		database_name = user;
	}
	if (database_name)
	{
		code =
			AdbcDatabaseSetOption(database,
		                          "adbc.flight.sql.rpc.call_header.x-flight-sql-database",
		                          database_name,
		                          error);
		if (code != ADBC_STATUS_OK)
		{
			return code;
		}
	}
	return AdbcDatabaseInit(database, error);
}

static AdbcStatusCode
run(struct AdbcError* error)
{
	AdbcStatusCode code;
	struct AdbcDatabase database = {0};
	code = AdbcDatabaseNew(&database, error);
	if (code != ADBC_STATUS_OK)
	{
		return code;
	}
	code = database_init(&database, error);
	if (code == ADBC_STATUS_OK)
	{
		struct AdbcConnection connection = {0};
		code = AdbcConnectionNew(&connection, error);
		if (code == ADBC_STATUS_OK)
		{
			code = AdbcConnectionInit(&connection, &database, error);
			AdbcConnectionRelease(&connection, error);
		}
	}
	AdbcDatabaseRelease(&database, error);
	return code;
}

int
main(int argc, char** argv)
{
	struct AdbcError error = ADBC_ERROR_INIT;
	AdbcStatusCode code = run(&error);
	if (code == ADBC_STATUS_OK)
	{
		printf("Authenticated!\n");
		return EXIT_SUCCESS;
	}
	else
	{
		fprintf(stderr, "%s\n", error.message);
		error.release(&error);
		return EXIT_FAILURE;
	}
}
// End authentication
