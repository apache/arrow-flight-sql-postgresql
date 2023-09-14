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

#include <cstdlib>
#include <fstream>
#include <iostream>

#include <arrow/builder.h>
#include <arrow/flight/sql/client.h>
#include <arrow/table_builder.h>

namespace {
std::string
getenv(const char* name)
{
	auto value = std::getenv(name);
	if (value)
	{
		return std::string(value);
	}
	else
	{
		return std::string("");
	}
}

arrow::Result<std::unique_ptr<arrow::flight::sql::FlightSqlClient>>
connect(arrow::flight::FlightCallOptions& call_options)
{
	auto uri = getenv("PGFLIGHTSQLURI");
	if (uri.empty())
	{
		auto host = getenv("PGHOST");
		if (host.empty())
		{
			host = "localhost";
		}
		auto sslmode = getenv("PGSSLMODE");
		if (sslmode == "require" || sslmode == "verify-ca" || sslmode == "verify-full")
		{
			uri = std::string("grpc+tls://") + host + ":15432";
		}
		else
		{
			uri = std::string("grpc://") + host + ":15432";
		}
	}
	ARROW_ASSIGN_OR_RAISE(auto location, arrow::flight::Location::Parse(uri));
	arrow::flight::FlightClientOptions client_options;
	auto sslrootcert = getenv("PGSSLROOTCERT");
	if (sslrootcert.empty())
	{
		auto home = getenv("HOME");
		if (!home.empty())
		{
			sslrootcert = home + "/.postgresql/root.crt";
		}
	}
	if (!sslrootcert.empty())
	{
		std::ifstream input(sslrootcert);
		if (input)
		{
			client_options.tls_root_certs =
				std::string(std::istreambuf_iterator<char>{input}, {});
		}
	}
	ARROW_ASSIGN_OR_RAISE(auto client,
	                      arrow::flight::FlightClient::Connect(location, client_options));
	auto user = getenv("PGUSER");
	if (user.empty())
	{
		user = getenv("USER");
	}
	auto password = getenv("PGPASSWORD");
	if (password.empty())
	{
		password = "";
	}
	auto database = getenv("PGDATABASE");
	if (database.empty())
	{
		database = user;
	}
	call_options.headers.emplace_back("x-flight-sql-database", database);
	ARROW_ASSIGN_OR_RAISE(auto bearer_token,
	                      client->AuthenticateBasicToken(call_options, user, password));
	const auto& bearer_name = bearer_token.first;
	const auto& bearer_value = bearer_token.second;
	if (!bearer_name.empty() && !bearer_value.empty())
	{
		call_options.headers.emplace_back(bearer_name, bearer_value);
	}
	return std::make_unique<arrow::flight::sql::FlightSqlClient>(std::move(client));
}

// Start query
arrow::Status
run()
{
	arrow::flight::FlightCallOptions call_options;
	ARROW_ASSIGN_OR_RAISE(auto sql_client, connect(call_options));
	ARROW_ASSIGN_OR_RAISE(auto statement,
	                      sql_client->Prepare(call_options,
	                                          "SELECT i "
	                                          "  FROM generate_series(1, 100) "
	                                          "       AS series (i) "
	                                          "  WHERE i < $1"));
	auto schema = arrow::schema({arrow::field("i", arrow::int32())});
	ARROW_ASSIGN_OR_RAISE(
		auto record_batch_builder,
		arrow::RecordBatchBuilder::Make(schema, arrow::default_memory_pool()));
	auto i_builder = record_batch_builder->GetFieldAs<arrow::Int32Builder>(0);
	ARROW_RETURN_NOT_OK(i_builder->Append(10));
	ARROW_ASSIGN_OR_RAISE(auto record_batch, record_batch_builder->Flush());
	ARROW_RETURN_NOT_OK(statement->SetParameters(record_batch));
	ARROW_ASSIGN_OR_RAISE(auto info, statement->Execute(call_options));
	for (const auto& endpoint : info->endpoints())
	{
		ARROW_ASSIGN_OR_RAISE(auto reader,
		                      sql_client->DoGet(call_options, endpoint.ticket));
		while (true)
		{
			ARROW_ASSIGN_OR_RAISE(auto chunk, reader->Next());
			if (!chunk.data)
			{
				break;
			}
			std::cout << chunk.data->ToString() << std::endl;
		}
	}
	ARROW_RETURN_NOT_OK(statement->Close(call_options));
	return sql_client->Close();
}
// End query
};  // namespace

int
main(int argc, char** argv)
{
	auto status = run();
	if (status.ok())
	{
		return EXIT_SUCCESS;
	}
	else
	{
		std::cerr << status.ToString() << std::endl;
		return EXIT_FAILURE;
	}
}
