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
#include <cstdlib>
#include <fstream>
#include <iostream>

#include <arrow/flight/sql/client.h>

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

arrow::Status
run()
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
	arrow::flight::FlightCallOptions call_options;
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
	return client->Close();
}
};  // namespace

int
main(int argc, char** argv)
{
	auto status = run();
	if (status.ok())
	{
		std::cout << "Authenticated!" << std::endl;
		return EXIT_SUCCESS;
	}
	else
	{
		std::cerr << status.ToString() << std::endl;
		return EXIT_FAILURE;
	}
}
// End authentication
