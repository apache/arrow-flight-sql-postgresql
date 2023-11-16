/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <vector>

#include <libpq-fe.h>

class ConnectionFinisher {
   public:
	ConnectionFinisher(PGconn* connection) : connection_(connection) {}
	~ConnectionFinisher() { PQfinish(connection_); }

   private:
	PGconn* connection_;
};

class ResultClearner {
   public:
	ResultClearner(PGresult* result) : result_(result) {}
	~ResultClearner() { PQclear(result_); }

   private:
	PGresult* result_;
};

int
main(int argc, char** argv)
{
	std::string connectionString;
	if (!std::getenv("PGDATABASE"))
	{
		connectionString += "dbname=afs_benchmark";
	}
	auto connection = PQconnectdb(connectionString.c_str());
	ConnectionFinisher connectionFinisher(connection);
	if (PQstatus(connection) != CONNECTION_OK)
	{
		std::cerr << "failed to connect: " << PQerrorMessage(connection) << std::endl;
		return EXIT_FAILURE;
	}

	{
		auto result = PQexec(connection, "DROP TABLE IF EXISTS data_insert");
		ResultClearner resultClearner(result);
		if (PQresultStatus(result) != PGRES_COMMAND_OK)
		{
			std::cerr << "failed to drop: " << PQerrorMessage(connection) << std::endl;
			return EXIT_FAILURE;
		}
	}

	{
		auto result = PQexec(connection, "CREATE TABLE data_insert (LIKE data)");
		ResultClearner resultClearner(result);
		if (PQresultStatus(result) != PGRES_COMMAND_OK)
		{
			std::cerr << "failed to create: " << PQerrorMessage(connection) << std::endl;
			return EXIT_FAILURE;
		}
	}

	std::vector<std::string> buffers;
	{
		auto result = PQexec(connection, "COPY data TO STDOUT (FORMAT binary)");
		ResultClearner resultClearner(result);
		if (PQresultStatus(result) != PGRES_COPY_OUT)
		{
			std::cerr << "failed to copy to: " << PQerrorMessage(connection) << std::endl;
			return EXIT_FAILURE;
		}
		while (true)
		{
			char* data;
			auto size = PQgetCopyData(connection, &data, 0);
			if (size == -1)
			{
				break;
			}
			if (size == -2)
			{
				std::cerr << "failed to read copy data: " << PQerrorMessage(connection)
						  << std::endl;
				return EXIT_FAILURE;
			}
			buffers.emplace_back(data, size);
			free(data);
		}
	}
	auto before = std::chrono::steady_clock::now();
	{
		auto result = PQexec(connection, "COPY data_insert FROM STDOUT (FORMAT binary)");
		ResultClearner resultClearner(result);
		if (PQresultStatus(result) != PGRES_COPY_IN)
		{
			std::cerr << "failed to copy from: " << PQerrorMessage(connection)
					  << std::endl;
			return EXIT_FAILURE;
		}
		for (const auto& buffer : buffers)
		{
			auto copyDataResult = PQputCopyData(connection, buffer.data(), buffer.size());
			if (copyDataResult == -1)
			{
				std::cerr << "failed to put copy data: " << PQerrorMessage(connection)
						  << std::endl;
				return EXIT_FAILURE;
			}
		}
		auto copyEndResult = PQputCopyEnd(connection, nullptr);
		if (copyEndResult == -1)
		{
			std::cerr << "failed to end copy data: " << PQerrorMessage(connection)
					  << std::endl;
			return EXIT_FAILURE;
		}
	}
	{
		auto result = PQgetResult(connection);
		ResultClearner resultClearner(result);
		if (PQresultStatus(result) != PGRES_COMMAND_OK)
		{
			std::cerr << "failed to copy from: " << PQerrorMessage(connection)
					  << std::endl;
			return EXIT_FAILURE;
		}
	}
	auto elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(
						   std::chrono::steady_clock::now() - before)
	                       .count();
	printf("%.3f\n", elapsedTime / 1000.0);

	return EXIT_SUCCESS;
}
