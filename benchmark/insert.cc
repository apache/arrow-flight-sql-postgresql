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

#include <libpq-fe.h>

#include <catalog/pg_type_d.h>

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

	std::string insert = "INSERT INTO data_insert VALUES ";
	{
		auto result = PQexec(connection, "SELECT * FROM data");
		ResultClearner resultClearner(result);
		if (PQresultStatus(result) != PGRES_TUPLES_OK)
		{
			std::cerr << "failed to select: " << PQerrorMessage(connection) << std::endl;
			return EXIT_FAILURE;
		}
		auto nTuples = PQntuples(result);
		auto nFields = PQnfields(result);
		for (int iTuple = 0; iTuple < nTuples; iTuple++)
		{
			if (iTuple > 0)
			{
				insert += ", ";
			}
			for (int iField = 0; iField < nFields; iField++)
			{
				if (PQgetisnull(result, iTuple, iField))
				{
					insert += "(null)";
				}
				else
				{
					insert += "(";
					auto type = PQftype(result, iField);
					if (type == TEXTOID)
					{
						insert += "'";
					}
					insert += PQgetvalue(result, iTuple, iField);
					if (type == TEXTOID)
					{
						insert += "'";
					}
					insert += ")";
				}
			}
		}
	}
	auto before = std::chrono::steady_clock::now();
	{
		auto result = PQexec(connection, insert.c_str());
		ResultClearner resultClearner(result);
		if (PQresultStatus(result) != PGRES_COMMAND_OK)
		{
			std::cerr << "failed to insert: " << PQerrorMessage(connection) << std::endl;
			return EXIT_FAILURE;
		}
	}
	auto elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(
						   std::chrono::steady_clock::now() - before)
	                       .count();
	printf("%.3f\n", elapsedTime / 1000.0);

	return EXIT_SUCCESS;
}
