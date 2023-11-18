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

#include "insert.hh"

#include <arpa/inet.h>

namespace {
template <typename Type>
void
write_binary_data(std::ostream& stream, Type value)
{
	stream.write(reinterpret_cast<const char*>(&value), sizeof(Type));
}
};  // namespace

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

	std::vector<std::vector<Value>> records;
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
			std::vector<Value> values;
			for (int iField = 0; iField < nFields; iField++)
			{
				if (!append_value(values, result, iTuple, iField))
				{
					return EXIT_FAILURE;
				}
			}
			records.push_back(std::move(values));
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
		std::ostringstream copyDataStream;
		{
			// See the "Binary Format" section in
			// https://www.postgresql.org/docs/current/sql-copy.html for
			// details.

			const char signature[] = "PGCOPY\n\377\r\n";
			// The last '\0' is also part of the signature.
			copyDataStream << std::string_view(signature, sizeof(signature));
			const uint32_t flags = 0;
			write_binary_data(copyDataStream, htonl(flags));
			const uint32_t headerExtensionAreaLength = 0;
			write_binary_data(copyDataStream, htonl(headerExtensionAreaLength));
			auto nRecords = records.size();
			for (size_t iRecord = 0; iRecord < nRecords; ++iRecord)
			{
				const auto& values = records[iRecord];
				const auto nValues = values.size();
				write_binary_data(copyDataStream, htons(nValues));
				for (size_t iValue = 0; iValue < nValues; ++iValue)
				{
					const auto& value = values[iValue];
					if (std::holds_alternative<std::monostate>(value))
					{
						write_binary_data(copyDataStream,
						                  htonl(static_cast<uint32_t>(-1)));
					}
					else if (std::holds_alternative<int32_t>(value))
					{
						const auto& int32Value = std::get<int32_t>(value);
						write_binary_data(copyDataStream, htonl(sizeof(int32_t)));
						write_binary_data(copyDataStream,
						                  htonl(static_cast<uint32_t>(int32Value)));
					}
					else if (std::holds_alternative<std::string>(value))
					{
						const auto& stringValue = std::get<std::string>(value);
						write_binary_data(copyDataStream, htonl(stringValue.size()));
						copyDataStream << stringValue;
					}
				}
			}
			const uint16_t fileTrailer = -1;
			write_binary_data(copyDataStream, htons(fileTrailer));
		}
		const auto& copyData = copyDataStream.str();
		auto copyDataResult = PQputCopyData(connection, copyData.data(), copyData.size());
		if (copyDataResult == -1)
		{
			std::cerr << "failed to put copy data: " << PQerrorMessage(connection)
					  << std::endl;
			return EXIT_FAILURE;
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
