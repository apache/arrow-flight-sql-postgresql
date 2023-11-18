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

#pragma once

#include <charconv>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <variant>
#include <vector>

#include <libpq-fe.h>

#include <catalog/pg_type_d.h>

namespace {
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

using Value = std::variant<std::monostate, int32_t, std::string>;

bool
append_value(std::vector<Value>& values, PGresult* result, int iTuple, int iField)
{
	if (PQgetisnull(result, iTuple, iField))
	{
		values.push_back(std::monostate{});
		return true;
	}

	Oid type = PQftype(result, iField);
	char* rawValue = PQgetvalue(result, iTuple, iField);
	int length = PQgetlength(result, iTuple, iField);
	switch (type)
	{
		case INT4OID:
		{
			int32_t value;
			auto result = std::from_chars(rawValue, rawValue + length, value);
			if (result.ec != std::errc{})
			{
				std::cerr << "failed to parse integer value: " << rawValue << std::endl;
				return false;
			}
			values.emplace_back(value);
		}
		break;
		case TEXTOID:
		{
			values.emplace_back(std::string(rawValue, length));
		}
		break;
		default:
			std::cerr << "unsupported type: " << type << std::endl;
			return false;
	}
	return true;
}
};  // namespace
