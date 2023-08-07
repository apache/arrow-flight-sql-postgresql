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

#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>

#include <libpq-fe.h>

int
main(int argc, char** argv)
{
	PGconn* connection = PQconnectdb("dbname=afs_benchmark");
	PGresult* result;
	struct timeval before;
	struct timeval after;
	int nFields;
	int iField;
	int nTuples;
	int iTuple;

	if (PQstatus(connection) != CONNECTION_OK)
	{
		fprintf(stderr, "failed to connect: %s", PQerrorMessage(connection));
		PQfinish(connection);
		return EXIT_FAILURE;
	}

	gettimeofday(&before, NULL);
	result = PQexec(connection, "SELECT * FROM data");
	if (PQresultStatus(result) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "failed to select: %s", PQerrorMessage(connection));
		PQclear(result);
		PQfinish(connection);
		return EXIT_FAILURE;
	}

	nTuples = PQntuples(result);
	nFields = PQnfields(result);
	for (iTuple = 0; iTuple < nTuples; iTuple++)
	{
		for (iField = 0; iField < nFields; iField++)
		{
			PQgetvalue(result, iTuple, iField);
		}
	}
	gettimeofday(&after, NULL);
	printf("%.3fsec\n",
	       (after.tv_sec + (after.tv_usec / 1000000.0)) -
	           (before.tv_sec + (before.tv_usec / 1000000.0)));
	PQclear(result);

	return EXIT_SUCCESS;
}
