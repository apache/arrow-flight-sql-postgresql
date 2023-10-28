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

#include <stdbool.h>
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

	if (PQstatus(connection) != CONNECTION_OK)
	{
		fprintf(stderr, "failed to connect: %s\n", PQerrorMessage(connection));
		PQfinish(connection);
		return EXIT_FAILURE;
	}

	gettimeofday(&before, NULL);
	result = PQexec(connection, "COPY data TO STDOUT (FORMAT binary)");
	if (PQresultStatus(result) != PGRES_COPY_OUT)
	{
		fprintf(stderr, "failed to copy: %s\n", PQerrorMessage(connection));
		PQclear(result);
		PQfinish(connection);
		return EXIT_FAILURE;
	}

	while (true)
	{
		char* buffer;
		int size = PQgetCopyData(connection, &buffer, 0);
		if (size == -1)
		{
			break;
		}
		if (size == -2)
		{
			fprintf(stderr, "failed to read copy data: %s\n", PQerrorMessage(connection));
			PQclear(result);
			PQfinish(connection);
			return EXIT_FAILURE;
		}
		/* printf("%.*s\n", size, buffer); */
		free(buffer);
	}
	gettimeofday(&after, NULL);
	printf("%.3fsec\n",
	       (after.tv_sec + (after.tv_usec / 1000000.0)) -
	           (before.tv_sec + (before.tv_usec / 1000000.0)));
	PQclear(result);

	return EXIT_SUCCESS;
}
