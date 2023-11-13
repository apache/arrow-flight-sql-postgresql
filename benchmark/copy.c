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

#include <arpa/inet.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#include <libpq-fe.h>

#include <catalog/pg_type_d.h>

/* See the "Binary Format" section in
 * https://www.postgresql.org/docs/current/sql-copy.html for
 * details. */
static const char signature[] = "PGCOPY\n\377\r\n";
/* The last '\0' is also part of the signature. */
static const size_t signatureSize = sizeof(signature);

typedef struct {
	char* data;
	size_t size;
} Buffer;

static bool
read_uint16(Buffer* buffer, uint16_t* output, const char* tag)
{
	if (buffer->size < sizeof(uint16_t))
	{
		fprintf(stderr,
		        "%s: can't read uint16_t (%d bytes) value: %d",
		        tag,
		        (int)sizeof(uint16_t),
		        (int)(buffer->size));
		return false;
	}

	*output = htons(*((uint16_t*)(buffer->data)));
	buffer->data += sizeof(uint16_t);
	buffer->size -= sizeof(uint16_t);
	return true;
}

static bool
read_uint32(Buffer* buffer, uint32_t* output, const char* tag)
{
	if (buffer->size < sizeof(uint32_t))
	{
		fprintf(stderr,
		        "%s: can't read uint32_t (%d bytes) value: %d",
		        tag,
		        (int)sizeof(uint32_t),
		        (int)(buffer->size));
		return false;
	}

	*output = htonl(*((uint32_t*)(buffer->data)));
	buffer->data += sizeof(uint32_t);
	buffer->size -= sizeof(uint32_t);
	return true;
}

static bool
parse_header(Buffer* buffer)
{
	uint32_t flags;
	uint32_t extensionLength;

	if (buffer->size < signatureSize)
	{
		fprintf(stderr,
		        "Signature (%d bytes) doesn't exist: %d",
		        (int)signatureSize,
		        (int)(buffer->size));
		return false;
	}
	if (memcmp(buffer->data, signature, signatureSize) != 0)
	{
		fprintf(stderr, "Wrong signature: <%.*s>", (int)signatureSize, buffer->data);
		return false;
	}
	buffer->data += signatureSize;
	buffer->size -= signatureSize;

	if (!read_uint32(buffer, &flags, "header: flags"))
	{
		return false;
	}

	if (!read_uint32(buffer, &extensionLength, "header: extension length"))
	{
		return false;
	}
	if (buffer->size < extensionLength)
	{
		fprintf(stderr,
		        "Too large header extension length: %d: %d",
		        (int)extensionLength,
		        (int)(buffer->size));
		return false;
	}
	buffer->data += extensionLength;
	buffer->size -= extensionLength;

	return true;
}

static bool
parse_tuples(Buffer* buffer, Oid* types, bool* finished)
{
	while (buffer->size > 0)
	{
		uint16_t i;
		uint16_t nFields;

		if (!read_uint16(buffer, &nFields, "tuple: number of fields"))
		{
			return false;
		}
		if (nFields == (uint16_t)-1)
		{
			*finished = true;
			return true;
		}

		for (i = 0; i < nFields; i++)
		{
			Oid type = types[i];
			uint32_t size;
			if (!read_uint32(buffer, &size, "tuple: field size"))
			{
				return false;
			}
			if (size == (uint32_t)-1)
			{
				/* NULL */
				continue;
			}

			switch (type)
			{
				case INT4OID:
				{
					uint32_t value;
					if (!read_uint32(buffer, &value, "tuple: field: integer"))
					{
						return false;
					}
					/* printf("%d\n", (int32_t)value); */
					break;
				}
				case TEXTOID:
				{
					/* printf("%.*s\n", (int)size, buffer->data); */
					buffer->data += size;
					buffer->size -= size;
					break;
				}
				default:
					fprintf(stderr,
					        "tuple: field: %u: Unsupported type: %u: %u: %d\n",
					        i,
					        type,
					        size,
					        (int)(buffer->size));
					return false;
			}
		}
	}

	return true;
}

int
main(int argc, char** argv)
{
	PGconn* connection;
	PGresult* result;
	Oid* types = NULL;
	struct timeval before;
	struct timeval after;
	bool inTuples = false;

	if (getenv("PGDATABASE"))
	{
		connection = PQconnectdb("");
	}
	else
	{
		connection = PQconnectdb("dbname=afs_benchmark");
	}
	if (PQstatus(connection) != CONNECTION_OK)
	{
		fprintf(stderr, "failed to connect: %s\n", PQerrorMessage(connection));
		PQfinish(connection);
		return EXIT_FAILURE;
	}

	gettimeofday(&before, NULL);
	result = PQprepare(connection, "", "SELECT * FROM data", 0, NULL);
	if (PQresultStatus(result) != PGRES_COMMAND_OK)
	{
		fprintf(stderr,
		        "failed to prepare to infer schema: %s\n",
		        PQerrorMessage(connection));
		PQclear(result);
		PQfinish(connection);
		return EXIT_FAILURE;
	}
	PQclear(result);
	result = PQdescribePrepared(connection, "");
	if (PQresultStatus(result) != PGRES_COMMAND_OK)
	{
		fprintf(stderr,
		        "failed to describe prepared statement to infer schema: %s\n",
		        PQerrorMessage(connection));
		PQclear(result);
		PQfinish(connection);
		return EXIT_FAILURE;
	}
	{
		int i;
		int nFields = PQnfields(result);

		types = malloc(sizeof(Oid) * PQnfields(result));
		for (i = 0; i < nFields; i++)
		{
			types[i] = PQftype(result, i);
		}
	}
	PQclear(result);

	result = PQexec(connection, "COPY data TO STDOUT (FORMAT binary)");
	if (PQresultStatus(result) != PGRES_COPY_OUT)
	{
		fprintf(stderr, "failed to copy: %s\n", PQerrorMessage(connection));
		free(types);
		PQclear(result);
		PQfinish(connection);
		return EXIT_FAILURE;
	}

	while (true)
	{
		char* data;
		bool finished = false;
		Buffer buffer;
		int size = PQgetCopyData(connection, &data, 0);
		if (size == -1)
		{
			break;
		}
		if (size == -2)
		{
			fprintf(stderr, "failed to read copy data: %s\n", PQerrorMessage(connection));
			free(types);
			PQclear(result);
			PQfinish(connection);
			return EXIT_FAILURE;
		}
		buffer.data = data;
		buffer.size = (size_t)size;
		if (!inTuples)
		{
			if (!parse_header(&buffer))
			{
				free(types);
				PQclear(result);
				PQfinish(connection);
				return EXIT_FAILURE;
			}
			inTuples = true;
		}
		if (!parse_tuples(&buffer, types, &finished))
		{
			free(types);
			PQclear(result);
			PQfinish(connection);
			return EXIT_FAILURE;
		}
		free(data);
		if (finished)
		{
			break;
		}
	}
	gettimeofday(&after, NULL);
	printf("%.3f\n",
	       (after.tv_sec + (after.tv_usec / 1000000.0)) -
	           (before.tv_sec + (before.tv_usec / 1000000.0)));
	free(types);
	PQclear(result);
	PQfinish(connection);

	return EXIT_SUCCESS;
}
