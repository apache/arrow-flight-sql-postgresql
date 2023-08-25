<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Client

## How to connect

You must specify PostgreSQL user and PostgreSQL database to connect
Apache Arrow Flight SQL endpoint.

User name and password must be passed by `Handshake` call. Note that
basic authentication is only supported for now. mTLS (mutual-TLS)
isn't implemented yet. If you're interested in mTLS, please see the
issue for it:
https://github.com/apache/arrow-flight-sql-postgresql/issues/79

You need to use a header to specify PostgreSQL database. The header
name for PostgreSQL database is `x-flight-sql-database`.

````{tab} Apache Arrow Flight SQL C++ client
You need to use
[`arrow::flight::FlightClient::AuthenticateBasicToken()`][arrow-flight-authenticate-basic]
for authentication.

You need to add a `x-flight-sql-database` header to
[`arrow::flight::FlightCallOptions::headers`][arrow-flight-call-options-headers]
for database.

The following example uses `PGUSER` (fallback to `USER`) and
`PGPASSWORD` environment variables like
`libpq`. `AuthenticateBasicToken()` returns Bearer token on
success. So the example users the returned Bearer token to request
headers to use following requests.

The following example uses `PGDATABASE` (fallback to `PGUSER`/`USER`)
environment variable like `libpq`.

```{literalinclude} ../../example/flight-sql/authenticate-password.cc
:language: c++
:linenos:
:tab-width: 4
:start-after: // Start authentication
:end-before: // End authentication
```
````

````{tab} ADBC Flight SQL driver
You need to set the
`username` and `password` options for authentication. See also the
[Authentication][arrow-adbc-authentication] document.

You need to set the
`adbc.flight.sql.rpc.call_header.x-flight-sql-database` option for
database. See also the [Custom Call
Headers][arrow-adbc-custom-call-headers] document.

```{literalinclude} ../../example/adbc/authenticate-password.c
:language: c
:linenos:
:tab-width: 4
:start-after: // Start authentication
:end-before: // End authentication
```
````

## How to query

You can use an ad-hoc SQL statement or a prepared SQL statement to
query.

### Ad-hoc SQL statement

````{tab} Apache Arrow Flight SQL C++ client

You need to use
[`arrow::flight::sql::FlightSqlClient::Execute()`][arrow-flight-sql-client-execute]
to execute a query.

You need to use
[`arrow::flight::sql::FlightSqlClient::DoGet()`][arrow-flight-sql-client-do-get]
to get results.

```{literalinclude} ../../example/flight-sql/query-ad-hoc.cc
:language: c++
:linenos:
:tab-width: 4
:start-after: // Start query
:end-before: // End query
```
````

```{tab} ADBC Flight SQL driver
TODO
```

### Prepared SQL statement

````{tab} Apache Arrow Flight SQL C++ client
You need to use
[`arrow::flight::sql::FlightSqlClient::Prepare()`][arrow-flight-sql-client-prepare]
to prepare a query.

You need to use
[`arrow::flight::sql::PreparedStatement::SetParameters()`][arrow-flight-sql-prepared-statement-set-parameters]
to set parameters.

You need to use
[`arrow::flight::sql::PreparedStatement::Execute()`][arrow-flight-sql-prepared-statement-execute]
to execute a prepared statement with parameters.

You need to use
[`arrow::flight::sql::FlightSqlClient::DoGet()`][arrow-flight-sql-client-do-get]
to get results.

```{literalinclude} ../../example/flight-sql/query-prepared.cc
:language: c++
:linenos:
:tab-width: 4
:start-after: // Start query
:end-before: // End query
```
````

```{tab} ADBC Flight SQL driver
TODO
```

[arrow-adbc-authentication]: https://arrow.apache.org/adbc/current/driver/flight_sql.html#authentication
[arrow-adbc-custom-call-headers]: https://arrow.apache.org/adbc/current/driver/flight_sql.html#custom-call-headers
[arrow-flight-authenticate-basic]: https://arrow.apache.org/docs/cpp/api/flight.html#clients
[arrow-flight-call-options-headers]: https://arrow.apache.org/docs/cpp/api/flight.html#_CPPv4N5arrow6flight17FlightCallOptions7headersE
[arrow-flight-sql-client-do-get]: https://arrow.apache.org/docs/cpp/api/flightsql.html#_CPPv4N5arrow6flight3sql15FlightSqlClient5DoGetERK17FlightCallOptionsRK6Ticket
[arrow-flight-sql-client-execute]: https://arrow.apache.org/docs/cpp/api/flightsql.html#_CPPv4N5arrow6flight3sql15FlightSqlClient7ExecuteERK17FlightCallOptionsRKNSt6stringERK11Transaction
[arrow-flight-sql-client-prepare]: https://arrow.apache.org/docs/cpp/api/flightsql.html#_CPPv4N5arrow6flight3sql15FlightSqlClient7PrepareERK17FlightCallOptionsRKNSt6stringERK11Transaction
[arrow-flight-sql-prepared-statement-execute]: https://arrow.apache.org/docs/cpp/api/flightsql.html#_CPPv4N5arrow6flight3sql17PreparedStatement7ExecuteERK17FlightCallOptions
[arrow-flight-sql-prepared-statement-set-parameters]: https://arrow.apache.org/docs/cpp/api/flightsql.html#_CPPv4N5arrow6flight3sql17PreparedStatement13SetParametersENSt10shared_ptrI11RecordBatchEE
