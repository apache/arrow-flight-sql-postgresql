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

# Configuration

## `shared_preload_libraries`

You must add `arrow_flight_sql` to
[`shared_preload_libraries`][postgresql-shared-preload-libraries].

You need to restart your PostgreSQL after changing
`shared_preload_libraries`.

## `arrow_flight_sql.uri`

The endpoint URI for Apache Arrow Flight SQL.

The default is `grpc://127.0.0.1:15432`.

If you want to use TLS to connect the Apache Arrow Flight SQL
endpoint, you need to do the followings:

1. Use `grpc+tls` schema such as `grpc+tls://127.0.0.1:15432` for
   `arrow_flight_sql.uri`
2. Prepare the standard PostgreSQL TLS configurations such as
   [`ssl`][postgresql-ssl],
   [`ssl_ca_file`][postgresql-ca-file]. Apache Arrow Flight SQL
   adapter for PostgreSQL uses the PostgreSQL's TLS configurations. So
   you don't need to prepare TLS related things only for Apache Arrow
   Flight SQL for PostgreSQL. See also the PostgreSQL's [Secure TCP/IP
   Connections with SSL][postgresql-ssl-tcp] documentation.

Note that you also need to setup client side. For example, see the
following documentations for the C++ implementation of Apache Arrow
Flight SQL client:

* [Enable TLS][arrow-flight-tls]

```{note}
mTLS (mutual-TLS) isn't implemented yet. If you're interested in mTLS,
please see the issue for it: https://github.com/apache/arrow-flight-sql-postgresql/issues/79
```

## `arrow_flight_sql.session_timeout`

The maximum session duration in seconds.

The default is 300 seconds.

-1 means no timeout.

If no query is executed during the timeout, the session is closed
automatically.

## `arrow_flight_sql.max_n_rows_per_record_batch`

The maximum number of rows per record batch.

The default is 1 * 1024 * 1024 rows.

If this value is small, total data exchange time will be slower.

If this value is larger, latency will be larger.

[arrow-flight-tls]: https://arrow.apache.org/docs/cpp/flight.html#enabling-tls
[postgresql-ca-file]: https://www.postgresql.org/docs/current/runtime-config-connection.html#GUC-SSL-CA-FILE
[postgresql-shared-preload-libraries]: https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-SHARED-PRELOAD-LIBRARIES
[postgresql-ssl-tcp]: https://www.postgresql.org/docs/current/ssl-tcp.html
[postgresql-ssl]: https://www.postgresql.org/docs/current/runtime-config-connection.html#GUC-SSL
