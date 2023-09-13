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

# Overview

## What is this?

Apache Arrow Flight SQL adapter for PostgreSQL is a PostgreSQL
extension that adds an [Apache Arrow Flight
SQL][apache-arrow-flight-sql] endpoint to PostgreSQL.

Apache Arrow Flight SQL is a protocol to use [Apache Arrow
format][apache-arrow-format] to interact with SQL databases. You can
use Apache Arrow Flight SQL instead of [the PostgreSQL wire
protocol][postgresql-protocol] to interact with PostgreSQL by Apache
Arrow Flight SQL adapter for PostgreSQL.

Apache Arrow format is designed for fast typed table data exchange. If
you want to get large data by `SELECT` or `INSERT`/`UPDATE` large
data, Apache Arrow Flight SQL will be faster than the PostgreSQL wire
protocol.

## Benchmark

See also [a simple benchmark result][benchmark-integer] that just
executes `SELECT * FROM integer_only_table`. It shows that Apache
Arrow Flight SQL is faster than the PostgreSQL wire protocol when
result data is large.

[apache-arrow-flight-sql]: https://arrow.apache.org/docs/format/FlightSql.html
[apache-arrow-format]: https://arrow.apache.org/docs/format/Columnar.html
[postgresql-protocol]: https://www.postgresql.org/docs/current/protocol.html
[benchmark-integer]: https://github.com/apache/arrow-flight-sql-postgresql/tree/main/benchmark/integer
