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

# Benchmark

## How to run

Install Apache Arrow Flight SQL PostgreSQL adapter.

Run PostgreSQL with the following configuration:

```text
shared_preload_libraries = 'arrow_flight_sql'
```

Run the benchmark script:

```bash
benchmark/run.sh
```

It runs all benchmarks and outputs their results to
`benchmark/${BENCHMARK}/result.csv`.

You can visualize them:

```bash
benchmark/graph.rb
```

It generates `benchmark/${BENCHMARK}/result.svg`.

You can format them as Markdown:

```bash
benchmark/markdown.rb
```

It replaces benchmark results in `benchmark/${BENCHMARK}/README.md`.

### Details

Each sub directory have a shell script that outputs preparation
SQL. For example, you can use `benchmark/integer/prepare-sql.sh` for
integer benchmark:

```bash
benchmark/integer/prepare-sql.sh 1000000 afs_benchmark | psql -d postgres
```

It creates `afs_benchmark` database and `data` table in the database.
It also inserts 1000000 (1M) records with random integers to the
table.

You can use the following programs to measure each approach:

- `select.rb`: It uses Apache Arrow Flight SQL
- `select-adbc-flight-sql.rb`: It uses Apache Arrow Flight SQL via ADBC
- `select-adbc-postgresql.rb`: It uses PostgreSQL protocol via ADBC
- `select-pandas.py`: It uses pandas
- `select-polars.py`: It uses Polars
- `select`: It uses PostgreSQL's C API
- `select.sql`: You need to use `psql` to run this
- `copy`: It uses `COPY` and PostgreSQL's C API

All of them just run `SELECT * FROM data`.

## Results

- [integer](integer/README.md)
- [string](string/README.md)
