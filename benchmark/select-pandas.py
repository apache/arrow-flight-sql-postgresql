#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import time

import pandas
import sqlalchemy

database = os.environ.get("PGDATABASE", "afs_benchmark")
engine = sqlalchemy.create_engine("postgresql:///{database}")
with engine.connect() as connection, connection.begin():
    start = time.perf_counter()
    pandas.read_sql_table("data", connection)
    print(time.perf_counter() - start)
