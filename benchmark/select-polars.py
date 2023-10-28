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

import os
import time

import polars

user = os.environ.get("PGUSER", os.environ["USER"])
password = os.environ.get("PGPASSWORD", "")
host = os.environ.get("PGHOST", "localhost")
port = os.environ.get("PGPORT", "5432")
database = os.environ.get("PGDATABASE", "afs_benchmark")
uri = f"postgres://{user}:{password}@{host}:{port}/{database}"
start = time.perf_counter()
polars.read_database_uri(query="SELECT * FROM data",
                         uri=uri)
print(time.perf_counter() - start)

