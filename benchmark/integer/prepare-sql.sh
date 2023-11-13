#!/bin/bash
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

set -eu

size=100000
db_name=afs_benchmark
if [ $# -gt 0 ]; then
  size="${1}"
fi
if [ $# -gt 1 ]; then
  db_name="${2}"
fi

cat <<SQL
DROP DATABASE IF EXISTS ${db_name};
CREATE DATABASE ${db_name};
\\c ${db_name}

DROP TABLE IF EXISTS data;
CREATE TABLE data (int32 integer);
INSERT INTO data
  SELECT random() * 10000
    FROM generate_series(1, ${size});
SQL
