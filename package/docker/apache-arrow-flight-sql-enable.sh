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

set -eux

psql=(psql \
        --no-align \
        --no-psqlrc \
        --tuples-only)
select_shared_preload_libraries=<<SQL
SELECT setting
  FROM pg_file_settings
  WHERE
    name ='shared_preload_libraries' AND
    applied
SQL
shared_preload_libraries=$("${psql[@]}" --command "${select_shared_preload_libraries}")
if [ -n "${shared_preload_libraries}" ]; then
  shared_preload_libraries+=","
fi
shared_preload_libraries+="arrow_flight_sql"
echo "shared_preload_libraries = '${shared_preload_libraries}'" | \
  tee -a "${PGDATA}/postgresql.conf"

ssl=$(psql \
        --no-psqlrc \
        --tuples-only \
        --no-align \
        --command "SELECT setting FROM pg_settings WHERE name ='ssl'")
if [ "${ssl}" = "on" ]; then
  uri="grpc+tls://0.0.0.0:15432"
else
  uri="grpc://0.0.0.0:15432"
fi
echo "arrow_flight_sql.uri = '${uri}'" | \
  tee -a "${PGDATA}/postgresql.conf"
