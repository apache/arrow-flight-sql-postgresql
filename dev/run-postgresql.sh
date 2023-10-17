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

if [ $# -ne 1 -a $# -ne 4 ]; then
  echo "Usage: $0 DATA_DIRECTORY"
  echo " e.g.: $0 /tmp/afs"
  echo "Or:"
  echo "Usage: $0 DATA_DIRECTORY CA_NAME SERVER_NAME CLIENT_NAME"
  echo " e.g.: $0 /tmp/afs root.example.com server.example.com client.example.com"
  exit 1
fi

data_directory=$1
if [ $# -eq 1 ]; then
  scheme=grpc
  ssl=off
  ssl_ca_file=
  server_name=127.0.0.1
else
  scheme=grpc+tls
  ssl=on
  ssl_ca_file=root.crt
  root_name=$2
  server_name=$3
  client_name=$4
fi

base_directory="$(cd "$(dirname "$0")" && pwd)"

rm -rf "${data_directory}"

if LANG=C initdb --help | grep -q -- --set; then
  initdb \
    --locale=C \
    --set=arrow_flight_sql.uri=${scheme}://${server_name}:15432 \
    --set=shared_preload_libraries=arrow_flight_sql \
    --set=ssl=${ssl} \
    --set=ssl_ca_file=${ssl_ca_file} \
    "${data_directory}"
else
  initdb \
    --locale=C \
    "${data_directory}"
  (echo "arrow_flight_sql.uri = '${scheme}://${server_name}:15432'"; \
   echo "shared_preload_libraries = 'arrow_flight_sql'"; \
   echo "ssl = ${ssl}"; \
   echo "ssl_ca_file = '${ssl_ca_file}'") | \
    tee -a "${data_directory}/postgresql.conf"
fi
if [ "${ssl}" = "on" ]; then
  pushd "${data_directory}"
  "${base_directory}/prepare-tls.sh" \
    "${root_name}" \
    "${server_name}" \
    "${client_name}"
  popd
fi
LANG=C postgres -D "${data_directory}"
