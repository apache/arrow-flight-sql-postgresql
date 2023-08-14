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

if [ $# -ne 4 ]; then
  echo "Usage: $0 DATA_DIRECTORY CA_NAME SERVER_NAME CLIENT_NAME"
  echo " e.g.: $0 /tmp/afs root.example.com server.example.com client.example.com"
  exit 1
fi

data_directory=$1
root_name=$2
server_name=$3
client_name=$4

base_directory="$(cd "$(dirname "$0")" && pwd)"

rm -rf "${data_directory}"

initdb \
  --locale=C \
  --set=arrow_flight_sql.uri=grpc+tls://${server_name}:15432 \
  --set=shared_preload_libraries=arrow_flight_sql \
  --set=ssl=on \
  --set=ssl_ca_file=root.crt \
  "${data_directory}"
pushd "${data_directory}"
"${base_directory}/prepare-tls.sh" \
  "${root_name}" \
  "${server_name}" \
  "${client_name}"
popd
LANG=C postgres -D "${data_directory}"
