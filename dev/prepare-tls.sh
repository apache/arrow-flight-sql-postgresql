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

if [ $# -ne 3 ]; then
  echo "Usage: $0 CA_NAME SERVER_NAME CLIENT_NAME"
  echo " e.g.: $0 root.example.com server.example.com client.example.com"
  exit 1
fi

root_name=$1
server_name=$2
client_name=$3

openssl req \
        -new \
        -nodes \
        -text \
        -out root.csr \
        -keyout root.key \
        -subj "/CN=${root_name}"
chmod go-rwx root.key

openssl x509 \
        -req \
        -in root.csr \
        -text \
        -days 3650 \
        -extfile /etc/ssl/openssl.cnf \
        -extensions v3_ca \
        -signkey root.key \
        -out root.crt

openssl req \
        -new \
        -nodes \
        -text \
        -out server.csr \
        -keyout server.key \
        -subj "/CN=${server_name}"
chmod og-rwx server.key

openssl x509 \
        -req \
        -in server.csr \
        -text \
        -days 365 \
        -CA root.crt \
        -CAkey root.key \
        -CAcreateserial \
        -out server.crt

openssl req \
        -new \
        -nodes \
        -text \
        -out client.csr \
        -keyout client.key \
        -subj "/CN=${client_name}"
chmod og-rwx client.key

openssl x509 \
        -req \
        -in client.csr \
        -text \
        -days 365 \
        -CA root.crt \
        -CAkey root.key \
        -CAcreateserial \
        -out client.crt
