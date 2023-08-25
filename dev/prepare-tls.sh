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
        -addext "subjectAltName = DNS:${root_name}" \
        -keyout root.key \
        -new \
        -nodes \
        -out root.csr \
        -subj "/CN=${root_name}" \
        -text
chmod go-rwx root.key

openssl x509 \
        -copy_extensions copy \
        -days 3650 \
        -extensions v3_ca \
        -extfile /etc/ssl/openssl.cnf \
        -in root.csr \
        -out root.crt \
        -req \
        -signkey root.key \
        -text

openssl req \
        -addext "subjectAltName = DNS:${server_name}" \
        -keyout server.key \
        -new \
        -nodes \
        -out server.csr \
        -subj "/CN=${server_name}" \
        -text
chmod og-rwx server.key

openssl x509 \
        -CA root.crt \
        -CAcreateserial \
        -CAkey root.key \
        -copy_extensions copy \
        -days 365 \
        -in server.csr \
        -out server.crt \
        -req \
        -text

openssl req \
        -addext "subjectAltName = DNS:${client_name}" \
        -keyout client.key \
        -new \
        -nodes \
        -out client.csr \
        -subj "/CN=${client_name}" \
        -text
chmod og-rwx client.key

openssl x509 \
        -CA root.crt \
        -CAcreateserial \
        -CAkey root.key \
        -copy_extensions copy \
        -days 365 \
        -in client.csr \
        -out client.crt \
        -req \
        -text
