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

postgresql_version=$1

sudo apt update
sudo apt install -y -V \
  ca-certificates \
  gpg \
  lsb-release \
  wget
os=$(lsb_release --id --short | tr 'A-Z' 'a-z')
code_name=$(lsb_release --codename --short)

apt_source_deb=apache-arrow-apt-source-latest-${code_name}.deb
wget https://apache.jfrog.io/artifactory/arrow/${os}/${apt_source_deb}
sudo apt install -y -V ./${apt_source_deb}

wget -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | \
  gpg --no-default-keyring --keyring ./pgdg.kbx --import -
gpg --no-default-keyring --keyring ./pgdg.kbx --export --armor | \
  tee /usr/share/keyrings/pgdg.asc
(echo "Types: deb"; \
 echo "URIs: http://apt.postgresql.org/pub/repos/apt"; \
 echo "Suites: ${code_name}-pgdg"; \
 echo "Components: main"; \
 echo "Signed-By: /usr/share/keyrings/pgdg.asc") | \
  sudo tee /etc/apt/sources.list.d/pgdg.sources

sudo apt update
sudo apt -y -V purge '^postgresql'
sudo apt install -y -V \
  libarrow-flight-sql-glib-dev \
  libkrb5-dev \
  meson \
  ninja-build \
  postgresql-${postgresql_version} \
  postgresql-server-dev-${postgresql_version}
