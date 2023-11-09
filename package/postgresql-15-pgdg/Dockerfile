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

FROM postgres:15-bookworm

COPY package/docker/postgresql-15-pgdg-apache-arrow-flight-sql_*_amd64.deb ./
RUN \
  apt update && \
  apt install -y -V lsb-release wget && \
  wget https://apache.jfrog.io/artifactory/arrow/debian/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
  apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
  rm apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
  apt update && \
  apt install -y -V \
    ./postgresql-15-pgdg-apache-arrow-flight-sql_*.deb && \
  rm -f postgresql-15-pgdg-apache-arrow-flight-sql_*.deb && \
  apt clean && \
  rm -rf /var/lib/apt/lists/*

COPY package/docker/apache-arrow-flight-sql-enable.sh /docker-entrypoint-initdb.d/

EXPOSE 15432
