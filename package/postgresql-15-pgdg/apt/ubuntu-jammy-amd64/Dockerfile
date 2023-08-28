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

ARG FROM=ubuntu:22.04
FROM ${FROM}

RUN \
  echo "debconf debconf/frontend select Noninteractive" | \
    debconf-set-selections

RUN \
  echo 'APT::Install-Recommends "false";' > \
    /etc/apt/apt.conf.d/disable-install-recommends

ARG DEBUG
RUN \
  quiet=$([ "${DEBUG}" = "yes" ] || echo "-qq") && \
  apt update ${quiet} && \
  apt install -y -V ${quiet} \
    ca-certificates \
    lsb-release \
    wget && \
  distribution=$(lsb_release --id --short | tr 'A-Z' 'a-z') && \
  code_name=$(lsb_release --codename --short) && \
  wget https://apache.jfrog.io/artifactory/arrow/${distribution}/apache-arrow-apt-source-latest-${code_name}.deb && \
  apt install -y -V ${quiet} \
    ./apache-arrow-apt-source-latest-${code_name}.deb && \
  rm apache-arrow-apt-source-latest-${code_name}.deb && \
  echo "deb http://apt.postgresql.org/pub/repos/apt/ ${code_name}-pgdg main" > \
    /etc/apt/sources.list.d/pgdg.list && \
  wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | \
    apt-key add - && \
  apt update && \
  apt install -y -V ${quiet} \
    build-essential \
    ccache \
    cmake \
    debhelper \
    devscripts \
    libarrow-flight-sql-dev \
    libkrb5-dev \
    meson \
    ninja-build \
    postgresql-server-dev-15 && \
  apt clean
