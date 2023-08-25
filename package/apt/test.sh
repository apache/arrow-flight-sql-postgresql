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

relative_repositories_dir=$1

echo "::group::Prepare APT repositories"

echo "debconf debconf/frontend select Noninteractive" | \
  debconf-set-selections

apt update
apt install -V -y \
    lsb-release \
    wget

os=$(lsb_release --id --short | tr "A-Z" "a-z")
code_name=$(lsb_release --codename --short)
architecture=$(dpkg --print-architecture)

repositories_dir=/host/${relative_repositories_dir}
wget https://apache.jfrog.io/artifactory/arrow/${os}/apache-arrow-apt-source-latest-${code_name}.deb
apt install -V -y ./apache-arrow-apt-source-latest-${code_name}.deb

echo "deb http://apt.postgresql.org/pub/repos/apt/ ${code_name}-pgdg main" | \
  tee /etc/apt/sources.list.d/pgdg.list
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | \
  apt-key add -

echo "::endgroup::"


echo "::group::Install built packages"

apt update

apt install -V -y \
  ${repositories_dir}/${os}/pool/${code_name}/*/*/*/*_${architecture}.deb

echo "::endgroup::"


echo "::group::Install packages for test"

apt install -V -y \
    g++ \
    libarrow-flight-sql-glib-dev \
    make \
    ruby-dev \
    sudo
MAKELAGS="-j$(nproc)" \
        gem install \
        red-arrow-flight-sql \
        test-unit

echo "::endgroup::"


echo "::group::Run test"

sudo -u postgres -H \
     env PATH=$(pg_config --bindir):$PATH /host/test/run.rb

echo "::endgroup::"
