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

if [ $# -lt 2 ]; then
  echo "Usage: $0 VERSION rc"
  echo "       $0 VERSION staging-rc"
  echo "       $0 VERSION release"
  echo "       $0 VERSION staging-release"
  echo "       $0 VERSION local RELATIVE_REPOSITORIES_DIR"
  echo " e.g.: $0 1.0.0 rc                # Verify 1.0.0 RC"
  echo " e.g.: $0 1.0.0 staging-rc        # Verify 1.0.0 RC on staging"
  echo " e.g.: $0 1.0.0 release           # Verify 1.0.0"
  echo " e.g.: $0 1.0.0 staging-release   # Verify 1.0.0 on staging"
  echo " e.g.: $0 1.0.0 local package/postgresql-15-pgdg/apt/repositories"
  echo "       # Verify 1.0.0 on local"
  exit 1
fi

VERSION="$1"
TYPE="$2"
if [ "${TYPE}" = "local" ]; then
  RELATIVE_REPOSITORIES_DIR="$3"
  REPOSITORIES_DIR=/host/${RELATIVE_REPOSITORIES_DIR}
fi

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOP_SOURCE_DIR="${SOURCE_DIR}/../.."

echo "::group::Prepare APT repositories"

echo "debconf debconf/frontend select Noninteractive" | \
  debconf-set-selections

retry() {
  local n_tries=2
  while [ ${n_tries} -gt 0 ]; do
    if "$@"; then
      return
    fi
    n_tries=$((${n_tries} - 1))
  done
  "$@"
}

APT_UPDATE="retry apt update --error-on=any"
APT_INSTALL="retry apt install -y -V --no-install-recommends"

${APT_UPDATE}
${APT_INSTALL} \
  ca-certificates \
  gpg \
  lsb-release \
  wget

distribution=$(lsb_release --id --short | tr "A-Z" "a-z")
code_name=$(lsb_release --codename --short)
architecture=$(dpkg --print-architecture)

artifactory_base_url="https://apache.jfrog.io/artifactory/arrow/${distribution}"
case "${TYPE}" in
  rc|staging-rc|staging-release)
    suffix=${TYPE%-release}
    artifactory_base_url+="-${suffix}"
    ;;
esac
wget ${artifactory_base_url}/apache-arrow-apt-source-latest-${code_name}.deb
${APT_INSTALL} ./apache-arrow-apt-source-latest-${code_name}.deb

case "${TYPE}" in
  rc|staging-rc|staging-release)
    suffix=${TYPE%-release}
    sed \
      -i"" \
      -e "s,^URIs: \\(.*\\)/,URIs: \\1-${suffix}/,g" \
      /etc/apt/sources.list.d/apache-arrow.sources
    ;;
esac

wget -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | \
  gpg --no-default-keyring --keyring /usr/share/keyrings/pgdg.gpg --import -
(echo "Types: deb"; \
 echo "URIs: http://apt.postgresql.org/pub/repos/apt"; \
 echo "Suites: ${code_name}-pgdg"; \
 echo "Components: main"; \
 echo "Signed-By: /usr/share/keyrings/pgdg.gpg") | \
  tee /etc/apt/sources.list.d/pgdg.sources

echo "::endgroup::"


echo "::group::Install built packages"

${APT_UPDATE}

package=postgresql-15-pgdg-apache-arrow-flight-sql
package_version=${VERSION}-1
if [ "${TYPE}" = "local" ]; then
  ${APT_INSTALL} \
    ${REPOSITORIES_DIR}/${distribution}/pool/${code_name}/*/*/*/${package}_${package_version}_${architecture}.deb
else
  ${APT_INSTALL} postgresql-15-pgdg-apache-arrow-flight-sql=${package_version}
fi

echo "::endgroup::"


echo "::group::Install packages for test"

${APT_INSTALL} \
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
