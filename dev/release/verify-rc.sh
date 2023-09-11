#!/usr/bin/env bash
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

set -eo pipefail

if [ ${VERBOSE:-0} -gt 0 ]; then
  set -x
fi

case $# in
  0)
    VERSION="HEAD"
    SOURCE_KIND="local"
    TEST_BINARIES=0
    ;;
  1)
    VERSION="$1"
    SOURCE_KIND="git"
    TEST_BINARIES=0
    ;;
  2)
    VERSION="$1"
    RC_NUMBER="$2"
    SOURCE_KIND="tarball"
    ;;
  *)
    echo "Usage:"
    echo "  Verify release candidate:"
    echo "    $0 X.Y.Z RC_NUMBER"
    echo "  Verify only the source distribution:"
    echo "    TEST_DEFAULT=0 TEST_SOURCE=1 $0 X.Y.Z RC_NUMBER"
    echo "  Verify only the binary distributions:"
    echo "    TEST_DEFAULT=0 TEST_BINARIES=1 $0 X.Y.Z RC_NUMBER"
    echo ""
    echo "  Run the source verification tasks on a remote git revision:"
    echo "    $0 GIT-REF"
    echo "  Run the source verification tasks on this arrow checkout:"
    echo "    $0"
    exit 1
    ;;
esac

# Note that these point to the current verify-rc.sh directories
# which is different from the ADBC_SOURCE_DIR set in ensure_source_directory()
SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
TOP_SOURCE_DIR="$(cd "${SOURCE_DIR}/../.." && pwd)"

: ${SOURCE_REPOSITORY:="apache/arrow-flight-sql-postgresql"}

show_header() {
  echo ""
  printf '=%.0s' $(seq ${#1}); printf '\n'
  echo "${1}"
  printf '=%.0s' $(seq ${#1}); printf '\n'
}

show_info() {
  echo "└ ${1}"
}

ARROW_DIST_URL="https://dist.apache.org/repos/dist/dev/arrow"

download_dist_file() {
  curl \
    --silent \
    --show-error \
    --fail \
    --location \
    --remote-name "${ARROW_DIST_URL}/$1"
}

download_rc_file() {
  local file="$1"
  local releases_url="https://github.com/${SOURCE_REPOSITORY}/releases"
  curl \
    --silent \
    --show-error \
    --fail \
    --location \
    --remote-name "${releases_url}/download/${VERSION}-rc${RC_NUMBER}/${file}"
}

import_gpg_keys() {
  if [ "${GPGKEYS_ALREADY_IMPORTED:-0}" -gt 0 ]; then
    return 0
  fi
  download_dist_file KEYS
  gpg --import KEYS

  GPGKEYS_ALREADY_IMPORTED=1
}

if type shasum >/dev/null 2>&1; then
  sha512_verify="shasum -a 512 -c"
else
  sha512_verify="sha512sum -c"
fi

fetch_archive() {
  import_gpg_keys

  local dist_name=$1
  download_rc_file ${dist_name}.tar.gz
  download_rc_file ${dist_name}.tar.gz.asc
  download_rc_file ${dist_name}.tar.gz.sha512
  gpg --verify ${dist_name}.tar.gz.asc ${dist_name}.tar.gz
  ${sha512_verify} ${dist_name}.tar.gz.sha512
}

setup_tempdir() {
  cleanup() {
    if [ "${TEST_SUCCESS}" = "yes" ]; then
      rm -fr "${ARROW_TMPDIR}"
    else
      echo "Failed to verify release candidate. See ${ARROW_TMPDIR} for details."
    fi
  }

  show_header "Creating temporary directory"

  if [ -z "${ARROW_TMPDIR}" ]; then
    # clean up automatically if ARROW_TMPDIR is not defined
    ARROW_TMPDIR=$(mktemp -d -t "arrow-flight-sql-postgresql-${VERSION}.XXXXX")
    trap cleanup EXIT
  else
    # don't clean up automatically
    mkdir -p "${ARROW_TMPDIR}"
  fi

  echo "Working in sandbox ${ARROW_TMPDIR}"
}

ensure_source_directory() {
  show_header "Ensuring source directory"

  dist_name="apache-arrow-flight-sql-postgresql-${VERSION}"
  if [ "${SOURCE_KIND}" = "local" ]; then
    # Local repository, testing repositories should be already present
    if [ -z "${TOP_SOURCE_DIR}" ]; then
      export TARGET_SOURCE_DIR="${TOP_SOURCE_DIR}"
    fi
    echo "Verifying local checkout at ${TARGET_SOURCE_DIR}"
  elif [ "${SOURCE_KIND}" = "git" ]; then
    # Remote repository, testing repositories must be cloned
    echo "Verifying repository ${SOURCE_REPOSITORY} with revision checkout ${VERSION}"
    export TARGET_SOURCE_DIR="${ARROW_TMPDIR}/arrow-flight-sql-postgresql"
    if [ ! -d "${TARGET_SOURCE_DIR}" ]; then
      git clone \
          --branch ${VERSION} \
          --recurse-submodules \
          https://github.com/${SOURCE_REPOSITORY} \
          "${TARGET_SOURCE_DIR}"
    fi
  else
    # Release tarball, testing repositories must be cloned separately
    echo "Verifying official release candidate ${VERSION}-rc${RC_NUMBER}"
    export TARGET_SOURCE_DIR="${ARROW_TMPDIR}/${dist_name}"
    if [ ! -d "${TARGET_SOURCE_DIR}" ]; then
      pushd "${ARROW_TMPDIR}"
      mkdir -p "${TARGET_SOURCE_DIR}"
      fetch_archive ${dist_name}
      tar -C "${TARGET_SOURCE_DIR}" --strip-components 1 -xf "${dist_name}.tar.gz"
      popd
    fi
  fi
}

test_source_distribution() {
  pushd "${TARGET_SOURCE_DIR}"

  show_header "Build"

  if ! type pg_config > /dev/null 2>&1; then
    if brew --prefix --installed postgresql@15 > /dev/null 2>&1; then
      PATH="$(brew --prefix --installed postgresql@15):${PATH}"
    else
      echo "PostgreSQL isn't installed. You need PostgreSQL 15 or later."
      if type brew > /dev/null 2>&1; then
        echo "You can install PostgreSQL 15 by the following command line:"
        echo "  brew install postgresql@15"
      elif type apt info postgresql-server-dev-15 > /dev/null 2>&1; then
        echo "You can install PostgreSQL 15 by the following command line:"
        echo "  sudo apt install -V postgresql{,-server-dev}-15"
      fi
      return 1
    fi
  fi

  meson setup \
        --prefix="${ARROW_TMPDIR}/install" \
        -Dpostgresql_dir="$(pg_config --bindir)/.." \
        ${ARROW_FLIGHT_SQL_POSTGRESQL_MESON_SETUP_ARGS:-} \
        build
  meson compile -C build
  if [ ${TEST_SOURCE_MANUAL} -gt 0 ]; then
    echo "Please test the built artifacts!"
    echo
    echo "Hints:"
    echo "  * You can install by 'meson install -C build'"
    echo "  * You can run PostgreSQL by 'dev/run-postgresql.sh /tmp/afs'"
    echo "  * Flight SQL endpoint is grpc://127.0.0.1:15432"
    echo "  * We don't have command line Flight SQL client yet."
    echo "    You need to write a program to connect Flight SQL server for now..."
    echo "  * If you find any problem, you should exit this shell by 'exit 1'"
    echo "    to tell that your verification failed"
    echo "  * If you don't have any Flight SQL client, you may want to just finish"
    echo "    this verification by exiting this shell"
    ${SHELL}
  else
    case "$(uname)-$(pg_config --libdir)" in
      Linux-/usr/*)
        if type sudo > /dev/null 2>&1; then
          sudo meson install -C build
          sudo chown -R "${USER}:" "${ARROW_TMPDIR}/install"
        else
          meson install -C build
        fi
        ;;
      *)
        meson install -C build
        ;;
    esac
    PATH="$(pg_config --bindir):${PATH}" ruby test/run.rb
  fi

  popd
}

test_apt() {
  pushd "${ARROW_TMPDIR}"

  show_header "Testing APT packages"

  if [ "${GITHUB_ACTIONS}" = "true" ]; then
    local verify_type=rc
    if [ "${TEST_STAGING:-0}" -gt 0 ]; then
      verify_type=staging-${verify_type}
    fi
    for target in "debian:bookworm" \
                  "ubuntu:jammy"; do \
      show_info "Verifying ${target}..."
      if ! docker run \
           --rm \
           --security-opt="seccomp=unconfined" \
           --volume "${PWD}":/host:delegated \
           "${target}" \
           /host/package/apt/test.sh \
           "${VERSION}" \
           "${verify_type}"; then
        echo "Failed to verify the APT repository for ${target}"
        exit 1
      fi
    done
  else
    curl --get \
         --data branch=${VERSION}-rc${RC_NUMBER} \
         https://api.github.com/repos/${SOURCE_REPOSITORY}/actions/workflows/verify-rc.yaml/runs > \
         verify_rc_run.json
    if [ $(jq -r '.total_count' verify_rc_run.json) -eq 0 ]; then
      echo "APT packages test on GitHub Actions isn't ran yet."
      return 1
    fi
    echo "APT packages test was ran on GitHub Actions:"
    echo "  $(jq -r '.workflow_runs[0].html_url' verify_rc_run.json)"
    conclusion="$(jq -r '.workflow_runs[0].conclusion' verify_rc_run.json)"
    if [ "${conclusion}" != "success" ]; then
      echo "It was not succeeded: ${conclusion}"
      return 1
    fi
  fi

  popd
}

test_binary_distribution() {
  test_apt
}

# By default test all functionalities.
# To deactivate one test, deactivate the test and all of its dependents
# To explicitly select one test, set TEST_DEFAULT=0 TEST_X=1
: ${TEST_DEFAULT:=1}

# Verification groups
: ${TEST_SOURCE:=${TEST_DEFAULT}}
: ${TEST_BINARIES:=${TEST_DEFAULT}}

if [ "${GITHUB_ACTIONS:-}" = "true" ] || \
     ruby -r arrow-flight-sql -e 'true' > /dev/null 2>&1; then
  : ${TEST_SOURCE_MANUAL:=0}
else
  : ${TEST_SOURCE_MANUAL:=1}
fi

TEST_SUCCESS=no

setup_tempdir
if [ ${TEST_SOURCE} -gt 0 ]; then
  ensure_source_directory
  test_source_distribution
fi
if [ ${TEST_BINARIES} -gt 0 ]; then
  test_binary_distribution
fi

TEST_SUCCESS=yes

echo "Release candidate looks good!"
exit 0
