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
set -o pipefail

base_dir=$(dirname "$0")

measure()
{
  local n_tries=3
  for i in $(seq ${n_tries}); do
    "$@"
  done | sort --numeric-sort | head -n 1
}

benchmarks=()
benchmarks+=(integer)
benchmarks+=(string)

for benchmark in "${benchmarks[@]}"; do
  result="${base_dir}/${benchmark}/result.csv"
  echo "Approach,N records,Elapsed time (sec)" | tee "${result}"
  sizes=()
  sizes+=(100000)
  sizes+=(1000000)
  sizes+=(10000000)
  for size in "${sizes[@]}"; do
    export PGDATABASE="afs_benchmark_${benchmark}_${size}"
    echo "${benchmark}: ${size}: preparing"
    "${base_dir}/${benchmark}/prepare-sql.sh" "${size}" "${PGDATABASE}" | \
      psql -d postgres

    echo "${benchmark}: ${size}: Apache Arrow Flight SQL"
    elapsed_time=$(measure "${base_dir}/select.rb")
    echo "Apache Arrow Flight SQL,${size},${elapsed_time}" | tee -a "${result}"

    echo "${benchmark}: ${size}: SELECT"
    elapsed_time=$(measure benchmark/select)
    echo "SELECT,${size},${elapsed_time}" | tee -a "${result}"

    echo "${benchmark}: ${size}: COPY"
    elapsed_time=$(measure benchmark/copy)
    echo "COPY,${size},${elapsed_time}" | tee -a "${result}"
  done
done
