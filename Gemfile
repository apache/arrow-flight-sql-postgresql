# -*- ruby -*-
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

source "https://rubygems.org"

gem "rake"
# Use the version of red-arrow-flight-sql based on the available
# arrow-glib version
red_arrow_flight_sql_version = ">= 0"
IO.pipe do |input, output|
  begin
    pid = spawn("pkg-config", "--modversion", "arrow-flight-sql-glib",
                out: output,
                err: File::NULL)
    output.close
    Process.waitpid(pid)
    arrow_flight_sql_glib_version = input.read.strip.sub(/-SNAPSHOT\z/, "")
    red_arrow_flight_sql_version = "<= #{arrow_flight_sql_glib_version}"
  rescue SystemCallError
  end
end
gem "red-arrow-flight-sql", red_arrow_flight_sql_version
gem "test-unit"
