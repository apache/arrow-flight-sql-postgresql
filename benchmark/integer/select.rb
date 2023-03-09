#!/usr/bin/env ruby
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

require "time"

require "arrow-flight-sql"

call_options = ArrowFlight::CallOptions.new
call_options.add_header("x-flight-sql-database", "afs_benchmark")
client = ArrowFlight::Client.new("grpc://127.0.0.1:15432")
sql_client = ArrowFlightSQL::Client.new(client)

before = Time.now
info = sql_client.execute("SELECT * FROM data", call_options)
endpoint = info.endpoints.first
reader = sql_client.do_get(endpoint.ticket, call_options)
table = reader.read_all
# p table
puts("%.3fsec" % (Time.now - before))
