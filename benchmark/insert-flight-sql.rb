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
call_options.add_header("x-flight-sql-database",
                        ENV["PGDATABASE"] || "afs_benchmark")
client = ArrowFlight::Client.new("grpc://127.0.0.1:15432")
client.authenticate_basic(ENV["PGUSER"] || ENV["USER"],
                          ENV["PGPASSWORD"] || "",
                          call_options)
sql_client = ArrowFlightSQL::Client.new(client)

sql_client.execute_update("DROP TABLE IF EXISTS data_insert", call_options)
sql_client.execute_update("CREATE TABLE data_insert (LIKE data)", call_options)

info = sql_client.execute("SELECT * FROM data", call_options)
endpoint = info.endpoints.first
reader = sql_client.do_get(endpoint.ticket, call_options)
record_batches = []
loop do
  record_batch = reader.read_next
  break if record_batch.nil?
  record_batches << record_batch.data
end

before = Time.now
sql_client.prepare("INSERT INTO data_insert VALUES ($1)",
                   call_options) do |statement|
  record_batches.each do |record_batch|
    statement.record_batch = record_batch
    statement.execute_update(call_options)
  end
end
puts("%.3f" % (Time.now - before))
