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

require "adbc"

options = {
  "driver" => "adbc_driver_flightsql",
  "uri" => "grpc://127.0.0.1:15432",
  "adbc.flight.sql.rpc.call_header.x-flight-sql-database" => "afs_benchmark",
}
ADBC::Database.open(**options) do |database|
  database.connect do |connection|
    connection.open_statement do |statement|
      before = Time.now
      table, n_rows_affected = statement.query("SELECT * FROM data")
      # p table
      puts("%.3fsec" % (Time.now - before))
    end
  end
end
