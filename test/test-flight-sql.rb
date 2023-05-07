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

class FlightSQLTest < Test::Unit::TestCase
  include Helper::Sandbox

  setup do
    unless flight_client.respond_to?(:authenticate_basic)
      omit("ArrowFlight::Client#authenticate_basic is needed" )
    end
    @options = ArrowFlight::CallOptions.new
    @options.add_header("x-flight-sql-database", @test_db_name)
    user = @postgresql.user
    password = @postgresql.password
    flight_client.authenticate_basic(user, password, @options)
  end

  def test_select_int32
    info = flight_sql_client.execute("SELECT 1 AS value", @options)
    assert_equal(Arrow::Schema.new(value: :int32),
                 info.get_schema)
    endpoint = info.endpoints.first
    reader = flight_sql_client.do_get(endpoint.ticket, @options)
    assert_equal(Arrow::Table.new(value: Arrow::Int32Array.new([1])),
                 reader.read_all)
  end

  def test_select_from
    run_sql("CREATE TABLE data (value integer)")
    run_sql("INSERT INTO data VALUES (1), (-2), (3)")

    info = flight_sql_client.execute("SELECT * FROM data", @options)
    assert_equal(Arrow::Schema.new(value: :int32),
                 info.get_schema)
    endpoint = info.endpoints.first
    reader = flight_sql_client.do_get(endpoint.ticket, @options)
    assert_equal(Arrow::Table.new(value: Arrow::Int32Array.new([1, -2, 3])),
                 reader.read_all)
  end
end
