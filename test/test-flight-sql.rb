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

  def test_connect
    unless flight_client.respond_to?(:authenticate_basic_token)
      omit("red-flight-sql 12.0.0 or later is required")
    end
    flight_client.authenticate_basic_token(@postgresql.user, "password")
    exception = assert_raise(Arrow::Error::NotImplemented) do
      flight_sql_client.execute("SELECT 1")
    end
    assert_equal("[flight-sql-client][execute]: " +
                 "NotImplemented: GetFlightInfoStatement not implemented",
                 exception.message.lines(chomp: true).first)
  end
end
