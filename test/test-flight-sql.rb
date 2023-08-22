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

  data("int16", ["smallint", Arrow::Int16Array, -2])
  data("int32", ["integer",  Arrow::Int32Array, -2])
  data("int64", ["bigint",   Arrow::Int64Array, -2])
  def test_select_type
    pg_type, array_class, value = data
    values = array_class.new([value])
    info = flight_sql_client.execute("SELECT #{value}::#{pg_type} AS value",
                                     @options)
    assert_equal(Arrow::Schema.new(value: values.value_data_type),
                 info.get_schema)
    endpoint = info.endpoints.first
    reader = flight_sql_client.do_get(endpoint.ticket, @options)
    assert_equal(Arrow::Table.new(value: values),
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

  def test_insert_direct
    unless flight_sql_client.respond_to?(:execute_update)
      omit("red-arrow-flight-sql 13.0.0 or later is required")
    end

    run_sql("CREATE TABLE data (value integer)")

    n_changed_records = flight_sql_client.execute_update(
      "INSERT INTO data VALUES (1), (-2), (3)",
      @options)
    assert_equal(3, n_changed_records)
    assert_equal([<<-RESULT, ""], run_sql("SELECT * FROM data"))
SELECT * FROM data
 value 
-------
     1
    -2
     3
(3 rows)

    RESULT
  end

  data("int8",   ["smallint", Arrow::Int8Array,   [1, -2, 3]])
  data("int16",  ["smallint", Arrow::Int16Array,  [1, -2, 3]])
  data("int32",  ["integer",  Arrow::Int32Array,  [1, -2, 3]])
  data("int64",  ["bigint",   Arrow::Int64Array,  [1, -2, 3]])
  data("uint8",  ["smallint", Arrow::UInt8Array,  [1,  2, 3]])
  data("uint16", ["smallint", Arrow::UInt16Array, [1,  2, 3]])
  data("uint32", ["integer",  Arrow::UInt32Array, [1,  2, 3]])
  data("uint64", ["bigint",   Arrow::UInt64Array, [1,  2, 3]])
  def test_insert_type
    unless flight_sql_client.respond_to?(:prepare)
      omit("red-arrow-flight-sql 14.0.0 or later is required")
    end

    pg_type, array_class, values = data
    run_sql("CREATE TABLE data (value #{pg_type})")

    flight_sql_client.prepare("INSERT INTO data VALUES ($1)",
                              @options) do |statement|
      array = array_class.new(values)
      statement.record_batch = Arrow::RecordBatch.new(value: array)
      n_changed_records = statement.execute_update(@options)
      assert_equal(3, n_changed_records)
    end

    output = <<-RESULT
SELECT * FROM data
 value 
-------
    RESULT
    values.each do |value|
      output << (" %5d\n" % value)
    end
    output << "(#{values.size} rows)\n"
    output << "\n"
    assert_equal([output, ""], run_sql("SELECT * FROM data"))
  end
end
