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

  def to_sql(value)
    case value
    when Time
      "'#{value.dup.utc.strftime("%Y-%m-%d %H:%M:%S.%6N")}'"
    when String
      sql_string = "'"
      value.each_char do |char|
        case char
        when "'"
          sql_string << "''"
        when "\\"
          sql_string << "\\\\"
        else
          if (0..31).cover?(char.ord) or (127..255).cover?(char.ord)
            sql_string << ("\\%03d" % char.ord)
          else
            sql_string << char
          end
        end
      end
      sql_string << "'"
      sql_string
    else
      value.to_s
    end
  end

  timestamp_value = Time.parse("2023-08-02T18:03:13.526572321Z").utc

  data("int16",  ["smallint",         :int16, -2])
  data("int32",  ["integer",          :int32, -2])
  data("int64",  ["bigint",           :int64, -2])
  data("float",  ["real",             :float, -2.2])
  data("double", ["double precision", :double, -2.2])
  data("string - text",    ["text",        :string, "b"])
  data("string - varchar", ["varchar(10)", :string, "b"])
  data("binary", ["bytea", :binary, "\x0".b])
  data("timestamp", ["timestamp", [:timestamp, :micro], timestamp_value])
  def test_select_type
    pg_type, data_type, value = data
    data_type = Arrow::DataType.resolve(data_type)
    values = data_type.build_array([value])
    sql = "SELECT #{to_sql(value)}::#{pg_type} AS value"
    info = flight_sql_client.execute(sql, @options)
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

  timestamp_values = [
    Time.parse("2023-08-02T18:03:13.526572321Z").utc,
    Time.parse("2023-08-02T18:04:13.526572321Z").utc,
    Time.parse("2023-08-02T18:05:13.526572321Z").utc,
  ]

  data("int8",   ["smallint", :int8,   [1, -2, 3]])
  data("int16",  ["smallint", :int16,  [1, -2, 3]])
  data("int32",  ["integer",  :int32,  [1, -2, 3]])
  data("int64",  ["bigint",   :int64,  [1, -2, 3]])
  data("uint8",  ["smallint", :uint8,  [1,  2, 3]])
  data("uint16", ["smallint", :uint16, [1,  2, 3]])
  data("uint32", ["integer",  :uint32, [1,  2, 3]])
  data("uint64", ["bigint",   :uint64, [1,  2, 3]])
  data("float",  ["real",             :float,  [1.1, -2.2, 3.3]])
  data("double", ["double precision", :double, [1.1, -2.2, 3.3]])
  data("string - text",    ["text",        :string, ["a", "b", "c"]])
  data("string - varchar", ["varchar(10)", :string, ["a", "b", "c"]])
  data("binary", ["bytea", :binary, ["\x0".b, "\x1".b, "\x2".b]])
  data("timestamp(second)",
       ["timestamp", [:timestamp, :second], timestamp_values])
  data("timestamp(milli)",
       ["timestamp", [:timestamp, :milli], timestamp_values])
  data("timestamp(micro)",
       ["timestamp", [:timestamp, :micro], timestamp_values])
  data("timestamp(nano)",
       ["timestamp", [:timestamp, :nano], timestamp_values])
  def test_insert_type
    unless flight_sql_client.respond_to?(:prepare)
      omit("red-arrow-flight-sql 14.0.0 or later is required")
    end

    pg_type, data_type, values = data
    data_type = Arrow::DataType.resolve(data_type)
    run_sql("CREATE TABLE data (value #{pg_type})")

    array = data_type.build_array(values)
    flight_sql_client.prepare("INSERT INTO data VALUES ($1)",
                              @options) do |statement|
      statement.record_batch = Arrow::RecordBatch.new(value: array)
      n_changed_records = statement.execute_update(@options)
      assert_equal(values.size, n_changed_records)
    end

    case values.first
    when Time
      case data_type.unit
      when Arrow::TimeUnit::SECOND
        value_size = "1970-01-01 00:00:00".size
        strftime_format = "%Y-%m-%d %H:%M:%S"
      when Arrow::TimeUnit::MILLI
        value_size = "1970-01-01 00:00:00.000".size
        strftime_format = "%Y-%m-%d %H:%M:%S.%3N"
      when Arrow::TimeUnit::MICRO, Arrow::TimeUnit::NANO
        value_size = "1970-01-01 00:00:00.000000".size
        strftime_format = "%Y-%m-%d %H:%M:%S.%6N"
      else
        raise "unsupported: #{data_type.unit.inspect}"
      end
    else
      value_size = 5
    end
    output = <<-RESULT
SELECT * FROM data
 #{"value".center(value_size)} 
-#{"-" * value_size}-
    RESULT
    values.each do |value|
      case value
      when Float
        output << (" %5.1f\n" % value)
      when Integer
        output << (" %5d\n" % value)
      when Time
        output << " #{value.strftime(strftime_format)}\n"
      else
        if value.encoding == "".b.encoding
          output << " "
          value.each_byte do |byte|
            output << ("\\x%02x" % byte)
          end
          output << "\n"
        else
          output << " #{value}\n"
        end
      end
    end
    output << "(#{values.size} rows)\n"
    output << "\n"
    assert_equal([output, ""], run_sql("SELECT * FROM data"))
  end
end
