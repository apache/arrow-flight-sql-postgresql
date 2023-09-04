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

require "fileutils"
require "socket"
require "tempfile"
require "time"

require "arrow-flight-sql"

module Helper
  module CommandRunnable
    def spawn_process(*args)
      env = {
        "LC_ALL" => "C",
        "PGCLIENTENCODING" => "UTF-8",
      }
      if args.first.is_a?(Hash)
        env.merge!(args.shift)
      end
      output_read, output_write = IO.pipe
      error_read, error_write = IO.pipe
      options = {
        :out => output_write,
        :err => error_write,
      }
      pid = spawn(env, *args, options)
      output_write.close
      error_write.close
      [pid, output_read, error_read]
    end

    def read_command_output(input)
      return "" unless IO.select([input], nil, nil, 0)
      begin
        data = input.readpartial(4096).gsub(/\r\n/, "\n")
        data.force_encoding("UTF-8")
        data
      rescue EOFError
        ""
      end
    end

    def run_command(*args)
      pid, output_read, error_read = spawn_process(*args)
      output = ""
      error = ""
      status = nil
      timeout = 1
      loop do
        readables, = IO.select([output_read, error_read], nil, nil, timeout)
        if readables
          timeout = 0
          readables.each do |readable|
            if readable == output_read
              output << read_command_output(output_read)
            else
              error << read_command_output(error_read)
            end
          end
        else
          timeout = 1
        end
        _, status = Process.waitpid2(pid, Process::WNOHANG)
        break if status
      end
      output << read_command_output(output_read)
      error << read_command_output(error_read)
      unless status.success?
        command_line = args.join(" ")
        message = "failed to run: #{command_line}\n"
        message << "output:\n"
        message << output
        message << "error:\n"
        message << error
        raise message
      end
      [output, error]
    end
  end

  class PostgreSQL
    include CommandRunnable

    attr_reader :dir
    attr_reader :address
    attr_reader :port
    attr_reader :flight_sql_port
    attr_reader :flight_sql_uri
    attr_reader :user
    attr_reader :password
    def initialize(base_dir)
      @base_dir = base_dir
      @dir = nil
      @log_base_name = "postgresql.log"
      @log_path = nil
      @address = "127.0.0.1"
      @port = nil
      @flight_sql_port = nil
      @flight_sql_uri = nil
      @user = "arrow-flight-sql-test"
      @password = "Passw0rd!"
      @pid = nil
      @running = false
    end

    def running?
      @running
    end

    def initdb(shared_preload_libraries: [],
               db_path: "db",
               port: 25432,
               flight_sql_port: 35432)
      @dir = File.join(@base_dir, db_path)
      @log_path = File.join(@dir, "log", @log_base_name)
      @port = port
      @pgpass = Tempfile.new("arrow-flight-sql-test-pgpass")
      @pgpass.puts("#{@address}:#{@port}:*:#{@user}:#{@password}")
      @pgpass.close
      @flight_sql_port = flight_sql_port
      if use_tls?
        @flight_sql_uri = "grpc+tls://#{@address}:#{@flight_sql_port}"
      else
        @flight_sql_uri = "grpc://#{@address}:#{@flight_sql_port}"
      end
      Tempfile.create("arrow-flight-sql-test-password") do |password|
        password.print(@password)
        password.close
        run_command("initdb",
                    "--locale", "C",
                    "--encoding", "UTF-8",
                    "--username", @user,
                    "--pwfile", password.path,
                    "-D", @dir)
      end
      prepare_tls if use_tls?
      postgresql_conf = File.join(@dir, "postgresql.conf")
      File.open(postgresql_conf, "a") do |conf|
        conf.puts("listen_addresses = '#{@address}'")
        conf.puts("port = #{@port}")
        conf.puts("unix_socket_directories = ''")
        if use_tls?
          conf.puts("ssl = on")
          conf.puts("ssl_ca_file = 'root.crt'")
        end
        conf.puts("logging_collector = on")
        conf.puts("log_filename = '#{@log_base_name}'")
        conf.puts("shared_preload_libraries = " +
                  "'#{shared_preload_libraries.join(",")}'")
        conf.puts("arrow_flight_sql.uri = '#{@flight_sql_uri}'")
        yield(conf) if block_given?
      end
      pg_hba_conf = File.join(@dir, "pg_hba.conf")
      pg_hba = File.read(pg_hba_conf)
      pg_hba.gsub!(/^(host.+)trust$/, "\\1password")
      File.write(pg_hba_conf, pg_hba)
    end

    def start
      begin
        run_command("pg_ctl", "start",
                    "-w",
                    "-D", @dir)
      rescue => error
        error.message << "\nPostgreSQL log:\n#{read_log}"
        raise
      end
      loop do
        begin
          TCPSocket.open(@host, @port) do
          end
        rescue SystemCallError
          sleep(0.1)
        else
          break
        end
      end
      @running = true
      pid_path = File.join(@dir, "postmaster.pid")
      if File.exist?(pid_path)
        first_line = File.readlines(pid_path, chomp: true)[0]
        begin
          @pid = Integer(first_line, 10)
        rescue ArgumentError
        end
      end
    end

    def stop
      return unless running?
      begin
        run_command("pg_ctl", "stop",
                    "-D", @dir,
                    "-t", "60")
      rescue
        if @pid
          Process.kill(:KILL, @pid)
          @pid = nil
          @running = false
        end
        raise
      else
        @pid = nil
        @running = false
      end
    end

    def psql(db, sql)
      output, error = run_command({
                                    "PGPASSFILE" => @pgpass.path,
                                  },
                                  "psql",
                                  "--host", @address,
                                  "--port", @port.to_s,
                                  "--username", @user,
                                  "--dbname", db,
                                  "--echo-all",
                                  "--no-password",
                                  "--no-psqlrc",
                                  "--command", sql)
      [output, error]
    end

    def flight_client
      @flight_client ||=
        ArrowFlight::Client.new(@flight_sql_uri, flight_client_options)
    end

    def flight_client_options
      @flight_client_options ||= create_flight_client_options
    end

    def flight_sql_client
      @flight_sql_client ||= ArrowFlightSQL::Client.new(flight_client)
    end

    def read_log
      return "" unless File.exist?(@log_path)
      File.read(@log_path)
    end

    private
    def windows?
      /mingw|mswin|cygwin/.match?(RUBY_PLATFORM)
    end

    def use_tls?
      return false if windows?
      ArrowFlight::ClientOptions.method_defined?(:tls_root_certificates=)
    end

    def create_flight_client_options
      options = ArrowFlight::ClientOptions.new
      if use_tls?
        options.tls_root_certificates = File.read(File.join(@dir, "root.crt"))
        options.override_host_name = "server.example.com"
      end
      options
    end

    def prepare_tls
      prepare_tls_sh = File.join(__dir__, "..", "..", "dev", "prepare-tls.sh")
      prepare_tls_sh = File.expand_path(prepare_tls_sh)
      Dir.chdir(@dir) do
        run_command(prepare_tls_sh,
                    "root.example.com",
                    "server.example.com",
                    "client.example.com")
      end
    end
  end

  module Sandbox
    include CommandRunnable

    class << self
      def included(base)
        base.module_eval do
          setup :setup_tmp_dir

          setup :setup_db

          setup :setup_postgres

          setup :setup_test_db
        end
      end
    end

    def psql(db, sql)
      @postgresql.psql(db, sql)
    end

    def run_sql(sql)
      psql(@test_db_name, sql)
    end

    def flight_client
      @postgresql.flight_client
    end

    def flight_sql_client
      @postgresql.flight_sql_client
    end

    def setup_tmp_dir
      memory_fs = "/dev/shm"
      if File.exist?(memory_fs)
        tmp_dir = memory_fs
      else
        tmp_dir = nil
      end
      Dir.mktmpdir("arrow-flight-sql-", tmp_dir) do |dir|
        @tmp_dir = dir
        begin
          yield
        ensure
          debug_dir = ENV["AFS_TEST_DEBUG_DIR"]
          if debug_dir and File.exist?(@tmp_dir)
            FileUtils.rm_rf(debug_dir)
            FileUtils.mv(@tmp_dir, debug_dir)
          end
        end
      end
    end

    def setup_db
      @postgresql = PostgreSQL.new(@tmp_dir)
      options = {
        shared_preload_libraries: shared_preload_libraries,
      }
      @postgresql.initdb(**options)
      yield
    end

    def shared_preload_libraries
      ["arrow_flight_sql"]
    end

    def start_postgres
      @postgresql.start
    end

    def stop_postgres
      @postgresql.stop
    end

    def setup_postgres
      start_postgres
      begin
        yield
      ensure
        stop_postgres if @postgresql
      end
    end

    def create_db(postgresql, db_name)
      postgresql.psql("postgres", "CREATE DATABASE #{db_name}")
      postgresql.psql(db_name, "CHECKPOINT")
    end

    def setup_test_db
      @test_db_name = "test"
      create_db(@postgresql, @test_db_name)
      result, = run_sql("SELECT oid FROM pg_catalog.pg_database " +
                        "WHERE datname = current_database()")
      oid = result.lines[3].strip
      @test_db_dir = File.join(@postgresql.dir, "base", oid)
      yield
    end
  end
end
