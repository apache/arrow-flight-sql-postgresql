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

project('arrow-flight-sql-postgresql', 'c', 'cpp',
        default_options: [
          'buildtype=debug',
          'cpp_std=c++17',
        ],
        license: 'Apache-2.0',
        # Meson 1.1.0 or later is required.
        # license_files: ['LICENSE.txt', 'NOTICE.txt'],
        version: '0.2.0')

prefix = get_option('prefix')
data_dir = prefix / get_option('datadir')
doc_dir = data_dir / 'doc'
project_doc_dir = doc_dir / meson.project_name()

postgresql_dir = get_option('postgresql_dir')
pg_config_names = []
if postgresql_dir != ''
  pg_config_names += [postgresql_dir / 'bin' / 'pg_config']
endif
pg_config_names += ['pg_config']
pg_config = find_program(pg_config_names, version: '>= 15')

arrow_flight_sql = dependency(
  'ArrowFlightSql',
  method: 'cmake',
  modules: ['ArrowFlightSql::arrow_flight_sql_shared']
)
if arrow_flight_sql.version().version_compare('<14.0.0')
  # Workaround for a bug that ArrowFlightSQL::arrow_flight_sql_shared
  # doesn't depend on ArrowFlight::arrow_flight_shared.
  arrow_flight = dependency(
    'ArrowFlight',
    method: 'cmake',
    modules: ['ArrowFlight::arrow_flight_shared']
  )
  arrow_flight_sql = declare_dependency(
    dependencies: [arrow_flight_sql, arrow_flight],
  )
endif
postgresql = declare_dependency(
  compile_args: run_command(pg_config,
                            '--cflags_sl',
                            check: true).stdout().strip(),
  include_directories: [
    include_directories(
      run_command(pg_config,
                  '--includedir-server',
                  check: true).stdout().strip(),
      is_system: true
    ),
  ]
)
install_dir = run_command(pg_config, '--pkglibdir', check: true).stdout().strip()
name_suffix = 'so'
if host_machine.system() == 'darwin' and \
   pg_config.version().version_compare('>= 16')
  name_suffix = 'dylib'
endif
afs = shared_module(
  'arrow_flight_sql',
  'src/afs.cc',
  dependencies: [arrow_flight_sql, postgresql],
  name_prefix: '',
  name_suffix: name_suffix,
  install: true,
  install_dir: install_dir,
)

install_data(
  'LICENSE.txt',
  'NOTICE.txt',
  'README.md',
  install_dir: project_doc_dir
)

if get_option('benchmark')
  subdir('benchmark/integer')
endif

if get_option('example')
  subdir('example/adbc')
  subdir('example/flight-sql')
endif
