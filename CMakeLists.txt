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

cmake_minimum_required(VERSION 3.19)
message(STATUS "Building using CMake version: ${CMAKE_VERSION}")

project(
  arrow-flight-sql
  DESCRIPTION "Apache Arrow Flight SQL adapter"
  HOMEPAGE_URL https://arrow.apache.org/flight-sql-postgresql
  LANGUAGES C CXX
  VERSION 0.1.0)

include(GNUInstallDirs)

set(AFS_SOURCES src/afs.cc)

add_custom_target(format)
# Ubuntu 20.04 ships clang-format-12.
find_program(CLANG_FORMAT clang-format-12)
if(CLANG_FORMAT)
  add_custom_target(
    format-source
    ${CLANG_FORMAT} -i ${AFS_SOURCES}
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
  add_dependencies(format format-source)
endif()
find_program(CMAKE_FORMAT cmake-format)
if(CMAKE_FORMAT)
  add_custom_target(
    format-cmake
    ${CMAKE_FORMAT} --in-place CMakeLists.txt
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
  add_dependencies(format format-cmake)
endif()

set(AFS_POSTGRESQL_DIR
    "${CMAKE_INSTALL_PREFIX}"
    CACHE PATH "PostgreSQL binary directory")

find_program(AFS_PG_CONFIG "pg_config" HINTS "${AFS_POSTGRESQL_DIR}/bin")
if(NOT AFS_PG_CONFIG)
  message(FATAL_ERROR "pg_config isn't found")
endif()
add_executable(pg_config IMPORTED)
set_target_properties(pg_config PROPERTIES IMPORTED_LOCATION ${AFS_PG_CONFIG})

add_library(postgresql INTERFACE IMPORTED)
execute_process(
  COMMAND pg_config "--includedir-server"
  OUTPUT_VARIABLE AFS_PG_INCLUDE_DIR
  OUTPUT_STRIP_TRAILING_WHITESPACE ECHO_OUTPUT_VARIABLE COMMAND_ERROR_IS_FATAL
  ANY)
target_include_directories(postgresql INTERFACE ${AFS_PG_INCLUDE_DIR})
execute_process(
  COMMAND pg_config "--cflags_sl"
  OUTPUT_VARIABLE AFS_PG_CFLAGS
  OUTPUT_STRIP_TRAILING_WHITESPACE ECHO_OUTPUT_VARIABLE COMMAND_ERROR_IS_FATAL
  ANY)
separate_arguments(AFS_PG_COMPILE_OPTIONS NATIVE_COMMAND PROGRAM
                   SEPARATE_ARGS ${AFS_PG_CFLAGS})
target_compile_options(postgresql INTERFACE ${AFS_PG_COMPILE_OPTIONS})
execute_process(
  COMMAND pg_config "--libdir"
  OUTPUT_VARIABLE AFS_PG_LIB_DIR
  OUTPUT_STRIP_TRAILING_WHITESPACE ECHO_OUTPUT_VARIABLE COMMAND_ERROR_IS_FATAL
  ANY)
target_link_directories(postgresql INTERFACE ${AFS_PG_LIB_DIR})

add_library(libpq SHARED IMPORTED)
execute_process(
  COMMAND pg_config "--includedir"
  OUTPUT_VARIABLE AFS_LIBPG_INCLUDE_DIR
  OUTPUT_STRIP_TRAILING_WHITESPACE ECHO_OUTPUT_VARIABLE COMMAND_ERROR_IS_FATAL
  ANY)
target_include_directories(libpq INTERFACE ${AFS_LIBPG_INCLUDE_DIR})
execute_process(
  COMMAND pg_config "--cflags"
  OUTPUT_VARIABLE AFS_LIBPQ_CFLAGS
  OUTPUT_STRIP_TRAILING_WHITESPACE ECHO_OUTPUT_VARIABLE COMMAND_ERROR_IS_FATAL
  ANY)
separate_arguments(AFS_LIBPQ_COMPILE_OPTIONS NATIVE_COMMAND PROGRAM
                   SEPARATE_ARGS ${AFS_LIBPQ_CFLAGS})
target_compile_options(libpq INTERFACE ${AFS_LIBPQ_COMPILE_OPTIONS})
execute_process(
  COMMAND pg_config "--libdir"
  OUTPUT_VARIABLE AFS_LIBPQ_LIB_DIR
  OUTPUT_STRIP_TRAILING_WHITESPACE ECHO_OUTPUT_VARIABLE COMMAND_ERROR_IS_FATAL
  ANY)
find_library(AFS_LIBPQ_SHARED_LIBRARY pq PATHS ${AFS_LIBPQ_LIB_DIR})
set_target_properties(libpq PROPERTIES IMPORTED_LOCATION
                                       ${AFS_LIBPQ_SHARED_LIBRARY})

find_package(ArrowFlightSql REQUIRED)

add_library(arrow_flight_sql MODULE ${AFS_SOURCES})
set_target_properties(arrow_flight_sql PROPERTIES PREFIX "")
target_compile_definitions(arrow_flight_sql
                           PRIVATE "$<$<CONFIG:Debug>:AFS_DEBUG>")
target_link_libraries(arrow_flight_sql postgresql
                      ArrowFlightSql::arrow_flight_sql_shared)
execute_process(
  COMMAND pg_config "--pkglibdir"
  OUTPUT_VARIABLE AFS_PG_EXTENSION_DIR
  OUTPUT_STRIP_TRAILING_WHITESPACE ECHO_OUTPUT_VARIABLE COMMAND_ERROR_IS_FATAL
  ANY)
install(TARGETS arrow_flight_sql DESTINATION "${AFS_PG_EXTENSION_DIR}")

install(FILES LICENSE.txt NOTICE.txt DESTINATION "${CMAKE_INSTALL_DOCDIR}")

add_subdirectory(benchmark/integer)
