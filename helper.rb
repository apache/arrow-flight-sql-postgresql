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

require "pathname"

module Helper
  module_function
  def top_source_dir
    Pathname(__dir__)
  end

  def detect_version
    version_env = ENV["VERSION"]
    return version_env if version_env

    meson_build = top_source_dir / "meson.build"
    meson_build.read[/^\s*version: '(.+)'/, 1]
  end

  def detect_release_time
    release_time_env = ENV["RELEASE_TIME"]
    if release_time_env
      Time.parse(release_time_env).utc
    else
      Time.now.utc
    end
  end

  def archive_base_name(version)
    "apache-arrow-flight-sql-postgresql-#{version}"
  end

  def archive_name(version)
    "#{archive_base_name(version)}.tar.gz"
  end
end
