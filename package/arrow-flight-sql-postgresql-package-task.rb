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

arrow_source = ENV["ARROW_SOURCE"]
if arrow_source.nil?
  raise "You must set ARROW_SOURCE environment variable"
end
ARROW_SOURCE = File.expand_path(arrow_source)
require "#{ARROW_SOURCE}/dev/tasks/linux-packages/package-task"

require_relative "../helper"

class ArrowFlightSQLPostgreSQLPackageTask < PackageTask
  def initialize(package)
    release_time = Helper.detect_release_time
    super(package, Helper.detect_version, release_time)
  end

  def define
    super
    define_rc_tasks
    define_release_tasks
  end

  private
  def define_archive_task
    file @archive_name do
      build_archive
    end

    if deb_archive_name != @archive_name
      file deb_archive_name => @archive_name do
        cp(@archive_name, deb_archive_name)
      end
    end

    if rpm_archive_name != @archive_name
      file rpm_archive_name => @archive_name do
        cp(@archive_name, rpm_archive_name)
      end
    end
  end

  def build_archive
    top_source_dir = Helper.top_source_dir
    archive_name = top_source_dir + Helper.archive_name(@version)
    unless archive_name.exist?
      cd(top_source_dir) do
        ruby("-S", "rake", "dist")
      end
    end
    sh("tar", "xf", archive_name.to_s)
    mv(Helper.archive_base_name(@version), @archive_base_name)
    sh("tar", "czf", @full_archive_name, @archive_base_name)
    rm_rf(@archive_base_name)
  end

  def apt_targets_default
    [
      "debian-bookworm-amd64",
      # "debian-bookworm-arm64",
      "ubuntu-jammy-amd64",
      # "ubuntu-jammy-arm64",
      "ubuntu-noble-amd64",
      # "ubuntu-noble-arm64",
    ]
  end

  def yum_targets_default
    [
    ]
  end

  def github_repository
    ENV["GITHUB_REPOSITORY"] || "apache/arrow-flight-sql-postgresql"
  end

  def docker_image(os, architecture)
    "ghcr.io/#{github_repository}/package-#{super}"
  end

  def built_package_url(target_namespace, target)
    url = "https://github.com/#{github_repository}"
    url << "/releases/download/#{@version}/#{target}.tar.gz"
    url
  end

  def package_dir_name
    "#{@package}-#{@version}"
  end

  def download_packages(target_namespace)
    download_dir = "#{ARROW_SOURCE}/packages/#{package_dir_name}"
    mkdir_p(download_dir)
    __send__("#{target_namespace}_targets").each do |target|
      url = built_package_url(target_namespace, target)
      archive = download(url, download_dir)
      cd(download_dir) do
        sh("tar", "xf", archive)
      end
    end
  end

  def upload_rc(target_namespace)
    targets = __send__("#{target_namespace}_targets")
    cd(apache_arrow_dir) do
      env = {
        "CROSSBOW_JOB_ID" => package_dir_name,
        "DEB_PACKAGE_NAME" => @package,
        "STAGING" => ENV["STAGING"] || "no",
        "UPLOAD_DEFAULT" => "0",
      }
      targets.each do |target|
        distribution = target.split("-")[0].upcase
        env["UPLOAD_#{distribution}"] = "1"
      end
      sh(env,
         "dev/release/05-binary-upload.sh",
         @version,
         "0")
    end
  end

  def define_rc_tasks
    [:apt].each do |target_namespace|
      tasks = []
      namespace target_namespace do
        desc "Upload RC #{target_namespace} packages"
        task :rc do
          download_packages(target_namespace)
          upload_rc(target_namespace)
        end
        tasks << "#{target_namespace}:release"
      end
      task target_namespace => tasks
    end
  end

  def release(target_namespace)
    targets = __send__("#{target_namespace}_targets")
    cd(apache_arrow_dir) do
      env = {
        "STAGING" => ENV["STAGING"] || "no",
        "DEPLOY_DEFAULT" => "0",
      }
      targets.each do |target|
        distribution = target.split("-")[0].upcase
        env["DEPLOY_#{distribution}"] = "1"
      end
      sh(env,
         "dev/release/post-02-binary.sh",
         @version,
         "0")
    end
  end

  def define_release_tasks
    [:apt].each do |target_namespace|
      tasks = []
      namespace target_namespace do
        desc "Release #{target_namespace} packages"
        task :release do
          release(target_namespace)
        end
        tasks << "#{target_namespace}:release"
      end
      task target_namespace => tasks
    end
  end
end
