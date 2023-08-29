# -*- ruby -*-
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

require "tmpdir"

require_relative "helper"

version = Helper.detect_version
archive_base_name = "apache-arrow-flight-sql-postgresql-#{version}"
archive_name = "#{archive_base_name}.tar.gz"

file archive_name do
  sh("git",
     "archive",
     "HEAD",
     "--output", archive_name,
     "--prefix", "#{archive_base_name}/")
end
desc "Create #{archive_name}"
task :dist => archive_name

def build_doc(output_directory, release: nil, for_publish: false)
  env = {}
  env["RELEASE"] = release if release
  sh(env,
     "sphinx-build",
     "-b", "html",
     "-j", "auto",
     "doc/source",
     output_directory)
  if for_publish
    rm_f("#{output_directory}/.buildinfo")
    rm_rf("#{output_directory}/.doctrees")
  end
end

namespace :doc do
  desc "Build HTML documentation"
  task :html do
    build_doc("doc/build")
  end

  desc "Publish HTML documentation"
  task :publish do
    site = ENV["ASF_SITE"] || "site"
    asf_yaml = File.expand_path(".asf.yaml")
    index_html = File.expand_path("doc/index.html")

    Dir.mktmpdir do |tmp|
      is_release = (ENV["GITHUB_REF_TYPE"] == "tag")
      if is_release
        new_version = ENV["GITHUB_REF_NAME"].gsub(/-rc\d+\z/, "")
        new_doc = "#{tmp}/new"
        build_doc(new_doc, for_publish: true)
        current_doc = "#{tmp}/current"
        build_doc(current_doc, release: "current", for_publish: true)
      else
        devel_doc = "#{tmp}/devel"
        build_doc(devel_doc, release: "devel", for_publish: true)
      end

      add = lambda do |source, destination|
        rm_rf(destination)
        cp_r(source, destination)
        sh("git", "add", "--force", destination)
      end

      cd("site") do
        add.call(asf_yaml, ".asf.yaml")
        add.call(index_html, "index.html")
        if is_release
          add.call(new_doc, new_version)
          add.call(current_doc, "current")
        else
          add.call(devel_doc, "devel")
        end
        sh("git", "commit", "-m", "Publish", "--allow-empty")
        unless ENV["GITHUB_EVENT_NAME"] == "pull_request"
          dry_run = []
          dry_run << "--dry-run" unless ENV["GITHUB_REF_NAME"] == "main"
          sh("git", "push", *dry_run, "origin", "asf-site:asf-site")
        end
      end
    end
  end
end
