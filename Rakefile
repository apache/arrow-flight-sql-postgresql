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

namespace :doc do
  desc "Build HTML documentation"
  task :html do
    sh("sphinx-build",
       "-b", "html",
       "-j", "auto",
       "doc/source",
       "doc/build")
  end

  desc "Publish HTML documentation"
  task :publish do
    site = ENV["ASF_SITE"] || "site"
    asf_yaml = File.expand_path(".asf.yaml")
    cleaned_doc = File.expand_path("doc/build.clean")
    index_html = File.expand_path("doc/index.html")

    rm_rf(cleaned_doc)
    cp_r("doc/build", cleaned_doc)
    rm_f("#{cleaned_doc}/.buildinfo")
    rm_rf("#{cleaned_doc}/.doctrees")

    cd("site") do
      cp(asf_yaml, ".")
      sh("git", "add", "--force", ".asf.yaml")
      cp(index_html, ".")
      sh("git", "add", "--force", "index.html")
      if ENV["GITHUB_REF_TYPE"] == "tag"
        new_version = ENV["GITHUB_REF_NAME"].gsub(/-rc\d+\z/, "")
      else
        new_version = "devel"
      end
      rm_rf(new_version)
      cp_r(cleaned_doc, new_version)
      sh("git", "add", "--force", new_version)
      unless new_version == "devel"
        rm_rf("current")
        cp_r(cleaned_doc, "current")
        sh("git", "add", "--force", "current")
      end
      sh("git", "commit", "-m", "Publish", "--allow-empty")
      unless ENV["GITHUB_EVENT_NAME"] == "pull_request"
        dry_run = []
        if ENV["GITHUB_REF_TYPE"] != "tag" and ENV["GITHUB_REF_NAME"] != "main"
          dry_run << "--dry-run"
        end
        sh("git", "push", *dry_run, "origin", "asf-site:asf-site")
      end
    end
  end
end
