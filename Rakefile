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

require "digest/sha2"
require "json"
require "tmpdir"

require_relative "helper"

project_label = "Apache Arrow Flight SQL adapter for PostgreSQL"

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
        tag = ENV["GITHUB_REF_NAME"]
        new_version = tag.gsub(/-rc\d+\z/, "")
        is_release_candiate = (tag != new_version)
        new_doc = "#{tmp}/new"
        build_doc(new_doc, for_publish: true)
        unless is_release_candiate
          current_doc = "#{tmp}/current"
          build_doc(current_doc, release: "current", for_publish: true)
        end
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
          add.call(current_doc, "current") unless is_release_candiate
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

def load_env
  env_file = "dev/release/.env"
  unless File.exist?(env_file)
    raise "must create #{env_file} from #{env_file}.example"
  end
  File.readlines(env_file, chomp: true).each do |line|
    line.strip!
    next if line.empty?
    next if line.start_with?("#")
    name, value = line.split("=", 2)
    next if value.nil?
    name.strip!
    value.strip!
    ENV[name] ||= value
  end
end

def env_value(name, default=nil)
  value = ENV[name]
  if value.nil? and default.nil?
    raise "must set environment variable: #{name}"
  end
  value || default
end

def github_repository
  env_value("GITHUB_REPOSITORY", "apache/arrow-flight-sql-postgresql")
end

def gpg_key_id
  env_value("GPG_KEY_ID")
end

def arrow_source
  env_value("ARROW_SOURCE")
end

def detect_latest_rc(version)
  rc_tags = `git tag`.each_line(chomp: true).select do |tag|
    tag.start_with?("#{version}-rc")
  end
  rcs = rc_tags.collect do |rc_tag|
    Integer(rc_tag.delete_prefix("#{version}-rc"), 10)
  end
  rcs.max
end

def package_directories
  Dir.glob("package/postgresql-*")
end

def debian_changelog_latest_version(changelog_path)
  (File.readlines(changelog_path)[0] || "")[/\(([\d.]+)-\d+\)/, 1]
end

def validate_rc(version)
  package_directories.each do |dir|
    latest_version = debian_changelog_latest_version("#{dir}/debian/changelog")
    if latest_version != version
      raise "'rake release:rc:prepare && git push' is needed"
    end
  end

  release_notes = File.read("doc/source/release-notes.md").split(/^## /)
  latest_release_note = release_notes[1]
  latest_release_note_version = latest_release_note.lines[0].strip
  if latest_release_note_version != version
    raise "add a release note to doc/source/release-notes.md"
  end
end

def ensure_package_job_finished(rc_tag)
  run_id = nil
  while run_id.nil?
    runs = IO.pipe do |read, write|
      sh("gh", "run", "list",
         "--json", "databaseId,headBranch",
         "--repo", github_repository,
         "--workflow", "package.yaml",
         out: write)
      write.close
      read.read
    end
    run = JSON.parse(runs).find {|rc| rc["headBranch"] == rc_tag}
    run_id = run&.fetch("databaseId")
  end
  sh("gh", "run", "watch",
     "--repo", github_repository,
     "--exit-status",
     run_id.to_s)
end

namespace :release do
  namespace :rc do
    desc "Prepare a new RC"
    task :prepare do
      prepared = false
      package_directories.each do |dir|
        cd(dir) do
          debian_changelog = "debian/changelog"
          latest_version = debian_changelog_latest_version(debian_changelog)
          next if latest_version == version
          ruby("-S", "rake", "version:update")
          sh("git", "add", debian_changelog)
          prepared = true
        end
      end
      sh("git", "commit", "-m", "Prepare #{version} RC #{new_rc}")
    end

    desc "Tag a new RC"
    task :tag do
      validate_rc(version)
      new_rc = (detect_latest_rc(version) || 0) + 1
      rc_tag = "#{version}-rc#{new_rc}"
      sh("git", "tag",
         "-a", rc_tag,
         "-m", "#{project_label} #{version} RC #{new_rc}")
      if env_value("TAG_PUSH", "no") == "yes"
        sh("git", "push", "upstream", rc_tag)
      else
        puts("Push #{rc_tag}:")
        puts("  git push upstream #{rc_tag}")
      end
    end

    desc "Sign the latest RC"
    task :sign do
      load_env
      rc = detect_latest_rc(version)
      if rc.nil?
        raise "'rake release:rc:tag && git push ...' is needed"
      end
      rc_tag = "#{version}-rc#{rc}"
      ensure_package_job_finished(rc_tag)
      Dir.mktmpdir do |tmp|
        sh("gh", "release", "download",
           "--dir", tmp,
           "--pattern", archive_name,
           "--repo", github_repository,
           rc_tag)
        tmp_archive_name = "#{tmp}/#{archive_name}"
        tmp_sign_name = "#{tmp_archive_name}.asc"
        sh("gpg",
           "--armor",
           "--detach-sign",
           "--local-user", gpg_key_id,
           "--output", tmp_sign_name,
           "#{tmp_archive_name}")
        tmp_checksum_name = "#{tmp_archive_name}.sha512"
        File.open(tmp_checksum_name, "w") do |output|
          checksum = Digest::SHA512.file(tmp_archive_name)
          output.puts("#{checksum}  #{archive_name}")
        end
        sh("gh", "release", "upload",
           "--clobber",
           "--repo", github_repository,
           rc_tag,
           tmp_sign_name,
           tmp_checksum_name)
      end
    end

    desc "Upload Linux packages"
    task :linux do
      load_env
      rc = detect_latest_rc(version)
      if rc.nil?
        raise "'rake release:rc:tag && git push ...' is needed"
      end
      rc_tag = "#{version}-rc#{rc}"
      Dir.mktmpdir do |tmp|
        sh("gh", "release", "download",
           "--dir", tmp,
           "--pattern", "debian-*.tar.gz",
           "--pattern", "ubuntu-*.tar.gz",
           "--repo", github_repository,
           rc_tag)
        Dir.glob("#{tmp}/*.tar.gz") do |tar_gz|
          sh("tar", "xf", tar_gz,
             "--directory", tmp,
             "--one-top-level")
        end
        env = {
          "ARROW_ARTIFACTS_DIR" => tmp,
          "DEB_PACKAGE_NAME" => "apache-arrow-flight-sql-postgresql",
          "UPLOAD_DEFAULT" => "0",
          "UPLOAD_DEBIAN" => "1",
          "UPLOAD_UBUNTU" => "1",
        }
        sh(env,
           "#{arrow_source}/dev/release/05-binary-upload.sh",
           version,
           rc.to_s)
      end
    end

    desc "Generate a release vote e-mail"
    task :vote do
      load_env
      rc = detect_latest_rc(version)
      if rc.nil?
        raise "'rake release:rc:tag && git push ...' is needed"
      end
      rc_tag = "#{version}-rc#{rc}"
      commit = IO.pipe do |read, write|
        sh("git", "rev-list", "-n", "1", rc_tag, write: write)
        write.close
        read.read.strip
      end
      puts(<<-MAIL)
    local -r commit=$(git rev-list -n 1 "${tag}")

    cat <<MAIL
To: dev@arrow.apache.org
Subject: [VOTE] Release Apache Arrow Flight SQL adapter for PostgreSQL ${version} - RC#{rc}

Hi,

I would like to propose the following release candidate (RC#{rc}) of
Apache Arrow Flight SQL adapter for PostgreSQL version #{version}.

This release candidate is based on commit: #{commit} [1]

The source release rc#{rc} and changelog is hosted at [2].
The binary artifacts are hosted at [3][4].

Please download, verify checksums and signatures, build and run,
and vote on the release. See [5] for how to validate a release
candidate.

The vote will be open for at least 24 hours because Apache Arrow
Flight SQL adapter for PostgreSQL doesn't reach 1.0.0 yet.

[ ] +1 Release this as Apache Arrow Flight SQL adapter for PostgreSQL #{version}
[ ] +0
[ ] -1 Do not release this as Apache Arrow Flight SQL adapter for PostgreSQL #{version}
       because...

[1]: https://github.com/apache/arrow-flight-sql-postgresql/commit/#{commit}
[2]: https://github.com/apache/arrow-flight-sql-postgresql/releases/tag/#{rc_tag}
[3]: https://apache.jfrog.io/artifactory/arrow/debian-rc/
[4]: https://apache.jfrog.io/artifactory/arrow/ubuntu-rc/
[5]: https://arrow.apache.org/flight-sql-postgresql/devel/release.html#how-to-verify-release-candidates
      MAIL
    end

    namespace :publish do
      desc "Publish to https://dist.apache.org/"
      task :apache do
        load_env
        rc = detect_latest_rc(version)
        rc_tag = "#{version}-rc#{rc}"
        Dir.mktmpdir do |tmp|
          sh("svn", "co",
             "--depth=empty",
             "https://dist.apache.org/repos/dist/release/arrow",
             tmp)
          release_dir = "#{tmp}/apache-arrow-flight-sql-postgresql-#{verison}"
          mkdir_p(release_dir)
          sh("gh", "release", "download",
             "--dir", release_dir,
             "--pattern", "#{archive_name}*",
             "--repo", github_repository,
             rc_tag)
          sh("svn", "add", release_dir)
          sh("svn", "ci", "-m", "#{project_label} #{version}", release_dir)
        end
      end

      desc "Register a new release to https://reporter.apache.org/"
      task :reporter do
        sh("open", "https://reporter.apache.org/addrelease.html?arrow")
      end

      desc "Publish Linux packages"
      task :linux do
        rc = detect_latest_rc(version)
        env = {
          "UPLOAD_DEFAULT" => "0",
          "UPLOAD_DEBIAN" => "1",
          "UPLOAD_UBUNTU" => "1",
        }
        sh(env,
           "#{arrow_source}/dev/release/post-02-binary.sh",
           version,
           rc.to_s)
      end

      desc "Tag #{version}"
      task :tag do
        load_env
        rc = detect_latest_rc(version)
        rc_tag = "#{version}-rc#{rc}"
        sh("git", "tag",
           "-a", version,
           "-m", "#{project_label} #{version}",
           "#{rc_tag}^{}")
        if env_value("TAG_PUSH", "no") == "yes"
          sh("git", "push", "upstream", version)
        else
          puts("Push #{version}:")
          puts("  git push upstream #{version}")
        end
      end
    end

    desc "Publish the latest RC as a new release"
    task :publish => [
           "release:rc:publish:apache",
           "release:rc:publish:reporter",
           "release:rc:publish:linux",
           "release:rc:publish:tag",
         ]
  end
end
