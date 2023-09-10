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
require "open-uri"
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

def push_automatically?
  env_value("PUSH", "no") == "yes"
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

def release_remote
  env_value("RELEASE_REMOTE", "release")
end

def sh_capture_output(*command_line)
  IO.pipe do |read, write|
    sh(*command_line, out: write)
    write.close
    read.read
  end
end

def detect_latest_rc(version)
  rc_tags = sh_capture_output("git", "tag").each_line(chomp: true).select do |tag|
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

def git_remote_url(name)
  sh_capture_output("git", "remote", "get-url", "--push", name).chomp
end

def git_current_branch
  sh_capture_output("git", "rev-parse", "--abbrev-ref=HEAD").chomp
end

def git_last_commit(branch="HEAD")
  sh_capture_output("git", "log", "-n", "1", "--format=format:%H", branch).chomp
end

def repository_url_extract_repository(url)
  if url.start_with?("https://")
    # https://github.com/apache/arrow-flight-sql-postgresql.git ->
    # apache/arrow-flight-sql-postgresql.git
    repository = url.split("/")[-2..-1].join("/")
  else
    # git@github.com:apache/arrow-flight-sql-postgresql.git ->
    # apache/arrow-flight-sql-postgresql.git
    repository = url.split(":").last
  end
  respoitory.delete_suffix(".git")
end

def repository_last_commit(repository)
  URI("https://api.github.com/repos/#{repository}/branches/main").open do |input|
    JSON.parse(input.read)["commit"]["sha"]
  end
end

def ci_conclusions(repository, branch, commit)
  jq_filter = ".[] | select(.headSha == \"#{commit}\") | .conclusion"
  sh_capture_output("gh", "run", "list",
                    "--repo", repository,
                    "--branch", branch,
                    "--json", "headSha,conclusion",
                    "--jq", jq_filter).
    lines(chomp: true)
end

def validate_rc(version)
  package_directories.each do |dir|
    latest_version = debian_changelog_latest_version("#{dir}/debian/changelog")
    unless latest_version == version
      raise "'rake release:rc:prepare && git push' is needed"
    end
  end

  release_notes = File.read("doc/source/release-notes.md").split(/^## Version /)
  latest_release_note = release_notes[1]
  latest_release_note_version = latest_release_note.lines[0].strip
  unless latest_release_note_version == version
    raise "add a release note to doc/source/release-notes.md"
  end

  current_branch = git_current_branch
  unless current_branch == "main"
    raise "work on main not a branch: #{current_branch}"
  end

  local_commit = git_last_commit
  release_repository_url = git_remote_url(release_remote)
  release_repository = repository_url_extract_repository(release_repository_url)
  remote_commit = repository_last_commit(release_repository)
  unless remote_commit == local_commit
    raise "local commit isn't synchronized with release remote: #{release_repository}"
  end

  conclusions = ci_conclusions(release_repository, "main", remote_commit)
  unless conclusions.all? {|conclusion| conclusion == "success"}
    raise "CI failed: https://github.com/#{release_repository}/commit/#{remote_commit}"
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
    desc "Prepare new release"
    task :prepare do
      prepare_branch = "prepare-#{version}"
      sh("git", "switch", "-c", prepare_branch)
      package_directories.each do |dir|
        cd(dir) do
          debian_changelog = "debian/changelog"
          latest_version = debian_changelog_latest_version(debian_changelog)
          next if latest_version == version
          ruby("-S", "rake", "version:update")
          sh("git", "add", debian_changelog)
        end
      end
      sh("git", "commit", "-m", "Prepare #{version}")
      if push_automatically?
        sh("git", "push", "origin", prepare_branch)
      else
        puts("Push #{prepare_branch}:")
        puts("  git push origin #{prepare_branch}")
      end
      puts("Open a PR:")
      puts("  https://github.com/apache/arrow-flight-sql-postgresql/pulls")
    end

    desc "Ensure remote for releasing"
    task :ensure_release_remote do
      begin
        git_remote_url(release_remote)
      rescue RuntimeError => error
        raise unless error.message.start_with?("Command failed")
        sh("git", "remote", "add",
           release_remote,
           "git@github.com:apache/arrow-flight-sql-postgresql.git")
      end
    end

    desc "Validation before a new RC"
    task :validate do
      validate_rc(version)
    end

    desc "Tag for a new RC"
    task :tag => [:ensure_release_remote, :validate] do
      new_rc = (detect_latest_rc(version) || 0) + 1
      rc_tag = "#{version}-rc#{new_rc}"
      sh("git", "tag",
         "-a", rc_tag,
         "-m", "#{project_label} #{version} RC #{new_rc}")
      if push_automatically?
        sh("git", "push", release_remote, rc_tag)
      else
        puts("Push #{rc_tag}:")
        puts("  git push #{release_remote} #{rc_tag}")
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
      commit = git_last_commit(rc_tag)
      puts(<<-MAIL)
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
[5]: https://arrow.apache.org/flight-sql-postgresql/devel/development/release.html#how-to-verify
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
        if push_automatically?
          sh("git", "push", release_remote, version)
        else
          puts("Push #{version}:")
          puts("  git push #{release_remote} #{version}")
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

  namespace :announce do
    desc "Show blog announce template"
    task :blog do
      previous_version = env_value("PREVIOUS_VERSION")
      commit_range = "#{previous_version}..#{version}"
      n_commits = sh_capture_output("git", "rev-list", "--count", commit_range).chomp
      contributor_command = [
        "git",
        "shortlog",
        "--perl-regexp",
        "--author='^((?!dependabot\[bot\]).*)$'",
        "-sn",
        commit_range,
      ]
      contributors = sh_capture_output(*contributor_command)
      n_contributors = contributors.lines.size
      post_date = env_value("POST_DATE", Date.today.strftime("%Y-%m-%d"))
      puts(<<-POST)
---
layout: post
title: "Apache Arrow Flight SQL adapter for PostgreSQL #{version} Release"
date: "#{post_date} 00:00:00"
author: pmc
categories: [release]
---
{% comment %}
<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
{% endcomment %}

The Apache Arrow team is pleased to announce the #{version} release of
the Apache Arrow Flight SQL adapter for PostgreSQL. This includes
[**#{n_commits} commits][commits] from [**#{n_contributors} distinct
contributors**][contributors].

The release notes below are not exhaustive and only expose selected
highlights of the release. Many other bugfixes and improvements have
been made: we refer you to [the complete release notes][release-note].

## Release Highlights

<!-- TODO: fill this portion in. -->

## Contributors

```console
$ #{contributor_command.join(" ")}
#{contributors}
```

## Roadmap

<!-- TODO: fill this portion in. -->

## Getting Involved

We welcome questions and contributions from all interested. Issues can
be filed on [GitHub][issues], and questions can be directed to GitHub or
[the Arrow mailing lists][mailing-list].

[commits]: ${MILESTONE_URL}
[contributors]: #contributors
[release-note]: https://arrow.apache.org/flight-sql-postgresql/current/release-notes.html\#version-#{version}
[issues]: https://github.com/apache/arrow-flight-sql-postgresql/issues
[mailing-list]: {% link community.md %}
      POST
    end

    desc "Show mail announce template"
    task :mail do
      blog_path_date = Date.today("%Y/%m/%d")
      # Extract the first "## ..." section.
      overview = File.read("doc/source/overview.md").split(/^## /)[1]
      # Remove links:
      #   [Apache Arrow Flight SQL][apache-arrow-flight-sql] ->
      #   Apache Arrow Flight SQL
      overview.gsub!(/\[(.+?)\]\[.+?\]/m, "\\1")
      puts(<<-MAIL)
To: announce@apache.org, user@arrow.apache.org, dev@arrow.apache.org
From: XXX@apache.org
Subject: [ANNOUNCE] Apache Arrow Flight SQL adapter for PostgreSQL #{version} released

The Apache Arrow team is pleased to announce the #{version} release of
the Apache Arrow Flight SQL adapter for PostgreSQL.

The release is available now from our website:
  https://arrow.apache.org/flight-sql-postgresql/#{version}/install/

Read about what's new in the release:
  https://arrow.apache.org/blog/#{blog_path_date}/flight-sql-postgresql-#{version}-release/

Release note:
  https://arrow.apache.org/flight-sql-postgresql/#{version}/release-note.html\#version-#{version}


#{overview}


Please report any feedback to the GitHub issues or mailing lists:
  * GitHub: https://github.com/apache/arrow-flight-sql-postgresql/issues
  * ML: https://arrow.apache.org/community/


Thanks,
-- 
The Apache Arrow community
      MAIL
    end

    desc "Show PostgreSQL announce template"
    task :postgresql do
      puts(<<-ANNOUNCE)
TODO
      ANNOUNCE
    end
  end

  namespace :version do
    desc "Bump version"
    task :bump do
      new_version = env_value("NEW_VERSION")
      bump_version_branch = "bump-version-#{new_version}"
      sh("git", "switch", "-c", bump_version_branch)
      meson_build = File.read("meson.build")
      new_meson_build = meson_build.gsub(/version: '.+?'/) do
        "version: '#{new_version}'"
      end
      File.write("meson.build", new_meson_build)
      sh("git", "add", "meson.build")
      sh("git", "commit", "-m", "Bump version to #{new_version}")
      if push_automatically?
        sh("git", "push", "origin", bump_version_branch)
      else
        puts("Push #{bump_version_branch}:")
        puts("  git push origin #{bump_version_branch}")
      end
      puts("Open a PR:")
      puts("  https://github.com/apache/arrow-flight-sql-postgresql/pulls")
    end
  end
end
