<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Release

## Principles

The Apache Arrow Release follows the guidelines defined at the [Apache
Software Foundation Release
Policy](https://www.apache.org/legal/release-policy.html).

## Prepare

Some steps of the release require being a committer or a PMC member.

- A GPG key in the Apache Web of Trust to sign artifacts. This will
  have to be cross signed by other Apache committers/PMC members. You
  must set your GPG key ID in `dev/release/.env` (see
  `dev/release/.env.example` for a template).
- The GPG key needs to be added to this [Subversion repository for
  dev](https://dist.apache.org/repos/dist/dev/arrow/) and [Subversion
  repository for
  release](https://dist.apache.org/repos/dist/release/arrow/).
- An [Artifactory](https://apache.jfrog.io) API key (log in with your
  ASF credentials, then generate it from your profile in the
  upper-right). You must set the Artifactory API key in
  `dev/release/.env` (see `dev/release/.env.example` for a
  template).
- A GitHub personal access token that has at lease `workflow`
  scope. You must set the GitHub personal access token in
  `dev/release/.env` (see `dev/release/.env.example` for a template).
- Clone [apache/arrow](https://github.com/apache/arrow) and prepare
  `apache/arrow`'s `dev/release/.env` (see `dev/release/.env.example`
  for a template) like `apache/arrow-flight-sql-postgresql`'s
  `dev/release/.env`.

## Create a RC

Prepare a new RC when you create a first RC:

```bash
rake release:rc:prepare ARROW_SOURCE=/path/to/apache/arrow/repository/
```

You need to specify your cloned `apache/arrow` repository.

This creates a `prepare-${VERSION}` branch on local repository. You
need to push it and open a new pull request for it.

Tag for a new RC after the pull request is merged:

```bash
rake release:rc:tag
```

This creates a new tag for the new RC. You check it and push to
`apache/arrow-flight-sql-postgresql`.

Artifacts for the new RC are built automatically on GitHub Actions.

Sign these artifacts:

```bash
rake release:rc:sign
```

Note that you don't need to wait for finishing GitHub Actions jobs
before you run the above command line. Because the above command line
waits for finishing GitHub Actions jobs automatically.

Upload Linux packages:

```bash
rake release:rc:linux ARROW_SOURCE=/path/to/apache/arrow/repository/
```

You need to specify your cloned `apache/arrow` repository. You must
prepare its `dev/release/.env` (not only
`apache/arrow-flight-sql-postgresql`'s `dev/release/.env`.)

Re-run RC verify CI jobs on GitHub Actions:

```bash
rake release:rc:verify
```

Start a vote thread for the new RC on `dev@arrow.apache.org`. You can
generate a vote e-mail template:

```bash
rake release:vote
```

Approval requires a net of 3 +1 votes from PMC members. A release
cannot be vetoed.

## How to verify

### Install dependencies

At minimum, you will need:

- Apache Arrow C++ (Apache Arrow Flight SQL must be enabled)
- C++ compiler
- Git
- GnuPG
- Meson
- Ninja
- PostgreSQL 15 or later
- cURL
- jq
- shasum (built into macOS) or sha256sum/sha512sum (on Linux)

### Verify

Clone the project:

```bash
git clone https://github.com/apache/arrow-flight-sql-postgresql.git
```

Run the verification script:

```bash
cd arrow-flight-sql-postgresql
# Pass the version and the RC number.
# This stops after build is succeeded. You need to verify manually after it.
# See "Hints" the script shows what you do.
dev/release/verify-rc.sh 0.1.0 1
```

These environment variables may be helpful:

- `ARROW_TMPDIR=/path/to/directory` to specify the temporary directory
  used. Using a fixed directory can help avoid repeating the same
  setup and build steps if the script has to be run multiple times.
- `TEST_SOURCE_MANUAL=0` to disable manual verification.

Once finished and once the script passes, reply to the mailing list
vote thread with a `+1` or a `-1`.

## Post-release tasks

After the release vote, we must undertake the following tasks:

Publish artifacts:

```bash
rake release:rc:publish ARROW_SOURCE=/path/to/apache/arrow/repository/
```

This command line opens [Apache
reporter](https://reporter.apache.org/addrelease.html?arrow) by your
Web browser. Don't forget to register the new release by the form in
the page.

This command line creates the tag for the new release but doesn't push
it automatically by default. Don't forget to push it.

Publish release blog post. The following command line generates the
blog post outline, then fill out outline and create a PR on
[`apache/arrow-site`](https://github.com/apache/arrow-site). You need
to close your `apache/arrow-site` fork and specify the path of it as
`ARROW_SITE`:

```bash
rake release:announce:blog PREVIOUS_VERSION=X.Y.Z ARROW_SITE=/path/to/your/arrow-site/fork/repository
```

Announce the new release on mailing lists. The following command line
generates the announce e-mail outline, then fill out outline and send
the announce e-mail to `announce@apache.org`, `user@arrow.apache.org`
and `dev@arrow.apache.org`. You must use your `@apache.org` e-mail
address to send an e-mail to `announce@apache.org`:

```bash
rake release:announce:mail
```

Announce the new release on `pgsql-announce@postgresql.org`.

You need to create your PostgreSQL community account:
https://www.postgresql.org/account/

You need to join the Apache Arrow organization on PostgreSQL
community. Please contact one of the organization managers such as
[@kou](https://github.com/kou) to join the organization.

The following command line generates the announce content. You can
submit it to https://www.postgresql.org/account/edit/news/ .

```bash
rake release:announce:postgresql ARROW_SITE=/path/to/your/arrow-site/fork/repository
```

Bump version:

```bash
rake release:version:bump NEW_VERSION=X.Y.Z
```
