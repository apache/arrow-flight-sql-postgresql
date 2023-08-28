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

# How to contribute

Thanks for contributing this project!

## Report your problems or requests

Please file issues on the GitHub issue tracker:
https://github.com/apache/arrow-flight-sql-postgresql/issues

You can also use the following mailing lists:

- `dev@arrow.apache.org`: for discussions about contributing to the
  project development.
- `user@arrow.apache.org`: for questions on usage.

See https://arrow.apache.org/community/#mailing-lists how to subscribe
these mailing lists.

## Build for development

### Install dependencies

You need to install the following dependencies before you build Apache
Arrow Flight SQL adapter for PostgreSQL:

- PostgreSQL
- Apache Arrow Flight SQL C++
- Meson
- CMake
- Ninja
- C++ compiler such as `g++` and `clang++`

#### PostgreSQL

You can find packages and source archive of PostgreSQL at
https://www.postgresql.org/download/ . If you use packages, you need
to install packages for development. For example,
`postgresql-server-dev-XXX` is for deb packages and
`postgresqlXXX-devel` is for RPM packages.

The latest release is recommended.

#### Apache Arrow Flight SQL C++

You can find packages and source archive of Apache Arrow C++ at
https://arrow.apache.org/install/ . If you use packages, you need to
install packages for development. For example,
`libarrow-flight-sql-dev` is for deb packages and
`arrow-flight-sql-devel` is for RPM packages.

The latest release is recommended.

#### Meson

Meson is a build system that is also used by PostgreSQL.

You can install Meson by system package managers such as `apt` and
`brew`.

For example, you can install Meson by `apt` on Debian GNU/Linux and
Ubuntu:

```bash
sudo apt install meson
```

For example, you can install Meson by `brew` on macOS:

```bash
brew install meson
```

Or you can use `pip3` to install Meson:

```bash
pip3 install meson
```

See also: https://mesonbuild.com/Getting-meson.html

#### CMake

CMake is also a build system. Meson uses CMake to find CMake
packages. Apache Arrow Flight SQL adapter for PostgreSQL uses Apache
Arrow Flight SQL C++ CMake package. So both of Meson and CMake are
needed.

If installing CMake bothers contributors, we can improving our build
system to use CMake or pkg-config to find Apache Arrow Flight SQL
C++. If you want the improvement, please report it to our issue
tracker: https://github.com/apache/arrow-flight-sql-postgresql/issues

You can install CMake by package managers such as `apt` and `brew`.

For example, you can install CMake by `apt` on Debian GNU/Linux and
Ubuntu:

```bash
sudo apt install cmake
```

For example, you can install CMake by `brew` on macOS:

```bash
brew install cmake
```

See also: https://cmake.org/install/

#### Ninja

Ninja is also a build system but it differs from Meson and
CMake. Meson and CMake only generate configuration files for Ninja and
Ninja runs C++ compilers and so on based on the generated
configuration files.

You can install Ninja by package managers such as `apt` and `brew`.

For example, you can install Ninja by `apt` on Debian GNU/Linux and
Ubuntu:

```bash
sudo apt install ninja-build
```

For example, you can install Ninja by `brew` on macOS:

```bash
brew install ninja
```

See also: https://ninja-build.org/

### Build

If you install PostgreSQL and Apache Arrow Flight SQL C++ to system
directory such as `/usr`, you can use the following simple command
lines:

```bash
meson setup build
meson compile -C build
meson install -C build
```

If you install PostgreSQL to `/tmp/local`, you can use
`-Dpostgresql_dir=/tmp/local` option:

```bash
meson setup -Dpostgresql_dir=/tmp/local build
meson compile -C build
meson install -C build
```

If you specify `postgresql_dir`, it's recommended that you also
specify `--prefix` with the same location. Apache Arrow Flight SQL
adapter for PostgreSQL installs README and so on to
`--prefix`:

```bash
meson setup -Dpostgresql_dir=/tmp/local --prefix=/tmp/local build
meson compile -C build
meson install -C build
```

If you install Apache Arrow Flight SQL C++ to `/tmp/local`, you can
use `--cmake-prefix-path`:

```bash
meson setup --cmake-prefix-path=/tmp/local build
meson compile -C build
meson install -C build
```

I you want to build benchmark programs, you can use
`-Dbenchmark=true`:

```bash
meson setup -Dbenchmark=true build
meson compile -C build
meson install -C build
```

I you want to build example programs, you can use `-Dexmaple=true`:

```bash
meson setup -Dexample=true build
meson compile -C build
meson install -C build
```

### Test

You need Ruby and Red Arrow Flight SQL (red-arrow-flight-sql gem,
Apache Arrow Flight SQL C++ bindings for Ruby) to run tests.

You can install Ruby by package managers such as `apt` and `brew`.

For example, you can install Ruby by `apt` on Debian GNU/Linux and
Ubuntu:

```bash
sudo apt install ruby
```

```bash
brew install ruby
```

You can install Red Arrow Flight SQL by Bundler that is bundled in
Ruby:

```bash
bundle install
```

You can run tests in the build directory. We can change the current
directory before we run a Ruby script by `ruby`'s `-C` option:

```bash
bundle exec ruby -C build ../test/run.rb
```

### Run

You can use `dev/run-postgresql.sh` to run PostgreSQL with Apache
Arrow Flight SQL adapter for PostgreSQL. You need to specify
PostgreSQL data directory to use `dev/run-postgresql.sh`:

```bash
dev/run-postgresql.sh /tmp/afs
```

You can connect to `grpc://127.0.0.1:15432`.

If you build example programs, you can test the endpoint by the
following command line:

```bash
PGDATABASE=postgres example/flight-sql/authenticate-password
```

If you specify CA name, server name and client name, it also prepare
TLS.

For example, you can prepare `root.home`, `server.home` and
`client.home` by adding the following entry to `/etc/hosts`:

```text
127.0.0.1 localhost root.home server.home client.home
```

In this case, you can prepare TLS and run PostgreSQL by the following
command line:

```bash
dev/run-postgresql.sh /tmp/afs root.home server.home client.home
```

You can connect to `grpc+tls://server.home:15432`. You need to use
`/tmp/afs/root.crt` for TLS root certificates.

If you build example programs, you can test the endpoint by the
following command line:

```bash
PGDATABASE=postgres \
  PGHOST=server.home \
  PGSSLMODE=require \
  PGSSLROOTCERT=/tmp/afs/root.crt \
  example/flight-sql/authenticate-password
```

## Pull request

Please open a new issue before you open a pull request. It's for the
[Openness](http://theapacheway.com/open/) of this project.

We don't have rules for pull request titles and commit messages
yet. We may create rules later. Please see other merged pull requests
for now.

You can format codes automatically by
[`pre-commit`](https://pre-commit.com/).

You can install `pre-commit` by package managers such as `apt` and
`brew`.

For example, you can install `pre-commit` by `apt` on Debian GNU/Linux
and Ubuntu:

```bash
sudo apt install pre-commit
```

For example, you can install `pre-commit` by `brew` on macOS:

```bash
brew install pre-commit
```

You can run `pre-commit` before you commit:

```shell
pre-commit run
```
