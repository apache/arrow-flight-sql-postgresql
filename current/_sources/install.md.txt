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

# Install

## Debian GNU/Linux and Ubuntu

Supported versions:

- Debian GNU/Linux bookworm
- Ubuntu 22.04 LTS

Enable the PostgreSQL APT repository:

```bash
sudo apt update
sudo apt install -y ca-certificates gpg lsb-release wget
wget -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | \
  sudo gpg --no-default-keyring --keyring /usr/share/keyrings/pgdg.gpg --import -
(echo "Types: deb"; \
 echo "URIs: http://apt.postgresql.org/pub/repos/apt"; \
 echo "Suites: $(lsb_release --codename --short)-pgdg"; \
 echo "Components: main"; \
 echo "Signed-By: /usr/share/keyrings/pgdg.gpg") | \
  sudo tee /etc/apt/sources.list.d/pgdg.sources
```

Enable the Apache Arrow APT repository:

```bash
wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
rm apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt update
```

Install `postgresql-15-pgdg-apache-arrow-flight-sql`:

```bash
sudo apt install -y postgresql-15-pgdg-apache-arrow-flight-sql
```

See {doc}`configuration` how to configure Apache Arrow Flight SQL adapter for PostgreSQL.

## Source

You need to install the followings before you build Apache Arrow
Flight SQL adapter for PostgreSQL:

- PostgreSQL 15 or later: https://www.postgresql.org/download/
- Apache Arrow C++ with Flight SQL support: https://arrow.apache.org/install/
- Meson: https://mesonbuild.com/
- Ninja: https://ninja-build.org/
- C++ compiler such as `g++` and `clang++

Here are command lines to build Apache Arrow Flight SQL adapter for
PostgreSQL:

```{note}
Replase `${version}` with {{ env.config.version }} or define `version` variable with {{ env.config.version }}.
```

```bash
wget "https://www.apache.org/dyn/closer.lua?action=download&filename=arrow/apache-arrow-flight-sql-postgresql-${version}/apache-arrow-flight-sql-postgresql-${version}.tar.gz"
tar xfv apache-arrow-flight-sql-postgresql-${version}.tar.gz
meson setup \
  --prefix=/usr/local \
  -Dpostgresql_dir=$(pg_config --bindir)/.. \
  apache-arrow-flight-sql-postgresql-${version}.build \
  apache-arrow-flight-sql-postgresql-${version}
meson compile -C apache-arrow-flight-sql-postgresql-${version}.build
sudo meson install -C apache-arrow-flight-sql-postgresql-${version}.build
```

See {doc}`configuration` how to configure Apache Arrow Flight SQL adapter for PostgreSQL.
