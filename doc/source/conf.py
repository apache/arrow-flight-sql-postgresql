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
# Configuration file for the Sphinx documentation builder.

# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import pathlib
import re
from datetime import datetime

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'Apache Arrow Flight SQL Adapter for PostgreSQL'
copyright = f'2019-{datetime.now().year}, Apache Software Foundation'
trademark = [
    "Apache Arrow Flight, Arrow Flight, Apache, the Apache feather logo, and the Apache Arrow project logo",
    "are either registered trademarks or trademarks of The Apache Software Foundation in the United States and other countries."
]
author = 'Apache Software Foundation'

version = os.environ.get('VERSION')
if not version:
    meson_build_path = pathlib.Path(__file__).parent / '../../meson.build'
    with open(meson_build_path) as meson_build:
        version = re.search('version: \'(.+?)\'', meson_build.read())[1]
    release = os.environ.get('RELEASE', version)

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'myst_parser',
    'sphinx_inline_tabs',
]

source_suffix = {
    '.md': 'markdown',
}

myst_enable_extensions = [
    'amsmath',
    'attrs_inline',
    # 'colon_fence',
    'deflist',
    'dollarmath',
    'fieldlist',
    'html_admonition',
    'html_image',
    'linkify',
    # 'replacements',
    # 'smartquotes',
    'strikethrough',
    'substitution',
    'tasklist',
]

templates_path = ['_templates']
exclude_patterns = [
    '**/.#*',
]



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'pydata_sphinx_theme'
html_theme_options = {
    'logo': {
        'alt_text': 'Apache Arrow Flight SQL adapter for PostgreSQL',
        'image_light': '_static/logo-light.png',
        'image_dark': '_static/logo-dark.png',
    },
    'github_url': 'https://github.com/apache/arrow-flight-sql-postgresql',
    'switcher': {
        'json_url': 'https://arrow.apache.org/flight-sql-postgresql/devel/_static/switcher.json',
        'version_match': release,
    },
    'navbar_center': [
    ],
    'navbar_end': [
        'theme-switcher.html',
        'navbar-icon-links.html',
        'version-switcher.html',
    ],
    'use_edit_page_button': True,
    'show_nav_level': 2,
    "footer_start": ["copyright", "trademark"],
    "footer_end": ["sphinx-version", "theme-version"]
}
html_context = {
    'github_user': 'apache',
    'github_repo': 'arrow-flight-sql-postgresql',
    'github_version': 'main',
    'doc_path': 'doc/source',
    'trademark': trademark
}
html_static_path = ['_static']
html_favicon = "_static/favicon.ico"
