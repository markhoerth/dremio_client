#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2019 Ryan Murray.
#
# This file is part of Dremio Client
# (see https://github.com/rymurr/dremio_client).
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
#

"""The setup script."""

from setuptools import find_packages, setup


with open("README.rst") as readme_file:
    readme = readme_file.read()

with open("HISTORY.rst") as history_file:
    history = history_file.read()

requirements = ["Click>=6.0", "requests>=2.21.0", "confuse", "simplejson", "attrs", "six"]

requirements_noarrow = ["pandas>=0.24.2", "requests-futures==1.0.0", "markdown"]

requirements_full = ["pyarrow>=0.15.0", "pandas>=0.24.2", "requests-futures==1.0.0", "markdown"]

setup_requirements = []

test_requirements = []

setup(
    author="Ryan Murray",
    author_email="rymurr@gmail.com",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
    description="Python client for Dremio. See https://dremio.com",
    install_requires=requirements,
    license="Apache Software License 2.0",
    long_description=readme + "\n\n" + history,
    include_package_data=True,
    keywords="dremio_client",
    name="dremio_client",
    packages=find_packages(
        include=[
            "dremio_client",
            "dremio_client.flight",
            "dremio_client.auth",
            "dremio_client.model",
            "dremio_client.util",
            "dremio_client.conf",
        ]
    ),
    setup_requires=setup_requirements,
    test_suite="tests",
    tests_require=test_requirements,
    url="https://github.com/rymurr/dremio_client",
    version="0.10.1",
    zip_safe=False,
    extras_require={
        ':python_version == "2.7"': ["futures"],
        "full": requirements_full,
        "noarrow": requirements_noarrow,
    },
    entry_points={"console_scripts": ["dremio_client=dremio_client.cli:cli"]},
)
