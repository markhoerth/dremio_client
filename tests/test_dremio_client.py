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
# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function
import json

from click.testing import CliRunner

from dremio_client import cli
from dremio_client.auth import basic_auth
from dremio_client.model.catalog import catalog


def test_command_line_interface(requests_mock):
    """Test the CLI."""
    requests_mock.post("http://localhost:9047/apiv2/login", text=json.dumps({"token": "12345"}))
    with open("tests/data/sql.json", "r+") as f:
        txt = json.dumps(json.load(f))
    requests_mock.post("http://localhost:9047/api/v3/sql", text=txt)
    with open("tests/data/job_status.json", "r+") as f:
        txt = json.dumps(json.load(f))
    requests_mock.get("http://localhost:9047/api/v3/job/22b3b4fe-669a-4789-a9de-b1fc5ba7b500", text=txt)
    with open("tests/data/job_results.json", "r+") as f:
        txt = json.dumps(json.load(f))
    requests_mock.get("http://localhost:9047/api/v3/job/22b3b4fe-669a-4789-a9de-b1fc5ba7b500/results", text=txt)
    runner = CliRunner()
    result = runner.invoke(cli.query, ["--sql", "select * from sys.options"], obj={}, catch_exceptions=False)
    assert result.exit_code == 0
    assert "[]\n" in result.output
    help_result = runner.invoke(cli.query, ["--help"])
    assert help_result.exit_code == 0
    assert "execute a query given by sql and print results" in help_result.output


def test_auth(requests_mock):
    requests_mock.post("https://example.com/apiv2/login", text=json.dumps({"token": "12345"}))
    token = basic_auth("https://example.com", "foo", "bar")
    assert token == "12345"


def test_catalog(requests_mock):
    with open("tests/data/catalog.json", "r+") as f:
        txt = json.dumps(json.load(f))
    requests_mock.get("https://example.com/api/v3/catalog", text=txt)
    with open("tests/data/testsource.json", "r+") as f:
        txt = json.dumps(json.load(f))
    requests_mock.get("https://example.com/api/v3/catalog/71373b4d-b284-4007-bdc1-2c4d245563ec", text=txt)
    with open("tests/data/nyctaxi.json", "r+") as f:
        txt = json.dumps(json.load(f))
    requests_mock.get("https://example.com/api/v3/catalog/by-path/testsource/%22nyctaxi%22", text=txt)

    token = "12345"
    c = catalog(token, "https://example.com", print)
    assert "testsource" in dir(c)
    assert "nyctaxi" in dir(c.testsource)
    assert "foo_csv" in dir(c.testsource.nyctaxi)


def test_dataset(requests_mock):
    with open("tests/data/catalog.json", "r+") as f:
        txt = json.dumps(json.load(f))
    requests_mock.get("https://example.com/api/v3/catalog", text=txt)
    with open("tests/data/testsource.json", "r+") as f:
        txt = json.dumps(json.load(f))
    requests_mock.get("https://example.com/api/v3/catalog/71373b4d-b284-4007-bdc1-2c4d245563ec", text=txt)
    with open("tests/data/profiles.json", "r+") as f:
        txt = json.dumps(json.load(f))
    requests_mock.get("https://example.com/api/v3/catalog/by-path/testsource/profiles", text=txt)

    token = "12345"
    c = catalog(token, "https://example.com", lambda x: x)
    sql = c.testsource.profiles.sql
    assert sql("hello") == "hello"
