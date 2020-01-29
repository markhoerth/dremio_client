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
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
from __future__ import absolute_import, division, print_function

import pytest
from dremio_client.error import (
    DremioNotFoundException,
    DremioPermissionException,
    DremioUnauthorizedException,
)
from dremio_client.model.endpoints import (
    catalog,
    catalog_item,
    job_results,
    job_status,
    sql,
)


def test_catalog(requests_mock):
    requests_mock.get("http://localhost:9047/api/v3/catalog", status_code=401, reason="Unauthorized for url")
    with pytest.raises(DremioUnauthorizedException):
        catalog("1234", "http://localhost:9047")


def test_catalog_item(requests_mock):
    requests_mock.get("http://localhost:9047/api/v3/catalog/a", status_code=401, reason="Unauthorized for url")
    with pytest.raises(DremioUnauthorizedException):
        catalog_item("1234", "http://localhost:9047", "a", "b")
    requests_mock.get("http://localhost:9047/api/v3/catalog/b", status_code=403, reason="Unauthorized for url")
    with pytest.raises(DremioPermissionException):
        catalog_item("1234", "http://localhost:9047", "b", "b")
    requests_mock.get("http://localhost:9047/api/v3/catalog/by-path/b", status_code=404, reason="Unauthorized for url")
    with pytest.raises(DremioNotFoundException):
        catalog_item("1234", "http://localhost:9047", None, "b")


def test_sql(requests_mock):
    requests_mock.post("http://localhost:9047/api/v3/sql", status_code=401, reason="Unauthorized for url")
    with pytest.raises(DremioUnauthorizedException):
        sql("1234", "http://localhost:9047", "a")


def test_jobstatus(requests_mock):
    requests_mock.get("http://localhost:9047/api/v3/job/1", status_code=401, reason="Unauthorized for url")
    with pytest.raises(DremioUnauthorizedException):
        job_status("1234", "http://localhost:9047", "1")
    requests_mock.get("http://localhost:9047/api/v3/job/1", status_code=403, reason="Unauthorized for url")
    with pytest.raises(DremioPermissionException):
        job_status("1234", "http://localhost:9047", "1")
    requests_mock.get("http://localhost:9047/api/v3/job/1", status_code=404, reason="Unauthorized for url")
    with pytest.raises(DremioNotFoundException):
        job_status("1234", "http://localhost:9047", "1")


def test_jobresults(requests_mock):
    requests_mock.get("http://localhost:9047/api/v3/job/1/results", status_code=401, reason="Unauthorized for url")
    with pytest.raises(DremioUnauthorizedException):
        job_results("1234", "http://localhost:9047", "1")
    requests_mock.get("http://localhost:9047/api/v3/job/1/results", status_code=403, reason="Unauthorized for url")
    with pytest.raises(DremioPermissionException):
        job_results("1234", "http://localhost:9047", "1")
    requests_mock.get("http://localhost:9047/api/v3/job/1/results", status_code=404, reason="Unauthorized for url")
    with pytest.raises(DremioNotFoundException):
        job_results("1234", "http://localhost:9047", "1")
