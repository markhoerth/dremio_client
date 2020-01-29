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
import os

from dremio_client.conf import build_config


def test_config():
    config = build_config(None)
    assert config["auth"]["username"].get() == "dremio"
    assert config["auth"]["password"].get() == "dremio123"
    assert config["auth"]["type"].get() == "basic"
    assert config["hostname"].get() == "localhost"
    assert config["port"].get(int) == 9047
    assert config["ssl"].get(bool) is False


def test_config_with_override():
    config = build_config({"hostname": "dremio.url", "ssl": True})
    assert config["hostname"].get() == "dremio.url"
    assert config["ssl"].get(bool) is True
    assert config["auth"]["password"].get() == "dremio123"


def test_config_with_env_override():
    os.environ["DREMIO_AUTH_USERNAME"] = "furby"
    config = build_config()
    assert config["auth"]["username"].get() == "furby"
    assert config["auth"]["password"].get() == "dremio123"
