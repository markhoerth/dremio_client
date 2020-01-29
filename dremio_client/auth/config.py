# -*- coding: utf-8 -*-
from confuse import NotFoundError

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
from .basic import login as _login


def login(base_url, config_dict):
    """
    Log into dremio using basic auth and looking for config
    :param base_url: Dremio url
    :param config_dict: config dict
    :param timeout: optional timeout
    :return: auth token
    """
    username = config_dict["auth"]["username"].get()
    if not username:
        raise RuntimeError("No username available, can't login")
    password = config_dict["auth"]["password"].get()
    if not password:
        raise RuntimeError("No password available, can't login")
    try:
        timeout = config_dict["auth"]["timeout"].get(int)
    except NotFoundError:
        timeout = 10
    try:
        verify = config_dict["verify"].get(bool)
    except NotFoundError:
        verify = 10
    return _login(base_url, username, password, timeout, verify)
