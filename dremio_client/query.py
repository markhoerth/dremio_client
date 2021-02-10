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
import logging

try:
    import pandas as pd

    NO_PANDAS = False
except ImportError:
    NO_PANDAS = True

from .flight import query as _flight_query
from .odbc import query as _odbc_query
from .util import run as _rest_query


def query(
    token,
    base_url,
    hostname,
    odbc_port,
    flight_port,
    username,
    password,
    ssl_verify,
    sql,
    pandas=True,
    method="flight",
    context=None,
):
    failed = False
    if method == "flight":
        try:
            return _flight_query(
                sql, hostname=hostname, port=flight_port, username=username, password=password, pandas=pandas
            )
        except Exception:
            logging.warning("Unable to run query as flight, downgrading to odbc")
            failed = True
    if method == "odbc" or failed:
        try:
            return _odbc_query(sql, hostname=hostname, port=odbc_port, username=username, password=password)
        except Exception:
            logging.warning("Unable to run query as odbc, downgrading to rest")
    results = _rest_query(token, base_url, sql, ssl_verify=ssl_verify)
    if pandas and not NO_PANDAS:
        return pd.concat(pd.DataFrame(i['rows']) for i in results)
    return list(results)
