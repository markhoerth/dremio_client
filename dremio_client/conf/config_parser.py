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
import os

import confuse


def _get_env_args():
    args = dict()
    for k, v in os.environ.items():
        if "DREMIO_" in k and k != "DREMIO_CLIENTDIR":
            name = k.replace("DREMIO_", "").lower().replace("_", ".")
            if name == "port" or name == "auth.timeout":
                v = int(v)
            elif name == "ssl":
                v = v.lower() in ["true", "1", "t", "y", "yes", "yeah", "yup", "certainly", "uh-huh"]
            args[name] = v
    return args


def build_config(args=None):
    config = confuse.Configuration("dremio_client", __name__)
    if args:
        config.set_args(args, dots=True)
    env_args = _get_env_args()
    config.set_args(env_args, dots=True)
    config["auth"]["password"].redact = True
    return config
