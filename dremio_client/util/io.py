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
from collections import defaultdict

import simplejson as json
from dremio_client.error import (
    DremioBadRequestException,
    DremioNotFoundException,
)


def _recursve_catalog(catalog):
    data = []
    acls = []
    collabs = {"tags": list(), "wiki": list()}
    for name in catalog:
        item = catalog[name].get()
        if item.meta.entityType == "home":
            continue
        if item.meta.entityType != "file":
            if item.meta.entityType == "source" or item.meta.entityType == "dataset":
                data.append(item)
                acls.append(item.meta["accessControlList"])
                try:
                    collabs["tags"].append(item.tags())
                except (DremioBadRequestException, DremioNotFoundException):
                    pass
                try:
                    collabs["wiki"].append(item.wiki())
                except (DremioBadRequestException, DremioNotFoundException):
                    pass
            else:
                data.append(item)
                d, a, c = _recursve_catalog(item)
                data.extend(d)
                acls.extend(a)
                collabs["tags"].extend(c["tags"])
                collabs["wiki"].extend(c["wiki"])
    return data, acls, collabs


def serialize_catalog(catalog, with_extra=False):
    """
    export a catalog as json

    :note: Enterprise only: optionally include acl and collaboration data
    :param catalog: dremio data catalog (or sub-catalog)
    :param with_extra: export acl and collaboration data
    :return: json string
    """
    data, acls, collabs = _recursve_catalog(catalog)
    if with_extra:
        return json.dumps(data), json.dumps(acls), json.dumps(collabs)
    else:
        return json.dumps(data)


def deserialize_catalog(catalog, client):
    """
    turn a json string into a catalog/sub-catalog for a given client

    :param catalog: json document representing output
    :param client: client for a dremio host
    :return: fully reconstituted catalog
    """
    try:
        data = json.loads(catalog)
    except Exception:
        data = catalog
    for item in data:
        client.data.add_by_path(json.loads(item))
    return client


def commit_all(catalog):
    """
    commit all changed catalog entities in sorted order

    :param catalog: updated catalog
    :return: None
    """
    data = _sort(catalog)
    for i in ("space", "folder", "dataset"):
        for x in data[i]:
            x.commit()


def _sort(catalog):
    data = defaultdict(list)
    for name in catalog:
        item = catalog[name]
        if item.meta.entityType == "home":
            continue
        if item.meta.entityType != "file":
            if item.meta.entityType == "source" or item.meta.entityType == "dataset":
                data[item.meta.entityType].append(item)
            else:
                data[item.meta.entityType].append(item)
                xxx = _sort(item)
                [data[i].extend(xxx[i]) for i in xxx.keys()]
    return data
