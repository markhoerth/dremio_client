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

import attr
import simplejson as json
from six.moves.urllib.parse import quote

from ..error import DremioException
from ..util import refresh_metadata
from .endpoints import (
    catalog_item,
    graph,
    collaboration_tags,
    collaboration_wiki,
    delete_catalog,
    refresh_pds,
    set_catalog,
    update_catalog,
    promote_catalog,
)


@attr.s
class VoteMetadata(object):
    id = attr.ib(default=None)
    votes = attr.ib(default=None)
    datasetId = attr.ib(default=None)  # todo
    datasetPath = attr.ib(default=None)  # todo
    datasetType = attr.ib(default=None)
    datasetReflectionCount = attr.ib(default=None)
    entityType = attr.ib(default=None)

    def to_json(self):
        return json.dumps(attr.asdict(self))


@attr.s
class QueueMetadata(object):
    id = attr.ib(default=None)
    tag = attr.ib(default=None)
    name = attr.ib(default=None)
    cpuTier = attr.ib(default=None)
    maxAllowedRunningJobs = attr.ib(default=None)
    maxStartTimeoutMs = attr.ib(default=None)

    def to_json(self):
        return json.dumps(attr.asdict(self))


@attr.s
class RuleMetadata(object):
    name = attr.ib(default=None)
    conditions = attr.ib(default=None)
    acceptId = attr.ib(default=None)
    acceptName = attr.ib(default=None)
    action = attr.ib(default=None)
    id = attr.ib(default=None)

    def to_json(self):
        return json.dumps(attr.asdict(self))


@attr.s
class ReflectionSummaryMetadata(object):
    entityType = attr.ib(default=None)
    id = attr.ib(default=None)
    createdAt = attr.ib(default=None)
    updatedAt = attr.ib(default=None)
    type = attr.ib(default=None)
    name = attr.ib(default=None)
    datasetId = attr.ib(default=None)
    datasetPath = attr.ib(default=None)
    datasetType = attr.ib(default=None)
    currentSizeBytes = attr.ib(default=None)
    totalSizeBytes = attr.ib(default=None)
    enabled = attr.ib(default=None)
    status = attr.ib(default=None)

    def to_json(self):
        return json.dumps(attr.asdict(self))


@attr.s
class ReflectionMetadata(object):
    entityType = attr.ib(default=None)
    id = attr.ib(default=None)
    tag = attr.ib(default=None)
    name = attr.ib(default=None)
    enabled = attr.ib(default=None)
    createdAt = attr.ib(default=None)
    updatedAt = attr.ib(default=None)
    type = attr.ib(default=None)
    datasetId = attr.ib(default=None)
    currentSizeBytes = attr.ib(default=None)
    totalSizeBytes = attr.ib(default=None)
    status = attr.ib(default=None)
    dimensionFields = attr.ib(default=None)
    measureFields = attr.ib(default=None)
    displayFields = attr.ib(default=None)
    distributionFields = attr.ib(default=None)
    partitionFields = attr.ib(default=None)
    sortFields = attr.ib(default=None)
    partitionDistributionStrategy = attr.ib(default=None)

    def to_json(self):
        return json.dumps(attr.asdict(self))


@attr.s
class RootMetaData(object):
    id = attr.ib(default=None)

    def to_json(self):
        return json.dumps(attr.asdict(self))


@attr.s
class WikiData(object):
    text = attr.ib(default=None)
    version = attr.ib(default=None)

    def to_json(self):
        return json.dumps(attr.asdict(self))


@attr.s
class TagsData(object):
    tags = attr.ib(default=None)
    version = attr.ib(default=None)

    def to_json(self):
        return json.dumps(attr.asdict(self))


@attr.s
class MetadataPolicy(object):
    authTTLMs = attr.ib(default=None)
    datasetRefreshAfterMs = attr.ib(default=None)
    datasetExpireAfterMs = attr.ib(default=None)
    namesRefreshMs = attr.ib(default=None)
    datasetUpdateMode = attr.ib(default=None)

    def to_json(self):
        return json.dumps(attr.asdict(self))


@attr.s
class AccessControl(object):
    id = attr.ib(default=None)
    permissions = attr.ib(default=None)

    def to_json(self):
        return json.dumps(attr.asdict(self))


@attr.s
class AccessControlList(object):
    users = attr.ib(default=None)
    groups = attr.ib(default=None)
    version = attr.ib(default=None)

    def to_json(self):
        return json.dumps(attr.asdict(self))


@attr.s
class SourceState(object):
    status = attr.ib(default=None)
    message = attr.ib(default=None)

    def to_json(self):
        return json.dumps(attr.asdict(self))


@attr.s
class DatasetMetaData(object):
    entityType = attr.ib(default=None)
    id = attr.ib(default=None)
    path = attr.ib(default=None)
    tag = attr.ib(default=None)
    type = attr.ib(default=None)
    fields = attr.ib(default=None)
    createdAt = attr.ib(default=None)
    accelerationRefreshPolicy = attr.ib(default=None)
    sql = attr.ib(default=None)
    sqlContext = attr.ib(default=None)
    format = attr.ib(default=None)
    approximateStatisticsAllowed = attr.ib(default=None)
    accessControlList = attr.ib(default=None)

    def to_json(self):
        return json.dumps(attr.asdict(self))


@attr.s
class SpaceMetaData(object):
    entityType = attr.ib(default=None)
    id = attr.ib(default=None)
    name = attr.ib(default=None)
    tag = attr.ib(default=None)
    path = attr.ib(default=None)
    accessControlList = attr.ib(default=None)

    def to_json(self):
        return json.dumps(attr.asdict(self))


@attr.s
class FolderMetaData(object):
    entityType = attr.ib(default=None)
    id = attr.ib(default=None)
    path = attr.ib(default=None)
    tag = attr.ib(default=None)
    accessControlList = attr.ib(default=None)

    def to_json(self):
        return json.dumps(attr.asdict(self))


@attr.s
class FileMetaData(object):
    entityType = attr.ib(default=None)
    id = attr.ib(default=None)
    path = attr.ib(default=None)
    accessControlList = attr.ib(default=None)

    def to_json(self):
        return json.dumps(attr.asdict(self))


@attr.s
class SourceMetadata(object):
    entityType = attr.ib(default=None)
    id = attr.ib(default=None)
    name = attr.ib(default=None)
    description = attr.ib(default=None)
    tag = attr.ib(default=None)
    type = attr.ib(default=None)
    config = attr.ib(default=None)
    createdAt = attr.ib(default=None)
    metadataPolicy = attr.ib(default=None)
    state = attr.ib(default=None)
    accelerationGracePeriodMs = attr.ib(default=None)
    accelerationRefreshPeriodMs = attr.ib(default=None)
    accelerationNeverExpire = attr.ib(default=None)
    accelerationNeverRefresh = attr.ib(default=None)
    path = attr.ib(default=None)
    accessControlList = attr.ib(default=None)

    def to_json(self):
        return json.dumps(attr.asdict(self))


def _clean(string):
    return string.replace('"', "").replace(" ", "_").replace("-", "_").replace("@", "").replace(".", "_")


def get_path(item, trim_path):
    path = item.get("path", [item.get("name", None)])
    return path[trim_path:] if trim_path > 0 else path


def create(item, token, base_url, flight_endpoint, trim_path=0, ssl_verify=True, dirty=False):
    path = get_path(item, trim_path)
    name = _clean("_".join(path))
    obj_type = item.get("type", None)
    container_type = item.get("containerType", None)
    entity_type = item.get("entityType", None)
    sql = item.get("sql", None)
    if obj_type == "CONTAINER":
        if container_type == "HOME":
            return name, Home(token, base_url, flight_endpoint, ssl_verify, dirty, **item)
        elif container_type == "SPACE":
            return name, Space(token, base_url, flight_endpoint, ssl_verify, dirty, **item)
        elif container_type == "SOURCE":
            return name, Source(token, base_url, flight_endpoint, ssl_verify, dirty, **item)
        elif container_type == "FOLDER":
            return name, Folder(token, base_url, flight_endpoint, ssl_verify, dirty, **item)
    else:
        if obj_type == "DATASET":
            if sql:
                return name, VirtualDataset(token, base_url, flight_endpoint, ssl_verify, dirty, **item)
            else:
                return name, PhysicalDataset(token, base_url, flight_endpoint, ssl_verify, dirty, **item)
        elif obj_type == "FILE":
            return name, File(token, base_url, flight_endpoint, ssl_verify, dirty, **item)
        if entity_type == "source":
            return name, Source(token, base_url, flight_endpoint, ssl_verify, dirty, **item)
        elif entity_type == "folder":
            return name, Folder(token, base_url, flight_endpoint, ssl_verify, dirty, **item)
        elif entity_type == "home":
            return name, Home(token, base_url, flight_endpoint, ssl_verify, dirty, **item)
        elif entity_type == "space":
            return name, Space(token, base_url, flight_endpoint, ssl_verify, dirty, **item)
        elif entity_type == "dataset":
            if "VIRTUAL" in obj_type:
                return name, VirtualDataset(token, base_url, flight_endpoint, ssl_verify, dirty, **item)
            else:
                return name, PhysicalDataset(token, base_url, flight_endpoint, ssl_verify, dirty, **item)
    raise KeyError("unsupported type")


class Catalog(dict):
    def __init__(self, token=None, base_url=None, flight_endpoint=None, ssl_verify=True, dirty=False):
        dict.__init__(self)
        self._base_url = base_url
        self._token = token
        self._flight_endpoint = flight_endpoint
        self._ssl_verify = ssl_verify
        self._dirty = dirty
        self.meta = None

        def try_id_and_path(x, y):
            try:
                return catalog_item(token, base_url, x, ssl_verify=ssl_verify)
            except Exception:  # NOQA
                return catalog_item(token, base_url, path=y, ssl_verify=ssl_verify)

        self._catalog_item = try_id_and_path

    def keys(self):
        keys = dict.keys(self)
        return [i for i in keys if i not in {"_catalog_item", "_base_url", "_token", "_flight_endpoint"}]

    def delete(self):
        _delete(self)

    def commit(self):
        if self._dirty:
            if self.meta.id:
                self.meta = _put(self)
            else:
                self.meta = _post(self)
            self._dirty = False

    def get(self):
        dir(self)
        return self

    def refresh(self):
        try:
            for i in dir(self):
                if isinstance(self[i], Catalog):
                    del self[i]
        except:  # NOQA
            pass

    def insert(self, entity_type, name, commit=True, **kwargs):
        if entity_type == "space":
            obj = create_space(self, name)
        elif entity_type == "folder":
            try:
                path = self.meta.path
                if not path:
                    raise KeyError
            except:  # NOQA
                path = [self.meta.name]
                if not path:
                    path = []
            obj = create_folder(self, path + [name])
        elif entity_type == "vds":
            try:
                path = self.meta.path
                if not path:
                    raise KeyError
            except:  # NOQA
                path = [self.meta.name]
                if not path:
                    path = []
            obj = create_vds(self, path + [name], kwargs["sql"], kwargs.get("sqlContext", None))
        else:
            raise NotImplementedError
        self[name] = obj
        if commit:
            self[name].commit()

    def promote(self, file_format="parquet", **kwargs):
        """ promote this file/folder to a PDS

        .. note:: can only be run on a file or folder in a Source

        :param file_format: file format of the file/folder (only parquet, xls, excel, json, text are accepted)
        :return None
        """
        if file_format == "parquet":
            format_dict = {"type": "Parquet"}
        elif file_format == "json":
            format_dict = {"type": "JSON"}
        elif file_format == "csv":
            format_dict = {
                "type": "Text",
                "fieldDelimiter": kwargs.get("fieldDelimiter"),
                "lineDelimiter": kwargs.get("lineDelimiter"),
                "quote": kwargs.get("quote"),
                "comment": kwargs.get("comment"),
                "escape": kwargs.get("escape"),
                "skipFirstLine": kwargs.get("skipFirstLine", True),
                "extractHeader": kwargs.get("extractHeader", True),
                "trimHeader": kwargs.get("trimHeader", True),
                "autoGenerateColumnNames": kwargs.get("autoGenerateColumnNames", True),
            }
        elif file_format == "excel":
            format_dict = {
                "type": "Excel",
                "sheetName": kwargs.get("sheetName"),
                "extractHeader": kwargs.get("extractHeader", True),
                "hasMergedCells": kwargs.get("hasMergedCells", True),
            }
        elif file_format == "xls":
            format_dict = {
                "type": "XLS",
                "sheetName": kwargs.get("sheetName"),
                "extractHeader": kwargs.get("extractHeader", True),
                "hasMergedCells": kwargs.get("hasMergedCells", True),
            }
        else:
            raise NotImplementedError("{} format is not applicable".format(file_format))
        cid = quote(self.meta.id, safe="")
        promote_catalog(
            self._token,
            self._base_url,
            cid,
            {
                "id": cid,
                "path": self.meta.path,
                "type": "PHYSICAL_DATASET",
                "entityType": "dataset",
                "format": format_dict,
            },
            self._ssl_verify,
        )

    def __dir__(self):
        if len(self.keys()) == 0 and "meta" in self.__dict__:
            if self.meta.entityType in {"source", "home", "space", "folder", "root", "dataset"}:
                result = self._catalog_item(
                    self.meta.id if hasattr(self.meta, "id") else None,
                    self.meta.path if hasattr(self.meta, "path") else None,
                )
                _, obj = create(result, self._token, self._base_url, self._flight_endpoint, ssl_verify=self._ssl_verify)
                self.update(obj)
                self.meta = attr.evolve(self.meta, **{k: v for k, v in attr.asdict(obj.meta).items() if v})
                return list(self.keys())
        return list(self.keys()) + ["_repr_html_"]

    def to_json(self):
        result = attr.asdict(self.meta)
        children = list()
        for child in self.keys():
            try:
                children.append(child.to_json())
            except:  # NOQA
                pass
        if len(children) > 1:
            result["children"] = children
        return json.dumps(result)

    def __getattr__(self, item):
        if item == "_ipython_canary_method_should_not_exist_":
            raise AttributeError
        try:
            value = dict.__getitem__(self, item)
            if value is None:
                raise KeyError()
            if isinstance(value, Catalog) and value["_base_url"] is None:
                raise KeyError()
            return value
        except KeyError:
            self.__dir__()
            return dict.__getitem__(self, item)

    def wiki(self):
        result = collaboration_wiki(self._token, self._base_url, self.meta.id, ssl_verify=self._ssl_verify)
        return make_wiki(result)

    def tags(self):
        result = collaboration_tags(self._token, self._base_url, self.meta.id, ssl_verify=self._ssl_verify)
        return make_tags(result)

    def __repr__(self):
        return self.to_json()

    def _repr_html_(self):
        try:
            tags = self.tags().tags
            tags_html = "\n".join(['<span class="badge">{}</span>'.format(i) for i in tags])
            tag_html = "<div> <strong>Tags: </strong> {} </div>".format(tags_html)
        except DremioException:
            tag_html = ""
        try:
            import markdown

            wiki = self.wiki()
            text = wiki.text
            html = markdown.markdown(text)
            return tag_html + html
        except ImportError:
            return self.__repr__()
        except DremioException:
            return self.__repr__()

    def __delitem__(self, key):
        return self.remove()

    def remove(self):
        return delete_catalog(self._token, self._base_url, self.meta.id, self.meta.tag, self._ssl_verify)


def _get_item(catalog, cid=None, path=None):
    result = catalog._catalog_item(cid, path)
    _, obj = create(result, catalog._token, catalog._base_url, catalog._flight_endpoint, ssl_verify=catalog._ssl_verify)
    return obj


def _put(self):
    json = {k: v for k, v in attr.asdict(self.meta).items() if v}
    for i in ("state", "createdAt"):
        if i in json:
            del json[i]
    cid = self.meta.id
    
    result = update_catalog(self._token, self._base_url, cid, json, self._ssl_verify)
    _, obj = create(result, self._token, self._base_url, self._flight_endpoint, ssl_verify=self._ssl_verify)
    return obj.meta


def _delete(self):
    cid = self.meta.id
    delete_catalog(self._token, self._base_url, cid, self.meta.tag, self._ssl_verify)
    return None


def _post(self):
    json = {k: v for k, v in attr.asdict(self.meta).items() if v}
    for i in ("state", "id", "tag", "createdAt"):
        if i in json:
            del json[i]
    result = set_catalog(self._token, self._base_url, json, self._ssl_verify)
    _, obj = create(result, self._token, self._base_url, self._flight_endpoint, ssl_verify=self._ssl_verify)
    return obj.meta


class Root(Catalog):
    def __init__(self, token=None, base_url=None, flight_endpoint=None, ssl_verify=True, dirty=False):
        Catalog.__init__(self, token, base_url, flight_endpoint, ssl_verify, dirty)
        self.meta = RootMetaData("root")

    def add(self, item):
        name, obj = create(item, self._token, self._base_url, self._flight_endpoint, ssl_verify=self._ssl_verify)
        self[name] = obj

    def add_by_path(self, item, new_entity=True):
        if new_entity:
            cid = item.pop("id")
            tag = item.pop("tag")
        name, obj = create(
            item, self._token, self._base_url, self._flight_endpoint, ssl_verify=self._ssl_verify, dirty=True
        )
        if new_entity:
            item["id"] = cid  # NOQA
            item["tag"] = tag  # NOQA
        base = self
        subpath = list()
        for p in obj.meta.path[:-1]:
            subpath.append(_clean(p))
            try:
                base = base[_clean(p)]
            except KeyError as e:
                if isinstance(obj, PhysicalDataset):
                    self.add({"entityType": "folder", "path": subpath})
                else:
                    raise e
        base[_clean(obj.meta.path[-1])] = obj


def _get_acl(acl):
    if not acl:
        return
    return [AccessControl(ac.get("id"), ac.get("permissions")) for ac in acl]


def _get_acls(acl):
    if not acl:
        return
    return AccessControlList(
        users=_get_acl(acl.get("users")), groups=_get_acl(acl.get("groups")), version=acl.get("version")
    )


class Space(Catalog):
    def __init__(self, token=None, base_url=None, flight_endpoint=None, ssl_verify=True, dirty=False, **kwargs):
        Catalog.__init__(self, token, base_url, flight_endpoint, ssl_verify, dirty)
        self.meta = SpaceMetaData(
            entityType="space",
            id=kwargs.get("id"),
            tag=kwargs.get("tag"),
            name=kwargs.get("name"),
            path=kwargs.get("path"),
            accessControlList=_get_acls(kwargs.get("accessControlList")),
        )
        for child in kwargs.get("children", list()):
            name, item = create(
                child,
                token,
                base_url,
                self._flight_endpoint,
                trim_path=len(child.get("path", list())) - 1,
                ssl_verify=self._ssl_verify,
            )
            self[name] = item


class Home(Space):
    def __init__(self, token=None, base_url=None, flight_endpoint=None, ssl_verify=True, dirty=False, **kwargs):
        Space.__init__(self, token, base_url, flight_endpoint, ssl_verify, dirty, **kwargs)
        self.meta = attr.evolve(self.meta, entityType="home")


class Folder(Catalog):
    def __init__(self, token=None, base_url=None, flight_endpoint=None, ssl_verify=True, dirty=False, **kwargs):
        Catalog.__init__(self, token, base_url, flight_endpoint, ssl_verify, dirty)
        self.meta = FolderMetaData(
            entityType="folder",
            id=kwargs.get("id", None),
            tag=kwargs.get("tag", None),
            path=kwargs.get("path", None),
            accessControlList=_get_acls(kwargs.get("accessControlList")),
        )
        for child in kwargs.get("children", list()):
            name, item = create(
                child,
                token,
                base_url,
                self._flight_endpoint,
                trim_path=len(kwargs.get("path", list())),
                ssl_verify=self._ssl_verify,
            )
            self[name] = item


class File(Catalog):
    def __init__(self, token=None, base_url=None, flight_endpoint=None, ssl_verify=True, dirty=False, **kwargs):
        Catalog.__init__(self, token, base_url, flight_endpoint, ssl_verify, dirty)
        self.meta = FileMetaData(
            entityType="file",
            id=kwargs.get("id", None),
            path=kwargs.get("path", None),
            accessControlList=_get_acls(kwargs.get("accessControlList")),
        )


def _get_source_type(source_type):
    return source_type  # todo may do more with this at some point


def _get_source_config(config):
    return config  # todo these should be turned into source specific objects. Will do at some stage


def _get_metadata_policy(metadata_policy):
    if not metadata_policy:
        return None
    return MetadataPolicy(
        authTTLMs=metadata_policy.get("authTTLMs"),
        datasetRefreshAfterMs=metadata_policy.get("datasetRefreshAfterMs"),
        datasetExpireAfterMs=metadata_policy.get("datasetExpireAfterMs"),
        namesRefreshMs=metadata_policy.get("namesRefreshMs"),
        datasetUpdateMode=metadata_policy.get("datasetUpdateMode"),
    )


def _get_source_state(state):
    if not state:
        return None
    return SourceState(status=state.get("status"), message=state.get("message"))


def _get_source_meta(kwargs):
    return SourceMetadata(
        entityType="source",
        id=kwargs.get("id"),
        name=kwargs.get("name"),
        description=kwargs.get("description"),
        tag=kwargs.get("tag"),
        type=_get_source_type(kwargs.get("type")),
        config=_get_source_config(kwargs.get("config")),
        createdAt=kwargs.get("createdAt"),
        metadataPolicy=_get_metadata_policy(kwargs.get("metadataPolicy")),
        state=_get_source_state(kwargs.get("state")),
        accelerationGracePeriodMs=kwargs.get("accelerationGracePeriodMs"),
        accelerationRefreshPeriodMs=kwargs.get("accelerationRefreshPeriodMs"),
        accelerationNeverExpire=kwargs.get("accelerationNeverExpire"),
        accelerationNeverRefresh=kwargs.get("accelerationNeverRefresh"),
        path=kwargs.get("path"),
        accessControlList=_get_acls(kwargs.get("accessControlList")),
    )


class Source(Catalog):
    def __init__(self, token=None, base_url=None, flight_endpoint=None, ssl_verify=True, dirty=False, **kwargs):
        Catalog.__init__(self, token, base_url, flight_endpoint, ssl_verify, dirty)
        self.meta = _get_source_meta(kwargs)
        path = self.meta.path
        for child in kwargs.get("children", list()):
            name, item = create(
                child,
                token,
                base_url,
                self._flight_endpoint,
                trim_path=(len(path) if path else 1),
                ssl_verify=self._ssl_verify,
            )
            self[name] = item


class Dataset(Catalog):
    def __init__(self, token=None, base_url=None, flight_endpoint=None, ssl_verify=True, dirty=False, **kwargs):
        Catalog.__init__(self, token, base_url, flight_endpoint, ssl_verify, dirty)
        self.meta = DatasetMetaData(
            entityType="dataset",
            id=kwargs.get("id"),
            path=kwargs.get("path"),
            tag=kwargs.get("tag"),
            type=kwargs.get("type"),
            fields=kwargs.get("fields"),
            createdAt=kwargs.get("createdAt"),
            accelerationRefreshPolicy=kwargs.get("accelerationRefreshPolicy"),
            sql=kwargs.get("sql"),
            sqlContext=kwargs.get("sqlContext"),
            format=kwargs.get("format"),
            approximateStatisticsAllowed=kwargs.get("approximateStatisticsAllowed"),
            accessControlList=_get_acls(kwargs.get("accessControlList")),
        )

    def get_graph(self):
        try:
            return graph(self._token, self._base_url, self.meta.id, ssl_verify=self._ssl_verify)
        except Exception:  # NOQA
            return graph(self._token, self._base_url, path=self.meta.path, ssl_verify=self._ssl_verify)

    def get_table(self):
        return '.'.join('"{0}"'.format(w) for w in self.meta.path)

    def query(self):
        return self.sql("select * from " + self.get_table() + " limit 1000")

    def sql(self, sql):
        return self._flight_endpoint(sql)


class PhysicalDataset(Dataset):
    def __init__(self, token=None, base_url=None, flight_endpoint=None, ssl_verify=True, dirty=False, **kwargs):
        Dataset.__init__(self, token, base_url, flight_endpoint, ssl_verify, dirty, **kwargs)

    def metadata_refresh(self):
        refresh_metadata(self._token, self._base_url, ".".join(self.meta.path))

    def refresh(self):
        refresh_pds(self._token, self._base_url, self.meta.id, self._ssl_verify)


class VirtualDataset(Dataset):
    def __init__(self, token=None, base_url=None, flight_endpoint=None, ssl_verify=True, dirty=False, **kwargs):
        Dataset.__init__(self, token, base_url, flight_endpoint, ssl_verify, dirty, **kwargs)


def make_reflection(data, summary=False):
    if summary:
        return ReflectionSummaryMetadata(
            entityType="reflection-summary",
            id=data.get("id"),
            createdAt=data.get("createdAt"),
            updatedAt=data.get("updatedAt"),
            type=data.get("type"),
            name=data.get("name"),
            datasetId=data.get("datasetId"),
            datasetPath=data.get("datasetPath"),
            datasetType=data.get("datasetType"),
            currentSizeBytes=data.get("currentSizeBytes"),
            totalSizeBytes=data.get("totalSizeBytes"),
            enabled=data.get("enabled"),
            status=data.get("status"),
        )
    return ReflectionMetadata(
        entityType="reflection",
        id=data.get("id"),
        tag=data.get("tag"),
        name=data.get("name"),
        enabled=data.get("enabled"),
        createdAt=data.get("createdAt"),
        updatedAt=data.get("updatedAt"),
        type=data.get("type"),
        datasetId=data.get("datasetId"),  # todo link to dataset
        currentSizeBytes=data.get("currentSizeBytes"),
        totalSizeBytes=data.get("totalSizeBytes"),
        status=data.get("status"),  # todo object
        dimensionFields=data.get("dimensionFields"),  # todo object
        measureFields=data.get("measureFields"),  # todo object
        displayFields=data.get("displayFields"),  # todo object
        distributionFields=data.get("distributionFields"),  # todo object
        partitionFields=data.get("partitionFields"),  # todo object
        sortFields=data.get("sortFields"),  # todo object
        partitionDistributionStrategy=data.get("partitionDistributionStrategy"),
    )


def make_tags(tags):
    return TagsData(tags=tags.get("tags"), version=tags.get("version"))


def make_wiki(wiki):
    return WikiData(text=wiki.get("text"), version=wiki.get("version"))


def make_wlm_rule(rule):
    return RuleMetadata(
        id=rule.get("id"),
        conditions=rule.get("conditions"),
        name=rule.get("name"),
        acceptId=rule.get("acceptId"),
        acceptName=rule.get("acceptName"),
        action=rule.get("action"),
    )


def make_wlm_queue(queue):
    return QueueMetadata(
        id=queue.get("id"),
        tag=queue.get("tag"),
        name=queue.get("name"),
        cpuTier=queue.get("cpuTier"),
        maxAllowedRunningJobs=queue.get("maxAllowedRunningJobs"),
        maxStartTimeoutMs=queue.get("maxStartTimeoutMs"),
    )


def make_vote(vote):
    return VoteMetadata(
        id=vote.get("id"),
        votes=vote.get("votes"),
        datasetId=vote.get("datasetId"),
        datasetPath=vote.get("datasetPath"),
        datasetType=vote.get("datasetType"),
        datasetReflectionCount=vote.get("datasetReflectionCount"),
        entityType="vote-summary",
    )


def create_vds(catalog, path, sql, sqlContext):
    return VirtualDataset(
        catalog._token,
        catalog._base_url,
        catalog._flight_endpoint,
        catalog._ssl_verify,
        True,
        path=path,
        sql=sql,
        sqlContext=sqlContext,
        entityType="dataset",
        type="VIRTUAL_DATASET",
    )


def create_space(catalog, name):
    return Space(catalog._token, catalog._base_url, catalog._flight_endpoint, catalog._ssl_verify, True, name=name)


def create_folder(catalog, path):
    return Folder(catalog._token, catalog._base_url, catalog._flight_endpoint, catalog._ssl_verify, True, path=path)
