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
import requests
import json as jsonlib
from requests.exceptions import HTTPError
from six.moves.urllib.parse import quote

from ..error import (
    DremioBadRequestException,
    DremioException,
    DremioNotFoundException,
    DremioPermissionException,
    DremioUnauthorizedException,
)


def _get_headers(token):
    headers = {"Authorization": "_dremio{}".format(token), "content-type": "application/json"}
    return headers


def _get(url, token, details="", ssl_verify=True):
    r = requests.get(url, headers=_get_headers(token), verify=ssl_verify)
    return _check_error(r, details)


def _post(url, token, json=None, details="", ssl_verify=True):
    if isinstance(json, str):
        json = jsonlib.loads(json)
    r = requests.post(url, headers=_get_headers(token), verify=ssl_verify, json=json)
    return _check_error(r, details)


def _delete(url, token, details="", ssl_verify=True):
    r = requests.delete(url, headers=_get_headers(token), verify=ssl_verify)
    return _check_error(r, details)


def _put(url, token, json=None, details="", ssl_verify=True):
    if isinstance(json, str):
        json = jsonlib.loads(json)
    r = requests.put(url, headers=_get_headers(token), verify=ssl_verify, json=json)
    return _check_error(r, details)


def _check_error(r, details=""):
    error, code, _ = _raise_for_status(r)
    if not error:
        try:
            data = r.json()
            return data
        except:  # NOQA
            return r.text
    if code == 400:
        raise DremioBadRequestException("Requested object does not exist on entity " + details, error, r)
    if code == 401:
        raise DremioUnauthorizedException("Unauthorized on api endpoint " + details, error, r)
    if code == 403:
        raise DremioPermissionException("Not permissioned to view entity at " + details, error, r)
    if code == 404:
        raise DremioNotFoundException("No entity exists at " + details, error, r)
    raise DremioException("unknown error", error)


def catalog_item(token, base_url, cid=None, path=None, ssl_verify=True):
    """fetch a specific catalog item by id or by path

    https://docs.dremio.com/rest-api/catalog/get-catalog-id.html
    https://docs.dremio.com/rest-api/catalog/get-catalog-path.html

    :param token: auth token from previous login attempt
    :param base_url: base Dremio url
    :param cid: unique dremio id for resource
    :param path: list ['space', 'folder', 'vds']
    :param ssl_verify: ignore ssl errors if False
    :return: json of resource
    """
    if cid is None and path is None:
        raise TypeError("both id and path can't be None for a catalog_item call")
    idpath = (cid if cid else "") + ", " + (".".join(path) if path else "")
    cpath = [quote(i, safe="") for i in path] if path else ""
    endpoint = "/{}".format(cid) if cid else "/by-path/{}".format("/".join(cpath).replace('"', ""))
    return _get(base_url + "/api/v3/catalog{}".format(endpoint), token, idpath, ssl_verify=ssl_verify)


def catalog(token, base_url, ssl_verify=True):
    """
    https://docs.dremio.com/rest-api/catalog/get-catalog.html populate the root dremio catalog

    :param token: auth token from previous login attempt
    :param base_url: base Dremio url
    :param ssl_verify: ignore ssl errors if False
    :return: json of root resource
    """
    return _get(base_url + "/api/v3/catalog", token, ssl_verify=ssl_verify)


def sql(token, base_url, query, context=None, ssl_verify=True):
    """submit job w/ given sql

    https://docs.dremio.com/rest-api/sql/post-sql.html

    :param token: auth token
    :param base_url: base Dremio url
    :param query: sql query
    :param context: optional dremio context
    :param ssl_verify: ignore ssl errors if False
    :return: job id json object
    """
    return _post(base_url + "/api/v3/sql", token, ssl_verify=ssl_verify, json={"sql": query, "context": context})


def job_status(token, base_url, job_id, ssl_verify=True):
    """fetch job status

    https://docs.dremio.com/rest-api/jobs/get-job.html

    :param token: auth token
    :param base_url: sql query
    :param job_id: job id (as returned by sql)
    :param ssl_verify: ignore ssl errors if False
    :return: status object
    """
    return _get(base_url + "/api/v3/job/{}".format(job_id), token, ssl_verify=ssl_verify)


def job_results(token, base_url, job_id, offset=0, limit=100, ssl_verify=True):
    """fetch job results

    https://docs.dremio.com/rest-api/jobs/get-job.html

    :param token: auth token
    :param base_url: sql query
    :param job_id: job id (as returned by sql)
    :param offset: offset of result set to return
    :param limit: number of results to return (max 500)
    :param ssl_verify: ignore ssl errors if False
    :return: result object
    """
    return _get(
        base_url + "/api/v3/job/{}/results?offset={}&limit={}".format(job_id, offset, limit),
        token,
        ssl_verify=ssl_verify,
    )


def reflections(token, base_url, summary=False, ssl_verify=True):
    """fetch all reflections

    https://docs.dremio.com/rest-api/reflections/get-reflection.html

    :param token: auth token
    :param base_url: sql query
    :param summary: fetch only the reflection summary
    :param ssl_verify: ignore ssl errors if False
    :return: result object
    """
    return _get(base_url + "/api/v3/reflection" + ("/summary" if summary else ""), token, ssl_verify=ssl_verify)


def reflection(token, base_url, reflectionid, ssl_verify=True):
    """fetch a single reflection by id

    https://docs.dremio.com/rest-api/reflections/get-reflection.html

    :param token: auth token
    :param base_url: sql query
    :param reflectionid: id of the reflection to fetch
    :param ssl_verify: ignore ssl errors if False
    :return: result object
    """
    return _get(base_url + "/api/v3/reflection/{}".format(reflectionid), token, ssl_verify=ssl_verify)


def wlm_queues(token, base_url, ssl_verify=True):
    """fetch all wlm queues

    https://docs.dremio.com/rest-api/wlm/get-wlm-queue.html

    :param token: auth token
    :param base_url: sql query
    :param ssl_verify: ignore ssl errors if False
    :return: result object
    """
    return _get(base_url + "/api/v3/wlm/queue", token, ssl_verify=ssl_verify)


def wlm_rules(token, base_url, ssl_verify=True):
    """fetch all wlm rules

    https://docs.dremio.com/rest-api/wlm/get-wlm-rule.html

    :param token: auth token
    :param base_url: sql query
    :param ssl_verify: ignore ssl errors if False
    :return: result object
    """
    return _get(base_url + "/api/v3/wlm/rule", token, ssl_verify=ssl_verify)


def votes(token, base_url, ssl_verify=True):
    """fetch all votes

    https://docs.dremio.com/rest-api/reflections/get-vote.html

    :param token: auth token
    :param base_url: sql query
    :param ssl_verify: ignore ssl errors if False
    :return: result object
    """
    return _get(base_url + "/api/v3/vote", token, ssl_verify=ssl_verify)


def user(token, base_url, uid=None, name=None, ssl_verify=True):
    """
    fetch user based on id or name
    https://docs.dremio.com/rest-api/reflections/get-user.html

    :param token: auth token
    :param base_url: sql query
    :param uid: unique dremio id for user
    :param name: name for a user
    :param ssl_verify: ignore ssl errors if False
    :return: result object
    """
    if uid is None and name is None:
        raise TypeError("both id and name can't be None for a user call")
    idpath = (uid if uid else "") + ", " + (".".join(name) if name else "")
    endpoint = "/{}".format(uid) if uid else "/by-name/{}".format("/".join(name).replace('"', ""))
    return _get(base_url + "/api/v3/user{}".format(endpoint), token, idpath, ssl_verify=ssl_verify)


def group(token, base_url, gid=None, name=None, ssl_verify=True):
    """fetch a group based on id or name

    https://docs.dremio.com/rest-api/reflections/get-group.html

    :param token: auth token
    :param base_url: sql query
    :param gid: unique dremio id for group
    :param name: name for a group
    :param ssl_verify: ignore ssl errors if False
    :return: result object
    """
    if gid is None and name is None:
        raise TypeError("both id and name can't be None for a user call")
    idpath = (gid if gid else "") + ", " + (".".join(name) if name else "")
    endpoint = "/{}".format(gid) if gid else "/by-name/{}".format("/".join(name).replace('"', ""))
    return _get(base_url + "/api/v3/group{}".format(endpoint), token, idpath, ssl_verify=ssl_verify)


def personal_access_token(token, base_url, uid, ssl_verify=True):
    """fetch a PAT for a user based on id

    https://docs.dremio.com/rest-api/user/get-user-id-token.html

    :param token: auth token
    :param base_url: sql query
    :param uid: id of a user
    :return: result object
    :param ssl_verify: ignore ssl errors if False
    """
    return _get(base_url + "/api/v3/user/{}/token".format(uid), token, ssl_verify=ssl_verify)


def collaboration_tags(token, base_url, cid, ssl_verify=True):
    """fetch tags for a catalog entry

    https://docs.dremio.com/rest-api/user/get-catalog-collaboration.html

    :param token: auth token
    :param base_url: sql query
    :param cid: id of a catalog entity
    :param ssl_verify: ignore ssl errors if False
    :return: result object
    """
    return _get(base_url + "/api/v3/catalog/{}/collaboration/tag".format(cid), token, ssl_verify=ssl_verify)


def collaboration_wiki(token, base_url, cid, ssl_verify=True):
    """fetch wiki for a catalog entry

    https://docs.dremio.com/rest-api/user/get-catalog-collaboration.html

    :param token: auth token
    :param base_url: sql query
    :param cid: id of a catalog entity
    :param ssl_verify: ignore ssl errors if False
    :return: result object
    """
    return _get(base_url + "/api/v3/catalog/{}/collaboration/wiki".format(cid), token, ssl_verify=ssl_verify)


def refresh_pds(token, base_url, pid, ssl_verify=True):
    """ refresh a physical dataset and all its child reflections

    https://docs.dremio.com/rest-api/catalog/post-catalog-id-refresh.html

    :param token: auth token
    :param base_url: sql query
    :param pid: id of a catalog entity
    :param ssl_verify: ignore ssl errors if False
    :return: None
    """
    return _post(base_url + "/api/v3/catalog/{}/refresh".format(pid), token, ssl_verify=ssl_verify)


def set_collaboration_tags(token, base_url, cid, tags, ssl_verify=True):
    """ set tags on a given catalog entity

    https://docs.dremio.com/rest-api/catalog/post-catalog-collaboration.html

    :param token: auth token
    :param base_url: sql query
    :param cid: id of a catalog entity
    :param tags: list of strings for tags
    :param ssl_verify: ignore ssl errors if False
    :return: None
    """
    json = {"tags": tags}
    try:
        old_tags = collaboration_tags(token, base_url, cid, ssl_verify)
        json["version"] = old_tags["version"]
    except:  # NOQA
        pass
    return _post(base_url + "/api/v3/catalog/{}/collaboration/tag".format(cid), token, ssl_verify=ssl_verify, json=json)


def set_collaboration_wiki(token, base_url, cid, wiki, ssl_verify=True):
    """ set wiki on a given catalog entity

    https://docs.dremio.com/rest-api/catalog/post-catalog-collaboration.html

    :param token: auth token
    :param base_url: sql query
    :param cid: id of a catalog entity
    :param wiki: text representing markdown for entity
    :param ssl_verify: ignore ssl errors if False
    :return: None
    """
    json = {"text": wiki}
    try:
        old_wiki = collaboration_wiki(token, base_url, cid, ssl_verify)
        json["version"] = old_wiki["version"]
    except:  # NOQA
        pass
    return _post(
        base_url + "/api/v3/catalog/{}/collaboration/wiki".format(cid), token, ssl_verify=ssl_verify, json=json
    )


def delete_catalog(token, base_url, cid, tag, ssl_verify=True):
    """ remove a catalog item from Dremio

    https://docs.dremio.com/rest-api/catalog/delete-catalog-id.html

    :param token: auth token
    :param base_url: sql query
    :param cid: id of a catalog entity
    :param tag: version tag of entity
    :param ssl_verify: ignore ssl errors if False
    :return: None
    """
    return _delete(base_url + "/api/v3/catalog/{}?tag={}".format(cid, tag), token, ssl_verify=ssl_verify)


def set_catalog(token, base_url, json, ssl_verify=True):
    """ add a new catalog entity

    https://docs.dremio.com/rest-api/catalog/post-catalog.html

    :param token: auth token
    :param base_url: sql query
    :param json: json document for new catalog entity
    :param ssl_verify: ignore ssl errors if False
    :return: new catalog entity
    """
    return _post(base_url + "/api/v3/catalog", token, json, ssl_verify=ssl_verify)


def update_catalog(token, base_url, cid, json, ssl_verify=True):
    """ update a catalog entity

    https://docs.dremio.com/rest-api/catalog/put-catalog-id.html

    :param token: auth token
    :param base_url: sql query
    :param cid: id of catalog entity
    :param json: json document for new catalog entity
    :param ssl_verify: ignore ssl errors if False
    :return: updated catalog entity
    """
    return _put(base_url + "/api/v3/catalog/{}".format(cid), token, json, ssl_verify=ssl_verify)


def promote_catalog(token, base_url, cid, json, ssl_verify=True):
    """ promote a catalog entity (only works on folders and files in sources

    https://docs.dremio.com/rest-api/catalog/post-catalog-id.html

    :param token: auth token
    :param base_url: sql query
    :param cid: id of catalog entity
    :param json: json document for new catalog entity
    :param ssl_verify: ignore ssl errors if False
    :return: updated catalog entity
    """
    return _post(base_url + "/api/v3/catalog/{}".format(cid), token, json, ssl_verify=ssl_verify)


def set_personal_access_token(token, base_url, uid, label, lifetime=24, ssl_verify=True):
    """ create a pat for a given user

    https://docs.dremio.com/rest-api/user/post-user-uid-token.html

    :param token: auth token
    :param base_url: sql query
    :param uid: id user
    :param label: label of token
    :param lifetime: lifetime in hours of token
    :param ssl_verify: ignore ssl errors if False
    :return: updated catalog entity
    """
    return _post(
        base_url + "/api/v3/user/{}/token".format(uid),
        token,
        {"label": label, "lifeTime": 1000 * 60 * 60 * lifetime},
        ssl_verify=ssl_verify,
    )


def delete_personal_access_token(token, base_url, uid, tid=None, ssl_verify=True):
    """ create a pat for a given user

    https://docs.dremio.com/rest-api/user/delete-user-uid-token.html

    :param token: auth token
    :param base_url: sql query
    :param uid: id user
    :param tid: label of token (optional)
    :param ssl_verify: ignore ssl errors if False
    :return: updated catalog entity
    """
    return _delete(
        base_url + "/api/v3/user/{}/token{}".format(uid, ("/" + tid) if tid else ""), token, ssl_verify=ssl_verify
    )


def modify_reflection(token, base_url, reflectionid, json, ssl_verify=True):
    """update a single reflection by id

    https://docs.dremio.com/rest-api/reflections/put-reflection.html

    :param token: auth token
    :param base_url: sql query
    :param reflectionid: id of the reflection to fetch
    :param json: json document for modified reflection
    :param ssl_verify: ignore ssl errors if False
    :return: result object
    """
    return _put(base_url + "/api/v3/reflection/{}".format(reflectionid), token, json, ssl_verify=ssl_verify)


def create_reflection(token, base_url, json, ssl_verify=True):
    """create a single reflection

    https://docs.dremio.com/rest-api/reflections/post-reflection.html

    :param token: auth token
    :param base_url: sql query
    :param json: json document for new reflection
    :param ssl_verify: ignore ssl errors if False
    :return: result object
    """
    return _post(base_url + "/api/v3/reflection/", token, json, ssl_verify=ssl_verify)


def delete_reflection(token, base_url, reflectionid, ssl_verify=True):
    """delete a single reflection by id

    https://docs.dremio.com/rest-api/reflections/delete-reflection.html

    :param token: auth token
    :param base_url: sql query
    :param reflectionid: id of the reflection to delete
    :param ssl_verify: ignore ssl errors if False
    :return: result object
    """
    _delete(base_url + "/api/v3/reflection/{}".format(reflectionid), token, ssl_verify=ssl_verify)


def cancel_job(token, base_url, jid, ssl_verify=True):
    """cancel running job with job id = jid

    https://docs.dremio.com/rest-api/jobs/post-job.html

    :param token: auth token
    :param base_url: sql query
    :param jid: id of the job to cancel
    :param ssl_verify: ignore ssl errors if False
    :return: result object
    :exception DremioNotFoundException no job found
    :exception DremioBadRequestException job already finished
    """
    _post(base_url + "/api/v3/job/{}/cancel".format(jid), token, ssl_verify=ssl_verify)


def modify_queue(token, base_url, queueid, json, ssl_verify=True):
    """update a single queue by id

    https://docs.dremio.com/rest-api/wlm/put-wlm-queue.html

    :param token: auth token
    :param base_url: sql query
    :param queueid: id of the reflection to fetch
    :param json: json document for modified queue
    :param ssl_verify: ignore ssl errors if False
    :return: result object
    """
    return _put(base_url + "/api/v3/wlm/queue/{}".format(queueid), token, json, ssl_verify=ssl_verify)


def create_queue(token, base_url, json, ssl_verify=True):
    """create a single queue

    https://docs.dremio.com/rest-api/wlm/post-wlm-queue.html

    :param token: auth token
    :param base_url: sql query
    :param json: json document for new queue
    :param ssl_verify: ignore ssl errors if False
    :return: result object
    """
    return _post(base_url + "/api/v3/wlm/queue/", token, json, ssl_verify=ssl_verify)


def delete_queue(token, base_url, queueid, ssl_verify=True):
    """delete a single queue by id

    https://docs.dremio.com/rest-api/wlm/delete-wlm-queue.html

    :param token: auth token
    :param base_url: sql query
    :param queueid: id of the queue to delete
    :param ssl_verify: ignore ssl errors if False
    :return: result object
    """
    _delete(base_url + "/api/v3/wlm/queue/{}".format(queueid), token, ssl_verify=ssl_verify)


def modify_rules(token, base_url, json, ssl_verify=True):
    """update wlm rules. Order of rules array is important!

    The order of the rules is the order in which they will be applied. If a rule isn't included it will be deleted
    new ones will be created
    https://docs.dremio.com/rest-api/wlm/put-wlm-rule.html

    :param token: auth token
    :param base_url: sql query
    :param json: json document for modified reflection
    :param ssl_verify: ignore ssl errors if False
    :return: result object
    """
    return _put(base_url + "/api/v3/wlm/rule/", token, json, ssl_verify=ssl_verify)


def _raise_for_status(self):
    """Raises stored :class:`HTTPError`, if one occurred. Copy from requests request.raise_for_status()"""

    http_error_msg = ""
    if isinstance(self.reason, bytes):
        try:
            reason = self.reason.decode("utf-8")
        except UnicodeDecodeError:
            reason = self.reason.decode("iso-8859-1")
    else:
        reason = self.reason

    if 400 <= self.status_code < 500:
        http_error_msg = u"%s Client Error: %s for url: %s" % (self.status_code, reason, self.url)

    elif 500 <= self.status_code < 600:
        http_error_msg = u"%s Server Error: %s for url: %s" % (self.status_code, reason, self.url)

    if http_error_msg:
        return HTTPError(http_error_msg, response=self), self.status_code, reason
    else:
        return None, self.status_code, reason


def graph(token, base_url, cid=None, ssl_verify=True):
    """Retrieves graph information about a specific catalog entity by id

    https://docs.dremio.com/rest-api/catalog/get-catalog-id-graph.html

    :param token: auth token from previous login attempt
    :param base_url: base Dremio url
    :param cid: unique dremio id for resource
    :param ssl_verify: ignore ssl errors if False
    :return: json of resource
    """
    if cid is None:
        raise TypeError("resource id can't be None for a graph call")
    return _get(base_url + "/api/v3/catalog/{}/graph".format(cid), token, ssl_verify=ssl_verify)
