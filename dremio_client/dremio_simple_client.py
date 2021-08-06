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
from .auth import auth
from .model.endpoints import (
    cancel_job,
    catalog,
    catalog_item,
    collaboration_tags,
    set_collaboration_tags,
    set_collaboration_wiki,
    collaboration_wiki,
    create_queue,
    create_reflection,
    delete_catalog,
    delete_personal_access_token,
    delete_queue,
    delete_reflection,
    get_privilege,
    get_privilege_by_grant_type,
    update_privilege,
    delete_privilege,
    graph,
    group,
    job_results,
    job_status,
    modify_queue,
    modify_reflection,
    modify_rules,
    personal_access_token,
    reflection,
    reflections,
    refresh_pds,
    set_catalog,
    set_personal_access_token,
    sql,
    update_catalog,
    promote_catalog,
    create_user,
    update_user,
    user,
    votes,
    wlm_queues,
    wlm_queue,
    wlm_rules,
    get_all_users, delete_user, update_role, delete_role, get_role, create_role, get_all_roles,
    get_members_of_role, get_privileges_of_role, get_privileges_of_user,
    update_member_of_role
)
from .util import refresh_metadata, run, run_async, refresh_vds_reflection_by_path, refresh_reflections_of_one_dataset


class SimpleClient(object):
    def __init__(self, config):
        """
        Create a Dremio Simple Client instance. This currently only supports basic auth from the constructor.
        Will be extended for oauth, token auth and storing auth on disk or in stores in the future

        :param config: config dict from confuse
        """

        port = config["port"].get(int)
        self._hostname = config["hostname"].get()
        self._base_url = (
            ("https" if config["ssl"].get(bool) else "http")
            + "://"
            + self._hostname
            + (":{}".format(port) if port else "")
        )
        self._token = auth(self._base_url, config)
        self._ssl_verify = config["verify"].get(bool)

    def catalog(self):
        return catalog(self._token, self._base_url, ssl_verify=self._ssl_verify)

    def job_status(self, jobid):
        return job_status(self._token, self._base_url, jobid, ssl_verify=self._ssl_verify)

    def catalog_item(self, cid, path):
        return catalog_item(self._token, self._base_url, cid, path, ssl_verify=self._ssl_verify)

    def job_results(self, jobid):
        return job_results(self._token, self._base_url, jobid, ssl_verify=self._ssl_verify)

    def sql(self, query, context=None):
        return sql(self._token, self._base_url, query, context, ssl_verify=self._ssl_verify)

    def reflections(self, summary=False):
        return reflections(self._token, self._base_url, summary, ssl_verify=self._ssl_verify)

    def reflection(self, reflectionid):
        return reflection(self._token, self._base_url, reflectionid, ssl_verify=self._ssl_verify)

    def wlm_queues(self):
        """ return details all workload management queues

        Summarizes all workload management queues in the system
        https://docs.dremio.com/rest-api/wlm/get-wlm-queue.html
        .. note:: can only be run by admin
        .. note:: Enterprise only

        :raise: DremioUnauthorizedException if token is incorrect or invalid
        :raise: DremioPermissionException user does not have permission
        :raise: DremioNotFoundException queues not found
        :return: queues as a list of dicts
        """
        return wlm_queues(self._token, self._base_url, ssl_verify=self._ssl_verify)

    def wlm_queue(self, qid=None, name=None):
        """ return details for a queue

        User must supply one of qid or name. qid takes precedence if both are supplied
        .. note:: can only be run by admin
        .. note:: Enterprise only

        :param qid: queue id
        :param name: queue name
        :raise: DremioUnauthorizedException if token is incorrect or invalid
        :raise: DremioPermissionException user does not have permission
        :raise: DremioNotFoundException queue could not be found
        :return: queue info as a dict
        """
        return wlm_queue(self._token, self._base_url, qid, name, ssl_verify=self._ssl_verify)

    def wlm_rules(self):
        """ return details all workload management rules

        Summarizes all workload management rules in the system
        https://docs.dremio.com/rest-api/wlm/get-wlm-rule.html
        .. note:: can only be run by admin
        .. note:: Enterprise only

        :raise: DremioUnauthorizedException if token is incorrect or invalid
        :raise: DremioPermissionException user does not have permission
        :raise: DremioNotFoundException ruleset is not found
        :return: rules as a list of dicts
        """
        return wlm_rules(self._token, self._base_url, ssl_verify=self._ssl_verify)

    def votes(self):
        """ return details all reflection votes

        Summarizes all votes in the system
        https://docs.dremio.com/rest-api/votes/get-vote.html
        .. note:: can only be run by admin

        :raise: DremioUnauthorizedException if token is incorrect or invalid
        :raise: DremioPermissionException user does not have permission
        :return: votes as a list of dicts
        """
        return votes(self._token, self._base_url, ssl_verify=self._ssl_verify)

    def create_user(self, json):
        """ returns the new user after creating it
            :param json: json document for new user (name is required)
            :return: result object
        """
        return create_user(self._token, self._base_url, json, ssl_verify=self._ssl_verify)

    def delete_user(self, uid , tag):
        """
        Delete user corresponding to the the given uid and version tag.
        :param uid: user id
        :param tag: version parameter for a given user
        :raise: DremioUnauthorizedException if token is incorrect or invalid
        :raise: DremioNotFoundException if user could not be found
        :return: None
        """
        return delete_user(self._token, self._base_url, uid, tag ,ssl_verify=self._ssl_verify)

    def update_user(self, uid, json):
        """
        Returns user corresponding to the the given uid or name (Atleast one of name or uid should be given)
        :param uid: user id
        :param json: json document for updating the user info
        :raise: DremioUnauthorizedException if token is incorrect or invalid
        :raise: DremioNotFoundException if user could not be found
        :return: result object
        """
        return update_user(self._token, self._base_url, uid, json, ssl_verify=self._ssl_verify)

    def get_all_users(self, startIndex=None , count=None ,query=None ):
        """ return all the users

                startIndex , count , and query are optional parameters

                :param startIndex: returns users starting from this index
                :param count: maximum number of users to return
                :param query: filter based on this query
                :return: users info as a dict
                """
        return get_all_users(self._token,self._base_url  ,startIndex , count ,query , ssl_verify=self._ssl_verify)

    def get_privileges_of_user(self, uid, startIndex=None, count=None):
        """
        Fetches priviliges of a user
        :param uid: user id
        :param startIndex: index starting from which to fetch privileges
        :param count: Maximum number of privileges to fetch
        :return: result object
        """
        return get_privileges_of_user(self._token, self._base_url, uid, startIndex, count, ssl_verify=self._ssl_verify)

    def user(self, uid=None, name=None):
        """ return details for a user

        User must supply one of uid or name. uid takes precedence if both are supplied
        .. note:: can only be run by admin

        :param uid: group id
        :param name: group name
        :raise: DremioUnauthorizedException if token is incorrect or invalid
        :raise: DremioPermissionException user does not have permission
        :raise: DremioNotFoundException user could not be found
        :return: user info as a dict
        """
        return user(self._token, self._base_url, uid, name, ssl_verify=self._ssl_verify)

    def create_role(self, json):
        """ returns the new role after creating it
        :param json: json document for new role
        :return: result object
        """
        return create_role(self._token, self._base_url, json, ssl_verify=self._ssl_verify)

    def get_role(self, rid=None ,name=None):
        """
        Returns role corresponding to the the given rid or name (Atleast one of name or rid should be given)
        :param rid: role id
        :param name: role name
        :raise: DremioUnauthorizedException if token is incorrect or invalid
        :raise: DremioNotFoundException if role could not be found
        :return: result object
        """
        return get_role(self._token, self._base_url, rid, name,ssl_verify=self._ssl_verify)

    def delete_role(self, rid ):
        """
        Deletes a role corresponding to  the given rid if it exists
        :param rid: role id
        :raise: DremioUnauthorizedException if token is incorrect or invalid
        :raise: DremioNotFoundException if role could not be found
        :return: None
        """
        return delete_role(self._token, self._base_url, rid,ssl_verify=self._ssl_verify)

    def update_role(self, rid, json):
        """
        Returns role corresponding to the the given rid or name (Atleast one of name or rid should be given)
        :param rid: role id
        :param json: json document for updating the role
        :raise: DremioUnauthorizedException if token is incorrect or invalid
        :raise: DremioNotFoundException if role could not be found
        :return: result object
        """
        return update_role(self._token, self._base_url, rid, json, ssl_verify=self._ssl_verify)

    def get_all_roles(self, startIndex=None, count=None, query=None):
        """ return all the roles

                startIndex , count , and query are optional parameters

                :param startIndex: returns roles starting from this index
                :param count: maximum number of roles return
                :param query: filter based on this query
                :raise: DremioUnauthorizedException if token is incorrect or invalid
                :raise: DremioNotFoundException role could not be found
                :return: roles info as a dict
                """
        return get_all_roles(self._token, self._base_url, startIndex, count, query, ssl_verify=self._ssl_verify)

    def get_members_of_role(self, rid, startIndex=None, count=None):
        """
        Return members for a given role
        :param rid: role id
        :param startIndex:(optional) Index from which to return all members
        :param count: (optional) maximum number of members to return
        :return: result object
        """
        return get_members_of_role(self._token ,self._base_url,rid,startIndex,count,ssl_verify=self._ssl_verify)

    def get_privileges_of_role(self, rid, startIndex=None, count=None):
        """
        Fetches the privileges for a role
        :param rid: role id
        :param startIndex: index starting from which to fetch privileges
        :param count: Maximum number of priviliges to fetch
        :return: result object
        """
        return get_privileges_of_role(self._token, self._base_url, rid, startIndex, count, ssl_verify=self._ssl_verify)

    def update_member_of_role(self, rid, json):
        """
        Add/remove a member from a role
        :param rid: role id
        :param json: json document for member of the role
        :return: result object
        """
        return update_member_of_role(self._token, self._base_url, rid, json, ssl_verify=self._ssl_verify)

    def group(self, gid=None, name=None):
        """ return details for a group

        User must supply one of gid or name. gid takes precedence if both are supplied
        .. note:: can only be run by admin

        :param gid: group id
        :param name: group name
        :raise: DremioUnauthorizedException if token is incorrect or invalid
        :raise: DremioPermissionException user does not have permission
        :raise: DremioNotFoundException group could not be found
        :return: group info as a dict
        """
        return group(self._token, self._base_url, gid, name, ssl_verify=self._ssl_verify)

    def personal_access_token(self, uid):
        """ return a list of personal access tokens for a user

        .. note:: can only be run for the logged in user

        :param uid: user id
        :raise: DremioUnauthorizedException if token is incorrect or invalid
        :raise: DremioPermissionException user does not have permission
        :raise: DremioNotFoundException user could not be found
        :return: personal access token list
        """
        return personal_access_token(self._token, self._base_url, uid, ssl_verify=self._ssl_verify)

    def collaboration_tag(self, cid):
        """ returns a list of tags for catalog entity

        :param cid: catalog entity id
        :raise: DremioBadRequestException if tags can't exist on this entity
        :raise: DremioUnauthorizedException if token is incorrect or invalid
        :raise: DremioPermissionException user does not have permission
        :raise: DremioNotFoundException user could not be found
        :return: list of tags
        """
        return collaboration_tags(self._token, self._base_url, cid, ssl_verify=self._ssl_verify)

    def set_collaboration_tag(self, cid, tags):
        """ returns a list of tags for catalog entity

        :param cid: catalog entity id
        :param tags: string list
        :raise: DremioBadRequestException if tags can't exist on this entity
        :raise: DremioUnauthorizedException if token is incorrect or invalid
        :raise: DremioPermissionException user does not have permission
        :raise: DremioNotFoundException user could not be found
        :return: list of tags
        """
        return set_collaboration_tags(self._token, self._base_url, cid, tags, ssl_verify=self._ssl_verify)

    def set_collaboration_wiki(self, cid, wiki):
        """ returns a list of wiki for catalog entity

        :param cid: catalog entity id
        :param tags: string list
        :raise: DremioBadRequestException if wiki can't exist on this entity
        :raise: DremioUnauthorizedException if token is incorrect or invalid
        :raise: DremioPermissionException user does not have permission
        :raise: DremioNotFoundException user could not be found
        :return: list of wikis
        """
        return set_collaboration_wiki(self._token, self._base_url, cid, wiki, ssl_verify=self._ssl_verify)

    def collaboration_wiki(self, cid):
        """ returns a wiki details for catalog entity


        :param cid: catalog entity id
        :raise: DremioUnauthorizedException if token is incorrect or invalid
        :raise: DremioPermissionException user does not have permission
        :raise: DremioNotFoundException user could not be found
        :return: wiki details
        """
        return collaboration_wiki(self._token, self._base_url, cid, ssl_verify=self._ssl_verify)

    def query(self, query, context=None, sleep_time=10, asynchronous=False):
        """ Run a single sql query asynchronously

        This executes a single sql query against the rest api asynchronously and returns a future for the result

        :param query: valid sql query
        :param context: optional context in which to execute the query
        :param sleep_time: seconds to sleep between checking for finished state
        :param asynchronous: boolean execute asynchronously
        :raise: DremioException if job failed
        :raise: DremioUnauthorizedException if token is incorrect or invalid
        :return: concurrent.futures.Future for the result

        :example:

        >>> client =
        >>> f = query('select * from sys.options', asynchronous=True)
        >>> f.result()
        [{'record':'1'}, {'record':'2'}]
        """
        if asynchronous:
            return run_async(self._token, self._base_url, query, context, sleep_time, ssl_verify=self._ssl_verify)
        return run(self._token, self._base_url, query, context, sleep_time, ssl_verify=self._ssl_verify)

    def refresh_metadata(self, table):
        """ Refresh the metadata for a given physical dataset

        :param table: the physical dataset to be refreshed
        :raise: DremioException if job failed
        :raise: DremioUnauthorizedException if token is incorrect or invalid
        :return: None
        """
        return refresh_metadata(self._token, self._base_url, table, ssl_verify=self._ssl_verify)

    def update_catalog(self, cid, json):
        """ update a catalog entity

        https://docs.dremio.com/rest-api/catalog/put-catalog-id.html

        :param cid: id of catalog entity
        :param json: json document for new catalog entity
        :return: updated catalog entity
        """
        return update_catalog(self._token, self._base_url, cid, json, ssl_verify=self._ssl_verify)

    def promote_catalog(self, cid, json):
        """ promote a catalog entity

        https://docs.dremio.com/rest-api/catalog/put-catalog-id.html

        :param cid: id of catalog entity
        :param json: json document for new catalog entity
        :return: updated catalog entity
        """
        return promote_catalog(self._token, self._base_url, cid, json, ssl_verify=self._ssl_verify)

    def delete_catalog(self, cid, tag=None):
        """ remove a catalog item from Dremio

        https://docs.dremio.com/rest-api/catalog/delete-catalog-id.html

        :param cid: id of a catalog entity
        :param tag: version tag of entity
        :return: None
        """
        return delete_catalog(self._token, self._base_url, cid, tag, ssl_verify=self._ssl_verify)

    def set_catalog(self, json):
        """ add a new catalog entity

        https://docs.dremio.com/rest-api/catalog/post-catalog.html

        :param json: json document for new catalog entity
        :return: new catalog entity
        """
        return set_catalog(self._token, self._base_url, json, ssl_verify=self._ssl_verify)

    def refresh_pds(self, pid):
        """ refresh a physical dataset and all its child reflections

        https://docs.dremio.com/rest-api/catalog/post-catalog-id-refresh.html

        :param pid: id of a catalog entity
        :return: None
        """
        return refresh_pds(self._token, self._base_url, pid, ssl_verify=self._ssl_verify)

    def set_personal_access_token(self, uid, label, lifetime=24):
        """ create a pat for a given user

        https://docs.dremio.com/rest-api/user/post-user-uid-token.html

        :param uid: id user
        :param label: label of token
        :param lifetime: lifetime in hours of token
        :return: updated catalog entity
        """
        return set_personal_access_token(self._token, self._base_url, uid, label, lifetime, ssl_verify=self._ssl_verify)

    def delete_personal_access_token(self, uid=None, tid=None):
        """ delete a pat for a given user

        https://docs.dremio.com/rest-api/user/delete-user-uid-token.html
        https://docs.dremio.com/rest-api/user/delete-user-uid-token-tid.html
        https://docs.dremio.com/rest-api/token/

        :param uid: id user
        :param tid: token id
        :return: Returns boolean success/failure or None.
        """
        return delete_personal_access_token(self._token, self._base_url, uid, tid, ssl_verify=self._ssl_verify)

    def create_reflection(self, json):
        """create a single reflection

        https://docs.dremio.com/rest-api/reflections/post-reflection.html

        :param json: json document for new reflection
        :return: result object
        """
        return create_reflection(self._token, self._base_url, json, ssl_verify=self._ssl_verify)

    def modify_reflection(self, reflectionid, json):
        """update a single reflection by id

        https://docs.dremio.com/rest-api/reflections/put-reflection.html

        :param reflectionid: id of the reflection to fetch
        :param json: json document for modified reflection
        :return: result object
        """
        return modify_reflection(self._token, self._base_url, reflectionid, json, ssl_verify=self._ssl_verify)

    def delete_reflection(self, reflectionid):
        """delete a single reflection by id

        https://docs.dremio.com/rest-api/reflections/delete-reflection.html

        :param reflectionid: id of the reflection to fetch
        :return: result object
        """
        return delete_reflection(self._token, self._base_url, reflectionid, ssl_verify=self._ssl_verify)

    def cancel_job(self, jobid):
        """cancel running job with job id = jobid

        https://docs.dremio.com/rest-api/jobs/post-job.html

        :param jobid: id of the job to cancel
        :return: result object
        :exception DremioNotFoundException no job found
        :exception DremioBadRequestException job already finished
        """
        return cancel_job(self._token, self._base_url, jobid, ssl_verify=self._ssl_verify)

    def modify_queue(self, queueid, json):
        """update a single queue by id

        https://docs.dremio.com/rest-api/reflections/put-wlm-queue.html

        :param queueid: id of the reflection to fetch
        :param json: json document for modified queue
        :return: result object
        """
        return modify_queue(self._token, self._base_url, queueid, json, ssl_verify=self._ssl_verify)

    def create_queue(self, json):
        """create a single queue

        https://docs.dremio.com/rest-api/reflections/post-wlm-queue.html

        :param json: json document for new queue
        :return: result object
        """
        return create_queue(self._token, self._base_url, json, ssl_verify=self._ssl_verify)

    def delete_queue(self, queueid):
        """delete a single queue by id

        https://docs.dremio.com/rest-api/reflections/delete-wlm-queue.html

        :param queueid: id of the queue to delete
        :return: result object
        """
        return delete_queue(self._token, self._base_url, queueid, ssl_verify=self._ssl_verify)

    def modify_rules(self, json):
        """update wlm rules. Order of rules array is important!

        The order of the rules is the order in which they will be applied. If a rule isn't included it will be deleted
        new ones will be created
        https://docs.dremio.com/rest-api/reflections/put-wlm-rule.html

        :param json: json document for modified reflection
        :return: result object
        """
        return modify_rules(self._token, self._base_url, json, ssl_verify=self._ssl_verify)

    def graph(self, cid):
        return graph(self._token, self._base_url, cid, ssl_verify=self._ssl_verify)

    def refresh_vds_reflection_by_path(self, path):
        """ Refresh the reflection for a given virtual dataset

        :param path: list ['space', 'folder', 'vds']
        :return: None
        """
        return refresh_vds_reflection_by_path(self, path)

    def refresh_reflections_of_one_dataset(self, path):
        """ Refresh the reflection for a given dataset
        Refresh will disable and enable reflection.
        As long reflection will be reenabled all queries will be redirected to source dataset.
        All VDS Reflection derived from this VDS will be refreshed as well

        :param path: list ['space', 'folder', 'vds']
        :return: None

        """

        return refresh_reflections_of_one_dataset(self, path)

    def get_privilege(self, pid):
        return get_privilege(self._token, self._base_url, pid, ssl_verify=self._ssl_verify)

    def get_privilege_by_grant_type(self, grantType):
        return get_privilege_by_grant_type(self._token, self._base_url, grantType, ssl_verify=self._ssl_verify)

    def update_privilege(self, pid, json):
        return update_privilege(self._token, self._base_url, pid, json, ssl_verify=self._ssl_verify)

    def delete_privilege(self, pid, grants):
        return delete_privilege(self._token, self._base_url, pid, grants, ssl_verify=self._ssl_verify)
