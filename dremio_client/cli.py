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

"""Console script for dremio_client."""
import os
import sys

import click

import simplejson as json

from . import __version__
from .conf import get_base_url_token
from .error import DremioNotFoundException
from .model.endpoints import (
    cancel_job as _cancel_job,
    catalog as _catalog,
    catalog_item as _catalog_item,
    collaboration_tags as _collaboration_tags,
    collaboration_wiki as _collaboration_wiki,
    create_queue as _create_queue,
    create_reflection as _create_reflection,
    delete_personal_access_token as _delete_personal_access_token,
    delete_queue as _delete_queue,
    delete_reflection as _delete_reflection,
    graph as _graph,
    group as _group,
    job_results as _job_results,
    job_status as _job_status,
    modify_queue as _modify_queue,
    modify_reflection as _modify_reflection,
    modify_rules as _modify_rules,
    personal_access_token as _pat,
    reflection as _reflection,
    reflections as _reflections,
    refresh_pds as _refresh_pds,
    set_catalog as _set_catalog,
    set_personal_access_token as _set_personal_access_token,
    sql as _sql,
    update_catalog as _update_catalog,
    promote_catalog as _promote_catalog,
    user as _user,
    votes as _votes,
    wlm_queues as _wlm_queues,
    wlm_rules as _wlm_rules,
)
from .util.query import run
from .util.delete import delete_catalog as _delete_catalog


def print_version(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click.echo(__version__)
    ctx.exit()


@click.group()
@click.option("--config", type=click.Path(exists=True, dir_okay=True, file_okay=False), help="Custom config file.")
@click.option("-h", "--hostname", help="Hostname if different from config file")
@click.option("-p", "--port", type=int, help="Hostname if different from config file")
@click.option("--ssl", is_flag=True, help="Use SSL if different from config file")
@click.option("-u", "--username", help="username if different from config file")
@click.option("-p", "--password", help="password if different from config file")
@click.option("--skip-verify", is_flag=True, help="skip verificatoin of ssl cert")
@click.option("--version", is_flag=True, callback=print_version, expose_value=False, is_eager=True)
@click.pass_context
def cli(ctx, config, hostname, port, ssl, username, password, skip_verify):
    if config:
        os.environ["DREMIO_CLIENTDIR"] = config
    ctx.obj = dict()
    if hostname:
        ctx.obj["hostname"] = hostname
    if port:
        ctx.obj["port"] = port
    if ssl:
        ctx.obj["ssl"] = ssl
    if username:
        ctx.obj["auth.username"] = username
    if password:
        ctx.obj["auth.password"] = password
    if skip_verify:
        ctx.obj["verify"] = not skip_verify


@cli.command()
@click.option("--sql", help="sql query to execute.", required=True)
@click.pass_obj
def query(args, sql):
    """
    execute a query given by sql and print results
    """
    base_url, token, verify = get_base_url_token(args)
    results = list()
    for x in run(token, base_url, sql, ssl_verify=verify):
        results.extend(x["rows"])
    click.echo(json.dumps(results))


@cli.command()
@click.argument("sql-query", nargs=-1, required=True)
@click.option("--context", help="context in which the sql query should execute.")
@click.pass_obj
def sql(args, sql_query, context):
    """
    Execute sql statement and return job id

    """
    base_url, token, verify = get_base_url_token(args)
    x = _sql(token, base_url, " ".join(sql_query), context, ssl_verify=verify)
    click.echo(json.dumps(x))


@cli.command()
@click.argument("jobid", nargs=1, required=True)
@click.pass_obj
def job_status(args, jobid):
    """
    Return status of job for a given job id

    """
    base_url, token, verify = get_base_url_token(args)
    x = _job_status(token, base_url, jobid, ssl_verify=verify)
    click.echo(json.dumps(x))


@cli.command()
@click.argument("jobid", nargs=1, required=True)
@click.option("-o", "--offset", type=int, default=0, help="offset of first result")
@click.option("-l", "--limit", type=int, default=100, help="number of results to return")
@click.pass_obj
def job_results(args, jobid, offset, limit):
    """
    return results for a given job id

    pagenated with offset and limit

    """
    base_url, token, verify = get_base_url_token(args)
    x = _job_results(token, base_url, jobid, offset, limit, ssl_verify=verify)
    click.echo(json.dumps(x))


@cli.command()
@click.pass_obj
def catalog(args):
    """
    return the root catalog

    """
    base_url, token, verify = get_base_url_token(args)
    x = _catalog(token, base_url, ssl_verify=verify)
    click.echo(json.dumps(x))


@cli.command()
@click.argument("path", nargs=-1)
@click.option("-c", "--cid", help="id of a given catalog item")
@click.pass_obj
def catalog_item(args, path, cid):
    """
    return the details of a given catalog item

    if cid and path are both specified id is used
    if neither are specified it causes an error

    """
    base_url, token, verify = get_base_url_token(args)
    x = _catalog_item(token, base_url, cid, [i.replace(".", "/") for i in path] if path else None, ssl_verify=verify,)
    click.echo(json.dumps(x))


@cli.command()
@click.option("--summary", "-s", is_flag=True, help="only return summary reflection info")
@click.pass_obj
def reflections(args, summary):
    """
    return the reflection set

    """
    base_url, token, verify = get_base_url_token(args)
    x = _reflections(token, base_url, summary, ssl_verify=verify)
    click.echo(json.dumps(x))


@cli.command()
@click.argument("reflectionid", nargs=1, required=True)
@click.pass_obj
def reflection(args, reflectionid):
    """
    return the reflection set

    """
    base_url, token, verify = get_base_url_token(args)
    x = _reflection(token, base_url, reflectionid, ssl_verify=verify)
    click.echo(json.dumps(x))


@cli.command()
@click.pass_obj
def wlm_rules(args):
    """
    return the list of wlm rules

    """
    base_url, token, verify = get_base_url_token(args)
    x = _wlm_rules(token, base_url, ssl_verify=verify)
    click.echo(json.dumps(x))


@cli.command()
@click.pass_obj
def wlm_queues(args):
    """
    return the list of wlm queues

    """
    base_url, token, verify = get_base_url_token(args)
    x = _wlm_queues(token, base_url, ssl_verify=verify)
    click.echo(json.dumps(x))


@cli.command()
@click.pass_obj
def votes(args):
    """
    return reflection votes

    """
    base_url, token, verify = get_base_url_token(args)
    x = _votes(token, base_url, ssl_verify=verify)
    click.echo(json.dumps(x))


@cli.command()
@click.option("--uid", "-u", help="unique id for a user")
@click.option("--name", "-n", help="human readable name of a user")
@click.pass_obj
def user(args, gid, name):
    """
    return user info

    """
    base_url, token, verify = get_base_url_token(args)
    x = _user(token, base_url, gid, name, ssl_verify=verify)
    click.echo(json.dumps(x))


@cli.command()
@click.option("--gid", "-g", help="unique id for a group")
@click.option("--name", "-n", help="human readable name of a group")
@click.pass_obj
def group(args, gid, name):
    """
    return group info

    """
    base_url, token, verify = get_base_url_token(args)
    x = _group(token, base_url, gid, name, ssl_verify=verify)
    click.echo(json.dumps(x))


@cli.command()
@click.argument("uid", nargs=1, required=True)
@click.pass_obj
def pat(args, uid):
    """
    return personal access token info for a given user id

    """
    base_url, token, verify = get_base_url_token(args)
    x = _pat(token, base_url, uid, ssl_verify=verify)
    click.echo(json.dumps(x))


@cli.command()
@click.option("--cid", "-c", help="unique id for a catalog entity")
@click.option("--path", "-p", help="path of a catalog entity")
@click.pass_obj
def tags(args, cid, path):
    """
    returns tags for a given catalog entity id or path
    only cid or path can be specified. path incurs a second lookup to get the id

    """
    base_url, token, verify = get_base_url_token(args)
    if path:
        res = _catalog_item(token, base_url, None, [path.replace(".", "/")], ssl_verify=verify)
        cid = res["id"]
    try:
        x = _collaboration_tags(token, base_url, cid, ssl_verify=verify)
        click.echo(json.dumps(x))
    except DremioNotFoundException:
        click.echo("Wiki not found or entity does not exist")


@cli.command()
@click.option("--cid", "-c", help="unique id for a catalog entity")
@click.option("--path", "-p", help="path of a catalog entity")
@click.option("--pretty-print", "-v", is_flag=True, help="format markdown for terminal")
@click.pass_obj
def wiki(args, cid, path, pretty_print):
    """
    returns wiki for a given catalog entity id or path
    only cid or path can be specified. path incurs a second lookup to get the id

    activating the pretty-print flag will attempt to convert the text field to plain-text for the console

    """
    base_url, token, verify = get_base_url_token(args)
    if path:
        res = _catalog_item(token, base_url, None, [path.replace(".", "/")], ssl_verify=verify)
        cid = res["id"]
    try:
        x = _collaboration_wiki(token, base_url, cid, ssl_verify=verify)
        if pretty_print:
            try:
                text = _to_text(x["text"])
                click.echo(text)
            except ImportError:
                click.echo("Can't convert text to console, please install markdown and BeautifulSoup")
                click.echo(json.dumps(x))
        else:
            click.echo(json.dumps(x))
    except DremioNotFoundException:
        click.echo("Wiki not found or entity does not exist")


def _to_text(text):
    from markdown import Markdown
    from io import StringIO

    def unmark_element(element, stream=None):
        if stream is None:
            stream = StringIO()
        if element.text:
            stream.write(element.text)
        for sub in element:
            unmark_element(sub, stream)
        if element.tail:
            stream.write(element.tail)
        return stream.getvalue()

    # patching Markdown
    Markdown.output_formats["plain"] = unmark_element
    __md = Markdown(output_format="plain")
    __md.stripTopLevelTags = False

    return __md.convert(text)


@cli.command()
@click.argument("data", nargs=1, required=True)
@click.option("-i", "--cid", help="catalog entity")
@click.pass_obj
def update_catalog(args, data, cid):
    """
    update a catalog entity (cid) given a json data object

    """
    base_url, token, verify = get_base_url_token(args)
    x = _update_catalog(token, base_url, cid, data, ssl_verify=verify)
    click.echo(json.dumps(x))


@cli.command()
@click.argument("data", nargs=1, required=True)
@click.option("-i", "--cid", help="catalog entity")
@click.pass_obj
def promote_catalog(args, data, cid):
    """
    update a catalog entity (cid) given a json data object

    """
    base_url, token, verify = get_base_url_token(args)
    x = _promote_catalog(token, base_url, cid, data, ssl_verify=verify)
    click.echo(json.dumps(x))


@cli.command()
@click.option("--cid", "-c", help="unique id for a catalog entity")
@click.option("--path", "-p", help="path of a catalog entity")
@click.pass_obj
def delete_catalog(args, cid, path):
    """
    delete a catalog entity given by cid or path

    warning, this process is destructive and permanent
    """
    base_url, token, verify = get_base_url_token(args)
    x = _delete_catalog(base_url, token, verify, cid, path)
    click.echo(json.dumps(x))


@cli.command()
@click.argument("data", nargs=1, required=True)
@click.pass_obj
def set_catalog(args, data):
    """
    set a catalog entity with the given json string

    """
    base_url, token, verify = get_base_url_token(args)
    x = _set_catalog(token, base_url, data, ssl_verify=verify)
    click.echo(json.dumps(x))


@cli.command()
@click.argument("pid", nargs=1, required=True)
@click.pass_obj
def refresh_pds(args, pid):
    """
    refresh metadata/reflections for a given physical dataset

    """
    base_url, token, verify = get_base_url_token(args)
    x = _refresh_pds(token, base_url, pid, ssl_verify=verify)
    click.echo(json.dumps(x))


@cli.command()
@click.argument("uid", nargs=1, required=True)
@click.option("-l", "--lifetime", help="lifetime of token in hours", default=24, type=int)
@click.option("-n", "--name", help="name of token")
@click.pass_obj
def set_pat(args, uid, lifetime, name):
    """
    create a personal access token

    """
    base_url, token, verify = get_base_url_token(args)
    x = _set_personal_access_token(token, base_url, uid, name, lifetime, ssl_verify=verify)
    click.echo(json.dumps(x))


@cli.command()
@click.argument("uid", nargs=1, required=True)
@click.pass_obj
def delete_pat(args, uid):
    """
    delete a personal access token

    """
    base_url, token, verify = get_base_url_token(args)
    x = _delete_personal_access_token(token, base_url, uid, ssl_verify=verify)
    click.echo(json.dumps(x))


@cli.command()
@click.argument("data", nargs=1, required=True)
@click.pass_obj
def modify_rules(args, data):
    """
    update rules given a data json object

    """
    base_url, token, verify = get_base_url_token(args)
    x = _modify_rules(token, base_url, data, ssl_verify=verify)
    click.echo(json.dumps(x))


@cli.command()
@click.argument("jobid", nargs=1, required=True)
@click.pass_obj
def cancel_job(args, jobid):
    """
    cancel a job

    """
    base_url, token, verify = get_base_url_token(args)
    x = _cancel_job(token, base_url, jobid, ssl_verify=verify)
    click.echo(json.dumps(x))


@cli.command()
@click.argument("json", nargs=1, required=True)
@click.option("-q", "--queue-id", help="queue id to modify")
@click.pass_obj
def modify_queue(args, json_queue, rid):
    """
    modify a queue

    """
    base_url, token, verify = get_base_url_token(args)
    x = _modify_queue(token, base_url, rid, json_queue, ssl_verify=verify)
    click.echo(json.dumps(x))


@cli.command()
@click.argument("json", nargs=1, required=True)
@click.pass_obj
def create_queue(args, json_queue):
    """
    create a queue

    """
    base_url, token, verify = get_base_url_token(args)
    x = _create_queue(token, base_url, json_queue, ssl_verify=verify)
    click.echo(json.dumps(x))


@cli.command()
@click.option("-r", "--queue-id", help="queue id to modify")
@click.pass_obj
def delete_queue(args, rid):
    """
    delete a queue

    """
    base_url, token, verify = get_base_url_token(args)
    x = _delete_queue(token, base_url, rid, ssl_verify=verify)
    click.echo(json.dumps(x))


@cli.command()
@click.argument("json", nargs=1, required=True)
@click.option("-r", "--reflection-id", help="reflection id to modify")
@click.pass_obj
def modify_reflection(args, json_reflection, rid):
    """
    modify a reflection

    """
    base_url, token, verify = get_base_url_token(args)
    x = _modify_reflection(token, base_url, rid, json_reflection, ssl_verify=verify)
    click.echo(json.dumps(x))


@cli.command()
@click.argument("json", nargs=1, required=True)
@click.pass_obj
def create_reflection(args, json_reflection):
    """
    create a reflection

    """
    base_url, token, verify = get_base_url_token(args)
    x = _create_reflection(token, base_url, json_reflection, ssl_verify=verify)
    click.echo(json.dumps(x))


@cli.command()
@click.option("-r", "--reflection-id", help="reflection id to modify")
@click.pass_obj
def delete_reflection(args, rid):
    """
    delete a reflection

    """
    base_url, token, verify = get_base_url_token(args)
    x = _delete_reflection(token, base_url, rid, ssl_verify=verify)
    click.echo(json.dumps(x))


@cli.command()
@click.argument("cid", nargs=1, required=True)
@click.pass_obj
def graph(args, cid):
    """
    return the parent/child details of a given item

    provide cid
    if cid is not specified it causes an error

    """
    base_url, token, verify = get_base_url_token(args)
    x = _graph(token, base_url, cid, ssl_verify=verify)
    click.echo(json.dumps(x))


if __name__ == "__main__":
    sys.exit(cli())  # pragma: no cover
