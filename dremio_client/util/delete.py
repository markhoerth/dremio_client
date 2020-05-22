# -*- coding: utf-8 -*-
from ..model.endpoints import (
    catalog_item as _catalog_item,
    delete_catalog as _delete_catalog,
)


def delete_catalog(base_url, token, verify, cid, path):
    """
    delete a catalog entity given by cid or path

    warning, this process is destructive and permanent
    """
    res = _catalog_item(token, base_url, None, path, ssl_verify=verify)
    tag = res["tag"]
    if path:
        cid = res["id"]
    x = _delete_catalog(token, base_url, cid, tag, ssl_verify=verify)
    return x
