# -*- coding: utf-8 -*-
from six.moves.urllib.parse import quote


def promote_catalog(client, catalog, file_format="parquet", **kwargs):
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
    cid = quote(catalog.get().meta.id, safe="")
    return promote(
        client,
        cid,
        {
            "id": cid,
            "path": catalog.meta.path,
            "type": "PHYSICAL_DATASET",
            "entityType": "dataset",
            "format": format_dict,
        },
    )


def promote(client, cid, json):
    return client.simple().promote_catalog(cid, json)
