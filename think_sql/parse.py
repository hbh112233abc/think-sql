#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = "hbh112233abc@163.com"

import sql_metadata
import sqlparse


def alter_sql(alter_sql: str) -> dict:
    """解析alter语句

    Args:
        alter_sql (str): alter sql

    Raises:
        ValueError: "Not a ALTER SQL"

    Returns:
        dict: {"table": "","field": "","field_type": "","is_null": "","default": "","comment": "","after": ""}
    """
    alter_sql = sqlparse.format(alter_sql, reindent=True, keyword_case="upper")
    print("sql format:", alter_sql)
    # 解析sql
    parsed = sqlparse.parse(alter_sql)
    # 获取第一个sql
    sql = parsed[0]
    # 获取sql类型
    sql_type = sql.get_type()
    if sql_type != "ALTER":
        raise ValueError("Not a ALTER SQL")

    res = {
        "table": "",
        "field": "",
        "field_type": "",
        "is_null": "",
        "default": "",
        "comment": "",
        "after": "",
    }

    for i, token in enumerate(sql.tokens):
        if token.is_keyword:
            key = token.value.lower()
            if key == "table":
                res["table"] = sql.tokens[i + 2].value
                continue
            if key in (
                "add",
                "modify",
            ):
                offset = 0
                if (
                    sql.tokens[i + 2].is_keyword
                    and sql.tokens[i + 2].value.lower() == "column"
                ):
                    offset = 2
                res["field"] = sql.tokens[i + offset + 2].value
                res["field_type"] = sql.tokens[i + offset + 4].value
                res["is_null"] = sql.tokens[i + offset + 6].value
                continue
            if key == "default":
                res["default"] = sql.tokens[i + 2].value
                if res["default"] in ("''", '""'):
                    res["default"] = ""
                continue
            if key == "comment":
                res["comment"] = sql.tokens[i + 2].value.replace("'", "")
                continue
            if key == "after":
                res["after"] = sql.tokens[i + 2].value
                continue

    return res


def select_sql(sql) -> dict:
    """解析select语句

    Args:
        sql (str): select sql

    Returns:
        dict: {"table":[],"select":[],"join":[],"where":[],"order_by":[],"group_by":[]}
    """
    sql = sqlparse.format(sql, reindent=True, keyword_case="upper")
    parser = sql_metadata.Parser(sql)
    table_names = parser.tables
    data = parser.columns_dict
    res = {
        "table": [],
        "select": [],
        "join": [],
        "where": [],
        "order_by": [],
        "group_by": [],
    }
    data["table"] = table_names
    res.update(data)
    return res
