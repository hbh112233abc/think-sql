#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = "hbh112233abc@163.com"

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
                res["field"] = sql.tokens[i + 2].value
                res["field_type"] = sql.tokens[i + 4].value
                res["is_null"] = sql.tokens[i + 6].value
                continue
            if key == "default":
                res["default"] = sql.tokens[i + 2].value
                if res["default"] in ("''", '""'):
                    res["default"] = ""
                continue
            if key == "comment":
                res["comment"] = sql.tokens[i + 2].value
                continue
            if key == "after":
                res["after"] = sql.tokens[i + 2].value
                continue

    return res


if __name__ == "__main__":
    sql = """
    alter     table
    slow_log_test
    add
    iii int not null default 0  comment   'a';
    """

    print(alter_sql(sql))
