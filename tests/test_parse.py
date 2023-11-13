#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = "hbh112233abc@163.com"

from pprint import pprint
from sql_metadata import Parser


def test_parse():
    sql = """
        SELECT a.language_code,
            b.code_name language_name,
            a.default_language,
            '1' selected
        FROM sys_tenant_language a
        LEFT JOIN maas_public.sys_code b ON a.language_code = b.code_value
        AND b.type_code = 'TENANT_LANGUAGE'
        WHERE a.tenant_id = 'zh00012'
        ORDER BY b.seq ASC,
                b.modify_time DESC
    """
    parser = Parser(sql)
    table_names = parser.tables
    # log(f"表名是: {table_names}")
    table_aliases = parser.tables_aliases
    data = parser.columns_dict
    if not data:
        return "sql解析无法获取字段或相关查询条件,无法分析"
    pprint(data)
    select_fields = data.get("select", [])
    join_fields = data.get("join", [])
    where_fields = data.get("where", [])
    # log(f"WHERE字段是：{where_fields}")
    order_by_fields = data.get("order_by", [])
    group_by_fields = data.get("group_by", [])
