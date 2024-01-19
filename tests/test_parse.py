#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = "hbh112233abc@163.com"

from pprint import pprint
from sql_metadata import Parser
from think_sql import parse


def test_alter_sql():
    sql = """
    alter     table
    slow_log_test
    add
    iii int not null default 0  comment   'a';
    """

    res = parse.alter_sql(sql)
    print(0, res)
    result = {
        "table": "slow_log_test",
        "field": "iii",
        "field_type": "int",
        "is_null": "NOT NULL",
        "default": "0",
        "comment": "a",
        "after": "",
    }
    assert res == result

    sql = "ALTER TABLE `sysbase_organization` ADD COLUMN `ref_org_id` varchar(36) NULL COMMENT '第三方组织id' AFTER `online_type`"
    res = parse.alter_sql(sql)
    print(1, res)
    assert res == {
        "table": "`sysbase_organization`",
        "field": "`ref_org_id`",
        "field_type": "varchar(36)",
        "is_null": "NULL",
        "default": "",
        "comment": "第三方组织id",
        "after": "`online_type`",
    }

    sql = "ALTER TABLE `sysbase_organization` ADD COLUMN `ref_parent_org_id` varchar(36) NULL COMMENT '第三方父组织id' AFTER `ref_org_id`"
    res = parse.alter_sql(sql)
    print(2, res)
    assert res == {
        "table": "`sysbase_organization`",
        "field": "`ref_parent_org_id`",
        "field_type": "varchar(36)",
        "is_null": "NULL",
        "default": "",
        "comment": "第三方父组织id",
        "after": "`ref_org_id`",
    }

    sql = "ALTER TABLE `sysbase_organization` MODIFY COLUMN `create_time` timestamp(3) NOT NULL DEFAULT current_timestamp(3) COMMENT '创建时间' AFTER `create_user_id`"
    res = parse.alter_sql(sql)
    print(3, res)
    assert res == {
        "table": "`sysbase_organization`",
        "field": "`create_time`",
        "field_type": "timestamp(3)",
        "is_null": "NOT\xa0NULL",
        "default": "current_timestamp(3)",
        "comment": "创建时间",
        "after": "`create_user_id`",
    }

    sql = "ALTER TABLE `sysbase_organization` MODIFY COLUMN `modify_time` timestamp(3) NOT NULL DEFAULT '0000-00-00 00:00:00.000' ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '修改时间' AFTER `modify_user_id`;"
    res = parse.alter_sql(sql)
    print(4, res)
    assert res == {
        "table": "`sysbase_organization`",
        "field": "`modify_time`",
        "field_type": "timestamp(3)",
        "is_null": "NOT\xa0NULL",
        "default": "'0000-00-00\xa000:00:00.000'",
        "comment": "修改时间",
        "after": "`modify_user_id`",
    }


def test_select_sql():
    sql = """
        SELECT a.language_code,
            b.code_name language_name,
            a.default_language,
            '1' selected
        FROM sys_tenant_language a
        LEFT JOIN maas_public.sys_code b ON a.language_code = b.code_value
        AND b.type_code = 'TENANT_LANGUAGE'
        WHERE a.tenant_id = 'zh00012'
        limit (1,100)
        ORDER BY b.seq ASC,
                b.modify_time DESC
    """
    res = parse.select_sql(sql)
    print(res)
