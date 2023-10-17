#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = "hbh112233abc@163.com"

from think_sql.database import DB
from think_sql.sql_helper import help


def test_sql_helper():
    """
    测试sql_helper
    :return:
    """
    db_dsn = "root:'xmhymake'@192.168.102.154:3306/cab100001"
    with DB(db_dsn) as db:
        sql = "select * from hy_cabrecs where finished_count > 0"
        result = help(db, sql)
        assert len(result) > 0
