#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = "hbh112233abc@163.com"

from think_sql.mysql.sql_helper import *


def test_suggestion():
    sql = """SELECT cid,cdefid,dcode,cname,subject,kvs,nodetype FROM hy_cabrecs  WHERE status = 1 AND ccode regexp '[A-Z0-9]{16}' AND dcodepath like "%_ls20221107174046_ls20221107174046007%" AND dcode like concat("%",'a.1.1','%')"""
    where = parse_where_condition(sql)
    suggestion(where)
