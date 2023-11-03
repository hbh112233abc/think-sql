#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = "hbh112233abc@163.com"

import time
from think_sql.database import DB
from think_sql.sql_helper import count_column_value


def test_count():
    cfg = {
        "database": "cab100151",
        "host": "192.168.102.154",
        "port": 3306,
        "username": "root",
        "password": "xmhymake",
    }
    with DB(cfg) as db:
        field_name = "unselect_count"
        table_name = "hy_multi_archives_dirs"
        sample_size = 100000
        t = time.time()
        res = count_column_value(db, table_name, field_name, sample_size)
        print(time.time() - t)
        print(res)

        t = time.time()
        sql = f"""
        SELECT COUNT(*) as count
        FROM (
            SELECT {field_name}
            FROM {table_name}
            LIMIT {sample_size}
        ) AS subquery
        GROUP BY {field_name}
        HAVING COUNT(*) >=
        CASE
            WHEN (SELECT COUNT(*) FROM {table_name} LIMIT {sample_size}) < {sample_size}
            THEN (SELECT COUNT(*) FROM {table_name}) / 2
            ELSE {sample_size} / 2
        END;
        """
        result = db.query(sql)
        print(time.time() - t)
        print(result)
