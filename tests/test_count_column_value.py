#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = "hbh112233abc@163.com"

import time

from line_profiler import LineProfiler
from think_sql.database import DB
from think_sql.sql_helper import count_column_value


def old_count_column_value(db, table_name, field_name, sample_size):
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
    return result


def test_count_column_value():
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
        result = old_count_column_value(db, table_name, field_name, sample_size)
        print(time.time() - t)
        print(result)


def test_line_profile():
    pf = LineProfiler(count_column_value)
    cfg = {
        "database": "cab100151",
        "host": "192.168.102.154",
        "port": 3306,
        "username": "root",
        "password": "xmhymake",
    }
    db = DB(cfg)
    field_name = "unselect_count"
    table_name = "hy_multi_archives_dirs"
    sample_size = 100000
    pf.runcall(count_column_value, db, table_name, field_name, sample_size)
    pf.print_stats()


def test_memory_profile():
    from memory_profiler import LineProfiler, show_results

    lf = LineProfiler()
    cfg = {
        "database": "cab100151",
        "host": "192.168.102.154",
        "port": 3306,
        "username": "root",
        "password": "xmhymake",
    }
    db = DB(cfg)
    field_name = "unselect_count"
    table_name = "hy_multi_archives_dirs"
    sample_size = 100000
    run = lf(count_column_value)
    run(db, table_name, field_name, sample_size)
    show_results(lf)
