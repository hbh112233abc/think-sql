#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'hbh112233abc@163.com'

import pytest
from think_sql.dm.db import DB
from think_sql.dm.table import Table
from think_sql.tool.util import DBConfig

# 假设的数据库配置信息
db_config = DBConfig(
    host="localhost",
    port=5236,
    username="SYSDBA",
    password="SYSDBA",
    database="DMHR"
)

def test_db_connection():
    db = DB(db_config)
    with db.start_trans():
        assert db.connector is not None
        assert db.cursor is not None
    db.close()

def test_db_execute():
    db = DB(db_config)
    sql = "SELECT 1 FROM DUAL;"
    with db.start_trans():
        result = db.exec(sql)
        assert result == 1

def test_db_query():
    db = DB(db_config)
    sql = "SELECT * FROM DUAL;"
    with db.start_trans():
        result = db.query(sql)
        assert len(result) >= 0

def test_db_error():
    db = DB(db_config)
    with pytest.raises(Exception):
        sql = "SELECT * FROM non_existing_table;"
        db.exec(sql)


def test_db_table():
    db = DB(db_config)
    table = db.table("test_table")
    assert isinstance(table, Table)
