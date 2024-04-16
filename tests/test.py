#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'hbh112233abc@163.com'

import pytest
from think_sql import DB,db
from think_sql.mysql import DB as MySQL
from think_sql.dm import DB as DM

def test_mysql_db():
    cfg = {
        'host': '127.0.0.1',
        'port': 3306,
        'username': 'root',
        'password': 'root',
        'database': 'test'
    }
    with DB(cfg) as d:
        assert isinstance(d,MySQL)

    d = db(cfg)
    assert isinstance(d,MySQL)

def test_dm_db():
    cfg = {
        'type': 'dm',
        'host': 'localhost',
        'port': 5236,
        'username': 'SYSDBA',
        'password': 'SYSDBA',
        'database': 'DMHR'
    }
    with DB(cfg) as d:
        assert isinstance(d,DM)
    d = db(cfg)
    assert isinstance(d,DM)

def test_error_type():
    cfg = {
        'type': 'error',
        'host': '127.0.0.1',
        'port': 3306,
        'username': 'root',
        'password': 'root',
        'database': 'test'
    }
    with pytest.raises(Exception):
        db(cfg)
    with pytest.raises(Exception):
        with DB(cfg) as d:
            pass
