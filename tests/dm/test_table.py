#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'hbh112233abc@163.com'

from decimal import Decimal
import pytest
from think_sql.dm.table import Table
from think_sql.dm.db import DB
from think_sql.tool.util import DBConfig

table_name = "TEST"
columns = ("ID","USERNAME","AGE","STATE")
test_data = {"USERNAME":"Judy","AGE":25,"STATE":1}

def create_table(db):
    sql = """
    CREATE TABLE DMHR.TEST (
        ID INT NOT NULL IDENTITY(1, 1),
        USERNAME VARCHAR(100) NOT NULL,
        AGE TINYINT NOT NULL,
        STATE TINYINT NOT NULL,
        PRIMARY KEY (ID)
    );
    """
    db.execute(sql)

def insert_data(db):
    sql = """
    INSERT INTO DMHR.TEST (USERNAME,AGE,STATE) VALUES ('Judy',29,2);
    INSERT INTO DMHR.TEST (USERNAME,AGE,STATE) VALUES ('Alice',25,1);
    INSERT INTO DMHR.TEST (USERNAME,AGE,STATE) VALUES ('Bob',30,2);
    INSERT INTO DMHR.TEST (USERNAME,AGE,STATE) VALUES ('Charlie',22,1);
    INSERT INTO DMHR.TEST (USERNAME,AGE,STATE) VALUES ('David',28,2);
    INSERT INTO DMHR.TEST (USERNAME,AGE,STATE) VALUES ('Eve',35,1);
    INSERT INTO DMHR.TEST (USERNAME,AGE,STATE) VALUES ('Frank',40,2);
    INSERT INTO DMHR.TEST (USERNAME,AGE,STATE) VALUES ('Grace',19,1);
    INSERT INTO DMHR.TEST (USERNAME,AGE,STATE) VALUES ('Heidi',31,2);
    INSERT INTO DMHR.TEST (USERNAME,AGE,STATE) VALUES ('Ivan',23,1);
    """
    for s in sql.split(";"):
        db.execute(s)

def drop_table(db,table_name):
    sql = f"DROP TABLE IF EXISTS DMHR.{table_name};"
    db.execute(sql)

# 假设的数据库实例和配置
@pytest.fixture(scope="module")
def db():
    db_config = DBConfig(
        host="localhost",
        port=5236,
        user="SYSDBA",
        password="SYSDBA",
        database="DMHR"
    )
    db = DB(db_config)
    drop_table(db,table_name)
    create_table(db)
    insert_data(db)
    yield db
    drop_table(db,table_name)
    db.close()


def test_table_init(db):
    table = Table(db, table_name)
    assert table.db is db
    assert table.table_name == table_name

def test_table_get_fields(db):
    table = Table(db, table_name)
    fields = table.get_fields()
    assert isinstance(fields, tuple)
    assert len(fields) > 0

def test_table_query(db):
    table = Table(db, table_name)
    sql = f"SELECT * FROM DMHR.{table_name};"
    result = table.query(sql)
    assert isinstance(result, list)
    assert len(result) >= 0

def test_table_execute(db):
    table = Table(db, table_name)
    sql = f"UPDATE DMHR.{table_name} SET USERNAME = 'arbing' WHERE ID=1;"
    result = table.execute(sql)
    assert result >= 0

def test_table_error(db):
    table = Table(db, table_name)
    with pytest.raises(Exception):
        sql = "UPDATE non_existing_table SET column_name = 'new_value' WHERE 1=1;"
        table.execute(sql)


def test_table_limit(db):
    table = Table(db, table_name)
    table.page(1)
    assert table.limit_dict == {"sql": " LIMIT %s,%s", "params": (0, 20)}
    table.page(2,10)
    assert table.limit_dict == {"sql": " LIMIT %s,%s", "params": (10, 10)}

def test_table_order(db):
    table = Table(db, table_name)
    table.order("AGE", "desc")
    assert table.order_by == " ORDER BY \"AGE\" DESC"

def test_table_distinct(db):
    table = Table(db, table_name)
    table.distinct("USERNAME")
    assert table.distinct_by == "DISTINCT \"USERNAME\""

def test_table_group(db):
    table = Table(db, table_name)
    table.group("AGE")
    assert table.group_by == " GROUP BY \"AGE\""

def test_table_field(db):
    table = Table(db, table_name)
    table.field("USERNAME")
    assert table.select_fields == ("USERNAME",)
    table.init()
    table.field("ID",True)
    assert "USERNAME" in table.select_fields
    assert "AGE" in table.select_fields
    assert "STATE" in table.select_fields

def test_table_select(db):
    table = Table(db, table_name)
    result = table.select()
    assert isinstance(result, list)
    assert len(result) >= 0

def test_table_find(db):
    table = Table(db, table_name)
    table.where("ID", 1)
    result = table.find()
    assert isinstance(result, dict)
    assert result['ID'] == 1
    table.where("ID", 100)
    result = table.find()
    assert isinstance(result, dict)
    assert result == {}

def test_table_value(db):
    table = Table(db, table_name)
    table.where("ID", 1)
    result = table.value("USERNAME")
    user = table.where("ID", 1).find()
    assert result == user["USERNAME"]
    table.where("ID", 100)
    result = table.value("USERNAME")
    assert result == ""

def test_table_column(db):
    table = Table(db, table_name)
    result = table.column("USERNAME", "ID")
    assert isinstance(result, list)
    assert len(result) > 0

def test_table_alias(db):
    table = Table(db, table_name)
    table.alias("t")
    assert table.table_name == f"DMHR.{table_name} AS t"

def test_table_join(db):
    table = Table(db, table_name)
    table.join("another_table", "at", "at.id = test_table.id", "inner")
    join_str = "INNER JOIN \"DMHR\".\"another_table\" AS \"at\" ON at.id = test_table.id"
    assert table.join_list[0] == join_str

def test_table_union(db):
    table = Table(db, table_name)
    table.union("SELECT * FROM test_table", "SELECT * FROM another_table", False)
    assert table.table_name.startswith("(SELECT * FROM test_table UNION SELECT * FROM another_table) AS t")
    table.union("SELECT * FROM test_table", "SELECT * FROM another_table", True)
    assert table.table_name.startswith("(SELECT * FROM test_table UNION ALL SELECT * FROM another_table) AS t")

def test_table_insert(db):
    table = Table(db, table_name)
    result = table.insert(test_data)
    assert result > 0
    test_data["USERNAME"] = "bing'he"
    result = table.insert(test_data)
    assert result > 0
    test_data.update({"ID": 99})
    result = table.insert(test_data)
    assert result > 0

def test_table_update(db):
    table = Table(db, table_name)
    update_data = {"STATE":2}
    with pytest.raises(ValueError):
        table.update(update_data)
    result = table.where("ID",2).update(update_data)
    assert result == 1
    result = table.update(update_data,True)
    assert result > 1

def test_table_delete(db):
    table = Table(db, table_name)
    with pytest.raises(ValueError):
        table.delete()
    result = table.where("ID",10).delete()
    assert result > 0


def test_table_inc(db):
    table = Table(db, table_name)
    user = table.where("ID", 1).find()
    result = table.where("ID", 1).inc("AGE", 1)
    assert result > 0
    user_new = table.where("ID", 1).find()
    assert user_new["AGE"] == (user["AGE"] + 1)

def test_table_dec(db):
    table = Table(db, table_name)
    user = table.where("ID", 1).find()
    result = table.where("ID", 1).dec("AGE", 1)
    assert result > 0
    user_new = table.where("ID", 1).find()
    assert user_new["AGE"] == (user["AGE"] - 1)

def test_table_max(db):
    table = Table(db, table_name)
    result = table.max("AGE")
    assert isinstance(result, (int, float))
    sql = "SELECT MAX(AGE) as max_age FROM DMHR.TEST;"
    res = db.query(sql)
    age = res[0]['max_age']
    assert age == result

def test_table_sum(db):
    table = Table(db, table_name)
    result = table.sum("AGE")
    assert isinstance(result, (int, float, Decimal))
    sql = "SELECT SUM(AGE) as sum_age FROM DMHR.TEST;"
    res = db.query(sql)
    sum_age = res[0]['sum_age']
    assert sum_age == result

def test_table_avg(db):
    table = Table(db, table_name)
    result = table.avg("AGE")
    assert isinstance(result, (int, float, Decimal))
    sql = "SELECT avg(AGE) as avg_age FROM DMHR.TEST;"
    avg_age = db.query(sql)[0]['avg_age']
    assert avg_age == result

def test_table_count(db):
    table = Table(db, table_name)
    result = table.count()
    assert isinstance(result, int)
    sql = "SELECT COUNT(1) as ct FROM DMHR.TEST;"
    count = db.query(sql)[0]['ct']
    assert count == result


def test_table_exists(db):
    table = Table(db, table_name)
    assert table.where('ID',1).exists() is True

def test_table_batch_update(db):
    table = Table(db, table_name)
    update_data = [
        {
            "USERNAME": "Judy",
            "AGE": 35,
            "STATE": 2
        },
        {
            "USERNAME":"Alice",
            "AGE": 30,
            "STATE": 2
        },
    ]
    result = table.batch_update(update_data, "USERNAME")
    assert result > 0
    user1 = table.where("USERNAME", "Judy").find()
    assert user1["AGE"] == 35
    user2 = table.where("USERNAME", "Alice").find()
    assert user2["AGE"] == 30
