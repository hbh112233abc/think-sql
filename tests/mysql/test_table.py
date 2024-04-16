from decimal import Decimal

import pytest
import pymysql
import cacheout
from redis import Redis

from think_sql.mysql.db import DB
from think_sql.mysql.table import Table
from think_sql.mysql.util import parse_where
from think_sql.tool.util import DBConfig

table_name = "test"
columns = ("id","username","age","state")
test_data = {"username":"Judy","age":25,"state":1}

def create_table(db):
    sql = """
    CREATE TABLE test.test (
        id INT UNSIGNED auto_increment NOT NULL,
        username varchar(100) NOT NULL,
        age TINYINT UNSIGNED NOT NULL,
        state TINYINT DEFAULT 0 NOT NULL,
        CONSTRAINT test_pk PRIMARY KEY (id)
    )
    ENGINE=InnoDB
    DEFAULT CHARSET=utf8mb4
    COLLATE=utf8mb4_general_ci;
    """
    db.execute(sql)

def insert_data(db):
    sql = """
    INSERT INTO test (username,age,state) VALUES ('Judy',29,2);
    INSERT INTO test (username,age,state) VALUES ('Alice',25,1);
    INSERT INTO test (username,age,state) VALUES ('Bob',30,2);
    INSERT INTO test (username,age,state) VALUES ('Charlie',22,1);
    INSERT INTO test (username,age,state) VALUES ('David',28,2);
    INSERT INTO test (username,age,state) VALUES ('Eve',35,1);
    INSERT INTO test (username,age,state) VALUES ('Frank',40,2);
    INSERT INTO test (username,age,state) VALUES ('Grace',19,1);
    INSERT INTO test (username,age,state) VALUES ('Heidi',31,2);
    INSERT INTO test (username,age,state) VALUES ('Ivan',23,1);
    """
    for s in sql.split(";"):
        db.execute(s)

def drop_table(db,table_name):
    sql = f"DROP TABLE IF EXISTS {table_name};"
    db.execute(sql)

@pytest.fixture(scope="module")
def db():
    db_config = DBConfig(
        host="localhost",
        port=3306,
        user="root",
        password="root",
        database="test"
    )
    db = DB(db_config)
    drop_table(db,table_name)
    create_table(db)
    insert_data(db)
    yield db
    drop_table(db,table_name)
    db.close()



def test_debug(db):
    table = db.table('test')
    table.debug()
    assert table._debug == True
    table.debug(False)
    assert table._debug == False

def test_set_cache_storage(db):
    table = db.table('test')
    assert isinstance(table.cache_storage, cacheout.Cache)
    storage = Redis('192.168.102.154', 6379, 0, 'Efileredis0516')
    table.set_cache_storage(storage)
    assert isinstance(table.cache_storage, Redis)

def test_cursor(db):
    res = db.table('test').where('id', 1).cursor()
    assert isinstance(res, pymysql.cursors.Cursor)

def test_get_last_sql(db):
    table = db.table('test')
    table.where('id', 1).find()
    sql = table.get_last_sql()
    assert isinstance(sql, str)
    assert sql == "SELECT * FROM test  WHERE id = '1' LIMIT 1"

def test_find(db):
    data_id = 1
    result = db.table('test').where('id', data_id).find()
    assert isinstance(result, dict)
    assert result.get('id') == data_id

def test_select(db):
    res = db.table('test').where('id', 1).select()
    assert isinstance(res, list)
    assert isinstance(res[0], dict)
    assert len(res) == 1

def test_build_sql(db):
    res = db.table('test').where(
        'id', 1).fetch_sql(True).find()
    assert isinstance(res, str)
    assert res == "SELECT * FROM test  WHERE id = '1' LIMIT 1"

def test_cache(db):
    a = db.table('test').cache().where('id', 1).find()
    b = db.table('test').cache().where('id', 1).find()
    assert a == b

def test_where(db):
    # 单条件
    a1 = db.table('test').where('id', 1).find()
    a2 = db.table('test').where('id', '=', 1).find()
    a3 = db.table('test').where('id', 'eq', 1).find()
    a4 = db.table('test').where([['id', '=', 1]]).find()
    a5 = db.table('test').where({'id': 1}).find()
    assert a1 == a2
    assert a2 == a3
    assert a3 == a4
    assert a5 == a4

    with pytest.raises(ValueError):
        db.table('test').where(
            [['id', '=', 1, 100002]]).find()
    with pytest.raises(ValueError):
        db.table('test').where([['id=1']]).find()

    with pytest.raises(ValueError):
        db.table('test').where(11).find()

def test_parse_where(db):
    for x in ('=', 'eq'):
        result = parse_where('id', x, 1)
        assert result == (" AND id = %s", (1,))
    for x in ('neq', '!=', '<>'):
        result = parse_where('id', x, 1)
        assert result == (" AND id <> %s", (1,))
    for x in ('gt', '>'):
        result = parse_where('id', x, 1)
        assert result == (" AND id > %s", (1,))
    for x in ('egt', '>='):
        result = parse_where('id', x, 1)
        assert result == (" AND id >= %s", (1,))
    for x in ('lt', '<'):
        result = parse_where('id', x, 1)
        assert result == (" AND id < %s", (1,))
    for x in ('elt', '<='):
        result = parse_where('id', x, 1)
        assert result == (" AND id <= %s", (1,))
    for x in ('in', 'not in'):
        result = parse_where('id', x, '1,100002,100003')
        assert result == (
            f" AND id {x} (%s,%s,%s)", ('1', '100002', '100003'))
        result = parse_where('id', x, [1, 100002, 100003])
        assert result == (
            f" AND id {x} (%s,%s,%s)", (1, 100002, 100003))

        result = parse_where('id', x, '(1,100002,100003)')
        assert result == (f" AND id {x} %s", ('(1,100002,100003)',))

    for x in ('between', 'not between'):
        result = parse_where('id', x, '1 and 100002')
        assert result == (f" AND id {x} %s", ('1 and 100002',))

        result = parse_where('id', x, '1,100002')
        assert result == (f" AND id {x} %s", ('1 AND 100002',))

        result = parse_where('id', x, (1, 100002))
        assert result == (f" AND id {x} %s", ('1 AND 100002',))

        with pytest.raises(ValueError):
            result = parse_where('id', x, (1,))
        with pytest.raises(ValueError):
            result = parse_where('id', x, (1, 100002, 100003))
        with pytest.raises(ValueError):
            result = parse_where('id', x, '1')

    for x in ('like', 'not like'):
        with pytest.raises(ValueError):
            parse_where('id', x, 1)
        with pytest.raises(ValueError):
            parse_where('id', x, '1')
        result = parse_where('id', x, '1%')
        assert result == (f" AND id {x} %s", ('1%',))
        result = parse_where('id', x, '1_')
        assert result == (f" AND id {x} %s", ('1_',))

    with pytest.raises(ValueError):
        parse_where('id', 'xx', 1)

    res = parse_where('id', 'is', 100)
    assert res == (f" AND id is %s", (100,))

    for x in ('null', 'is null'):
        res = parse_where('id', x)
        assert res == (f" AND id is null", tuple())

    for x in ('not null', 'is not null'):
        res = parse_where('id', x)
        assert res == (f" AND id is not null", tuple())

    for x in ('exists', 'not exists'):
        res = parse_where('id', x)
        assert res == (f" AND {x}(id)", tuple())

    res = parse_where('id', 'exp', 'in(1,2,3)')
    assert res == (f" AND id in(1,2,3)", tuple())

    with pytest.raises(ValueError):
        res = parse_where('id', 'exp', 1)

    res = parse_where('id', 1)
    assert res == (f" AND id = %s", ('1',))

    for x in ('eq', '=', 'neq', '!=', '<>', 'gt', '>', 'egt', '>=', 'lt', '<', 'elt', '<=', 'in', 'not in', 'between', 'not between', 'like', 'not like', 'is'):
        with pytest.raises(ValueError):
            parse_where('id', x, None)

        with pytest.raises(ValueError):
            parse_where('id', x)

def test_field(db):
    table = db.table('test')
    table.field(True)
    for k in columns:
        assert k in table.select_fields

    table.field('*')
    for k in columns:
        assert k in table.select_fields

    table.field('id,pid')
    assert table.select_fields == ('id', 'pid')

    table.init()
    table.field('id', exclude=True)
    assert 'username' in table.select_fields
    assert 'age' in table.select_fields
    assert 'state' in table.select_fields

def test_value(db):
    res = db.table('test').where('id', 1).value('username')
    assert res == 'Judy'

def test_column(db):
    res = db.table('test').where('state', 1).column('username')
    assert isinstance(res, list)
    assert len(res) > 1
    res = db.table('test').where(
        'state', 1).column('username', 'id')
    assert isinstance(res, dict)
    assert len(res.keys()) > 0
    assert res[2] == 'Alice'

    res = db.table('test').where(
        'state', 1).column('username,age', 'id')
    assert isinstance(res, dict)
    assert len(res.keys()) > 0
    assert res[2]['username'] == 'Alice'

    res = db.table('test').where(
        'state', 1).column('username,age')
    assert isinstance(res, list)
    assert len(res) > 0
    assert isinstance(res[0], dict)

def test_alias(db):
    table = db.table('test').alias('xx')
    assert table.table_name == 'test AS xx'

def test_join(db):
    table = db.table('test').alias('province')
    table.join('test', 'city', 'province.id = city.pid')
    assert isinstance(table.join_list, list)
    assert table.join_list[0] == 'INNER JOIN test AS city ON province.id = city.pid'

    table = db.table('test').alias('province').join(
        'left join test AS city ON province.id = city.pid')
    assert isinstance(table.join_list, list)
    assert table.join_list[0] == 'left join test AS city ON province.id = city.pid'

    table = db.table('test').alias('province').join(
        'test', 'city', 'province.id = city.pid', 'left', 'city.id>=100102')
    assert isinstance(table.join_list, list)
    assert table.join_list[0] == 'LEFT JOIN test AS city ON province.id = city.pid AND city.id>=100102'

    with pytest.raises(ValueError):
        table = db.table('test').alias('province').join(
            'test', 'city', 'province.id = city.pid', 'in')

    table = db.table('test').alias('a')
    table.join('test', 'b', 'a.id=b.pid')
    table.join('test', 'c', 'b.id=c.pid')

    assert len(table.join_list) == 2

def test_insert(db):
    res = db.table('test').fetch_sql().insert(
        {'id': 1, 'username': 'china'})
    assert res == "INSERT INTO test (id,username) VALUES (1,'china');"

    res = db.table('test').fetch_sql().insert([
        {'id': 1, 'username': 'china'}])

    assert res == "INSERT INTO test (id,username) VALUES (1,'china');"

    res = db.table('test').fetch_sql().insert([
        {'id': 1, 'username': 'china'},
        {'id': 2, 'username': 'fujian'},
    ])

    assert res == "INSERT INTO test (id,username) VALUES (1,'china'),(2,'fujian');"

    res = db.table('test').fetch_sql().insert(
        {'id': 1, 'name': 'china'}, True)
    assert res == "REPLACE INTO test (id,name) VALUES (1,'china');"

def test_update(db):
    with pytest.raises(ValueError):
        res = db.table('test').fetch_sql().update(
            {'name': 'china'})

    res = db.table('test').where(
        'id', 1).fetch_sql().update({'name': 'china'})
    assert res == "UPDATE test SET name='china' WHERE id = '1';"

    res = db.table('test').fetch_sql().update(
        {'name': 'china'}, True)
    assert res == "UPDATE test SET name='china' WHERE 1=1;"

def test_delete(db):
    with pytest.raises(ValueError):
        res = db.table('test').fetch_sql().delete()

    res = db.table('test').where('id', 1).fetch_sql().delete()
    assert res == "DELETE FROM test WHERE id = '1';"

    res = db.table('test').fetch_sql().delete(True)
    assert res == "DELETE FROM test WHERE 1=1;"

def test_inc(db):
    with pytest.raises(ValueError):
        res = db.table('test').fetch_sql().inc('username')

    res = db.table('test').fetch_sql().where('id', 1).inc('username')
    assert res == "UPDATE test SET `username` = `username`+1 WHERE id = '1'"

    res = db.table('test').fetch_sql().where(
        'id', 1).inc('username', 2)
    assert res == "UPDATE test SET `username` = `username`+2 WHERE id = '1'"

    res = db.table('test').fetch_sql().where(
        'id', 1).inc('username', '1.2')
    assert res == "UPDATE test SET `username` = `username`+1.2 WHERE id = '1'"

    with pytest.raises(ValueError):
        res = db.table('test').fetch_sql().where(
            'id', 1).inc('username', 'a')

def test_dec(db):
    with pytest.raises(ValueError):
        res = db.table('test').fetch_sql().dec('username')

    res = db.table('test').fetch_sql().where('id', 1).dec('username')
    assert res == "UPDATE test SET `username` = `username`-1 WHERE id = '1'"

    res = db.table('test').fetch_sql().where(
        'id', 1).dec('username', 2)
    assert res == "UPDATE test SET `username` = `username`-2 WHERE id = '1'"

    res = db.table('test').fetch_sql().where(
        'id', 1).dec('username', '1.2')
    assert res == "UPDATE test SET `username` = `username`-1.2 WHERE id = '1'"

    with pytest.raises(ValueError):
        res = db.table('test').fetch_sql().where(
            'id', 1).dec('username', 'a')

def test_max(db):
    res = db.table('test').max('id')
    assert res == 10

    res = db.table('test').where('id', '<', 0).max('id')
    assert res == 0
    with pytest.raises(Exception):
        res = db.table('test').max('ids')

def test_sum(db):
    sql = db.table('test').fetch_sql().sum('id')
    assert sql == 'SELECT SUM(`id`) AS sum FROM `test` WHERE 1=1 LIMIT 1'

    res = db.table('test').where('state', 1).sum('age')
    assert isinstance(res, Decimal)
    assert res > 0

def test_avg(db):
    sql = db.table('test').fetch_sql().where(
        'pid', 1).avg('username')
    assert sql == "SELECT AVG(`username`) AS avg FROM `test` WHERE pid = '1' LIMIT 1"

def test_count(db):
    sql = db.table('test').fetch_sql().where(
        'pid', 1).count()
    assert sql == "SELECT COUNT(1) AS count FROM `test` WHERE pid = '1' LIMIT 1"
    sql = db.table('test').fetch_sql().where(
        'pid', 1).count('id')
    assert sql == "SELECT COUNT(id) AS count FROM `test` WHERE pid = '1' LIMIT 1"

def test_copy_to(db):
    sql = db.table('test').fetch_sql().copy_to()
    assert sql == 'SELECT * INTO test_copy FROM test WHERE 1=1'
    sql = db.table('test').fetch_sql().copy_to(
        create_blank_table=True)
    assert sql == 'SELECT * INTO test_copy FROM test WHERE 1=0'

    sql = db.table('test').fetch_sql().copy_to('new_table')
    assert sql == 'SELECT * INTO new_table FROM test WHERE 1=1'

    sql = db.table('test').fetch_sql().field(
        'id, name').copy_to('new_table')
    assert sql == 'SELECT id, name INTO new_table FROM test WHERE 1=1'

    sql = db.table('test').fetch_sql().field(
        ['id', 'name']).copy_to('new_table')
    assert sql == 'SELECT id, name INTO new_table FROM test WHERE 1=1'

    sql = db.table('test').fetch_sql().field(
        ['id', 'name']).where('id', 1).copy_to('new_table')
    assert sql == "SELECT id, name INTO new_table FROM test WHERE id = '1'"

def test_insert_to(db):
    sql = db.table('test').fetch_sql(
    ).insert_to('new_table', 'id,name')
    assert sql == "INSERT INTO new_table ('id','name') SELECT * FROM test  WHERE 1=1"

def test_exists(db):
    res = db.table('test').where('id', 1).exists()
    assert res == True
    res = db.table('test').where('id', 999).exists()
    assert res == False

def test_batch_update(db):
    data = []
    for i in range(1, 10):
        data.append({'id': i, 'state': 1})
    res = db.table('test').batch_update(data, key='id')
    assert isinstance(res, int)
    assert res > 0
