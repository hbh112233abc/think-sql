from decimal import Decimal
import pytest
from redis import Redis
import cacheout
import pymysql
from think_sql.database import DB
from think_sql.table import Table


class TestTable:
    def setup(self):
        self.config = {
            'database': 'test',
            'host': '127.0.0.1',
            'port': 3306,
            'username': 'root',
            'password': 'root',
        }
        self.db = DB(**self.config)

    def test_debug(self):
        table = self.db.table('sys_area')
        table.debug()
        assert table._Table__debug == True
        table.debug(False)
        assert table._Table__debug == False

    def test_set_cache_storage(self):
        table = self.db.table('sys_area')
        assert isinstance(table.cache_storage, cacheout.Cache)
        storage = Redis('192.168.102.154', 6379, 0, 'Efileredis0516')
        table.set_cache_storage(storage)
        assert isinstance(table.cache_storage, Redis)

    def test_cursor(self):
        res = self.db.table('sys_area').where('pid', 100001).cursor()
        assert isinstance(res, pymysql.cursors.Cursor)

    def test_get_last_sql(self):
        table = self.db.table('sys_area')
        table.where('pid', 100001).find()
        sql = table.get_last_sql()
        assert isinstance(sql, str)
        assert sql == "SELECT * FROM sys_area  WHERE pid = '100001' LIMIT 1"

    def test_find(self):
        data_id = 100001
        result = self.db.table('sys_area').where('id', data_id).find()
        assert isinstance(result, dict)
        assert result.get('id') == data_id

    def test_select(self):
        res = self.db.table('sys_area').where('pid', 100001).select()
        assert isinstance(res, list)
        assert isinstance(res[0], dict)
        assert len(res) == 16

    def test_build_sql(self):
        res = self.db.table('sys_area').where(
            'id', 100001).fetch_sql(True).find()
        assert isinstance(res, str)
        assert res == "SELECT * FROM sys_area  WHERE id = '100001' LIMIT 1"

    def test_cache(self):
        a = self.db.table('sys_area').cache().where('id', 100001).find()
        b = self.db.table('sys_area').cache().where('id', 100001).find()
        assert a == b

    def test_where(self):
        # 单条件
        a1 = self.db.table('sys_area').where('id', 100001).find()
        a2 = self.db.table('sys_area').where('id', '=', 100001).find()
        a3 = self.db.table('sys_area').where('id', 'eq', 100001).find()
        a4 = self.db.table('sys_area').where([['id', '=', 100001]]).find()
        a5 = self.db.table('sys_area').where({'id': 100001}).find()
        assert a1 == a2
        assert a2 == a3
        assert a3 == a4
        assert a5 == a4

        with pytest.raises(ValueError):
            self.db.table('sys_area').where(
                [['id', '=', 100001, 100002]]).find()
        with pytest.raises(ValueError):
            self.db.table('sys_area').where([['id=1']]).find()

        with pytest.raises(ValueError):
            self.db.table('sys_area').where(11).find()

    def test_parse_where(self):
        for x in ('=', 'eq'):
            result = Table.parse_where('id', x, 100001)
            assert result == (" AND id = %s", (100001,))
        for x in ('neq', '!=', '<>'):
            result = Table.parse_where('id', x, 100001)
            assert result == (" AND id <> %s", (100001,))
        for x in ('gt', '>'):
            result = Table.parse_where('id', x, 100001)
            assert result == (" AND id > %s", (100001,))
        for x in ('egt', '>='):
            result = Table.parse_where('id', x, 100001)
            assert result == (" AND id >= %s", (100001,))
        for x in ('lt', '<'):
            result = Table.parse_where('id', x, 100001)
            assert result == (" AND id < %s", (100001,))
        for x in ('elt', '<='):
            result = Table.parse_where('id', x, 100001)
            assert result == (" AND id <= %s", (100001,))
        for x in ('in', 'not in'):
            result = Table.parse_where('id', x, '100001,100002,100003')
            assert result == (
                f" AND id {x} (%s,%s,%s)", ('100001', '100002', '100003'))
            result = Table.parse_where('id', x, [100001, 100002, 100003])
            assert result == (
                f" AND id {x} (%s,%s,%s)", (100001, 100002, 100003))

            result = Table.parse_where('id', x, '(100001,100002,100003)')
            assert result == (f" AND id {x} %s", ('(100001,100002,100003)',))

        for x in ('between', 'not between'):
            result = Table.parse_where('id', x, '100001 and 100002')
            assert result == (f" AND id {x} %s", ('100001 and 100002',))

            result = Table.parse_where('id', x, '100001,100002')
            assert result == (f" AND id {x} %s", ('100001 AND 100002',))

            result = Table.parse_where('id', x, (100001, 100002))
            assert result == (f" AND id {x} %s", ('100001 AND 100002',))

            with pytest.raises(ValueError):
                result = Table.parse_where('id', x, (100001,))
            with pytest.raises(ValueError):
                result = Table.parse_where('id', x, (100001, 100002, 100003))
            with pytest.raises(ValueError):
                result = Table.parse_where('id', x, '100001')

        for x in ('like', 'not like'):
            with pytest.raises(ValueError):
                Table.parse_where('id', x, 1)
            with pytest.raises(ValueError):
                Table.parse_where('id', x, '1')
            result = Table.parse_where('id', x, '1%')
            assert result == (f" AND id {x} %s", ('1%',))
            result = Table.parse_where('id', x, '1_')
            assert result == (f" AND id {x} %s", ('1_',))

        with pytest.raises(ValueError):
            Table.parse_where('id', 'xx', 100001)

        res = Table.parse_where('id', 'is', 100)
        assert res == (f" AND id is %s", (100,))

        for x in ('null', 'is null'):
            res = Table.parse_where('id', x)
            assert res == (f" AND id is null", tuple())

        for x in ('not null', 'is not null'):
            res = Table.parse_where('id', x)
            assert res == (f" AND id is not null", tuple())

        for x in ('exists', 'not exists'):
            res = Table.parse_where('id', x)
            assert res == (f" AND {x}(id)", tuple())

        res = Table.parse_where('id', 'exp', 'in(1,2,3)')
        assert res == (f" AND id in(1,2,3)", tuple())

        with pytest.raises(ValueError):
            res = Table.parse_where('id', 'exp', 1)

        res = Table.parse_where('id', 100001)
        assert res == (f" AND id = %s", ('100001',))

        for x in ('eq', '=', 'neq', '!=', '<>', 'gt', '>', 'egt', '>=', 'lt', '<', 'elt', '<=', 'in', 'not in', 'between', 'not between', 'like', 'not like', 'is'):
            with pytest.raises(ValueError):
                Table.parse_where('id', x, None)

            with pytest.raises(ValueError):
                Table.parse_where('id', x)

    def test_field(self):
        table = self.db.table('sys_area')
        table.field(True)
        assert table.select_fields == ('id', 'pid', 'name', 'code')
        table.field('*')
        assert table.select_fields == ('id', 'pid', 'name', 'code')
        table.field('id,pid')
        assert table.select_fields == ('id', 'pid')
        table.init()
        table.field('id,pid', exclude=True)
        assert 'name' in table.select_fields
        assert 'code' in table.select_fields

    def test_value(self):
        res = self.db.table('sys_area').where('id', 100001).value('code')
        assert res == '110000'

    def test_column(self):
        res = self.db.table('sys_area').where('pid', 100001).column('code')
        assert isinstance(res, list)
        assert len(res) == 16
        res = self.db.table('sys_area').where(
            'pid', 100001).column('code', 'id')
        assert isinstance(res, dict)
        assert len(res.keys()) == 16
        assert res[100002]['code'] == '110101'

        res = self.db.table('sys_area').where(
            'pid', 100001).column('code,name', 'id')
        assert isinstance(res, dict)
        assert len(res.keys()) == 16
        assert res[100002]['code'] == '110101'

        res = self.db.table('sys_area').where(
            'pid', 100001).column('code,name')
        assert isinstance(res, list)
        assert len(res) == 16
        assert isinstance(res[0], dict)
        assert res[0]['code'] == '110101'

    def test_alias(self):
        table = self.db.table('sys_area').alias('xx')
        assert table.table_name == 'sys_area AS xx'

    def test_join(self):
        table = self.db.table('sys_area').alias('province')
        table.join('sys_area', 'city', 'province.id = city.pid')
        assert isinstance(table.join_list, list)
        assert table.join_list[0] == 'INNER JOIN sys_area AS city ON province.id = city.pid'

        res = table.where('province.id', '100001').select()
        assert isinstance(res, list)
        table = self.db.table('sys_area').alias('province').join(
            'left join sys_area AS city ON province.id = city.pid')
        assert isinstance(table.join_list, list)
        assert table.join_list[0] == 'left join sys_area AS city ON province.id = city.pid'

        table = self.db.table('sys_area').alias('province').join(
            'sys_area', 'city', 'province.id = city.pid', 'left', 'city.id>=100102')
        assert isinstance(table.join_list, list)
        assert table.join_list[0] == 'LEFT JOIN sys_area AS city ON province.id = city.pid AND city.id>=100102'

        res = table.where('province.id', '100001').select()
        assert isinstance(res, list)

        with pytest.raises(ValueError):
            table = self.db.table('sys_area').alias('province').join(
                'sys_area', 'city', 'province.id = city.pid', 'in')

        table = self.db.table('sys_area').alias('a')
        table.join('sys_area', 'b', 'a.id=b.pid')
        table.join('sys_area', 'c', 'b.id=c.pid')

        assert len(table.join_list) == 2

    def test_insert(self):
        res = self.db.table('sys_area').fetch_sql().insert(
            {'id': 1, 'name': 'china'})
        assert res == "INSERT INTO sys_area (id,name) VALUES (1,'china');"

        res = self.db.table('sys_area').fetch_sql().insert([
            {'id': 1, 'name': 'china'}])

        assert res == "INSERT INTO sys_area (id,name) VALUES (1,'china');"

        res = self.db.table('sys_area').fetch_sql().insert([
            {'id': 1, 'name': 'china'},
            {'id': 2, 'name': 'fujian'},
        ])

        assert res == "INSERT INTO sys_area (id,name) VALUES (1,'china'),(2,'fujian');"

        res = self.db.table('sys_area').fetch_sql().insert(
            {'id': 1, 'name': 'china'}, True)
        assert res == "REPLACE INTO sys_area (id,name) VALUES (1,'china');"

    def test_update(self):
        with pytest.raises(ValueError):
            res = self.db.table('sys_area').fetch_sql().update(
                {'name': 'china'})

        res = self.db.table('sys_area').where(
            'id', 1).fetch_sql().update({'name': 'china'})
        assert res == "UPDATE sys_area SET name='china' WHERE id = '1';"

        res = self.db.table('sys_area').fetch_sql().update(
            {'name': 'china'}, True)
        assert res == "UPDATE sys_area SET name='china' WHERE 1=1;"

    def test_delete(self):
        with pytest.raises(ValueError):
            res = self.db.table('sys_area').fetch_sql().delete()

        res = self.db.table('sys_area').where('id', 1).fetch_sql().delete()
        assert res == "DELETE FROM sys_area WHERE id = '1';"

        res = self.db.table('sys_area').fetch_sql().delete(True)
        assert res == "DELETE FROM sys_area WHERE 1=1;"

    def test_inc(self):
        with pytest.raises(ValueError):
            res = self.db.table('sys_area').fetch_sql().inc('code')

        res = self.db.table('sys_area').fetch_sql().where('id', 1).inc('code')
        assert res == "UPDATE sys_area SET `code` = `code`+1 WHERE id = '1'"

        res = self.db.table('sys_area').fetch_sql().where(
            'id', 1).inc('code', 2)
        assert res == "UPDATE sys_area SET `code` = `code`+2 WHERE id = '1'"

        res = self.db.table('sys_area').fetch_sql().where(
            'id', 1).inc('code', '1.2')
        assert res == "UPDATE sys_area SET `code` = `code`+1.2 WHERE id = '1'"

        with pytest.raises(ValueError):
            res = self.db.table('sys_area').fetch_sql().where(
                'id', 1).inc('code', 'a')

    def test_dec(self):
        with pytest.raises(ValueError):
            res = self.db.table('sys_area').fetch_sql().dec('code')

        res = self.db.table('sys_area').fetch_sql().where('id', 1).dec('code')
        assert res == "UPDATE sys_area SET `code` = `code`-1 WHERE id = '1'"

        res = self.db.table('sys_area').fetch_sql().where(
            'id', 1).dec('code', 2)
        assert res == "UPDATE sys_area SET `code` = `code`-2 WHERE id = '1'"

        res = self.db.table('sys_area').fetch_sql().where(
            'id', 1).dec('code', '1.2')
        assert res == "UPDATE sys_area SET `code` = `code`-1.2 WHERE id = '1'"

        with pytest.raises(ValueError):
            res = self.db.table('sys_area').fetch_sql().where(
                'id', 1).dec('code', 'a')

    def test_max(self):
        res = self.db.table('sys_area').max('id')
        assert res == 147181

        res = self.db.table('sys_area').where('id', '<', 0).max('id')
        assert res == 0
        with pytest.raises(Exception):
            res = self.db.table('sys_area').max('ids')

    def test_sum(self):
        sql = self.db.table('sys_area').fetch_sql().sum('id')
        assert sql == 'SELECT SUM(`id`) AS sum FROM `sys_area` WHERE 1=1 LIMIT 1'

        res = self.db.table('sys_area').where('pid', 100001).sum('id')
        assert isinstance(res, Decimal)
        assert res == 1602698

    def test_avg(self):
        sql = self.db.table('sys_area').fetch_sql().where(
            'pid', 100001).avg('code')
        assert sql == "SELECT AVG(`code`) AS avg FROM `sys_area` WHERE pid = '100001' LIMIT 1"

    def test_count(self):
        sql = self.db.table('sys_area').fetch_sql().where(
            'pid', 100001).count()
        assert sql == "SELECT COUNT(*) AS count FROM `sys_area` WHERE pid = '100001' LIMIT 1"
        sql = self.db.table('sys_area').fetch_sql().where(
            'pid', 100001).count('id')
        assert sql == "SELECT COUNT(id) AS count FROM `sys_area` WHERE pid = '100001' LIMIT 1"

    def test_copy_to(self):
        sql = self.db.table('sys_area').fetch_sql().copy_to()
        assert sql == 'SELECT * INTO sys_area_copy FROM sys_area WHERE 1=1'
        sql = self.db.table('sys_area').fetch_sql().copy_to(
            create_blank_table=True)
        assert sql == 'SELECT * INTO sys_area_copy FROM sys_area WHERE 1=0'

        sql = self.db.table('sys_area').fetch_sql().copy_to('new_table')
        assert sql == 'SELECT * INTO new_table FROM sys_area WHERE 1=1'

        sql = self.db.table('sys_area').fetch_sql().field(
            'id, name').copy_to('new_table')
        assert sql == 'SELECT id, name INTO new_table FROM sys_area WHERE 1=1'

        sql = self.db.table('sys_area').fetch_sql().field(
            ['id', 'name']).copy_to('new_table')
        assert sql == 'SELECT id, name INTO new_table FROM sys_area WHERE 1=1'

        sql = self.db.table('sys_area').fetch_sql().field(
            ['id', 'name']).where('id', 100001).copy_to('new_table')
        assert sql == "SELECT id, name INTO new_table FROM sys_area WHERE id = '100001'"

    def test_insert_to(self):
        sql = self.db.table('sys_area').fetch_sql(
        ).insert_to('new_table', 'id,name')
        assert sql == "INSERT INTO new_table ('id','name') SELECT * FROM sys_area  WHERE 1=1"

    def test_exists(self):
        res = self.db.table('sys_area').where('id', 100001).exists()
        assert res == True
        res = self.db.table('sys_area').where('id', 1).exists()
        assert res == False

    def test_batch_update(self):
        data = []
        for i in range(101, 200):
            data.append({'id': i, 'status': 0})
        res = self.db.table('user').batch_update(data, key='id')
        assert isinstance(res, int)
        assert res == 99
        self.db.table('user').batch_update(
            [{'id': i, 'status': 1} for i in range(101, 200)], key='id')
