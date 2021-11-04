#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'hbh112233abc@163.com'

import re
from hashlib import md5
import itertools
import cacheout
from typing import Any, Union, Tuple, List
from decimal import Decimal

from pymysql.cursors import Cursor
from pymysql.connections import Connection

from .util import get_logger
from .cache import CacheStorage


class Table():
    def __init__(
        self,
        connector: Connection,
        cursor: Cursor,
        table_name: str,
        debug: bool = True
    ):
        self.connector = connector
        self.db_cursor = cursor
        self.table_name = table_name
        self.__debug = debug
        self.__fetch_sql = False
        self.log = get_logger()
        self.cache_storage = cacheout.Cache()
        self.init()

    def init(self):
        """初始化查询条件
        """
        self.condition_str = '1=1'
        self.condition_val = tuple()
        self.limit_dict = {}
        self.order_by = ''
        self.group_by = ''
        self.select_fields = ('*',)
        self.join_list = []
        # self.__fetch_sql = False
        # 缓存相关设置
        self.use_cache = False
        self.cache_key = None
        self.cache_expire = 3600
        return self

    def debug(self, flag: bool = True):
        """设置调试模式

        Args:
            flag (bool, optional): 调试标识. Defaults to True.

        Returns:
            self: 支持链式调用
        """
        self.__debug = flag
        return self

    def set_cache_storage(self, storage: CacheStorage):
        """设置缓存驱动

        Args:
            storage (CacheStorage): 缓存驱动

        Returns:
            self: 支持链式调用
        """
        self.cache_storage = storage
        return self

    def cache(self, key: str = None, expire: int = 3600):
        """数据缓存

        Args:
            key (str, optional): 缓存键名. Defaults to None.
            expire (int, optional): 缓存期限(-1 表示永久期限). Defaults to 3600.
        """
        self.use_cache = True
        self.cache_key = key
        self.cache_expire = expire
        return self

    def cursor(self) -> Cursor:
        """查询操作,返回cursor对象

        Args:
            sql (str): sql语句
            params (list, optional): 绑定参数. Defaults to [].

        Returns:
            cursor: 游标
        """
        sql = 'SELECT {select_fields} FROM {table} {join} WHERE {where}{group}{order}{limit}'.format(
            select_fields=','.join(self.select_fields),
            table=self.table_name,
            join=' '.join(self.join_list),
            where=self.__condition_str_fix(),
            group=self.group_by,
            order=self.order_by,
            limit=self.limit_dict.get('sql', '')
        )
        params = self.condition_val + self.limit_dict.get('params', ())

        self.db_cursor.execute(sql, params)
        return self.db_cursor

    def __get_fields(self) -> tuple:
        """获取数据表字段名列表

        Returns:
            tuple: 字段名列表
        """
        sql = f'desc `{self.table_name}`;'
        data = self.query(sql)
        fields = []
        fields = tuple(d['Field'] for d in data)
        return fields

    def get_last_sql(self) -> str:
        '''获取最后执行的sql'''
        return self.db_cursor._last_executed

    def get_lastid(self) -> str:
        '''获取最后作用的id'''
        return self.db_cursor.lastrowid

    def get_rowcount(self) -> int:
        '''获取sql影响条数'''
        return self.db_cursor.rowcount

    def fetch_sql(self, flag: bool = True):
        """输出sql语句

        Args:
            flag (bool, optional): 是否输出sql. Defaults to True.
        """
        self.__fetch_sql = flag
        return self

    def build_sql(self, operation: str, params: list = []) -> str:
        """生成sql语句

        Args:
            operation (str): sql语句
            params (list, optional): 参数. Defaults to [].

        Returns:
            str: 组装后的sql语句
        """
        return self.db_cursor.mogrify(operation, params)

    def __condition_str_fix(self) -> str:
        """清理查询条件前缀

        Returns:
            str: 去除前缀的查询语句
        """
        self.condition_str = self.condition_str.replace(
            '1=1 AND ', '').replace('1=1 OR ', '')
        return self.condition_str

    def query(self, sql: str, params: list = []) -> list:
        """查询操作(读操作)

        Args:
            sql (str): sql语句
            params (list, optional): 绑定参数. Defaults to [].

        Returns:
            tuple: 查询结果
        """
        try:
            if self.__fetch_sql:
                return self.build_sql(sql, params)

            # 缓存操作
            if self.use_cache:
                if not self.cache_key:
                    build_sql = self.build_sql(sql, params)
                    hash = md5()
                    hash.update(build_sql.encode('utf-8'))
                    sql_md5 = hash.hexdigest()
                    self.cache_key = f"{self.table_name}:{sql_md5}"
                if self.cache_storage.get(self.cache_key):
                    return self.cache_storage.get(self.cache_key)
                else:
                    self.db_cursor.execute(sql, params)
                    result = self.db_cursor.fetchall()
                    self.cache_storage.set(
                        self.cache_key,
                        result,
                        self.cache_expire
                    )
                    self.__log_sql()
                    return result

            self.db_cursor.execute(sql, params)
            result = self.db_cursor.fetchall()
            self.__log_sql()
            return result
        except Exception as e:
            self.log.error(sql)
            self.log.error(params)
            raise e
        finally:
            self.init()

    def execute(self, sql: str, params: list = []) -> int:
        """执行操作(写操作)

        Args:
            sql (str): sql语句
            params (list, optional): 绑定参数. Defaults to [].

        Returns:
            int: 影响行数
        """
        try:
            if self.__fetch_sql:
                return self.build_sql(sql, params)

            self.db_cursor.execute(sql, params)
            self.connector.commit()
            result = self.db_cursor.rowcount
            self.__log_sql()
            return result
        except Exception as e:
            self.log.error(self.build_sql(sql, params))
            raise e
        finally:
            self.init()

    def __log_sql(self):
        """记录sql日志
        """
        if self.__debug:
            self.log.info(
                f"[sql]({self.connector.db}) {self.db_cursor._executed}"
            )

    @staticmethod
    def parse_where(field: str, symbol: str = '', value: Any = None) -> Tuple[str, tuple]:
        """解析where条件语句

        Args:
            field (str): 字段名
            symbol (str): 条件符号
            value (mix): 条件值

        Raises:
            Exception: symbol is error
            Exception: value could not be none

        Returns:
            str: 解析后的条件语句
        """
        check_value = True
        condition_str = ''
        condition_val = tuple()

        symbol = str(symbol).strip().lower()
        if symbol in ('eq', '='):
            symbol = '='
        elif symbol in ('neq', '!=', '<>'):
            symbol = '<>'
        elif symbol in ('gt', '>'):
            symbol = '>'
        elif symbol in ('egt', '>='):
            symbol = '>='
        elif symbol in ('lt', '<'):
            symbol = '<'
        elif symbol in ('elt', '<='):
            symbol = '<='
        elif symbol in ('in', 'not in'):
            symbol = symbol
            if isinstance(value, str):
                if not (value.startswith('(') and value.endswith(')')):
                    value = value.split(',')
        elif symbol in ('between', 'not between'):
            symbol = symbol
            r = re.compile(' and ', re.I)
            if isinstance(value, str):
                if not r.findall(value):
                    value = value.split(',')

            if isinstance(value, (list, tuple)):
                if len(value) != 2:
                    raise ValueError(
                        '`between` optional `value` must 2 arguments')
                value = f'{value[0]} AND {value[1]}'

            if not isinstance(value, str) or not r.findall(value):
                raise ValueError('`between` optional `value` error')

        elif symbol in ('like', 'not like'):
            symbol = symbol
            if not isinstance(value, str):
                raise ValueError(
                    '`like` optional `value` must be a string')
            if '%' not in value and '_' not in value:
                raise ValueError(
                    '`like` optional `value` should contain `%` or `_`')
        elif symbol == 'is':
            symbol = 'is'
        elif symbol in ('null', 'is null'):
            symbol = ' is null'
            check_value = False
        elif symbol in ('not null', 'is not null'):
            symbol = ' is not null'
            check_value = False
        elif symbol in ('exists', 'not exists'):
            field = f'{symbol}({field})'
            symbol = ''
            check_value = False
        elif symbol == 'exp':
            if not isinstance(value, str):
                raise ValueError('`exp` optional `value` should be a string')
            field = f'{field} {value}'
            symbol = ''
            check_value = False
        elif symbol == '' and value is None:
            # field 原生sql
            pass
        else:
            if value is None:
                value = symbol
                symbol = '='
            else:
                raise ValueError('symbol is error')
        if check_value:
            if value is None:
                raise ValueError('value could not be none')

            condition_str += ' AND {} {}'.format(field, symbol)
            if isinstance(value, (list, tuple)):
                condition_str += ' (%s)' % ('%s,' * len(value))[:-1]
                condition_val += tuple(value)
            else:
                condition_str += ' %s'
                condition_val += (value,)
        else:
            condition_str += ' AND ' + field + symbol

        return condition_str, condition_val

    def where(self, field: Union[str, list, tuple], symbol: str = '', value: Any = None):
        """条件设置

        Args:
            field (str|list|dict, require): 字段名|条件列表|sql语句.
            symbol (str, optional): 条件符号. Defaults to ''.
            value (mix, optional): 条件值. Defaults to None.

        Raises:
            ValueError: conditions error

        Returns:
            self: 对象本身
        """
        if isinstance(field, str):
            condition_str, condition_val = Table.parse_where(
                field, symbol, value)
            self.condition_str += condition_str
            self.condition_val += condition_val
        elif isinstance(field, list):
            for condition in field:
                if isinstance(condition, (list, tuple)) and len(condition) == 3:
                    self.where(*condition)
                else:
                    raise ValueError(
                        'conditions error => {}'.format(condition))
        elif isinstance(field, dict):
            for k, v in field.items():
                self.where(k, v)
        else:
            raise ValueError(
                f'where error:field={field}, symbol={symbol}, value={value}'
            )
        return self

    def where_or(self, field: Union[str, list], symbol: str = '', value: Any = None):
        """或条件设置

        Args:
            field (str|list, require): 字段名|条件列表|sql语句.
            symbol (str, optional): 条件符号. Defaults to ''.
            value (mix, optional): 条件值. Defaults to None.

        Raises:
            Exception: conditions error

        Returns:
            self: 对象本身
        """
        condition_str = ''
        if isinstance(field, str):
            condition_str, condition_val = Table.parse_where(
                field, symbol, value)
        elif isinstance(field, list):
            condition_val = tuple()
            for condition in field:
                if isinstance(condition, (list, tuple)) and len(condition) == 3:
                    c_s, c_v = Table.parse_where(*condition)
                    condition_str += c_s
                    condition_val += c_v
                else:
                    raise Exception('conditions error => {}'.format(condition))
        self.condition_str += ' OR (' + condition_str[5:] + ')'
        self.condition_val += condition_val
        return self

    def limit(self, start: int, step: int = None):
        """分页设置

        Args:
            start (int): 如果step=None,表示取start条数据,否则表示起始行
            step (int, optional): 分页条数

        Returns:
            [type]: [description]
        """
        if step is None:
            self.limit_dict = {
                'sql': ' LIMIT %s',
                'params': (start,)
            }
        else:
            self.limit_dict = {
                'sql': ' LIMIT %s,%s',
                'params': (start, step)
            }
        return self

    def page(self, index: int = 1, size: int = 20):
        """分页操作

        Args:
            index (int, optional): 页码. Defaults to 1
            size (int, optional): 分页数量. Defaults to 20.

        Returns:
            self: 对象本身
        """
        start = int(size * (int(index) - 1))
        end = int(size)
        self.limit(start, end)
        return self

    def order(self, field: str, sort: str = 'asc'):
        """排序操作

        Args:
            field (str): 排序字段名
            sort (str, optional): 排序类型asc:顺序;desc:倒序. Defaults to 'asc'.

        Raises:
            Exception: sort must be ASC or DESC

        Returns:
            self: 对象本身
        """
        sort = str(sort).strip().upper()
        if sort not in ('ASC', 'DESC'):
            raise Exception('sort must be ASC or DESC')
        if not self.order_by:
            self.order_by = ' ORDER BY `{}` {}'.format(field, sort)
        else:
            self.order_by += ',`{}` {}'.format(field, sort)
        return self

    def group(self, field: str):
        """分组设置

        Args:
            field (str): 字段名

        Returns:
            self: 对象本身
        """
        if not self.group_by:
            self.group_by = ' GROUP BY {}'.format(
                ','.join([f'`{x}`' for x in field.split(',')]))
        else:
            self.group_by += ',`{}`' + field

        return self

    def field(self, fields: Any, exclude: bool = False):
        """字段限制

        Args:
            fields (mix): True表示所有字段|以逗号隔开的字符串|列表|元祖
            exclude (bool, optional): 是否过滤. Defaults to False.

        Returns:
            self: 对象本身
        """
        if fields is True or fields == '*':
            fields = self.__get_fields()
        elif isinstance(fields, str):
            fields = [x.strip() for x in fields.split(',')]
        fields = tuple(fields)
        if exclude:
            if not self.select_fields or self.select_fields == ('*',):
                self.select_fields = self.__get_fields()
            fields = tuple(set(self.select_fields).difference(set(fields)))
        self.select_fields = fields
        return self

    def select(self, build_sql: bool = False) -> list:
        """查询数据

        Returns:
            tuple: 查询结果
        """
        sql = 'SELECT {select_fields} FROM {table} {join} WHERE {where}{group}{order}{limit}'.format(
            select_fields=','.join(self.select_fields),
            table=self.table_name,
            join=' '.join(self.join_list),
            where=self.__condition_str_fix(),
            group=self.group_by,
            order=self.order_by,
            limit=self.limit_dict.get('sql', '')
        )
        params = self.condition_val + self.limit_dict.get('params', ())

        if build_sql:
            real_sql = self.build_sql(sql, params)
            return f'({real_sql})'

        return self.query(sql, params)

    def find(self):
        """查询一条数据

        Returns:
            dict: 查询结果
        """
        self.limit(1)
        result = self.select()
        if self.__fetch_sql:
            return result
        return result[0] if len(result) > 0 else {}

    def value(self, field: str) -> Any:
        """获取某个字段值

        Args:
            field (str): 字段名

        Returns:
            str: 对应字段值
        """
        self.select_fields = [field]
        result = self.find()
        return result.get(field, '')

    def column(self, fields: str, key: str = '') -> Union[list, dict]:
        """按列取数据

        Args:
            fields (str|list|tuple): 获取字段名(逗号分隔或数组)
            key (str, optional): 作为键名字段. Defaults to ''.

        Returns:
            list|dict: 包含键名的返回字典
        """
        if isinstance(fields, str):
            fields = fields.split(',')
        if key and key not in fields:
            fields.append(key)
        self.select_fields = fields
        result = self.select()
        # 无指定键名
        if not key:
            if len(fields) == 1:
                return [x[fields[0]] for x in result]
            return result
        # 包含键名
        if len(fields) == 1:
            return {x[key]: x[fields[0]] for x in result}
        else:
            return {x[key]: x for x in result}

    def alias(self, short_name: str = ''):
        """数据表别名

        Args:
            short_name (str, optional): 别名. Defaults to ''.

        Returns:
            Table: 链式调用
        """
        self.table_name = f"{self.table_name} AS {short_name}"
        return self

    def join(self, table_name: str, as_name: str = '', on: str = '', join: str = 'inner', and_str: str = ''):
        """连表操作

        Args:
            table_name (str): 连表表名或连表完整sql
            as_name (str, optional): 连表别名. Defaults to ''.
            on (str, optional): 连表条件. Defaults to ''.
            join (str, optional): 连表类型('INNER', 'LEFT', 'RIGHT', 'FULL OUTER'). Defaults to 'inner'.
            and_str (str, optional): 附加过滤条件. Defaults to ''.

        Returns:
            Table: 支持链式调用
        """
        if not as_name:
            if table_name == self.table_name:
                raise ValueError('table name should set `as_name`')

            as_name = table_name

        join = join.upper()
        if join not in ('INNER', 'LEFT', 'RIGHT', 'FULL OUTER'):
            raise ValueError(
                "`join` type must in ('INNER','LEFT','RIGHT','FULL OUTER')")

        def make_join_str():
            if 'join' in table_name:
                return table_name

            join_str = f'{join} JOIN {table_name} AS {as_name} ON {on}'
            if and_str:
                join_str += f' AND {and_str}'

            return join_str

        join_str = make_join_str()
        self.join_list.append(join_str)
        return self

    def union(self, sql1: str, sql2: str, union_all: bool = False):
        """UNION操作

        Args:
            sql1 (str): sql语句
            sql2 (str): sql语句
            union_all (bool, optional): 是否union all. Defaults to False.
        """
        symbol = 'UNION ALL' if union_all else 'UNION'
        self.table_name = f'({sql1} {symbol} {sql2}) AS t'
        return self

    def insert(self, data: Union[dict, list], replace: bool = False) -> int:
        """插入数据

        Args:
            data (dict|list): 待新增的数据
            replace (bool): 是否使用replace

        Returns:
            int: 影响行数
        """

        if isinstance(data, dict):
            keys = ','.join(data.keys())
            inputs = '(' + ','.join(['%s'] * len(data)) + '),'
            params = tuple(data.values())
        elif isinstance(data, list):
            inputs = ''
            params = tuple()
            for d in data:
                keys = ','.join(d.keys())
                inputs += '(' + ','.join(['%s'] * len(d)) + '),'
                params += tuple(d.values())

        inputs = inputs[:-1]
        action = 'REPLACE' if replace else 'INSERT'
        sql = '{action} INTO {table} ({keys}) VALUES {inputs};'.format(
            action=action,
            table=self.table_name,
            keys=keys,
            inputs=inputs
        )
        result = self.execute(sql, params)
        return result

    def update(self, data: dict, all_record: bool = False) -> int:
        """更新数据

        Args:
            data (dict): 更新数据内容
            all_record (bool): 是否更新全部数据,默认False

        Returns:
            int: 影响行数
        """
        if not all_record and self.condition_str == '1=1':
            raise ValueError('please set `where` conditions!')
        inputs = ','.join(
            map(lambda k: k + '=%s', data.keys()))
        params = tuple(data.values()) + self.condition_val
        sql = 'UPDATE {table} SET {inputs} WHERE {where};'.format(
            table=self.table_name,
            inputs=inputs,
            where=self.__condition_str_fix()
        )
        result = self.execute(sql, params)
        return result

    def delete(self, all_record: bool = False) -> int:
        """删除数据

        Args:
            all_record (bool): 是否更新全部数据,默认False

        Raises:
            Exception: please set delete conditions!

        Returns:
            int: 影响行数
        """
        if not all_record and self.condition_str == '1=1':
            raise ValueError('please set `where` conditions!')
        sql = 'DELETE FROM {table} WHERE {where};'.format(
            table=self.table_name,
            where=self.__condition_str_fix()
        )
        result = self.execute(sql, self.condition_val)
        return result

    @staticmethod
    def to_number(s: Union[str, int, float], key: str = ''):
        """转换为数值

        Args:
            s (Union[str, int, float]): 字符串或数字
            key (str, optional): 键名. Defaults to ''.

        Raises:
            ValueError: 错误内容

        Returns:
            [int,float]: 转换后的数值
        """
        if isinstance(s, (int, float)):
            return s

        if isinstance(s, str):
            s = s.strip()
            if re.match(r'^\d+$', s):
                s = int(s)
                return s
            if re.match(r'^\d+\.\d+$', s):
                s = float(s)
                return s

        if not isinstance(s, (int, float)) and key:
            raise ValueError(f'`{key}` must number')

        return s

    def inc(self, field: str, step: Union[str, int, float] = 1) -> int:
        """递增

        Args:
            field (str): 字段名
            step (int, optional): 步长. Defaults to 1.

        Returns:
            bool: 递增结果
        """
        step = Table.to_number(step, 'step')

        if self.condition_str == '1=1':
            raise ValueError('please set `where` conditions!')
        symbol = '+' if step > 0 else ''

        sql = 'UPDATE {table} SET `{field}` = `{field}`{symbol}{step} WHERE {where}'.format(
            table=self.table_name,
            field=field,
            symbol=symbol,
            step=step,
            where=self.__condition_str_fix(),
        )
        result = self.execute(sql, self.condition_val)
        return result

    def dec(self, field: str, step: int = 1) -> int:
        """递减

        Args:
            field (str): 字段名
            step (int, optional): 步长. Defaults to 1.

        Returns:
            bool: 递减结果
        """
        step = Table.to_number(step, 'step')
        return self.inc(field, step=(0 - step))

    def max(self, field: str) -> Union[int, float]:
        """最大值

        Args:
            field (str): 字段名

        Returns:
            number: 最大值
        """
        sql = 'SELECT MAX({field}) AS max FROM `{table}` WHERE {where} LIMIT 1'.format(
            table=self.table_name,
            field=field,
            where=self.__condition_str_fix(),
        )
        result = self.query(sql, self.condition_val)

        if self.__fetch_sql:
            return result

        if not result:
            return 0

        max_value = result[0]['max']
        if max_value is None:
            return 0

        if isinstance(max_value, (int, float, Decimal)):
            return max_value

        return Table.to_number(max_value)

    def sum(self, field: str) -> Union[int, float, Decimal]:
        """合计值

        Args:
            field (str): 字段名

        Returns:
            number: 合计
        """
        sql = 'SELECT SUM(`{field}`) AS sum FROM `{table}` WHERE {where} LIMIT 1'.format(
            table=self.table_name,
            field=field,
            where=self.__condition_str_fix(),
        )
        result = self.query(sql, self.condition_val)

        if self.__fetch_sql:
            return result

        if not result:
            return 0

        sum_value = result[0]['sum']
        if sum_value is None:
            return 0

        if isinstance(sum_value, (int, float, Decimal)):
            return sum_value

        return Table.to_number(sum_value)

    def avg(self, field: str) -> Union[int, float, Decimal]:
        """平均值

        Args:
            field (str): 字段名

        Returns:
            number: 平均值
        """
        sql = 'SELECT AVG(`{field}`) AS avg FROM `{table}` WHERE {where} LIMIT 1'.format(
            table=self.table_name,
            field=field,
            where=self.__condition_str_fix(),
        )
        result = self.query(sql, self.condition_val)

        if self.__fetch_sql:
            return result

        if not result:
            return 0

        avg_value = result[0]['avg']
        if avg_value is None:
            return 0

        if isinstance(avg_value, (int, float, Decimal)):
            return avg_value

        return Table.to_number(avg_value)

    def count(self, field: str = '*') -> Union[str, int]:
        """获取数据行数

        Args:
            field (str, optional): 字段名. Defaults to '*'.

        Returns:
            int: 行数量
        """
        sql = 'SELECT COUNT({field}) AS count FROM `{table}` WHERE {where} LIMIT 1'.format(
            field=field,
            table=self.table_name,
            where=self.__condition_str_fix(),
        )
        result = self.query(sql, self.condition_val)

        if self.__fetch_sql:
            return result

        if not result:
            return 0
        return (result[0]['count'] or 0)

    def copy_to(self, new_table: str = None, create_blank_table: bool = False) -> int:
        """复制表 SELECT INTO

        Args:
            new_table (str, optional): 新表名称. Defaults to None.
            create_blank_table (bool, optional): 是否只创建表结构. Defaults to False.

        Returns:
            int: 执行结果
        """
        if new_table is None:
            new_table = f'{self.table_name}_copy'
        if isinstance(self.select_fields, (list, tuple)):
            fields = ', '.join(self.select_fields)

        sql = f'SELECT {fields} INTO {new_table} FROM {self.table_name}'
        if create_blank_table:
            sql += ' WHERE 1=0'
        else:
            sql += f' WHERE {self.__condition_str_fix()}'
        return self.execute(sql, self.condition_val)

    def insert_to(self, new_table: str, fields: Union[str, list, tuple] = None) -> int:
        """复制表 INSERT INTO

        Args:
            new_table (str): 新表名称
            fields (str, optional): 字段名. Defaults to None.

        Returns:
            int: [description]
        """
        sql = f'INSERT INTO {new_table}'
        if fields is not None:
            if isinstance(fields, str):
                if fields.startswith('('):
                    sql += f' {fields} '
                else:
                    fields = fields.split(',')
            if isinstance(fields, (list, tuple)):
                sql += ' ({})'.format(','.join(itertools.repeat('%s', len(fields))))

        sql += ' SELECT {select_fields} FROM {table} {join} WHERE {where}{group}{order}{limit}'.format(
            select_fields=','.join(self.select_fields),
            table=self.table_name,
            join=' '.join(self.join_list),
            where=self.__condition_str_fix(),
            group=self.group_by,
            order=self.order_by,
            limit=self.limit_dict.get('sql', '')
        )

        params = self.condition_val + self.limit_dict.get('params', tuple())
        if isinstance(fields, (list, tuple)):
            params = tuple(fields) + params

        return self.execute(sql, params)

    def exists(self) -> bool:
        """判断是否存在数据

        Returns:
            bool: 判断当前查询条件下是否存在数据
        """
        sql = 'SELECT 1 FROM {table} {join} WHERE {where} LIMIT 1'.format(
            table=self.table_name,
            join=' '.join(self.join_list),
            where=self.__condition_str_fix()
        )
        result = self.query(sql, self.condition_val)

        if not result:
            return False
        return True

    def batch_update(self, data: List[dict], key: str) -> int:
        """批量更新

        Args:
            data (List[dict]): 数据列表List[dict]
            key (str): data中存在的键名,一般是主键

        Raises:
            ValueError: key不存在时报错

        Return:
            int: 更新行数
        """
        sql = []
        for row in data:
            if key not in row:
                raise ValueError(f'key:{key} not in data item')
            self.init()
            sql.append(
                self.where(key, row[key]).fetch_sql().update(row)
            )
        result = 0
        for x in range(0, len(sql), 100):
            for s in sql[x:x + 100]:
                self.db_cursor.execute(s)
                result += self.get_rowcount()
            self.connector.commit()
        return result
