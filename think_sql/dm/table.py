#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = "hbh112233abc@163.com"

from copy import deepcopy
from hashlib import md5
import itertools
from typing import Any, Union, Tuple, List
from decimal import Decimal

from dmPython import Cursor

from think_sql.tool.base import Database, TableBase
from think_sql.tool.interface import TableInterface

from think_sql.tool.util import to_number
from think_sql.dm.util import parse_key, parse_value,parse_where


class Table(TableBase,TableInterface):
    def __init__(
        self, db:Database, table_name: str
    ):
        super().__init__(
            db,
            table_name
        )

        self.schema = self.db.config.database.upper()

        self.columns = {}
        self.pk = {}

        self.get_fields()

        self.init()

    def init(self):
        """初始化查询条件"""
        self.condition_str = "1=1"
        self.condition_val = tuple()
        self.limit_dict = {}
        self.order_by = ""
        self.group_by = ""
        self.distinct_by = ""
        self.select_fields = ("*",)
        self.join_list = []
        self._fetch_sql = False
        # 缓存相关设置
        self.use_cache = False
        self.cache_key = None
        self.cache_expire = 3600
        return self


    def cursor(self) -> Cursor:
        """查询操作,返回cursor对象

        Args:
            sql (str): sql语句
            params (list, optional): 绑定参数. Defaults to [].

        Returns:
            cursor: 游标
        """
        select_fields=self.__select_fields_str()
        join=" ".join(self.join_list)
        where=self.__condition_str_fix()
        group=self.group_by
        order=self.order_by
        limit=self.limit_dict.get("sql", "")
        sql = f"SELECT {select_fields} FROM {self.schema}.{parse_key(self.table_name)} {join} WHERE {where}{group}{order}{limit}"
        params = self.condition_val + self.limit_dict.get("params", ())

        self.db_cursor.execute(sql, params)
        return self.db_cursor

    def get_fields(self) -> tuple:
        """获取数据表字段名列表

        Returns:
            tuple: 字段名列表
        """
        if self.columns:
            return tuple(self.columns.keys())
        sql = f"""
        SELECT
                a.column_name,
                data_type,
                DECODE (nullable, 'Y', 0, 1) notnull,
                data_default,
                DECODE (a.column_name,b.column_name,1,0) pk,
                DECODE (a.column_name,d.column_name,1,0) autoinc
            FROM
            all_tab_columns a,
            (
                SELECT column_name
                FROM
                all_constraints c,
                all_cons_columns col
                WHERE
                c.constraint_name = col.constraint_name
                AND c.constraint_type = 'P'
                AND c.table_name = '{self.table_name}'
                AND c.owner = '{self.schema}'
            ) b,
            (
                SELECT COL.NAME as column_name
                FROM SYSOBJECTS TAB
                    ,SYSCOLUMNS COL
                    ,DBA_OBJECTS OBJ
                where TAB.ID = COL.ID
                AND TAB.SCHID = OBJ.OBJECT_ID
                AND TAB.TYPE$ = 'SCHOBJ'
                AND TAB.SUBTYPE$ = 'UTAB'
                AND COL.INFO2 & 0x01 = 1
                AND OBJ.OWNER = '{self.schema}'
                AND TAB.NAME = '{self.table_name}'
            ) d
            WHERE table_name = '{self.table_name}'
            AND owner = '{self.schema}'
            AND a.column_name = d.column_name (+)
            AND a.column_name = b.column_name (+)
        """

        self.db_cursor.execute(sql)
        data = self.db_cursor.fetchall()
        fields = []
        fields = tuple([d['column_name'] for d in data])
        self.columns = {}
        for d in data:
            field = {
                "name": d['column_name'],
                "type": d['data_type'],
                "notnull": d['notnull'],
                "default": d['data_default'],
                "primary": d['pk'],
                "autoinc": d['autoinc'],
            }
            self.columns[field['name']] = field
            if field['primary'] == 1:
                self.pk = field
        return fields

    def get_last_sql(self) -> str:
        """获取最后执行的sql"""
        return self.db_cursor.statement

    def get_lastid(self) -> str:
        """获取最后作用的id"""
        return self.db_cursor.lastrowid

    def get_rowcount(self) -> int:
        """获取sql影响条数"""
        return self.db_cursor.rowcount

    def fetch_sql(self,flag:bool=True):
        """设置获取sql语句标识

        Args:
            flag (bool, optional): 是否生成sql. Defaults to True.
        """
        self._fetch_sql = flag
        return self


    def build_sql(self, operation: str, params: list = []) -> str:
        """生成sql语句

        Args:
            operation (str): sql语句
            params (list, optional): 参数. Defaults to [].

        Returns:
            str: 组装后的sql语句
        """
        return operation % tuple(params)

    def __condition_str_fix(self) -> str:
        """清理查询条件前缀

        Returns:
            str: 去除前缀的查询语句
        """
        self.condition_str = self.condition_str.replace("1=1 AND ", "").replace(
            "1=1 OR ", ""
        )
        return self.condition_str

    def query(self, sql: str, params: list = []) -> List[dict]:
        """查询操作(读操作)

        Args:
            sql (str): sql语句
            params (list, optional): 绑定参数. Defaults to [].

        Returns:
            tuple: 查询结果
        """
        try:
            finally_sql = ""
            finally_sql = self.build_sql(sql, params)
            if self._fetch_sql:
                return finally_sql
            # 缓存操作
            if self.use_cache:
                if not self.cache_key:
                    hash = md5()
                    hash.update(finally_sql.encode("utf-8"))
                    sql_md5 = hash.hexdigest()
                    self.cache_key = f"{self.table_name}:{sql_md5}"
                result = self.get_cache()
                if result:
                    return result

            self.db_cursor.execute(finally_sql)
            result = self.db_cursor.fetchall()
            self.set_cache(result)
            self.__log_sql()
            return result
        except Exception as e:
            if finally_sql:
                self.log.error(finally_sql)
            else:
                self.log.error(sql, params)
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
            finally_sql = ''
            finally_sql = self.build_sql(sql, params)
            if self._fetch_sql:
                return finally_sql
            self.db_cursor.execute(finally_sql)
            self.connector.commit()
            result = self.db_cursor.rowcount
            self.__log_sql()
            return result
        except Exception as e:
            if finally_sql:
                self.log.error(finally_sql)
            else:
                self.log.error(sql, params)
            raise e
        finally:
            self.init()

    def __log_sql(self):
        """记录sql日志"""
        if self._debug:
            self.log.info(f"[sql]({self.schema}) {self.get_last_sql()}")

    def where(
        self, field: Union[str, list, tuple], symbol: str = "", value: Any = None
    ):
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
            condition_str, condition_val = parse_where(field, symbol, value)
            self.condition_str += condition_str
            self.condition_val += condition_val
        elif isinstance(field, list):
            for condition in field:
                if isinstance(condition, (list, tuple)) and len(condition) == 3:
                    self.where(*condition)
                else:
                    raise ValueError(f"conditions error => {condition}")
        elif isinstance(field, dict):
            for k, v in field.items():
                self.where(k, v)
        else:
            raise ValueError(
                f"where error:field={field}, symbol={symbol}, value={value}"
            )
        return self

    def where_or(self, field: Union[str, list], symbol: str = "", value: Any = None):
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
        condition_str = ""
        if isinstance(field, str):
            condition_str, condition_val = parse_where(field, symbol, value)
        elif isinstance(field, list):
            condition_val = tuple()
            for condition in field:
                if isinstance(condition, (list, tuple)) and len(condition) == 3:
                    c_s, c_v = parse_where(*condition)
                    condition_str += c_s
                    condition_val += c_v
                else:
                    raise Exception(f"conditions error => {condition}")
        self.condition_str += " OR (" + condition_str[5:] + ")"
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
            self.limit_dict = {"sql": " LIMIT %s", "params": (start,)}
        else:
            self.limit_dict = {"sql": " LIMIT %s,%s", "params": (start, step)}
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

    def order(self, field: str, sort: str = "asc"):
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
        if sort not in ("ASC", "DESC"):
            raise Exception("sort must be ASC or DESC")
        field = parse_key(field)
        if not self.order_by:
            self.order_by = f" ORDER BY {field} {sort}"
        else:
            self.order_by += f",{field} {sort}"
        return self

    def distinct(self, field: str):
        """去重操作

        Args:
            field (str): 去重字段名

        Returns:
            self: 对象本身
        """
        fields = ",".join([parse_key(x) for x in field.split(",")])
        self.distinct_by = f"DISTINCT {fields}"
        return self

    def group(self, field: str):
        """分组设置

        Args:
            field (str): 字段名

        Returns:
            self: 对象本身
        """
        fields = ",".join([parse_key(x) for x in field.split(",")])
        if not self.group_by:
            self.group_by = f" GROUP BY {fields}"
        else:
            self.group_by += f",{fields}"

        return self

    def field(self, fields: Any, exclude: bool = False):
        """字段限制

        Args:
            fields (mix): True表示所有字段|以逗号隔开的字符串|列表|元祖
            exclude (bool, optional): 是否过滤. Defaults to False.

        Returns:
            self: 对象本身
        """
        if fields is True or fields == "*":
            fields = self.get_fields()
        elif isinstance(fields, str):
            fields = [x.strip() for x in fields.split(",")]
        fields = tuple(fields)
        if exclude:
            if not self.select_fields or self.select_fields == ("*",):
                self.select_fields = self.get_fields()
            fields = tuple(set(self.select_fields).difference(set(fields)))
        self.select_fields = fields
        return self

    def __select_fields_str(self)->str:
        """获取select字段字符串

        Returns:
            str: ","分隔的select字段字符串
        """
        return ",".join([parse_key(x) for x in self.select_fields])

    def select(self, build_sql: bool = False) -> List[dict]:
        """查询数据

        Returns:
            tuple: 查询结果
        """
        fields = self.__select_fields_str()
        if self.distinct_by:
            fields = self.distinct_by
        join=" ".join(self.join_list)
        where=self.__condition_str_fix()
        group=self.group_by
        order=self.order_by
        limit=self.limit_dict.get("sql", "")
        sql = f"SELECT {fields} FROM {self.schema}.{parse_key(self.table_name)} {join} WHERE {where}{group}{order}{limit}"
        params = self.condition_val + self.limit_dict.get("params", ())

        if build_sql:
            real_sql = self.build_sql(sql, params)
            return f"({real_sql})"

        return self.query(sql, params)

    def find(self):
        """查询一条数据

        Returns:
            dict: 查询结果
        """
        self.limit(1)
        result = self.select()
        if self._fetch_sql:
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
        return result.get(field, "")

    def column(self, fields: str, key: str = "") -> Union[list,dict]:
        """按列取数据

        Args:
            fields (str|list|tuple): 获取字段名(逗号分隔或数组)
            key (str, optional): 作为键名字段. Defaults to ''.

        Returns:
            Union[list,dict]: 包含键名的返回字典
        """
        if isinstance(fields, str):
            fields = fields.split(",")
        self.select_fields = deepcopy(fields)
        if key and key not in fields:
            self.select_fields.append(key)
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

    def alias(self, short_name: str = ""):
        """数据表别名

        Args:
            short_name (str, optional): 别名. Defaults to ''.

        Returns:
            Table: 链式调用
        """
        self.table_name = f"{self.schema}.{self.table_name} AS {short_name}"
        return self

    def join(
        self,
        table_name: str,
        as_name: str = "",
        on: str = "",
        join: str = "inner",
        and_str: str = "",
    ):
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
                raise ValueError("table name should set as_name")

            as_name = table_name

        join = join.upper()
        if join not in ("INNER", "LEFT", "RIGHT", "FULL OUTER"):
            raise ValueError(
                "join type must in ('INNER','LEFT','RIGHT','FULL OUTER')"
            )

        def make_join_str(table_name, as_name, on, join, and_str):
            if "join" in table_name:
                return table_name
            if '.' not in table_name:
                table_name = f"{self.schema}.{table_name}"
            table_name = parse_key(table_name)
            as_name = parse_key(as_name)
            on1,on2 = on.split('=')
            on = f"{parse_key(on1)} = {parse_key(on2)}"
            join_str = f"{join} JOIN {table_name} AS {as_name} ON {on}"
            if and_str:
                join_str += f" AND {and_str}"

            return join_str

        join_str = make_join_str(table_name, as_name, on, join, and_str)
        self.join_list.append(join_str)
        return self

    def union(self, sql1: str, sql2: str, union_all: bool = False):
        """UNION操作

        Args:
            sql1 (str): sql语句
            sql2 (str): sql语句
            union_all (bool, optional): 是否union all. Defaults to False.
        """
        symbol = "UNION ALL" if union_all else "UNION"
        self.table_name = f"({sql1} {symbol} {sql2}) AS t"
        return self

    def insert(self, data: Union[dict, List[dict]], replace: bool = False) -> int:
        """插入数据

        Args:
            data (dict|List[dict]): 待新增的数据
            replace (bool): 是否使用replace

        Returns:
            int: 影响行数
        """

        if isinstance(data, dict):
            keys = ",".join([parse_key(key) for key in data.keys()])
            inputs = "(" + ",".join(["'%s'"] * len(data)) + "),"
            params = tuple([parse_value(value) for value in data.values()])
        elif isinstance(data, list):
            inputs = ""
            params = tuple()
            for d in data:
                keys = ",".join([parse_key(key) for key in d.keys()])
                inputs += "(" + ",".join(["'%s'"] * len(d)) + "),"
                params += tuple([parse_value(value) for value in d.values()])
        else:
            raise TypeError("data must be dict or List[dict]")

        if self.pk and self.pk['name'] in data:
            if self.pk['autoinc']:
                self.execute(f"SET IDENTITY_INSERT {self.schema}.{parse_key(self.table_name)} ON;")
            if replace:
                exist = self.where(self.pk['name'],data[self.pk['name']]).exists()
                if exist:
                    return self.where(self.pk['name'],data[self.pk['name']]).update(data)

        inputs = inputs[:-1]

        sql = f"INSERT INTO {self.schema}.{parse_key(self.table_name)} ({keys}) VALUES {inputs};"
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
        if not all_record and self.condition_str == "1=1":
            raise ValueError("please set where conditions!")
        inputs = ",".join(map(lambda k: f"{parse_key(k)}='%s'", data.keys()))
        params = tuple([parse_value(v) for v in data.values()]) + self.condition_val
        where=self.__condition_str_fix()
        sql = f"UPDATE {self.schema}.{parse_key(self.table_name)} SET {inputs} WHERE {where};"
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
        if not all_record and self.condition_str == "1=1":
            raise ValueError("please set where conditions!")
        where=self.__condition_str_fix()
        sql = f"DELETE FROM {self.schema}.{parse_key(self.table_name)} WHERE {where};"
        result = self.execute(sql, self.condition_val)
        return result


    def inc(self, field: str, step: Union[str, int, float] = 1) -> int:
        """递增

        Args:
            field (str): 字段名
            step (int, optional): 步长. Defaults to 1.

        Returns:
            bool: 递增结果
        """
        step = to_number(step, "step")

        if self.condition_str == "1=1":
            raise ValueError("please set where conditions!")
        symbol = "+" if step > 0 else ""
        where=self.__condition_str_fix()
        field = parse_key(field)
        sql = f"UPDATE {self.schema}.{parse_key(self.table_name)} SET {field} = {field}{symbol}{step} WHERE {where}"
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
        step = to_number(step, "step")
        return self.inc(field, step=(0 - step))

    def max(self, field: str) -> Union[int, float]:
        """最大值

        Args:
            field (str): 字段名

        Returns:
            number: 最大值
        """
        where=self.__condition_str_fix()
        field = parse_key(field)
        sql = f"SELECT MAX({field}) AS _max FROM {self.schema}.{parse_key(self.table_name)} WHERE {where} LIMIT 1"
        result = self.query(sql, self.condition_val)

        if self._fetch_sql:
            return result

        if not result:
            return 0

        max_value = result[0]["_max"]
        if max_value is None:
            return 0

        if isinstance(max_value, (int, float, Decimal)):
            return max_value

        return to_number(max_value)

    def sum(self, field: str) -> Union[int, float, Decimal]:
        """合计值

        Args:
            field (str): 字段名

        Returns:
            number: 合计
        """
        where=self.__condition_str_fix()
        field = parse_key(field)
        sql = f"SELECT SUM({field}) AS _sum FROM {self.schema}.{parse_key(self.table_name)} WHERE {where} LIMIT 1"
        result = self.query(sql, self.condition_val)

        if self._fetch_sql:
            return result

        if not result:
            return 0

        sum_value = result[0]["_sum"]
        if sum_value is None:
            return 0

        if isinstance(sum_value, (int, float, Decimal)):
            return sum_value

        return to_number(sum_value)

    def avg(self, field: str) -> Union[int, float, Decimal]:
        """平均值

        Args:
            field (str): 字段名

        Returns:
            number: 平均值
        """
        where=self.__condition_str_fix()
        field = parse_key(field)
        sql = f"SELECT AVG({field}) AS _avg FROM {self.schema}.{parse_key(self.table_name)} WHERE {where} LIMIT 1"
        result = self.query(sql, self.condition_val)

        if self._fetch_sql:
            return result

        if not result:
            return 0

        avg_value = result[0]["_avg"]
        if avg_value is None:
            return 0

        if isinstance(avg_value, (int, float, Decimal)):
            return avg_value

        return to_number(avg_value)

    def count(self, field: str = "") -> Union[str, int]:
        """获取数据行数

        Args:
            field (str, optional): 字段名. Defaults to '*'.

        Returns:
            int: 行数量
        """
        where=self.__condition_str_fix()
        if field == "":
            field = 1
        field = parse_key(field)
        sql = f"SELECT COUNT({field}) AS _count FROM {self.schema}.{parse_key(self.table_name)} WHERE {where} LIMIT 1"
        result = self.query(sql, self.condition_val)

        if self._fetch_sql:
            return result

        if not result:
            return 0
        return result[0]["_count"] or 0


    def insert_to(self, new_table: str, fields: Union[str, list, tuple] = None) -> int:
        """复制表 INSERT INTO

        Args:
            new_table (str): 新表名称
            fields (str, optional): 字段名. Defaults to None.

        Returns:
            int: affect rows count
        """
        new_table = parse_key(new_table)
        sql = f"INSERT INTO {new_table}"
        if fields is not None:
            if isinstance(fields, str):
                if fields.startswith("("):
                    sql += f" {fields} "
                else:
                    fields = [parse_key(x) for x in fields.split(",")]
            if isinstance(fields, (list, tuple)):
                ss = ",".join(itertools.repeat("%s", len(fields)))
                sql += f" ({ss})"

            if len(self.select_fields) != len(fields):
                raise ValueError("fields count not match select_fields count")

        select_fields=self.__select_fields_str()
        join=" ".join(self.join_list)
        where=self.__condition_str_fix()
        group=self.group_by
        order=self.order_by
        limit=self.limit_dict.get("sql", "")
        sql += f" SELECT {select_fields} FROM {self.schema}.{parse_key(self.table_name)} {join} WHERE {where}{group}{order}{limit}"

        params = self.condition_val + self.limit_dict.get("params", tuple())
        if isinstance(fields, (list, tuple)):
            params = tuple(fields) + params

        return self.execute(sql, params)

    def exists(self) -> bool:
        """判断是否存在数据

        Returns:
            bool: 判断当前查询条件下是否存在数据
        """
        join=" ".join(self.join_list)
        where=self.__condition_str_fix()
        sql = f"SELECT 1 FROM {self.schema}.{parse_key(self.table_name)} {join} WHERE {where} LIMIT 1"
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
                raise ValueError(f"key:{key} not in data item")
            self.init()
            sql.append(self.where(key, row[key]).fetch_sql().update(row))
        result = 0
        for x in range(0, len(sql), 100):
            for s in sql[x : x + 100]:
                self.db_cursor.execute(s)
                result += self.get_rowcount()
            self.connector.commit()
        return result
