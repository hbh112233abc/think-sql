#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = "hbh112233abc@163.com"


import re
import contextlib
from typing import List, Union

import pymysql
from loguru import logger

from think_sql.tool.util import DBConfig
from think_sql.tool.base import Database
from think_sql.tool.interface import DatabaseInterface

from think_sql.mysql.table import Table


class DB(DatabaseInterface,Database):
    def __init__(
        self,
        config: Union[str, dict, DBConfig],
        params={},
    ):
        """实例化数据库连接

        Args:
            config: str|dict|DBConfig 数据库连接配置
            params: dict 数据库连接参数
        """
        super().__init__(config,params)

    def __repr__(self):
        return f"<class 'think_sql.mysql.DB' uri={self.config.host}:{self.config.port} database={self.database}>"

    @contextlib.contextmanager
    def start_trans(self):
        """开始事务"""
        self.auto_commit = False
        try:
            self.connector.begin()
            yield
            self.connector.commit()
        except Exception as e:
            logger.error(e)
            self.connector.rollback()
        finally:
            self.auto_commit = True

    def close(self):
        """关闭数据库连接"""
        self.cursor.close()
        self.connector.close()

    def connect(self):
        """连接数据库"""
        self.connector = pymysql.connect(
            host=self.config.host,
            port=int(self.config.port),
            user=self.config.user,
            password=self.config.password,
            database=self.config.database,
            **self.params,
        )
        # SSCursor (流式游标) 解决 Python 使用 pymysql 查询大量数据导致内存使用过高的问题
        self.cursor = self.connector.cursor(pymysql.cursors.SSDictCursor)

    def execute(self, sql:str, params:tuple=()) -> int:
        try:
            result = self.exec(sql,params)
            if self.auto_commit:
                self.connector.commit()
            return result
        except Exception as e:
            self.log.warning(sql)
            logger.exception(e)
            return 0

    def exec(self, sql:str, params:tuple=())->int:
        if not params:
            sql = sql.replace("%", "%%")
        else:
            sql = re.sub(r"(?<!%)%(?![%s])(?![%\(])", "%%", sql)
        return self.cursor.execute(sql.strip(), params)

    def query(self, sql:str, params:tuple=()) -> List[dict]:
        result = []
        try:
            self.exec(sql,params)
            if self.cursor.rowcount > 0:
                result = self.cursor.fetchall()
        except Exception as e:
            self.log.warning(sql)
            self.log.exception(e)
        return result

    def error(self,err):
        if self.cursor._executed:
            self.log.info(f"[sql]({self.config.database}) {self.cursor._executed}")
            self.connector.rollback()
        self.log.exception(err)

    def success(self):
        self.connector.commit()

    def table(self, table_name=""):
        """生成对应数据表

        Args:
            table_name (str): 表名

        Returns:
            Table: 数据表对象,可以执行链式操作
        """
        if not self.check_connected():
            self.connect()
        return Table(self, table_name)

    def check_connected(self):
        """检查mysql是否可用连接

        通过ping检查连接是否可用

        Keyword Arguments:
            max_count {number} -- 最大尝试次数 (default: {10})
            sleep_time {number} -- 连接重试间隔时间秒 (default: {3})

        Returns:
            bool -- 连接是否可用
        """
        try:
            self.connector.ping(True)
            return True
        except Exception as e:
            self.log.exception(e)
            return False
