#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = "hbh112233abc@163.com"


import re
import contextlib
from typing import List, Union

import pymysql
from loguru import logger

from .util import DBConfig
from .table import Table


class DB:
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
        if isinstance(config, str):
            config = DBConfig.parse_dsn(config)
        elif isinstance(config, dict):
            config = DBConfig.model_validate(config)
        if not isinstance(config, DBConfig):
            raise ValueError(
                """
                Invalid database config
                Right config ex1:
                  DB({"host": "127.0.0.1","port": 3306,"username": "root","password": "password","database": "test"})
                Right config ex2:
                  DB("root:'password'@127.0.0.1:3306/test")
                Right config ex3:
                  from think_sql.util import DBConfig
                  cfg = DBConfig(host="127.0.0.1", port=3306, username="root", password="password",database="test")
                  DB(cfg)
                """
            )
        self.config = config
        self.params = params

        self.log = logger
        self.connector = None
        self.cursor = None
        self.auto_commit = True

        self.connect()

    def __repr__(self):
        return f"<class 'think_sql.database.DB' database={self.database}>"

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
            user=self.config.username,
            password=self.config.password,
            database=self.config.database,
            **self.params,
        )
        # SSCursor (流式游标) 解决 Python 使用 pymysql 查询大量数据导致内存使用过高的问题
        self.cursor = self.connector.cursor(pymysql.cursors.SSDictCursor)

    def execute(self, sql, params=()) -> int:
        try:
            if not params:
                sql = sql.replace("%", "%%")
            else:
                sql = re.sub(r"(?<!%)%(?![%s])(?![%\(])", "%%", sql)
            result = self.cursor.execute(sql.strip(), params)
            if self.auto_commit:
                self.connector.commit()
            return result
        except Exception as e:
            self.log.warning(sql)
            logger.exception(e)
            return 0

    def query(self, sql, params=()) -> List[dict]:
        result = []
        try:
            if not params:
                sql = sql.replace("%", "%%")
            else:
                sql = re.sub(r"(?<!%)%(?![%s])(?![%\(])", "%%", sql)
            self.cursor.execute(sql.strip(), params)
            if self.cursor.rowcount > 0:
                result = self.cursor.fetchall()
        except Exception as e:
            self.log.warning(sql)
            self.log.exception(e)
        return result

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, trace):
        """退出操作
        如果return True可以阻止异常传播

        Args:
            exc_type (object): 异常类型,默认为None
            exc_value (object): 异常值,默认为None
            trace (traceback object): 异常位置,默认为None
        """
        if exc_value is not None:
            if self.cursor._executed:
                self.log.info(f"[sql]({self.config.database}) {self.cursor._executed}")
            self.log.exception(exc_value)
            self.connector.rollback()
        else:
            self.connector.commit()
        if self.cursor:
            self.cursor.close()
        if self.connector:
            self.connector.close()

    def table(self, table_name=""):
        """生成对应数据表

        Args:
            table_name (str): 表名

        Returns:
            Table: 数据表对象,可以执行链式操作
        """
        if not self.check_connected():
            self.connect()
        return Table(self.connector, self.cursor, table_name, True)

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
