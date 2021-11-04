#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'hbh112233abc@163.com'


import pymysql

from .util import get_logger
from .table import Table
from .cache import cache

import pretty_errors
# 【重点】进行配置
pretty_errors.configure(
    separator_character='*',
    filename_display=pretty_errors.FILENAME_EXTENDED,
    line_number_first=True,
    display_link=True,
    lines_before=5,
    lines_after=2,
    line_color=pretty_errors.RED + '> ' + pretty_errors.default_config.line_color,
    code_color='  ' + pretty_errors.default_config.line_color,
)

config = {
    'database': 'test',
    'host': '127.0.0.1',
    'port': 3306,
    'username': 'root',
    'password': 'root',
}


class DB():
    def __init__(
        self,
        database='test',
        host='127.0.0.1',
        username='root',
        password='root',
        port=3306,
        params={}
    ):
        """实例化数据库连接

        Args:
            database (str, optional): 数据库名称. Defaults to 'test'.
            host (str, optional): 数据库服务地址. Defaults to '127.0.0.1'.
            port (int, optional): 数据库服务端口. Defaults to 3306.
            username (str, optional): 用户名. Defaults to 'root'.
            password (str, optional): 密码. Defaults to 'root'.
        """
        self.database = database
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.params = params

        self.log = get_logger()
        self.connect()
        self.cursor = self.conn.cursor(pymysql.cursors.SSDictCursor)

    def __repr__(self):
        return f"<class 'think_sql.database.DB' database={self.database}>"

    def connect(self):
        """连接数据库
        """
        self.conn = pymysql.connect(
            host=self.host,
            port=int(self.port),
            user=self.username,
            password=self.password,
            database=self.database,
            **self.params,
        )

    def execute(self, sql, params=()):
        self.cursor.execute(sql, params)
        self.conn.commit()

    def query(self, sql, params=()):
        self.cursor.execute(sql, params)
        return self.cursor.fetchall()

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
                self.log.info(
                    f"[sql]({self.database}) {self.cursor._executed}")
            self.log.error(str(exc_value))
            self.conn.rollback()
        else:
            self.conn.commit()

        self.cursor.close()
        self.conn.close()

    def table(self, table_name=''):
        """生成对应数据表

        Args:
            table_name (str): 表名

        Returns:
            Table: 数据表对象,可以执行链式操作
        """
        if not self.check_connected():
            self.connect()
        return Table(self.conn, self.cursor, table_name, True)

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
            self.conn.ping(True)
            return True
        except Exception as e:
            self.log.exception(e)
            return False
