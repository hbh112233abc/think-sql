#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'hbh112233abc@163.com'

import cacheout
from loguru import logger
from typing import Any, Union

from think_sql.tool.util import DBConfig,db_config

from think_sql.tool.cache import CacheStorage

class Database:
    def __init__(
        self,
        config: Union[str, dict, DBConfig],
        params:dict={},
        debug: bool = False,
    ):
        """实例化数据库连接

        Args:
            config: str|dict|DBConfig 数据库连接配置
            params: dict 数据库连接参数
            debug: bool 调试模式
        """
        self.config = db_config(config)
        self.params = params
        self.log = logger
        self.connector = None
        self.cursor = None
        self.auto_commit = True
        self._debug = debug

        self.connect()

    def connect(self):
        pass

    def debug(self, flag: bool = True):
        """设置调试模式

        Args:
            flag (bool, optional): 调试标识. Defaults to True.
        """
        self._debug = flag
        return self

    def set_logger(self,logger):
        self.log = logger
        return self

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
            self.error()
        else:
            self.success()
        if self.cursor:
            self.cursor.close()
        if self.connector:
            self.connector.close()


class TableBase:
    def __init__(
        self, db:Database, table_name: str
    ):
        self.db = db
        self.connector = db.connector
        self.db_cursor = db.cursor
        self.table_name = table_name
        self._debug = db._debug
        self.log = db.logger
        self._fetch_sql = False
        self.use_cache = False
        self.cache_key = None
        self.cache_expire = 3600
        self.cache_storage = cacheout.Cache()

    def debug(self, flag: bool = True):
        """设置调试模式

        Args:
            flag (bool, optional): 调试标识. Defaults to True.

        Returns:
            self: 支持链式调用
        """
        self._debug = flag
        return self

    def set_logger(self,logger):
        self.log = logger
        return self

    def fetch_sql(self, flag: bool = True):
        """输出sql语句

        Args:
            flag (bool, optional): 是否输出sql. Defaults to True.
        """
        self._fetch_sql = flag
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

    def get_cache(self, key:str="")->Any:
        if not self.use_cache:
            return None
        if not key:
            key = self.cache_key
        if not key:
            return None
        return self.cache_storage.get(key)

    def set_cache(self, value:Any, key:str=""):
        if not self.cache_key:
            return
        if not key:
            key = self.cache_key
        if not key:
            return
        self.cache_storage.set(key, value, self.cache_expire)
