#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'hbh112233abc@163.com'

import abc
from decimal import Decimal
from typing import Any, List, Union

class TableInterface(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def debug(self,flag:bool=True):
        pass

    @abc.abstractmethod
    def cursor(self):
        pass

    @abc.abstractmethod
    def get_fields(self)->tuple:
        pass

    @abc.abstractmethod
    def get_last_sql(self)->str:
        pass

    @abc.abstractmethod
    def get_lastid(self)->str:
        pass

    @abc.abstractmethod
    def get_rowcount(self)->int:
        pass

    @abc.abstractmethod
    def fetch_sql(self,flag:bool=True):
        pass

    @abc.abstractmethod
    def build_sql(self,operation: str, params: list = [])->str:
        pass

    @abc.abstractmethod
    def execute(self,sql:str,params:Union[list,tuple]=None)->int:
        pass

    @abc.abstractmethod
    def where(self,field: Union[str, list, tuple], symbol: str = "", value: Any = None):
        pass

    @abc.abstractmethod
    def distinct(self,field:str=""):
        pass

    @abc.abstractmethod
    def group(self,field:str):
        pass

    @abc.abstractmethod
    def field(self, fields: Any, exclude: bool = False):
        pass

    @abc.abstractmethod
    def find(self):
        pass

    @abc.abstractmethod
    def value(self,field:str):
        pass

    @abc.abstractmethod
    def column(self, fields: str, key: str = "") -> Union[list, dict]:
        pass

    @abc.abstractmethod
    def select(self, build_sql: bool = False) -> List[dict]:
        pass

    @abc.abstractmethod
    def alias(self, short_name: str = ""):
        pass

    @abc.abstractmethod
    def join(
        self,
        table_name: str,
        as_name: str = "",
        on: str = "",
        join: str = "inner",
        and_str: str = "",
    ):
        pass

    @abc.abstractmethod
    def union(self, sql1: str, sql2: str, union_all: bool = False):
        pass

    @abc.abstractmethod
    def insert(self, data: Union[dict, List[dict]], replace: bool = False) -> int:
        pass

    @abc.abstractmethod
    def update(self,data: dict, all_record: bool = False)->int:
        pass

    @abc.abstractmethod
    def delete(self,all_record: bool = False)->int:
        pass

    @abc.abstractmethod
    def inc(self, field: str, step: Union[str, int, float] = 1) -> int:
        pass

    @abc.abstractmethod
    def dec(self, field: str, step: int = 1) -> int:
        pass

    @abc.abstractmethod
    def max(self, field: str) -> Union[int, float]:
        pass

    @abc.abstractmethod
    def sum(self, field: str) -> Union[int, float, Decimal]:
        pass

    @abc.abstractmethod
    def avg(self, field: str) -> Union[int, float, Decimal]:
        pass

    @abc.abstractmethod
    def count(self, field: str = "1") -> Union[str, int]:
        pass

    @abc.abstractmethod
    def exists(self) -> bool:
        pass



class DatabaseInterface(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def connect(self):
        pass

    @abc.abstractmethod
    def execute(self, sql:str, params:tuple=()) -> int:
        pass

    @abc.abstractmethod
    def query(self, sql:str, params:tuple=()) -> List[dict]:
        pass

    @abc.abstractmethod
    def table(self, table_name="")->TableInterface:
        pass

    @abc.abstractmethod
    def check_connected(self)->bool:
        pass

    @abc.abstractmethod
    def success(self):
        pass

    @abc.abstractmethod
    def error(self):
        pass
