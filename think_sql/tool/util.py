#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = "hbh112233abc@163.com"

import re
from typing import Optional, Union

from pydantic import BaseModel, Field


class DBConfig(BaseModel):
    type: str = Field("mysql", description="Type of the database,default is `mysql`")
    host: str = Field(
        default="127.0.0.1", description="Host of the database,default is `127.0.0.1`"
    )
    port: int = Field(
        default=3306, description="Port of the database,default is `3306`"
    )
    username: str = Field(
        default="root", description="Username of the database,default is `root`"
    )
    password: str = Field(
        default="root", description="Password of the Username,default is `root`"
    )
    database: Optional[str] = Field(default="", description="Database selected")

    @staticmethod
    def parse_dsn(dsn: str) -> "DBConfig":
        """
        Parse a database configuration string into a dictionary.

        :param cfg: A string in the format "username:'password'@host:port/database"
        :return: A dictionary with keys 'username', 'password', 'host', 'port', and 'database'
        :raises ValueError: If the input string does not match the expected format

        :return
        {
            "host":"",
            "port":3306,
            "username":"",
            "password":"",
            "database":"",
        }
        """
        pattern = r"(?P<username>.*?):'(?P<password>.*?)'@(?P<host>.*?):(?P<port>\d+)(/(?P<database>.*))?"
        match = re.match(pattern, dsn)
        if not match:
            raise ValueError(
                "Invalid db config format, expected `username:'password'@host:port/database`"
            )
        config = match.groupdict()
        dbc = DBConfig.model_validate(config)
        return dbc

def db_config(config:Union[str,dict,DBConfig])->DBConfig:
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
    return config

def to_number(s: Union[str, int, float], key: str = ""):
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
        if re.match(r"^\d+$", s):
            s = int(s)
            return s
        if re.match(r"^\d+\.\d+$", s):
            s = float(s)
            return s

    if not isinstance(s, (int, float)) and key:
        raise ValueError(f"`{key}` must number")

    return s
