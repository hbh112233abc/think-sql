#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = "hbh112233abc@163.com"

import re
from typing import Optional

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
