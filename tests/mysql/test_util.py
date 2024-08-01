#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = "hbh112233abc@163.com"

from think_sql.mysql.util import *


def test_parse_key():
    assert parse_key("id") == "`id`"
    assert parse_key("df.dir_id") == "`df`.`dir_id`"
