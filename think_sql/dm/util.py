#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'hbh112233abc@163.com'

from decimal import Decimal
import re
from typing import Any, Tuple

def parse_key(key, strict:bool=False)->str:
    """加工字段名，处理别名，JSON字段，严格模式检查

    Args:
        key (any): 字段
        strict (bool, optional): 是否严格模式. Defaults to False.

    Raises:
        Exception: 不支持的字段名

    Returns:
        str: 加工后的字段名
    """
    if isinstance(key,(int,float,Decimal)):
        return str(key)

    # 去除空白字符
    key = key.strip()
    table = ""
    if re.search(r'\s+as\s+', key, re.IGNORECASE):
        (key, alias) = re.split(r'\s+as\s+', key, flags=re.IGNORECASE)
        return f"{parse_key(key)} AS {parse_key(alias)}"

    # JSON字段支持
    if '->' in key and '(' not in key:
        # 使用split()方法分割字符串
        (field, name) = key.split('->')
        key = f"\"{field}\".\"{name}\""

    # 处理表别名
    elif '.' in key and not re.search(r'[,\'"()[]\s]', key):
        (table, key) = key.split('.', 1)

    # 严格模式检查
    if strict and not re.match(r'^[\w.*]+$', key):
        raise Exception('not support data:' + key)

    # 处理键名
    if key != '*' and not re.search(r'[,\'"*$$$$$$.\s]', key):
        key = f'"{key}"'
    # 构建完整的键名
    if table:
        key = f'"{table}".{key}'
    return key

def parse_value(value:Any)->Any:
    if isinstance(value,str):
        value = value.replace("'", "''")
        return f"{value}"
    if value is None:
        return ""
    return value

def parse_where(
    field: str, symbol: str = "", value: Any = None
) -> Tuple[str, tuple]:
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
    condition_str = ""
    condition_val = tuple()
    key_parsed = True

    symbol = str(symbol).strip().lower()
    if symbol in ("eq", "="):
        symbol = "="
    elif symbol in ("neq", "!=", "<>"):
        symbol = "<>"
    elif symbol in ("gt", ">"):
        symbol = ">"
    elif symbol in ("egt", ">="):
        symbol = ">="
    elif symbol in ("lt", "<"):
        symbol = "<"
    elif symbol in ("elt", "<="):
        symbol = "<="
    elif symbol in ("in", "not in"):
        symbol = symbol
        if isinstance(value, str):
            if not (value.startswith("(") and value.endswith(")")):
                value = value.split(",")
            else:
                value = value.strip()
                if not value.startswith("(") or not value.endswith(")"):
                    raise ValueError("in optional value error")
                value = value[1:-1].split(",")
        if not isinstance(value, (list, tuple)):
            raise ValueError("in optional value must be a list or tuple")

    elif symbol in ("between", "not between"):
        symbol = symbol
        r = re.compile(" and ", re.I)
        if isinstance(value, str):
            if not r.findall(value):
                value = value.split(",")

        if isinstance(value, (list, tuple)):
            if len(value) != 2:
                raise ValueError("between optional value must 2 arguments")
            value = f"'{parse_value(value[0])}' AND '{parse_value(value[1])}'"

        if not isinstance(value, str) or not r.findall(value):
            raise ValueError("between optional value error")

        symbol = f" {symbol} {value}"
        value = None
        check_value = False

    elif symbol in ("like", "not like"):
        symbol = symbol
        if not isinstance(value, str):
            raise ValueError("like optional value must be a string")
        if "%" not in value and "_" not in value:
            raise ValueError("like optional value should contain % or _")
    elif symbol == "is":
        symbol = "is"
    elif symbol in ("null", "is null"):
        symbol = " is null"
        check_value = False
    elif symbol in ("not null", "is not null"):
        symbol = " is not null"
        check_value = False
    elif symbol in ("exists", "not exists"):
        field = f"{symbol}({field})"
        symbol = ""
        check_value = False
    elif symbol == "exp":
        if not isinstance(value, str):
            raise ValueError("exp optional value should be a string")
        field = f"\"{field}\" {value}"
        symbol = ""
        check_value = False
    elif symbol == "" and value is None:
        # field 原生sql
        check_value = False
        key_parsed = False
    else:
        if value is None:
            value = symbol
            symbol = "="
        else:
            raise ValueError("symbol is error")

    if key_parsed:
        field = parse_key(field)

    if check_value:
        if value is None:
            raise ValueError("value could not be none")

        condition_str += f" AND {field} {symbol}"
        if isinstance(value, (list, tuple)):
            ss = ("'%s'," * len(value))[:-1]
            condition_str += f" ({ss})"
            condition_val += tuple([parse_value(v) for v in value])
        else:
            condition_str += " '%s'"
            condition_val += (parse_value(value),)
    else:
        condition_str += " AND " + field + symbol

    return condition_str, condition_val
