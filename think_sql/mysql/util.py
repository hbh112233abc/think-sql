#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'hbh112233abc@163.com'

import re
from typing import Any, Tuple


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
    elif symbol in ("between", "not between"):
        symbol = symbol
        r = re.compile(" and ", re.I)
        if isinstance(value, str):
            if not r.findall(value):
                value = value.split(",")

        if isinstance(value, (list, tuple)):
            if len(value) != 2:
                raise ValueError("`between` optional `value` must 2 arguments")
            value = f"{value[0]} AND {value[1]}"

        if not isinstance(value, str) or not r.findall(value):
            raise ValueError("`between` optional `value` error")

    elif symbol in ("like", "not like"):
        symbol = symbol
        if not isinstance(value, str):
            raise ValueError("`like` optional `value` must be a string")
        if "%" not in value and "_" not in value:
            raise ValueError("`like` optional `value` should contain `%` or `_`")
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
            raise ValueError("`exp` optional `value` should be a string")
        field = f"{field} {value}"
        symbol = ""
        check_value = False
    elif symbol == "" and value is None:
        # field 原生sql
        check_value = False
    else:
        if value is None:
            value = symbol
            symbol = "="
        else:
            raise ValueError("symbol is error")
    if check_value:
        if value is None:
            raise ValueError("value could not be none")

        condition_str += f" AND {field} {symbol}"
        if isinstance(value, (list, tuple)):
            condition_str += " (%s)" % ("%s," * len(value))[:-1]
            condition_val += tuple(value)
        else:
            condition_str += " %s"
            condition_val += (value,)
    else:
        condition_str += " AND " + field + symbol

    return condition_str, condition_val
