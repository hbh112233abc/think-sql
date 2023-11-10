#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = "hbh112233abc@163.com"


def func(x: int, y: int):
    a = [1] * (10**x)
    b = [2] * (2 * 10**y)
    del b
    return a
