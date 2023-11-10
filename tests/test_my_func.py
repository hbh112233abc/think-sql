#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = "hbh112233abc@163.com"

from memory_profiler import LineProfiler, show_results

from .my import func


def test_my_func():
    lp = LineProfiler()
    lp_wrapper = lp(func)
    lp_wrapper(6, **{"y": 7})
    show_results(lp)
