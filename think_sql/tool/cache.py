#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = "hbh112233abc@163.com"

import abc  # 利用abc模块实现抽象类
import dill
import hashlib
from functools import wraps

import cacheout

cache_storage = cacheout.Cache()

def params_to_key(function_name, *args, **kwargs):
    """按参数生成缓存键名

    Args:
        function_name (str): 方法名

    Returns:
        str: 缓存键名
    """
    key = function_name
    hash = hashlib.md5()
    for arg in args:
        b = arg
        if isinstance(arg, object):
            b = arg.__repr__()
        hash.update(dill.dumps(b))
    dict_keys = [x for x in kwargs.keys()]
    dict_keys.sort()
    for k in dict_keys:
        b = kwargs.get(k)
        if isinstance(b, object):
            b = b.__repr__()
        hash.update(dill.dumps(b))

    return key + hash.hexdigest()


def cache(key=None, ttl=3600, storage=cache_storage):
    """缓存装饰器

    Args:
        key (str, optional): 缓存标识key. Defaults to None.
        ttl (int, optional): 缓存有效期. Defaults to 3600.
        storage (object, optional): 缓存驱动对象. Defaults to cache_storage.

    Example:
        @cache()
        def func(a,b):
            return a+b
        未设置key值,默认按传参md5值为key值,传参一致的将返回缓存值
    """

    def cache_decorator(func):
        @wraps(func)
        def wrapped_function(*args, **kwargs):
            cache_key = key
            if not cache_key:
                cache_key = params_to_key(func.__name__, *args, **kwargs)
            else:
                cache_key = f"{func.__name__}_{key}"

            if storage.get(cache_key):
                return storage.get(cache_key)
            result = func(*args, **kwargs)
            if result:
                storage.set(cache_key, result, ttl)
            return result

        return wrapped_function

    return cache_decorator


class CacheStorage(metaclass=abc.ABCMeta):
    """缓存驱动抽象类"""

    @abc.abstractmethod  # 定义抽象方法，无需实现功能
    def get(self):
        "子类必须定义读功能"
        pass

    @abc.abstractmethod  # 定义抽象方法，无需实现功能
    def set(self):
        "子类必须定义写功能"
        pass
