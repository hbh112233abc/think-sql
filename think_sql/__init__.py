__version__ = '0.1.0'

import importlib
from contextlib import contextmanager
from typing import Union

from think_sql.tool.base import Database
from think_sql.tool.util import DBConfig, db_config

DRIVERS = {
    "mysql":{
        "path":"think_sql.mysql",
        "depend":["pymysql"],
    },
    "dm":{
        "path":"think_sql.dm",
        "depend":["dmPython"],
    },
}

def __import_module(config:DBConfig)->Database:
    if config.type not in DRIVERS:
        raise Exception(f'Unsupported database type: {config.type}')

    try:
        module_path = DRIVERS[config.type]['path']
        module = importlib.import_module(module_path)
    except ImportError:
        raise ImportError(f"Please install {DRIVERS[config.type]['depend']}")

    return getattr(module,'DB')


@contextmanager
def DB(cfg:Union[str,dict,DBConfig]):
    config = db_config(cfg)

    Database = __import_module(config)

    with Database(config) as db:
        yield db

def db(cfg:Union[str,dict,DBConfig]):
    config = db_config(cfg)

    Database = __import_module(config)

    return Database(config)
