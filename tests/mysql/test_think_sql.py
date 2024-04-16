from think_sql import __version__
from think_sql.mysql.db import DB


def test_version():
    assert __version__ == "0.1.0"


def test_query():
    cfg = {
        "host": "127.0.0.1",
        "port": 3306,
        "user": "root",
        "password": "root",
        "database": "cab100001",
    }
    with DB(cfg) as db:
        db.query("select * from hy_files where title like '01%'")
