from think_sql import __version__
from think_sql.database import DB


def test_version():
    assert __version__ == "0.1.0"


def test_query():
    cfg = {
        "database": "cab100001",
        "host": "127.0.0.1",
        "port": 3306,
        "username": "root",
        "password": "root",
    }
    with DB(cfg) as db:
        db.query("select * from hy_files where title like '01%'")
