import pytest
import pymysql
from think_sql.mysql.db import DB
from think_sql.tool.util import DBConfig

@pytest.fixture(scope="module")
def db():
    db_config = DBConfig(
        host="localhost",
        port=3306,
        user="root",
        password="root",
        database="test"
    )
    db = DB(db_config)
    yield db
    db.close()

def test_connect(db):
    assert isinstance(db.cursor, pymysql.cursors.SSDictCursor)
    result = db.query("select VERSION() as version;")
    assert isinstance(result, list)
    assert isinstance(result[0], dict)
    assert result[0]['version'] == '8.0.20'
