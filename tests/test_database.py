import pytest
import pymysql
from think_sql.database import DB


class TestDb():
    def setup(self):
        self.config = {
            'database': 'test',
            'host': '127.0.0.1',
            'port': 3306,
            'username': 'root',
            'password': 'root',
        }

    def test_connect(self):
        with DB(**self.config) as db:
            assert isinstance(db.cursor, pymysql.cursors.SSDictCursor)
            result = db.query("select VERSION() as version;")
            assert isinstance(result, list)
            assert isinstance(result[0], dict)
            assert result[0]['version'] == '5.6.48'


if __name__ == '__main__':
    pytest.main('-s test_database.py')
