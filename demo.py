from think_sql.database import DB

config = {
    'database': 'test',
    'host': '127.0.0.1',
    'port': 3306,
    'username': 'root',
    'password': 'root',
}

with DB(**config) as db:
    res = db.table('sys_area').where('id', 100001).find()
    print(res)
