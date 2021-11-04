
from faker import Faker

from think_sql.database import DB

config = {
    'database': 'test',
    'host': '127.0.0.1',
    'port': 3306,
    'username': 'root',
    'password': 'root',
}

data = []
faker = Faker()
for _ in range(100):
    data.append(
        {
            'name': faker.name(),
            'address': faker.address(),
            'age': faker.random.randint(1, 150),
            'remark': faker.text(),
            'status': 1,
        }
    )

with DB(**config) as db:
    db.table('user').insert(data)
