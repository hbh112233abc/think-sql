# ThinkSQL 类似ThinkPHP的数据库引擎

## Install
```
pip install think-sql
```

## How to use

### 1. simple demo

> Database: `test` Table: `user`

```
from think-sql.database import DB

config = {
    'database': 'test',
    'host': '127.0.0.1',
    'port': 3306,
    'username': 'root',
    'password': 'root',
}

with DB(**config) as db:
    data = db.table('user').where('id',1).find()
    print(data)
```
result
```
{
    "id":1,
    "username":"hbh112233abc",
    "age":"36",
    "address":"FUJIAN.XIAMEN"
}
```

### 2. Introduction

#### think_sql.database.DB

- __init__(database='test',host='127.0.0.1',username='root',password='root',port=3306,params={})

- connect()
    connect database use __init__ params

- table(table_name):Table
    return class Table <think_sql.table.Table>

- check_connected():bool
    check connected, try reconnect database

- query(sql,params=())
    query sql return cursor.fetchall

- execute(sql,params=())
    execute sql write operate(ex:insert,update,delete,...)

#### think_sql.table.Table

- __init__(connector: Connection,cursor: Cursor,table_name: str,debug: bool = True)

- init()
    initialize query condition

- debug(flag=True)
    set debug flag

- set_cache_storage(storage: CacheStorage)
    set cache storage ex: Redis

- cache(key: str = None, expire: int = 3600)
    use cache at query

- cursor(sql: str, params: list = []) -> Cursor
    return cursor object

- get_last_sql() -> str
    return last sql string

- get_lastid() -> str
    return last row id

- get_rowcount() -> int
    return affect rows count

- fetch_sql(flag: bool = True)
    set fetch sql flag,if flag = True then `query` and `execute` will only return sql

- build_sql(operation: str, params: list = []) -> str
    return build sql

- query(sql: str, params: list = []) -> list
    execute read operation sql and return cursor.fetchall()
    when `fetch_sql`=True then return sql and not execute the sql

- execute(sql: str, params: list = []) -> int
    execute write operation sql and return affect rows count
    when `fetch_sql`=True then return sql and not execute the sql

- where(field: Union[str, list, tuple], symbol: str = '', value: Any = None)
    set query conditions, support multipe use

    > where(field,value)

    ```
    where field = value
    ```
    > where(field,symbol,value)

    ```
    where field symbol value
    ```
    > where(
        [
            [field1,symbol1,value1],
            [field2,symbol2,value2]
        ]
    )

    ```
    where field1 symbol1 value1 and field2 symbol2 value2
    ```

    > where(field1,symbol1,value1).where(field2,symbol2,value2)

    ```
    where field1 symbol1 value1 and field2 symbol2 value2
    ```

    - symbol

    |symbol|another|demo|
    |-|-|-|
    |`=`|`eq`,`=`| where('id','=',1)|
    |`<>`|`neq`, `!=`, `<>`| where('id','<>',1)|
    |`>`|`gt`,`>`| where('id','>',1)|
    |`>=`|`egt`,`>=`| where('id','>=',1)|
    |`<`|`lt`, `<`|where('id','<',1)|
    |`<=`|`elt`,`<=`| where('id','<=',1)|
    |`in`|`in`,`not in`| where('id','in',[1,2,3])|
    |`between`|`between`,`not between`| where('id','between',[1,5]) where('id','between','1,5') where('id','between','1 and 5')|
    |`like`|`like`, `not like`| where('name','like','%hbh%')|
    |`null`|`is null`,`null`| where('remark','is null')|
    |`not null`|`is not null`,`not null`| where('remark','is not null')|
    |`exists`|`exists`, `not exists`| where('remark','exists')|
    |`exp`|`exp`| where('id','exp','in (1,2,3)')|

- where_or(field: Union[str, list], symbol: str = '', value: Any = None)

    > where('id',1).where_or('id',5)

    ```
    where id = 1 or id = 5
    ```

- limit(start: int, step: int = None)
    LIMIT start,step

- page(index: int = 1, size: int = 20)
    LIMIT index*size-1,size

- order(field: str, sort: str = 'asc')
    ORDER BY field sort

- group(field:str)
    GROUP BY field

- field(fields: Any, exclude: bool = False)
    SELECT fields
    if `exclude`=True then select the fields of table (exlude:`fields`)

- select(build_sql: bool = False) -> list
    return select query result
    if `build_sql`=True then return sql

- find()
    return select ... limit 1

- value(field: str)
    return the field of first row

- column(field: str,key: str = '')

    > column('name')

    return ['hbh','mondy']

    > column('name,score')

    return [{'hbh':80},{'mondy':88}]

    > column('score','name')

    return {'hbh':80, 'mondy':88}

    > column('id,score','name')

    return {
                'hbh':{'id':1,'score':80},
                'mondy':{'id':2,'score':88}
           }

- alias(short_name: str = '')
    set alias table_name

- join(table_name: str, as_name: str = '', on: str = '', join: str = 'inner', and_str: str = '')
    - `table_name` join table_name
    - `as_name` alias short_table_name for `table_name`
    - `on` join condition
    - `join` join type in 'INNER', 'LEFT', 'RIGHT', 'FULL OUTER'
    - `and_str` and condition
    demo
    ```
    db.table('table1').alias('a').join('table2','b','a.id=b.a_id','left').join('table2','c','c.a_id=a.id').field('a.id,a.name,b.id as b_id,b.score,c.id as c_id,c.remark').where('a.id',1).find()
    ```
    sql
    ```
    SELECT
        a.id,
        a.name,
        b.id AS b_id,
        b.score,
        c.id AS c_id,
        c.remark
    FROM
        table1 AS a
        LEFT JOIN table2 AS b ON a.id = b.a_id
        INNER JOIN table3 AS c ON c.a_id = a.id
    WHERE
        a.id = 1
        LIMIT 1
    ```
- union(sql1: str, sql2: str, union_all: bool = False)
    union sql1 and sql2
    - union_all if union_all is True then `UNION ALL`

    *demo*
    ```
    sql1 = db.table('table1').field('name,score').where('status',1).select(build_sql=True)
    sql2 = db.table('table2').field('name,score').where('status',1).select(build_sql=True)

    result = db.table().union(sql1,sql2).where('score','>',60).select()
    ```
    *sql*
    ```
    SELECT
	*
    FROM
        ( SELECT `name`, `score` FROM table1 WHERE `status` = 1 )
        UNION
        ( SELECT `name`, `score` FROM table2 WHERE `status` = 1 )
    WHERE
        score > 60
    ```

- insert(data: Union[dict, List[dict]], replace: bool = False) -> int
    insert data to database
    - `data` dict: insert one record; list: insert multiple records
    - `replace` bool if `replace` is True then use `REPLACE INTO`

- update(data: dict, all_record: bool = False) -> int
    update data
    - `data` dict you want update data
    - `all_record` bool if `all_record` is False then you must set update condition; if you want to update all records then you need set `all_record` = True

- delete(all_record: bool = False) -> int
    delete record
    - `all_record` bool if `all_record` is False then you must set delete condition; if you want to delete all records then you need set `all_record` = True

- inc(field: str, step: Union[str, int, float] = 1) -> int

    increase `field` +`step`

- dec(field: str, step: int = 1) -> int

    decrease `field` -`step`

- max(field: str) -> Union[int, float]

    get the max value of `field`

- sum(field: str) -> Union[int, float, Decimal]

    get the sum value of `field`

- avg(field: str) -> Union[int, float, Decimal]

    get the avg value of `field`

- count(field: str = '*') -> int

    get the count of records

- copy_to(new_table: str = None, create_blank_table: bool = False) -> int

    copy data to `new_table`
    - `new_table` if `new_table` is None then `new_table` will auto set like `{table_name}_copy`
    - `create_blank_table` bool if `create_blank_table` is True then only create a blank table like current table.

    *demo*
    ```
    db.table('user').field('name,score').where('score','>',60).copy_to('good_boy')
    ```

    *sql*
    ```
    SELECT
	    `name`,
        `score`
    INTO `good_boy`
    FROM
    `user`
    WHERE
        score > 60
    ```


- insert_to(new_table: str, fields: Union[str, list, tuple] = None) -> int

    ```
    INSERT INTO {new_table} SELECT {select_fields} FROM {table} {join} WHERE {where}{group}{order}{limit}
    ```

- exists(self) -> bool

    check record exists with some query conditions, it use `SELECT 1 FROM {table} {join} WHERE {where} LIMIT 1`

- batch_update(data:List[dict],key:str) -> int

    batch update multiple records

    *demo*
    ```
    data = [
        {'id':1,'score':66},
        {'id':2,'score':59},
        {'id':3,'score':86},
        {'id':4,'score':90},
    ]
    db.table('user').batch(data,key='id')
    ```
    *sql*
    ```
    update `user` set score = 66 where id = 1;
    update `user` set score = 59 where id = 2;
    update `user` set score = 86 where id = 3;
    update `user` set score = 90 where id = 4;
    ```




## Development

### poetry包管理器
[官网](https://python-poetry.org/)

[Python包管理之poetry的使用](https://blog.csdn.net/zhoubihui0000/article/details/104937285)

[Python包管理之poetry基本使用](https://zhuanlan.zhihu.com/p/110721747)


```
# 配置虚拟环境在项目目录下
poetry config virtualenvs.path true
# 安装依赖
poetry install
# 进入虚拟环境
poetry shell
```
### poetry command

|名称| 功能|
|-|-|
|new|创建一个项目脚手架，包含基本结构、pyproject.toml 文件|
|init|基于已有的项目代码创建 pyproject.toml 文件，支持交互式填写|
|install|安装依赖库|
|update|更新依赖库|
|add|添加依赖库|
|remove|移除依赖库|
|show|查看具体依赖库信息，支持显示树形依赖链|
|build|构建 tar.gz 或 wheel 包|
|publish|发布到 PyPI|
|run|运行脚本和代码|

## unit test
```
pytest --cov --cov-report=html
```

## publish
```
poetry build
poetry config pypi-token.pypi "your pypi.org api token"
poetry publish -n
```
