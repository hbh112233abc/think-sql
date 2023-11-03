#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = "hbh112233abc@163.com"

from collections import Counter
from line_profiler import profile
from typing import Any, List, Tuple

from think_sql.database import DB


@profile
def count_column_value(
    db: DB, table_name: str, field_name: str, sample_size: int
) -> List[Tuple[Any, int]]:
    """取列阈值
    <sample_size 取(SELECT COUNT(*) FROM {table_name}) / 2
    否则 {sample_size} / 2

    Args:
        db (DB): 数据库连接
        table_name (str): name of the table
        field_name (str): name of the field
        sample_size (int): number of samples

    Returns:
        List[Tuple[Any, int]]: count result


    在这个查询中，使用了CASE语句来判断数据行数是否小于sample_size。如果数据行数小于sample_size，则使用
    (SELECT COUNT(*) FROM {table_name}) / 2
    作为阈值，即表的实际大小除以2；否则使用 {sample_size} / 2 作为阈值。
    """

    # 如果你的数据库是MySQL 8.0，那么推荐用 CTE（公共表达式）的形式
    '''
    sql = f"""
    WITH subquery AS (
        SELECT {field_name}
        FROM {table_name}
        LIMIT {sample_size}
    )
    SELECT COUNT(*) as count
    FROM subquery
    GROUP BY {field_name}
    HAVING COUNT(*) >=
        CASE
            WHEN (SELECT COUNT(*) FROM subquery) < {sample_size} THEN (SELECT COUNT(*) FROM {table_name}) / 2
            ELSE {sample_size} / 2
        END;
    """
    '''

    # 默认采用子查询兼容MySQL 5.7版本
    '''
    sql = f"""
    SELECT COUNT(*) as count
    FROM (
        SELECT {field_name}
        FROM {table_name}
        LIMIT {sample_size}
    ) AS subquery
    GROUP BY {field_name}
    HAVING COUNT(*) >=
    CASE
        WHEN (SELECT COUNT(*) FROM {table_name} LIMIT {sample_size}) < {sample_size}
        THEN (SELECT COUNT(*) FROM {table_name}) / 2
        ELSE {sample_size} / 2
    END;
    """
    result = db.query(sql)
    '''
    field_list = db.table(table_name).limit(sample_size).column(field_name)
    distinct_fields = Counter(field_list).most_common(3)
    table_info = db.query(f"SHOW TABLE STATUS WHERE Name='{table_name}';")
    table_count = int(table_info[0]["Rows"])
    limit_count = min(table_count, sample_size) / 2
    result = list(filter(lambda x: x[1] >= limit_count, distinct_fields))
    return result


if __name__ == "__main__":
    cfg = {
        "database": "cab100151",
        "host": "192.168.102.154",
        "port": 3306,
        "username": "root",
        "password": "xmhymake",
    }
    db = DB(cfg)
    field_name = "unselect_count"
    table_name = "hy_multi_archives_dirs"
    sample_size = 100000
    res = count_column_value(db, table_name, field_name, sample_size)
    print(res)
