#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = "hbh112233abc@163.com"

import re
import textwrap
from collections import Counter
from typing import Any, Dict, List, Tuple

import sqlparse
from tabulate import tabulate
from sql_metadata import Parser

from think_sql.mysql.db import DB

logs = []


def log(s: str = "", level: str = "log") -> List[str]:
    global logs
    tpl = {
        "info": "\033[94m {} \033[0m",  # 蓝
        "error": "\033[91m {} \033[0m",  # 红
        "success": "\033[92m {} \033[0m",  # 绿
        "warning": "\033[93m {} \033[0m",  # 黄
        "debug": "\033[95m {} \033[0m",  # 紫
        "log": "{}",  # 无颜色
    }
    logs.append(s)
    s = tpl.get(level, "{}").format(s)
    print(s)
    return logs


def has_table_alias(table_alias: dict) -> bool:
    """判断是否有表别名

    Args:
        table_alias (dict): 表别名

    Returns:
        bool: 是否包含别名
    """
    if not table_alias:
        return False
    if not isinstance(table_alias, dict):
        return False
    check_key = (
        "join",
        "on",
        "where",
        "group by",
        "order by",
        "limit",
    )
    keys = [k.lower() for k in table_alias.keys()]
    for k in check_key:
        if k in keys:
            return False
    return True


def count_column_value(
    db: DB,
    table_name: str,
    field_name: str,
    sample_size: int,
    where_condition: str = "1=1",
) -> List[Tuple[Any, int]]:
    """取列阈值
    <sample_size 取(SELECT COUNT(*) FROM {table_name}) / 2
    否则 {sample_size} / 2

    Args:
        db (DB): 数据库连接
        table_name (str): name of the table
        field_name (str): name of the field
        sample_size (int): number of samples
        where_condition (str): where condition

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
    field_list = (
        db.table(table_name)
        .where(where_condition)
        .limit(sample_size)
        .column(field_name)
    )
    distinct_fields = Counter(field_list).most_common(3)
    try:
        if "." in table_name:
            database, table = table_name.split(".")
            sql = f"""
            SELECT TABLE_ROWS
            FROM information_schema.tables
            WHERE TABLE_NAME = "{table}"
            AND TABLE_SCHEMA = "{database}"
            AND TABLE_TYPE = "base table";
            """
            table_info = db.query(sql)
            table_count = int(table_info[0]["TABLE_ROWS"])
        else:
            table_info = db.query(f"SHOW TABLE STATUS WHERE Name='{table_name}';")
            table_count = int(table_info[0]["Rows"])
    except Exception as e:
        table_count = db.table(table_name).count()

    limit_count = min(table_count, sample_size) / 2
    result = list(filter(lambda x: x[1] >= limit_count, distinct_fields))
    return result


def execute_index_query(db: DB, table_name: str, index_columns: str) -> str:
    index_columns = index_columns
    index_columns = index_columns.split(",")
    updated_columns = [f"'{column.strip()}'" for column in index_columns]
    final_columns = ", ".join(updated_columns)
    sql = f"""SELECT
            TABLE_NAME,INDEX_NAME,COLUMN_NAME,cardinality
        FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = '{db.config.database}'
            AND TABLE_NAME = '{table_name}'
            AND COLUMN_NAME IN ({final_columns})"""

    index_result = db.query(sql)

    if not index_result:
        log(f"没有检测到 {table_name} 表 字段 {final_columns} 有索引。")
        return ""

    # 提取列名
    e_column_names = index_result[0].keys()

    # 提取结果值并进行自动换行处理
    e_result_values = []
    for row in index_result:
        values = list(row.values())
        wrapped_values = [textwrap.fill(str(value), width=30) for value in values]
        e_result_values.append(wrapped_values)

    # 将结果格式化为表格（包含竖线）
    e_table = tabulate(
        e_result_values, headers=e_column_names, tablefmt="grid", numalign="left"
    )

    return e_table


def check_index_exist(db: DB, table_name: str, index_column: str):
    show_index_sql = (
        f"show index from {table_name} where column_name = '{index_column}'"
    )

    index_result = db.query(show_index_sql)

    return index_result


def check_index_exist_multi(
    db: DB,
    table_name: str,
    index_columns: str,
    index_number: int,
) -> list:
    index_columns = index_columns
    index_columns = index_columns.split(",")
    updated_columns = [f"'{column.strip()}'" for column in index_columns]
    final_columns = ", ".join(updated_columns)
    sql = f"""SELECT
            TABLE_NAME,INDEX_NAME,COLUMN_NAME,cardinality
        FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = '{db.config.database}'
            AND TABLE_NAME = '{table_name}'
            AND COLUMN_NAME IN ({final_columns})
        GROUP BY INDEX_NAME
        HAVING COUNT(INDEX_NAME) = {index_number}"""

    index_result = db.query(sql)

    return index_result


def parse_where_condition(sql: str) -> Dict[str, str]:
    """解析查询条件,返回{"字段":"条件语句"}

    Args:
        sql (str): 查询语句

    Returns:
        Dict[str, str]: 返回结果{"字段":"条件语句"}
    """
    parsed = sqlparse.parse(sql)
    stmt = parsed[0]

    result = {}

    for token in stmt.tokens:
        if not isinstance(token, sqlparse.sql.Where):
            continue
        field = ""
        for cond_token in token.tokens:
            if isinstance(cond_token, sqlparse.sql.Comparison):
                left_token = cond_token.left.value.strip()
                result[left_token] = cond_token.value.strip()
                continue

            if isinstance(cond_token, sqlparse.sql.Identifier):
                field = cond_token.value.strip()
                result[field] = ""

            if not field:
                continue

            if isinstance(
                cond_token, sqlparse.sql.Token
            ) and cond_token.value.upper() in ["OR", "AND"]:
                field = ""
                break

            if isinstance(cond_token, sqlparse.sql.Parenthesis):
                field = ""
                break

            result[field] += cond_token.value.strip()

    return result


def suggestion(where_clauses: Dict[str, str]):
    if not where_clauses:
        return
    log()
    log("4) 额外的建议：")
    log("-" * 100)

    like_pattern = r"like\s+['\"]%|like\s+concat\(['\"]%|regexp\s+"
    func_pattern = r"\b(\w+(\(.*\).*[>=<!=<>]))"

    like_r = []
    func_r = []

    for _, condition in where_clauses.items():
        if re.findall(like_pattern, condition, re.IGNORECASE):
            pattern = r"like\s+(?:concat\(.*?\)|'%%'|[\'\"]%.*?[\'\"])|regexp\s+[\'\"].*?[\'\"]"
            like_match = re.search(pattern, condition, re.IGNORECASE)
            if like_match:
                like_r.append(like_match.group())

        func_match = re.findall(func_pattern, condition, re.IGNORECASE)
        if func_match:
            func_r.append(",".join([m[0] for m in func_match]))

    if like_r:
        log(
            f"like模糊匹配，百分号在首位，【{','.join(like_r)}】是不能用到索引的，例如like '%张三%'，可以考虑改成like '张三%'，这样是可以用到索引的，如果业务上不能改，可以考虑用全文索引。"
        )

    if func_r:
        log(
            f"索引列使用了函数作计算：【{','.join(func_r)}】，会导致索引失效。"
            f"如果你是MySQL 8.0可以考虑创建函数索引；如果你是MySQL 5.7，你要更改你的SQL逻辑了。"
        )


def help(
    db: DB, sql_query: str, tip: str = "输入的SQL语句", sample_size: int = 100000
) -> List[str]:
    global logs
    logs = []

    log(f"1) {tip}")
    log("-" * 100)
    # 美化SQL
    formatted_sql = sqlparse.format(sql_query, reindent=True, keyword_case="upper")
    log(formatted_sql)
    log("-" * 100)

    if "SELECT" not in sql_query.upper():
        return log("sql_helper工具仅支持select语句", "error")
    try:
        # 解析SQL，识别出表名和字段名
        parser = Parser(sql_query)
        table_names = parser.tables
        # log(f"表名是: {table_names}")
        table_aliases = parser.tables_aliases
        data = parser.columns_dict
        if not data:
            return log("sql解析无法获取字段或相关查询条件,无法分析", "error")

        select_fields = data.get("select", [])
        join_fields = data.get("join", [])
        where_fields = data.get("where", [])
        # log(f"WHERE字段是：{where_fields}")
        order_by_fields = data.get("order_by", [])
        group_by_fields = data.get("group_by", [])
    except Exception as e:
        return log("sql可能太复杂了,解析失败,请手动自查", "error")

    sql = f"EXPLAIN {sql_query}"
    try:
        db.cursor.execute(sql)
        explain_result = db.cursor.fetchall()
        if not explain_result:
            return log(f"Couldn't explain: {sql}", "warning")
    except Exception as e:
        return log(f"Mysql explain failed: {e}", "error")

    # 提取列名
    e_column_names = list(explain_result[0].keys())

    # 提取结果值并进行自动换行处理
    e_result_values = []
    for row in explain_result:
        values = list(row.values())
        wrapped_values = [textwrap.fill(str(value), width=20) for value in values]
        e_result_values.append(wrapped_values)

    # 将结果格式化为表格（包含竖线）
    e_table = tabulate(
        e_result_values, headers=e_column_names, tablefmt="grid", numalign="left"
    )

    log("2) EXPLAIN执行计划:")
    log(e_table)
    log("3) 索引优化建议：")
    log("-" * 100)

    contains_dot = False
    # 判断有无where条件
    if len(where_fields) == 0:
        log("你的SQL没有where条件.")
    else:
        contains_dot = any("." in field for field in where_fields)

    # 判断如果SQL里包含on，检查on后面的字段是否有索引。
    if len(join_fields) > 0:
        table_field_dict = {}

        for field in join_fields:
            table_field = field.split(".")
            if len(table_field) > 1:
                table_name = ".".join(table_field[:-1])
                field_name = table_field[-1]
                if table_name not in table_field_dict:
                    table_field_dict[table_name] = []
                table_field_dict[table_name].append(field_name)

        for table_name, on_columns in table_field_dict.items():
            for on_column in on_columns:
                show_index_sql = (
                    f"show index from {table_name} where column_name = '{on_column}'"
                )
                index_result = db.query(show_index_sql)
                if not index_result:
                    log("join联表查询，on关联字段必须增加索引！")
                    log(
                        f"需要添加索引：ALTER TABLE {table_name} ADD INDEX idx_{on_column}({on_column});",
                        "error",
                    )
                    log(f"【{table_name}】表 【{on_column}】字段，索引分析：")
                    index_static = execute_index_query(
                        db,
                        table_name=table_name,
                        index_columns=on_column,
                    )
                    log(index_static)

    table_aliases_exists = has_table_alias(table_aliases)

    where_clauses = parse_where_condition(formatted_sql)

    # 解析执行计划，查找需要加索引的字段
    for row in explain_result:
        # 获取查询语句涉及的表和字段信息
        table_name = row["table"]
        if table_name.startswith("<") and table_name.endswith(">"):
            continue
        add_index_fields = []
        # 判断是否需要加索引的条件
        if (row["type"] == "ALL" and row["key"] is None) or (
            isinstance(row["rows"], int)
            and row["rows"] >= (1 if len(join_fields) != 0 else 1000)
        ):
            # 判断表是否有别名，没有别名的情况：
            if not table_aliases_exists and not contains_dot:
                if len(where_fields) != 0:
                    for where_field in where_fields:
                        if where_field in where_clauses:
                            where_clause_value = (
                                where_clauses[where_field]
                                .replace("\n", "")
                                .replace("\r", "")
                            )
                            where_clause_value = re.sub(r"\s+", " ", where_clause_value)
                            cardinality = count_column_value(
                                db,
                                table_name,
                                where_field,
                                sample_size,
                                where_clause_value,
                            )
                        else:
                            cardinality = count_column_value(
                                db, table_name, where_field, sample_size
                            )
                        # log(f"cardinality: {cardinality}")
                        if cardinality:
                            count_value = cardinality[0][1]
                            log(
                                f"取出表 【{table_name}】 where条件字段 【{where_field}】 {sample_size} 条记录，重复的数据有：【{count_value}】 条，没有必要为该字段创建索引。"
                            )
                        else:
                            add_index_fields.append(where_field)

                if group_by_fields is not None and len(group_by_fields) != 0:
                    for group_field in group_by_fields:
                        cardinality = count_column_value(
                            db, table_name, group_field, sample_size
                        )
                        if cardinality:
                            count_value = cardinality[0][1]
                            log(
                                f"取出表 【{table_name}】 group by条件字段 【{group_field}】 {sample_size} 条记录，重复的数据有：【{count_value}】 条，没有必要为该字段创建索引。"
                            )
                        else:
                            add_index_fields.append(group_field)

                if len(order_by_fields) != 0:
                    for order_field in order_by_fields:
                        cardinality = count_column_value(
                            db, table_name, order_field, sample_size
                        )
                        if cardinality:
                            count_value = cardinality[0][1]
                            log(
                                f"取出表 【{table_name}】 order by条件字段 【{order_field}】 {sample_size} 条记录，重复的数据有：【{count_value}】 条，没有必要为该字段创建索引。"
                            )
                        else:
                            add_index_fields.append(order_field)

                add_index_fields = list(
                    dict.fromkeys(add_index_fields).keys()
                )  # 字段名如果一样，则去重，并确保元素的位置不发生改变

                if len(add_index_fields) == 0:
                    if "index_result" not in globals():
                        log(f"【{table_name}】 表，无需添加任何索引。", "success")
                    elif index_result:
                        log(f"【{table_name}】 表，无需添加任何索引。", "success")
                    else:
                        pass
                elif len(add_index_fields) == 1:
                    index_name = add_index_fields[0]
                    index_columns = add_index_fields[0]
                    index_result = check_index_exist(
                        db,
                        table_name=table_name,
                        index_column=index_columns,
                    )
                    if not index_result:
                        if row["key"] is None or (
                            isinstance(row["rows"], int) and row["rows"] >= 1
                        ):
                            log(
                                f"建议添加索引：ALTER TABLE {table_name} ADD INDEX idx_{index_name}({index_columns});",
                                "warning",
                            )
                    else:
                        log(
                            f"【{table_name}】表 【{index_columns}】字段，索引已经存在，无需添加任何索引。",
                            "success",
                        )
                    log(f"【{table_name}】表 【{index_columns}】字段，索引分析：")
                    index_static = execute_index_query(
                        db,
                        table_name=table_name,
                        index_columns=index_columns,
                    )
                    log(index_static)
                else:
                    merged_name = "_".join(add_index_fields)
                    merged_columns = ",".join(add_index_fields)
                    index_result_list = check_index_exist_multi(
                        db,
                        table_name=table_name,
                        index_columns=merged_columns,
                        index_number=len(add_index_fields),
                    )
                    if not index_result_list:
                        if row["key"] is None or (
                            isinstance(row["rows"], int) and row["rows"] >= 1
                        ):
                            log(
                                f"建议添加索引：ALTER TABLE {table_name} ADD INDEX idx_{merged_name}({merged_columns});",
                                "warning",
                            )
                    else:
                        log(
                            f"【{table_name}】表 【{merged_columns}】字段，联合索引已经存在，无需添加任何索引。",
                            "success",
                        )
                    log(f"【{table_name}】表 【{merged_columns}】字段，索引分析：")
                    index_static = execute_index_query(
                        db,
                        table_name=table_name,
                        index_columns=merged_columns,
                    )
                    log(index_static)

            # 判断表是否有别名，有别名的情况：
            if table_aliases_exists or contains_dot:
                if table_aliases_exists:
                    table_real_name = table_aliases[table_name]
                else:
                    table_real_name = table_name

                if len(where_fields) != 0:
                    where_matching_fields = []
                    for field in where_fields:
                        if field.startswith(table_real_name + "."):
                            where_matching_fields.append(field.split(".")[-1])
                    # log(f"where_fields: {where_fields}")
                    # log(f"where_matching_fields: {where_matching_fields}")
                    for where_field in where_matching_fields:
                        cardinality = count_column_value(
                            db,
                            table_real_name,
                            where_field,
                            sample_size,
                        )
                        if cardinality:
                            count_value = cardinality[0][1]
                            log(
                                f"取出表 【{table_real_name}】 where条件字段 【{where_field}】 {sample_size} 条记录，重复的数据有：【{count_value}】 条，没有必要为该字段创建索引。"
                            )
                        else:
                            add_index_fields.append(where_field)

                if group_by_fields is not None and len(group_by_fields) != 0:
                    group_matching_fields = []
                    for field in group_by_fields:
                        if field.startswith(table_real_name + "."):
                            group_matching_fields.append(field.split(".")[-1])
                    for group_field in group_matching_fields:
                        cardinality = count_column_value(
                            db,
                            table_real_name,
                            group_field,
                            sample_size,
                        )
                        if cardinality:
                            count_value = cardinality[0][1]
                            log(
                                f"取出表 【{table_real_name}】 group by条件字段 【{group_field}】 {sample_size} 条记录，重复的数据有：【{count_value}】 条，没有必要为该字段创建索引。"
                            )
                        else:
                            add_index_fields.append(group_field)

                if len(order_by_fields) != 0:
                    order_matching_fields = []
                    for field in order_by_fields:
                        if field.startswith(table_real_name + "."):
                            order_matching_fields.append(field.split(".")[-1])
                    for order_field in order_matching_fields:
                        cardinality = count_column_value(
                            db,
                            table_real_name,
                            order_field,
                            sample_size,
                        )
                        if cardinality:
                            count_value = cardinality[0][1]
                            log(
                                f"取出表 【{table_real_name}】 order by条件字段 【{order_field}】 {sample_size} 条记录，重复的数据有：【{count_value}】 条，没有必要为该字段创建索引。"
                            )
                        else:
                            add_index_fields.append(order_field)

                add_index_fields = list(
                    dict.fromkeys(add_index_fields).keys()
                )  # 字段名如果一样，则去重，并确保元素的位置不发生改变

                if len(add_index_fields) == 0:
                    if "index_result" not in globals():
                        log(f"【{table_real_name}】 表，无需添加任何索引。", "success")
                    elif index_result:
                        log(f"【{table_real_name}】 表，无需添加任何索引。", "success")
                    else:
                        pass
                elif len(add_index_fields) == 1:
                    index_name = add_index_fields[0]
                    index_columns = add_index_fields[0]
                    index_result = check_index_exist(
                        db,
                        table_name=table_real_name,
                        index_column=index_columns,
                    )
                    if not index_result:
                        if row["key"] is None or (
                            isinstance(row["rows"], int) and row["rows"] >= 1
                        ):
                            log(
                                f"建议添加索引：ALTER TABLE {table_real_name} ADD INDEX idx_{index_name}({index_columns});",
                                "warning",
                            )
                    else:
                        log(
                            f"【{table_real_name}】表 【{index_columns}】字段，索引已经存在，无需添加任何索引。",
                            "success",
                        )
                    log(f"【{table_real_name}】表 【{index_columns}】字段，索引分析：")
                    index_static = execute_index_query(
                        db,
                        table_name=table_real_name,
                        index_columns=index_columns,
                    )
                    log(index_static)
                else:
                    merged_name = "_".join(add_index_fields)
                    merged_columns = ",".join(add_index_fields)
                    index_result_list = check_index_exist_multi(
                        db,
                        table_name=table_real_name,
                        index_columns=merged_columns,
                        index_number=len(add_index_fields),
                    )
                    if not index_result_list:
                        if row["key"] is None or (
                            isinstance(row["rows"], int) and row["rows"] >= 1
                        ):
                            log(
                                f"建议添加索引：ALTER TABLE {table_real_name} ADD INDEX idx_{merged_name}({merged_columns});",
                                "warning",
                            )
                    else:
                        log(
                            f"【{table_real_name}】表 【{merged_columns}】字段，联合索引已经存在，无需添加任何索引。",
                            "success",
                        )
                    log(f"【{table_real_name}】表 【{merged_columns}】字段，索引分析：")
                    index_static = execute_index_query(
                        db,
                        table_name=table_real_name,
                        index_columns=merged_columns,
                    )
                    log(index_static)

    suggestion(where_clauses)

    return logs
