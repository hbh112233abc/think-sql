#!/usr/bin/python
# -*- coding: utf-8 -*-

# 作者: YinYi 2023-06-28
# 优化: hbh112233abc@163.com
import re
import copy
from pathlib import Path
from typing import Dict, List

import click
import pymysql
from loguru import logger

log_path = Path.cwd() / "logs"
log_path.mkdir(parents=True, exist_ok=True)
log_file = log_path / f"{Path(__file__).stem}.log"
logger.add(
    log_file,
    filter="",
    rotation="00:00",
    retention="10 days",
    backtrace=True,
    diagnose=True,
)

db_type = "mysql"


class DB:
    def __init__(self, config: dict):
        self.config = config
        self.connect = None
        self.cursor = None

    def __enter__(self):
        self.connect = pymysql.connect(
            host=self.config["host"],
            user=self.config["user"],
            passwd=self.config["password"],
            port=int(self.config["port"]),
            connect_timeout=5,
            db=self.config["database"],
            charset="utf8",
            cursorclass=pymysql.cursors.DictCursor,
        )
        self.cursor = self.connect.cursor()
        return self

    def query(self, sql, params: tuple = ()) -> List[dict]:
        # logger.info(sql)
        result = []
        try:
            self.cursor.execute(sql.strip(), params)
            if self.cursor.rowcount > 0:
                result = self.cursor.fetchall()
        except Exception as e:
            logger.exception(f"Failed to execute query: {e}")
        return result

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_val:
            logger.exception(exc_val)
        if self.cursor:
            self.cursor.close()
        if self.connect:
            self.connect.close()


def parse_db_config(cfg: str) -> dict:
    """
    Parse a database configuration string into a dictionary.

    :param cfg: A string in the format "user:'password'@host:port/database"
    :return: A dictionary with keys 'user', 'password', 'host', 'port', and 'database'
    :raises ValueError: If the input string does not match the expected format

    :return
    {
        "host":"",
        "port":3306,
        "user":"",
        "password":"",
        "database":"",
    }
    """
    pattern = r"(?P<user>.*?):'(?P<password>.*?)'@(?P<host>.*?):(?P<port>\d+)/(?P<database>.*)"
    match = re.match(pattern, cfg)
    if not match:
        raise ValueError(
            "Invalid db config format, expected `user:'password'@host:port/database`"
        )

    return match.groupdict()


def db_schema(db_config: dict, raise_not_tables=True) -> Dict[str, dict]:
    """数据库information_schema数据

    Args:
        db_config (dict): 数据库配置
        raise_not_tables (bool, optional): 未发现table是否抛出异常. Defaults to True.

    Raises:
        Exception: 数据库不存在
        Exception: 没有数据表

    Returns:
        Dict[str, dict]: schema数据 {schemata,tables}
    """
    schema = {
        "schemata": {},
        "tables": {},
    }
    with DB(db_config) as db:
        version_sql = "SELECT version() as version;"
        version = db.query(version_sql)[0]["version"]
        logger.info(f"{db_config['remark']}数据库版本: {version}")

        schema_sql = f"""SELECT * FROM `information_schema`.`SCHEMATA` WHERE `SCHEMA_NAME` = '{db_config["database"]}' """
        result = db.query(schema_sql)
        # 判断 源数据库是不是有表存在
        if not result:
            raise Exception(
                f"{db_config['remark']}数据库 `{db_config['database']}` 不存在。"
            )

        # 取一条记录
        schema["schemata"] = result[0]

        table_sql = f"""SELECT * FROM `information_schema`.`TABLES`
            WHERE `TABLE_SCHEMA` = '{db_config["database"]}'
            ORDER BY `TABLE_NAME` ASC"""
        tables = db.query(table_sql)

        if not tables:
            if raise_not_tables:
                raise Exception(
                    f"{db_config['remark']}数据库 `{db_config['database']}` 没有数据表。"
                )
            return schema

        for table in tables:
            table_name = table["TABLE_NAME"]
            schema["tables"][table_name] = {
                "info": table,
                "columns": [],
                "statistics": [],
                "partitions": [],
            }
            columns_sql = f"""SELECT
                `COLUMN_NAME`,
                `ORDINAL_POSITION`,
                `COLUMN_DEFAULT`,
                `IS_NULLABLE`,
                `DATA_TYPE`,
                `CHARACTER_MAXIMUM_LENGTH`,
                `CHARACTER_OCTET_LENGTH`,
                `NUMERIC_PRECISION`,
                `NUMERIC_SCALE`,
                `DATETIME_PRECISION`,
                `CHARACTER_SET_NAME`,
                `COLLATION_NAME`,
                `COLUMN_TYPE`,
                `EXTRA`,
                `COLUMN_COMMENT`,
                `GENERATION_EXPRESSION`
               FROM `information_schema`.`COLUMNS`
                WHERE `TABLE_SCHEMA` = '{db_config['database']}'
                AND `TABLE_NAME` = '{table_name}'
                ORDER BY `ORDINAL_POSITION` ASC
            """
            columns = db.query(columns_sql)
            if not columns:
                continue
            schema["tables"][table_name]["columns"] = columns

            statistics_sql = f"""SELECT
                `NON_UNIQUE`,
                `INDEX_NAME`,
                `SEQ_IN_INDEX`,
                `COLUMN_NAME`,
                `SUB_PART`,
                `INDEX_TYPE`
               FROM `information_schema`.`STATISTICS`
                WHERE `TABLE_SCHEMA` = '{db_config['database']}'
                AND `TABLE_NAME` = '{table_name}'
            """
            statistics = db.query(statistics_sql)
            schema["tables"][table_name]["statistics"] = statistics

            partitions_sql = f"""SELECT PARTITION_EXPRESSION,count(*) AS PARTITIONS
                FROM `information_schema`.`PARTITIONS`
                WHERE `TABLE_SCHEMA` = '{db_config["database"]}' AND `TABLE_NAME` = '{table_name}'
            """
            partitions = db.query(partitions_sql)
            schema["tables"][table_name]["partitions"] = partitions

    return schema


def make_character(column: dict) -> str:
    """
    根据输入的列信息和数据库类型，生成字符集字符串
    如果CHARACTER_SET_NAME为None，返回空字符串
    如果数据库类型不是'oceanbase'或CHARACTER_SET_NAME不是'binary'，则生成字符集字符串
    """
    global db_type
    if column["CHARACTER_SET_NAME"] is None:
        return ""
    if db_type.lower() != "oceanbase" or column["CHARACTER_SET_NAME"] != "binary":
        return f"""CHARACTER SET {column["CHARACTER_SET_NAME"]} COLLATE {column["COLLATION_NAME"]}"""
    return ""


def make_extra(column: dict) -> str:
    extra = ""
    column_extra = str(column["EXTRA"])
    if not column_extra:
        return extra
    if ("DEFAULT_GENERATED" in column_extra) or (
        "auto_increment" in column_extra.lower()
    ):
        extra = ""
    elif "VIRTUAL GENERATED" in column_extra:
        extra = "GENERATED ALWAYS AS ({}) VIRTUAL".format(
            str(column["GENERATION_EXPRESSION"]).replace("\\", "")
        )
    else:
        extra = "{}".format(column["EXTRA"].upper())

    return extra.strip()


def create_table_sql(src_schema: dict, table_name: str) -> str:
    """
    从源数据库的表结构信息中提取出表的基本信息，列信息，索引信息和分区信息。
        1. 根据列信息生成创建表的SQL语句的列部分。
        2. 根据索引信息生成创建表的SQL语句的索引部分。
        3. 根据分区信息生成创建表的SQL语句的分区部分。
        4. 最后将生成的SQL语句的各部分拼接成完整的SQL语句。
    """
    global db_type
    # CREATE TABLE...
    schemata = src_schema["schemata"]
    table_info = src_schema["tables"][table_name]["info"]
    src_statistics = src_schema["tables"][table_name]["statistics"]
    src_columns = src_schema["tables"][table_name]["columns"]
    if not src_columns:
        return ""

    sql = [
        f"CREATE TABLE IF NOT EXISTS `{table_name}` (",
    ]

    # COLUMN...
    for column in src_columns:
        null_able = get_column_default(column)
        dot = ""
        character = make_character(column)
        extra = make_extra(column)

        # 分割符
        if column != src_columns[-1] or len(src_statistics) > 0:
            dot = ","

        comment = (
            f"""COMMENT '{column["COLUMN_COMMENT"]}'"""
            if column["COLUMN_COMMENT"]
            else ""
        )

        params = [
            f'  `{column["COLUMN_NAME"]}`',
            column["COLUMN_TYPE"],
            character,
            null_able,
            extra,
            comment,
            dot,
        ]

        sql.append(" ".join(filter(lambda x: x, params)))

    # KEY...
    table_keys = []
    src_statistics_data = src_schema["tables"][table_name]["statistics"]
    if src_statistics_data:
        statistics_dict = {}

        for statistic in src_statistics_data:
            if statistic["INDEX_NAME"] in statistics_dict:
                # 如果源端表的index 已经写入statistics_dict 我们就update，反之则 写入dict
                # 创建一个index_name 的key,去序号 SEQ_IN_INDEX 的值 作为index_name的值
                statistics_dict[statistic["INDEX_NAME"]].update(
                    {statistic["SEQ_IN_INDEX"]: statistic}
                )
            else:
                # 创建一个index_name 的key,去序号 SEQ_IN_INDEX 的值 作为index_name的值
                statistics_dict[statistic["INDEX_NAME"]] = {
                    statistic["SEQ_IN_INDEX"]: statistic
                }
        # 拿出来
        for index_name, statistic in statistics_dict.items():
            table_keys.append(
                "  {key_slot}".format(key_slot=get_add_keys(index_name, statistic))
            )

    sql.append(",\n".join(table_keys))

    # 判断是否有分区
    partitions_data = src_schema["tables"][table_name]["partitions"]

    if (
        partitions_data[0]["PARTITIONS"] != 0
        and partitions_data[0]["PARTITION_EXPRESSION"] is not None
    ):
        partition_keyname = partitions_data[0]["PARTITION_EXPRESSION"]
        partition_cnt = partitions_data[0]["PARTITIONS"]
        # partition_strcom = '/*!50100 PARTITION BY KEY (order_sn) PARTITIONS 128 */'
        partition_strcom = "/*!50100 PARTITION BY KEY ({}) \n PARTITIONS {} */".format(
            partition_keyname, partition_cnt
        )

        table_end = ")  DEFAULT CHARSET={charset} COMMENT '{scomment}' \n {partions_com};".format(
            charset=schemata["DEFAULT_CHARACTER_SET_NAME"],
            scomment=table_info["TABLE_COMMENT"],
            partions_com=partition_strcom,
        )
    else:
        table_end = (
            ") ENGINE={engine} DEFAULT CHARSET={charset} COMMENT '{scomment}' ;".format(
                engine=table_info["ENGINE"],
                charset=schemata["DEFAULT_CHARACTER_SET_NAME"],
                scomment=table_info["TABLE_COMMENT"],
            )
        )

    if db_type.lower() == "oceanbase":
        table_end = table_end.replace(
            "ENGINE={engine} ".format(engine=table_info["ENGINE"]), ""
        )

    sql.append(table_end)

    return "\n".join(sql)


def alter_column_sql(event: str, column: dict, columns_pos: dict) -> str:
    global db_type
    # 拿到列是否允许为空，列的默认值
    null_able = get_column_default(column)
    character = make_character(column)
    extra = make_extra(column)

    after = get_column_after(
        column["ORDINAL_POSITION"],
        columns_pos,
    )
    comment = (
        f"""COMMENT '{column["COLUMN_COMMENT"]}'""" if column["COLUMN_COMMENT"] else ""
    )

    params = [
        f"""  {event.upper()} COLUMN `{column["COLUMN_NAME"]}`""",
        column["COLUMN_TYPE"],
        character,
        null_able,
        extra,
        comment,
        after,
    ]

    sql = " ".join(filter(lambda x: x, params))

    if db_type.lower() == "oceanbase" and "NOT NULL" in sql:
        sql = sql.replace(" NOT NULL", "")
        sql += " NOT NULL"

    return sql


def fix_column_type(column: dict) -> dict:
    #  int  bigint  smallint  这几个宽度不用对比
    pattern = r"\(\d+\)"
    type_prefix = column["COLUMN_TYPE"].split("(")[0]
    column_type_fixed = re.sub(pattern, "", column["COLUMN_TYPE"])
    nc = copy.deepcopy(column)
    if type_prefix in ("tinyint", "int", "smallint", "bigint"):
        nc["COLUMN_TYPE"] = column_type_fixed
    if type_prefix in ("datetime", "timestamp"):
        nc["EXTRA"] = ""

    return nc


def make_alter_columns(src_columns: List[dict], dst_columns: List[dict]) -> List[str]:
    sql = []
    if not src_columns or not dst_columns:
        return sql

    # 源端表列
    columns_src = {}
    # 目的端表列
    columns_dst = {}
    # 源端表列的position
    columns_pos_src = {}
    # 目的端表列的position
    columns_pos_dst = {}

    for src_column_data in src_columns:
        # 使用表中的列做一个源端表的一个dict
        columns_src[src_column_data["COLUMN_NAME"]] = src_column_data
        # 使用表中列的position 序列号做一个源端表的一个dict
        columns_pos_src[src_column_data["ORDINAL_POSITION"]] = fix_column_type(
            src_column_data
        )

    for dst_column_data in dst_columns:
        # 使用表中的列做一个源端表的一个dict
        columns_dst[dst_column_data["COLUMN_NAME"]] = dst_column_data
        # 使用表中列的position 序列号做一个源端表的一个dict
        columns_pos_dst[dst_column_data["ORDINAL_POSITION"]] = fix_column_type(
            dst_column_data
        )

    # 如果源端中表的position号，与目的端的position不相等，执行源表alter 操作
    if columns_pos_src == columns_pos_dst:
        return sql

    # DROP COLUMN
    for column_name, column_dst in columns_dst.items():
        # 判断目的端表的列是否在源端表的列中，如果不存在就drop column
        if column_name not in columns_src:
            # 判断key的顺序
            # 调整column position 顺序
            # status = 3 表示drop column
            reset_calc_position(
                column_name,
                column_dst["ORDINAL_POSITION"],
                columns_dst,
                3,
            )
            logger.info(f"drop column reset position:{column_name}")
            sql.append("  DROP COLUMN `%s`" % column_name)

    # ADD COLUMN
    for column_name, column_src in columns_src.items():
        # 判断本地列是否在目的端，如果不存在就add column
        if column_name in columns_dst:
            continue

        # 重新计算字段位置
        # status = 1 表示新增列 ，add column
        reset_calc_position(
            column_name,
            column_src["ORDINAL_POSITION"],
            columns_dst,
            1,
        )
        logger.info(f"add column reset position:{column_name}")
        sql.append(alter_column_sql("ADD", column_src, columns_pos_src))

    # MODIFY COLUMN   没有change 因为是根据position 来识别列的 modify add drop，没有办法判断是change column A AB
    for column_name, column_src in columns_src.items():
        if column_name not in columns_dst:
            continue
        if fix_column_type(column_src) == fix_column_type(columns_dst[column_name]):
            continue
        logger.info("+" * 20 + "column no eq" + "+" * 20)
        logger.info(f"src:")
        logger.info(column_src)
        logger.info(f"src fixed:")
        logger.info(fix_column_type(column_src))
        logger.info(f"dst:")
        logger.info(columns_dst[column_name])
        logger.info(f"dst fixed:")
        logger.info(fix_column_type(columns_dst[column_name]))
        logger.info("=" * 50)

        # 重新计算字段位置
        reset_calc_position(
            column_name,
            column_src["ORDINAL_POSITION"],
            columns_dst,
            2,
        )
        logger.info(f"modify column reset position:{column_name}")
        sql.append(alter_column_sql("MODIFY", column_src, columns_pos_src))
    return sql


def make_alter_keys(src_statistics, dst_statistics) -> List[str]:
    # 'NON_UNIQUE': 0 代表为unique key
    # 'NON_UNIQUE': 1 代表为二级index

    if not src_statistics:
        return []

    # 本地index
    statistics_src = {}
    # 目的端index
    statistics_dst = {}

    # 生存源端所有INDEX_NAME 为key的dict 数据
    for src_statistic in src_statistics:
        if src_statistic["INDEX_NAME"] in statistics_src:
            # 如果 索引名存在statistics_src中，就去修改INDEX_NAME中的 SEQ_IN_INDEX 序号对应的值，对应本地相同序号的index 信息
            statistics_src[src_statistic["INDEX_NAME"]].update(
                {src_statistic["SEQ_IN_INDEX"]: src_statistic}
            )
        else:
            # 如果索引名不存在statistics_src 中，就使用INDEX_NAME 创建一个dict src_statistic['SEQ_IN_INDEX']:
            statistics_src[src_statistic["INDEX_NAME"]] = {
                src_statistic["SEQ_IN_INDEX"]: src_statistic
            }
    # 生存目的端所有INDEX_NAME 为key的dict 数据
    for dst_statistic in dst_statistics:
        if dst_statistic["INDEX_NAME"] in statistics_dst:
            # 如果 索引名存在statistics_dst中，就去修改INDEX_NAME中的 SEQ_IN_INDEX 序号对应的值，对应本地相同序号的index 信息
            statistics_dst[dst_statistic["INDEX_NAME"]].update(
                {dst_statistic["SEQ_IN_INDEX"]: dst_statistic}
            )
        else:
            # 如果索引名不存在statistics_dst 中，就使用INDEX_NAME 创建一个dict src_statistic_data['SEQ_IN_INDEX']:
            statistics_dst[dst_statistic["INDEX_NAME"]] = {
                dst_statistic["SEQ_IN_INDEX"]: dst_statistic
            }
    # 开始拼接语句
    # 当本地的index dict 不等于 目的段的 index dict 开始拼接alter index 语句
    if statistics_src == statistics_dst:
        return []

    sql = []

    for index_name in set(statistics_dst) - set(statistics_src):
        # 如果目的的index 不在 源端表中，就dorp index
        if "PRIMARY" == index_name:
            sql.append("  DROP PRIMARY KEY")
        else:
            sql.append(f"  DROP INDEX `{index_name}`")

    for index_name, statistic_src in statistics_src.items():
        # 判断本地index 是否在目的端，如果不在 add index
        if index_name in statistics_dst:
            # DROP INDEX ... AND ADD KEY ...
            # 循环拿到本地的 INDEX_NAME 的值 与目的端 INDEX_NAME 比对，如果不一致
            if statistic_src != statistics_dst[index_name]:
                if "PRIMARY" == index_name:
                    sql.append("  DROP PRIMARY KEY")
                else:
                    sql.append("  DROP INDEX `%s`" % index_name)
                # 如果本地在index_name 在目的端存在，但是values不相等，就先drop index or drop primary key
                # 这里在 add index or add primary key
                sql.append("  ADD %s" % get_add_keys(index_name, statistic_src))
        else:
            # ADD KEY
            sql.append("  ADD %s" % get_add_keys(index_name, statistic_src))

    return sql


def alter_table_sql(src_schema: dict, dst_schema: dict, table_name: str) -> str:
    # 比对源库中的表与目的端中同名的表的列差异
    # ALTER TABLE
    # src
    src_table = src_schema["tables"][table_name]
    dst_table = dst_schema["tables"][table_name]
    src_columns = src_table["columns"]
    dst_columns = dst_schema["tables"][table_name]["columns"]

    # ALTER LIST...
    sql = []

    alter_keys = []

    # 表注释修改
    if src_table["info"]["TABLE_COMMENT"] != dst_table["info"]["TABLE_COMMENT"]:
        sql.append(
            "ALTER TABLE `{}` COMMENT '{}' ;".format(
                table_name,
                src_table["info"]["TABLE_COMMENT"],
            )
        )

    # 判断 原表 是不是字段 比目的表多
    alter_columns = make_alter_columns(src_columns, dst_columns)
    # INDEX 数据
    alter_keys = make_alter_keys(src_table["statistics"], dst_table["statistics"])

    if alter_keys:
        alter_columns.extend(alter_keys)

    if alter_columns:
        sql.append("ALTER TABLE `%s`" % table_name)
        for alter_column in alter_columns:
            if alter_column == alter_columns[-1]:
                column_dot = ";"
            else:
                column_dot = ","

            sql.append(f"{alter_column}{column_dot}")

    return "\n".join(sql)


# 处理default vls 和 处理 是否允许为空
def get_column_default(column: dict) -> str:
    def get_default_value(column: dict) -> str:
        if column["DATA_TYPE"] in ("timestamp", "datetime"):
            if column["COLUMN_DEFAULT"].upper() == "CURRENT_TIMESTAMP":
                if "DEFAULT_GENERATED" in column["EXTRA"]:
                    extra_c = column["EXTRA"].replace("DEFAULT_GENERATED", "")
                    return f"DEFAULT {column['COLUMN_DEFAULT']} {extra_c.upper()}"
                else:
                    return f"DEFAULT {column['COLUMN_DEFAULT']}"
            else:
                return f"DEFAULT '{column['COLUMN_DEFAULT']}'"
        elif column["DATA_TYPE"] == "json":
            dutfmb4 = (
                str(column["COLUMN_DEFAULT"]).replace("\\", "")
                if "_utf8mb4" in column["COLUMN_DEFAULT"]
                else column["COLUMN_DEFAULT"]
            )
            return f"DEFAULT ({dutfmb4})"
        else:
            return f"DEFAULT '{column['COLUMN_DEFAULT']}'"

    def get_no_default_value(column: dict) -> str:
        if column["DATA_TYPE"] in ("timestamp", "datetime"):
            return "DEFAULT NULL"
        elif "VIRTUAL GENERATED" in column["EXTRA"] or column["DATA_TYPE"] == "text":
            return ""
        else:
            return "DEFAULT NULL"

    if column["IS_NULLABLE"] == "NO":
        if column["COLUMN_DEFAULT"] is not None:
            return f"NOT NULL {get_default_value(column)}"
        else:
            return (
                "NOT NULL AUTO_INCREMENT"
                if "auto_increment" in column["EXTRA"].lower()
                else "NOT NULL"
            )
    else:
        if column["COLUMN_DEFAULT"] is not None:
            return get_default_value(column)
        else:
            return get_no_default_value(column)


def get_column_after(ordinal_position, column_pos):
    pos = ordinal_position - 1

    if pos in column_pos:
        return "AFTER `%s`" % column_pos[pos]["COLUMN_NAME"]
    else:
        return "FIRST"


def get_columns_name(statistic):
    columns_name = []
    for k in sorted(statistic):
        sub_part = ""
        if statistic[k]["SUB_PART"] is not None:
            sub_part = "(%d)" % statistic[k]["SUB_PART"]
        columns_name.append(
            "`{column_name}`{sub_part}".format(
                column_name=statistic[k]["COLUMN_NAME"], sub_part=sub_part
            )
        )
    return columns_name


def get_add_keys(index_name, statistic):
    non_unique = statistic[1]["NON_UNIQUE"]
    columns_name = get_columns_name(statistic)
    if 1 == non_unique:
        return "KEY `{index_name}` ({columns_name})".format(
            index_name=index_name, columns_name=",".join(columns_name)
        )
    elif "PRIMARY" == index_name:
        return "PRIMARY KEY ({columns_name})".format(
            columns_name=",".join(columns_name)
        )
    else:
        return "UNIQUE KEY `{index_name}` ({columns_name})".format(
            index_name=index_name, columns_name=",".join(columns_name)
        )


def reset_calc_position(
    column_name: str, local_pos: int, columns_dst: dict, status: int
) -> dict:
    """
    Adjust the "ORDINAL_POSITION" of columns in a dictionary based on the status.

    :param column_name: The name of the column to be modified.
    :param local_pos: The local position of the column.
    :param columns_dst: The dictionary of columns.
    :param status: The status of the operation.
    :return: The modified dictionary of columns.
    """
    if status in [1, 3]:
        # ADD or DROP
        adjustment = 1 if status == 1 else -1
        for k, v in columns_dst.items():
            cur_pos = v["ORDINAL_POSITION"]
            if cur_pos >= local_pos:
                columns_dst[k]["ORDINAL_POSITION"] += adjustment
    elif status == 2:
        # MODIFY
        if column_name in columns_dst:
            columns_dst[column_name]["ORDINAL_POSITION"] = local_pos

    return columns_dst


def save_sql(sql_list: List[str]):
    with open(log_path / "update.sql", "w", encoding="utf-8") as f:
        for sql in sql_list:
            # print(sql)
            print(sql.encode("utf-8").decode("utf-8", "ignore"))
            f.write(sql)
            f.write("\n")


@click.command()
@click.option(
    "--type", required=True, help="指定数据库类型。(格式: mysql oceanbase polardb 等)"
)
@click.option(
    "--src",
    required=True,
    help='指定源服务器。(格式: "<user>:<password>@<host>:<port>/<database>")"',
)
@click.option(
    "--dst",
    required=False,
    help='指定目标服务器。(格式: "<user>:<password>@<host>:<port>/<database>")',
)
@click.option(
    "--save", type=bool, default=True, help="是否保存sql,默认:True,保存为update.sql"
)
@click.help_option(
    "--help",
    help="""示例: python my_diff.py --type mysql --src "root:'root'@127.0.0.1:3306/buildadmin" --dst "root:'xmhymake'@192.168.102.154:3306/efile_archive" """,
)
def diff(type: str, src: str, dst: str = "", save: bool = True) -> List[str]:
    """SQL差异 工具 支持mysql 8.0 json, 支持mysql8 虚拟列, 支持阿里云polardb，待支持OceanBase"""
    if type.lower() not in ("mysql", "polardb", "oceanbase"):
        return logger.error(
            f"不支持该数据库:{type}\n该工具仅支持mysql polardb oceanbase"
        )

    global db_type
    db_type = type.lower()

    if not dst:
        dst = src

    try:
        src_cfg = parse_db_config(src)
        src_cfg.update({"remark": "source"})
        dst_cfg = parse_db_config(dst)
        dst_cfg.update({"remark": "target"})

        src_schema = db_schema(src_cfg)
        dst_schema = db_schema(dst_cfg, True)

        diff_sql = []

        # 如果目的段为空就初始化数据库中的表，create table
        if not dst_schema["tables"]:
            for table_name in src_schema["tables"]:
                sql = create_table_sql(src_schema, table_name)
                if sql:
                    diff_sql.append(sql)
        else:
            # 如果不为空走正常 create table，drop table，modify table

            # DROP TABLE...
            for dst_table_name in dst_schema["tables"]:
                # 如果目的的表名不在 源库中 就把目的库的表名 删掉
                # 源库不存在的表在目的库存在，就给目的端的该表删除
                if dst_table_name not in src_schema["tables"]:
                    diff_sql.append(f"DROP TABLE IF EXISTS `{dst_table_name}`;")

            for src_table_name in src_schema["tables"]:
                if src_table_name not in dst_schema["tables"]:
                    sql = create_table_sql(src_schema, src_table_name)
                else:
                    sql = alter_table_sql(src_schema, dst_schema, src_table_name)
                if sql:
                    diff_sql.append(sql)

            # 如果给代码使用函数拿掉工具传入的参数一下
        # 如果是作为工具使用可以试下下面，因为直接吐list 工具无法识别，处理麻烦
        if save:
            save_sql(diff_sql)
        else:
            for sql in diff_sql:
                print(sql)
        return diff_sql
    except Exception as e:
        logger.exception(e)


if __name__ == "__main__":
    diff()

# python -m think_sql.mysql.diff.py --type mysql --src "root:'root'@127.0.0.1:3306/buildadmin" --dst "root:'xmhymake'@192.168.102.154:3306/efile_archive"
# python -m think_sql.mysql.diff.py --type mysql --src "root:'xmhymake'@192.168.102.154:3306/efile_archive" --dst "root:'root'@127.0.0.1:3306/buildadmin"
