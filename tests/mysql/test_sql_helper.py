#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = "hbh112233abc@163.com"

from think_sql.mysql.db import DB
from think_sql.mysql.sql_helper import *


def test_sql_helper():
    """
    测试sql_helper
    :return:
    """
    db_dsn = "root:'xmhymake'@192.168.102.154:3306/cab100001"
    with DB(db_dsn) as db:
        sql = "select * from hy_cabrecs where finished_count > 0"
        result = help(db, sql)
        assert len(result) > 0


def test_parse_where_condition():
    sql = """
    SELECT `sno`,`cid`,`fid`,`fileid`,`ownermemid`,`mdid`,`fsize`,`title`,`sn`,`fext`,`is_finished`,`archive_tag`,`respo`,`file_type`,`report_date`,`page_count`,`etime`,`c_hide` AS `hide` FROM `hy_files` `a` WHERE  `a`.`fcabccode` = 'F8A007D15D94CA3A4B1E325C321AEB3A'  AND `a`.`c_hide` >= '0'  AND `a`.`fileid` = '0b0f13b4f0ade4eea00fee2841a3e6d3' ORDER BY `sn` DESC LIMIT 0,20
    """
    res = parse_where_condition(sql)
    assert isinstance(res, dict)
