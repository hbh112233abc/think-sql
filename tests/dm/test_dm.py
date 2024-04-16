#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'hbh112233abc@163.com'

import dmPython

def test_dm():
    conn=dmPython.connect(user='SYSDBA',password='SYSDBA',server= '127.0.0.1',port=5236)
    cursor = conn.cursor()
    cursor.execute('select username from dba_users')
    values = cursor.fetchall()
    print(values)
    assert len(values) > 0
    cursor.close()
    conn.close()
