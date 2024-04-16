#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'hbh112233abc@163.com'


import pytest
from think_sql.dm.util import parse_key, parse_value, parse_where

def test_parse_key():
    # 测试正常情况
    assert parse_key("a") == "\"a\""
    assert parse_key("a as alias") == "\"a\" AS \"alias\""
    assert parse_key("json_data->field") == '"json_data"."field"'
    assert parse_key("table.field") == '"table"."field"'
    assert parse_key("table.field", strict=True) == '"table"."field"'

    # 测试异常情况
    with pytest.raises(Exception):
        parse_key("field alias", strict=True)
    with pytest.raises(Exception):
        parse_key("field(expr)", strict=True)

def test_parse_value():
    assert parse_value("a") == "a"
    assert parse_value("'a'") == "''a''"
    assert parse_value("a'b") == "a''b"
    assert parse_value("a\\b") == "a\\b"
    assert parse_value(123) == 123
    assert parse_value(1.23) == 1.23
    assert parse_value(None) is None

def test_parse_where():
    for x in ("eq","="):
        s,v = parse_where("a", x, "b")
        assert s == " AND \"a\" = '%s'"
        assert v == ("b",)
    for x in ("neq","<>","!="):
        s,v = parse_where("a", x, "b")
        assert s == " AND \"a\" <> '%s'"
        assert v == ("b",)
    for x in ("gt",">"):
        s,v = parse_where("a", x, "b")
        assert s == " AND \"a\" > '%s'"
        assert v == ("b",)
    for x in ("lt","<"):
        s,v = parse_where("a", x, "b")
        assert s == " AND \"a\" < '%s'"
        assert v == ("b",)
    for x in ("egt",">="):
        s,v = parse_where("a", x, "b")
        assert s == " AND \"a\" >= '%s'"
        assert v == ("b",)
    for x in ("elt","<="):
        s,v = parse_where("a", x, "b")
        assert s == " AND \"a\" <= '%s'"
        assert v == ("b",)

    for x in ('in','not in'):
        s,v = parse_where("a", x, "b")
        assert s == f" AND \"a\" {x} ('%s')"
        assert v == ("b",)
        s,v = parse_where("a", x, "b,c")
        assert s == f" AND \"a\" {x} ('%s','%s')"
        assert v == ("b","c")
        s,v = parse_where("a", x, "(b,c)")
        assert s == f" AND \"a\" {x} ('%s','%s')"
        assert v == ("b","c")
        s,v = parse_where("a", x, ["b","c"])
        assert s == f" AND \"a\" {x} ('%s','%s')"
        assert v == ("b","c")
        s,v = parse_where("a", x, [1,2])
        assert s == f" AND \"a\" {x} ('%s','%s')"
        assert v == (1,2)


    for x in ('between','not between'):
        s,v = parse_where("a", x, "b,c")
        assert s == f" AND \"a\" {x} 'b' AND 'c'"
        assert v == tuple()
        s,v = parse_where("a", x, "b and c")
        assert s == f" AND \"a\" {x} b and c"
        assert v == tuple()
        s,v = parse_where("a", x, "b AND c")
        assert s == f" AND \"a\" {x} b AND c"
        assert v == tuple()
        s,v = parse_where("a", x, [1,2])
        assert s == f" AND \"a\" {x} '1' AND '2'"
        assert v == tuple()

        with pytest.raises(ValueError):
            s,v = parse_where("a", x, [1,2,3])

        with pytest.raises(ValueError):
            s,v = parse_where("a", x, [1])

        with pytest.raises(ValueError):
            s,v = parse_where("a", x, "b,c,d")

        with pytest.raises(ValueError):
            s,v = parse_where("a", x, "")

        with pytest.raises(ValueError):
            s,v = parse_where("a", x, 11)

    for x in ('like','not like'):
        s,v = parse_where("a", x, "%b")
        assert s == f" AND \"a\" {x} '%s'"
        assert v == ("%b",)

    s,v = parse_where("a", "is", "b")
    assert s == " AND \"a\" is '%s'"
    assert v == ("b",)
    s,v = parse_where("a", "null")
    assert s == " AND \"a\" is null"
    assert v == tuple()
    s,v = parse_where("a", "is null")
    assert s == " AND \"a\" is null"
    assert v == tuple()
    s,v = parse_where("a", "null","b")
    assert s == " AND \"a\" is null"
    assert v == tuple()
    s,v = parse_where("a", "not null")
    assert s == " AND \"a\" is not null"
    assert v == tuple()
    s,v = parse_where("a", "not null","b")
    assert s == " AND \"a\" is not null"
    assert v == tuple()
    if x in ("exists", "not exists"):
        s,v = parse_where("a", x)
        assert s == f"{x}(\"a\")"
        assert v == tuple()
        s,v = parse_where("a", x, "b")
        assert s == f"{x}(\"a\")"
        assert v == tuple()


    s,v = parse_where("a", "exp", "b")
    assert s == " AND \"a\" b"
    assert v == tuple()
    s,v = parse_where("sql", "")
    assert s == " AND sql"
    assert v == tuple()
    s,v = parse_where("sql")
    assert s == " AND sql"
    assert v == tuple()
    s,v = parse_where("a","b")
    assert s == " AND \"a\" = '%s'"
    assert v == tuple("b")
    with pytest.raises(Exception):
        parse_where("a", "x", "b")
