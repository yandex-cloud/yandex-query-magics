from yandex_query_magic import SqlParser
import pandas as pd
from datetime import datetime


def test_sqlrender_noop():
    test_str = "select * from a"
    parser = SqlParser()
    assert parser.reformat("select * from a", {}) == test_str


def test_sqlrender_simple():
    test_str = "select * from {{a}}"
    parser = SqlParser()
    assert parser.reformat(test_str, {"a": "T"}) == "select * from T"


def test_sqlrender_df():
    test_str = "select * from {{df}}"
    parser = SqlParser()
    dataframe = pd.DataFrame({'_float': [1.0],
                              '_int': [1],
                              '_datetime': [pd.Timestamp('20180310')],
                              '_string': ['foo']})

    result = parser.reformat(test_str, {"df": dataframe})

    assert result == 'select * from AS_TABLE(AsList(AsStruct(1.0 as `_float`,1 as `_int`,DateTime::MakeDatetime(DateTime::Parse("%Y-%m-%d %H:%M:%S")("2018-03-10 00:00:00")) as `_datetime`,"foo" as `_string`))) as `df`'  # noqa


def test_sqlrender_dict():
    test_str = "select * from {{a}}"
    a = {"a": 1, "b": 2.0, "c": "test", "d": datetime(2022, 2, 2, 21, 12, 12)}
    parser = SqlParser()
    assert parser.reformat(test_str, {"a": a}) == 'select * from AS_TABLE(AsList(AsStruct(1 as `a`,2.0 as `b`,"test" as `c`,DateTime::MakeDatetime(DateTime::Parse("%Y-%m-%d %H:%M:%S")("2022-02-02 21:12:12")) as `d`))) as `a`'  # noqa


def test_sqlrender_list():
    test_str = "select 1 in {{a}}"
    a = [1, 2, 3]
    parser = SqlParser()
    result = parser.reformat(test_str, {"a": a})
    assert result == 'select 1 in AsList(1,2,3) as `a`'  # noqa


def test_sqlrender_dict_special_symbols():
    test_str = "select * from {{a}}"
    a = {"a": "abc", "b": "a\"a", "c": "a\'c"}
    parser = SqlParser()
    result = parser.reformat(test_str, {"a": a})
    print(result)

    assert result == """select * from AS_TABLE(AsList(AsStruct("abc" as `a`,"a\\"a" as `b`,"a'c" as `c`))) as `a`"""  # noqa

