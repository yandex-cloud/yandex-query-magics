from yandex_query_magic import SqlParser
import pandas as pd
from datetime import datetime
import pytest


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
    assert result == 'select * from AS_TABLE(AsList(AsStruct(1.0 as `_float`,1l as `_int`,DateTime::MakeTimestamp(DateTime::Parse("%Y-%m-%d %H:%M:%S")("2018-03-10 00:00:00.000000")) as `_datetime`,"foo" as `_string`))) as `df`'  # noqa


def test_sqlrender_df_ms():
    test_str = "select * from {{df}}"
    parser = SqlParser()
    dataframe = pd.DataFrame({'_float': [1.0],
                              '_int': [1],
                              '_datetime': [pd.Timestamp(year=2018,
                                                         month=3,
                                                         day=10,
                                                         hour=1,
                                                         minute=12,
                                                         second=6,
                                                         microsecond=12)],
                              '_string': ['foo']})

    result = parser.reformat(test_str, {"df": dataframe})
    assert result == 'select * from AS_TABLE(AsList(AsStruct(1.0 as `_float`,1l as `_int`,DateTime::MakeTimestamp(DateTime::Parse("%Y-%m-%d %H:%M:%S")("2018-03-10 01:12:06.000012")) as `_datetime`,"foo" as `_string`))) as `df`'  # noqa


def test_sqlrender_df_ns():
    with pytest.raises(Exception, match="No support for nanoseconds of Pandas datetime64"):
        test_str = "select * from {{df}}"
        parser = SqlParser()
        dataframe = pd.DataFrame({'_float': [1.0],
                                  '_int': [1],
                                  '_datetime': [pd.Timestamp(year=2018,
                                                             month=3,
                                                             day=10,
                                                             hour=1,
                                                             minute=12,
                                                             second=6,
                                                             microsecond=12,
                                                             nanosecond=1)],
                                  '_string': ['foo']})
        parser.reformat(test_str,{"df":dataframe})


def test_sqlrender_dict():
    test_str = "select DictHasItems({{a}})"
    a = {"a": 1, "b": 2, "c": 3}
    parser = SqlParser()
    formatted = parser.reformat(test_str, {"a": a})
    print(formatted)
    assert formatted == 'select DictHasItems(ToDict(AsList(asTuple("a", 1l),asTuple("b", 2l),asTuple("c", 3l))))'  # noqa


def test_sqlrender_dict_distinct_value():
    with pytest.raises(Exception, match="All value types must be of one type. Found several {<class 'datetime.datetime'>, <class 'str'>, <class 'int'>, <class 'float'>}"):  # noqa
        test_str = "select * from {{a}}"
        a = {"a": 1, "b": 2.0, "c": "test", "d": datetime(2022, 2, 2, 21, 12, 12)}
        parser = SqlParser()
        formatted = parser.reformat(test_str, {"a": a})
        print(formatted)
        assert formatted == 'select * from AS_TABLE(AsList(AsStruct(1l as `a`,2.0 as `b`,"test" as `c`,DateTime::MakeTimestamp(DateTime::Parse("%Y-%m-%d %H:%M:%S")("2022-02-02 21:12:12.000000")) as `d`))) as `a`'  # noqa


def test_sqlrender_dict_distinct_keys():
    with pytest.raises(Exception, match="All key types must be of one type. Found several {<class 'str'>, <class 'datetime.datetime'>}"):  # noqa
        test_str = "select * from {{a}}"
        a = {"a": 1, "b": 1, "c": 1, datetime.now(): 1}
        parser = SqlParser()
        formatted = parser.reformat(test_str, {"a": a})
        print(formatted)
        assert formatted == 'select * from AS_TABLE(AsList(AsStruct(1l as `a`,2.0 as `b`,"test" as `c`,DateTime::MakeTimestamp(DateTime::Parse("%Y-%m-%d %H:%M:%S")("2022-02-02 21:12:12.000000")) as `d`))) as `a`'  # noqa


def test_sqlrender_list():
    test_str = "select 1 in {{a}}"
    a = [1, 2, 3]
    parser = SqlParser()
    result = parser.reformat(test_str, {"a": a})
    assert result == 'select 1 in AsList(1l,2l,3l)'  # noqa


def test_sqlrender_dict_special_symbols():
    test_str = "select * from {{a}}"
    a = {"a": "abc", "b": "a\"a", "c": "a\'c"}
    parser = SqlParser()
    result = parser.reformat(test_str, {"a": a})
    print(result)
    assert result == """select * from ToDict(AsList(asTuple("a", "abc"),asTuple("b", "a\\"a"),asTuple("c", "a'c")))"""  # noqa
