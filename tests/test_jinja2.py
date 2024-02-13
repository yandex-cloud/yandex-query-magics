from yandex_query_magic import JinjaTemplate
import pandas as pd


def test_simple_render():
    sql = "select * from {{var}}"

    rendered = JinjaTemplate.apply_template(sql, {"var": 1})
    assert rendered == "select * from 1"


def test_render_dict_as_yql():
    sql = "select * from {{dct|to_yq(name='dct')}}"

    rendered = JinjaTemplate.apply_template(sql, {"dct": {"a": "1", "b": "2"}})
    assert rendered == 'select * from ToDict(AsList(asTuple("a", "1"),asTuple("b", "2")))'


def test_render_list_as_yql():
    sql = "select * from {{lst|to_yq(name='lst')}}"

    rendered = JinjaTemplate.apply_template(sql, {"lst": [1, 2, 3]})
    assert rendered == 'select * from AsList(1l,2l,3l)'


def test_render_list_as_yql_noname():
    sql = "select * from {{lst|to_yq()}}"

    rendered = JinjaTemplate.apply_template(sql, {"lst": [1, 2, 3]})
    assert rendered == 'select * from AsList(1l,2l,3l)'


def test_render_df_as_yql():
    sql = "select * from {{df|to_yq(name='df')}}"

    df = pd.DataFrame({'_float': [1.0],
                       '_int': [1],
                       '_datetime': [pd.Timestamp('20180310')],
                       '_string': ['foo']})

    rendered = JinjaTemplate.apply_template(sql, {"df": df})
    assert rendered == 'select * from AS_TABLE(AsList(AsStruct(1.0 as `_float`,1l as `_int`,DateTime::MakeTimestamp(DateTime::Parse("%Y-%m-%d %H:%M:%S")("2018-03-10 00:00:00.000000")) as `_datetime`,"foo" as `_string`))) as `df`'  # noqa


def test_render_dict_as_yql_jinja():
    sql = """
    {% for item in items %}
        select {{item|to_yq(name='dct'+loop.index|string)}};
    {% endfor %}
        """

    rendered = JinjaTemplate.apply_template(sql, {
        "items":
            [
                {"a": 1, "b": 2},
                {"b": 2, "c": 3}
            ]
    }).strip()
    required = """select ToDict(AsList(asTuple("a", 1l),asTuple("b", 2l)));\n    \n        select ToDict(AsList(asTuple("b", 2l),asTuple("c", 3l)));"""
    assert rendered == required  # noqa
