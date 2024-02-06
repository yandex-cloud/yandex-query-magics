import pandas as pd
from datetime import datetime, timezone, timedelta
from yandex_query_magic import YandexQueryResults


def test_int32():
    data = [{'columns': [{'name': 'column0', 'type': 'Int32'}], 'rows': [[1]]}]
    parsed = YandexQueryResults(data).to_dataframe()
    assert parsed.equals(pd.DataFrame(columns=["column0"], data=[1]))


def test_int16_optional():
    data = [{'rows': [[[1]]], 'columns': [{'name': 'column0',
                                          'type': 'Optional<Uint16>'}]}]
    parsed = YandexQueryResults(data)
    assert parsed.to_dataframe().equals(pd.DataFrame(columns=["column0"], data=[1]))  # noqa


def test_uint32_optional():
    data = [{'rows': [[[1]]], 'columns': [{'name': 'column0',
                                          'type': 'Optional<Uint32>'}]}]
    parsed = YandexQueryResults(data).to_dataframe()
    assert parsed.equals(pd.DataFrame(columns=["column0"], data=[1]))


def test_uint32_none():
    data = [{'rows': [[[]]], 'columns': [{'name': 'column0',
                                         'type': 'Optional<Uint32>'}]}]
    parsed = YandexQueryResults(data).to_dataframe()
    assert parsed.equals(pd.DataFrame(columns=["column0"], data=[None]))


def test_utf8_optional():
    data = [{'rows': [[['a']]], 'columns': [{'name': 'column0',
                                             'type': 'Optional<Utf8>'}]}]
    parsed = YandexQueryResults(data).to_dataframe()
    assert parsed.equals(pd.DataFrame(columns=["column0"], data=['a']))


def test_date_optional():
    data = [{'rows': [[['1970-01-02']]], 'columns': [{'name': 'column0',
                                                      'type': 'Optional<Date>'}]}]  # noqa

    parsed = YandexQueryResults(data).to_dataframe()
    assert parsed.equals(pd.DataFrame(columns=["column0"],
                                      data=[datetime(1970, 1, 2)]))


def test_date():
    data = [{'rows': [['2020-01-01']], 'columns': [{'name': 'column0',
                                                    'type': 'Date'}]}]

    parsed = YandexQueryResults(data).to_dataframe()
    assert parsed.equals(pd.DataFrame(columns=["column0"],
                                      data=[datetime(2020, 1, 1)]))


def test_timestamp_optional():
    data = [{'rows': [[['2019-09-16T00:00:00Z']]], 'columns': [
        {'name': 'column0',
         'type': 'Optional<Timestamp>'}]}]

    parsed = YandexQueryResults(data).to_dataframe()
    assert parsed.equals(pd.DataFrame(columns=["column0"],
                                      data=[datetime(2019, 9, 16,
                                                     tzinfo=timezone(
                                                         offset=timedelta(seconds=0)))]))  # noqa
