import re
from datetime import datetime
import pandas as pd
from typing import Any


class SqlParser:
    mustache_re = re.compile(r"{{\s*([a-zA-Z_][a-zA-Z0-9_]*)(.*)\s*}}")

    def __init__(self):
        pass

    def reformat(self, sql, ns):
        new_sql = ""
        prev_position = 0

        while True:
            match = SqlParser.mustache_re.search(sql, prev_position)
            if match is None:
                new_sql += sql[prev_position:]
                break

            new_sql += sql[prev_position: match.start()]

            variable = match.group(1)

            if variable not in ns:
                raise Exception(f"{variable} not found as Jupyter variable")
            else:
                var = ns[variable]
                rendered = SqlParser.render_type(var, variable)
                new_sql += rendered

            prev_position = match.end()

        return new_sql

    @staticmethod
    def render_type(value, variable_name):
        if isinstance(value, str):
            return value
        elif isinstance(value, pd.DataFrame):
            if variable_name is None:
                raise Exception("DataFrame type must have a name")

            value = value.copy()
            value.infer_objects()
            return SqlParser.render_dataframe(value) + \
                " as `" + variable_name + "`"
        elif isinstance(value, dict):
            if variable_name is None:
                raise Exception("DataFrame type must have a name")

            return SqlParser.render_dict(value)
        elif isinstance(value, list):
            return SqlParser.render_list(value)

    @staticmethod
    def from_datetime64_ns(value):
        if value.nanosecond != 0:
            raise Exception("No support for nanoseconds of Pandas datetime64")

        value_str = value.strftime("%Y-%m-%d %H:%M:%S.%f")
        formatted = f'DateTime::MakeTimestamp(DateTime::Parse("%Y-%m-%d %H:%M:%S")("{value_str}"))'  # noqa
        return formatted

    @staticmethod
    def from_datetime(value):
        value_str = value.strftime("%Y-%m-%d %H:%M:%S.%f")
        formatted = f'DateTime::MakeTimestamp(DateTime::Parse("%Y-%m-%d %H:%M:%S")("{value_str}"))'  # noqa
        return formatted

    @staticmethod
    def from_str(value):
        value = value.replace('"', '\\"')
        return f"\"{value}\""

    @staticmethod
    def from_int(value):
        return f"{value}l"

    @staticmethod
    def render_value(value: Any) -> str:
        if isinstance(value, str):
            value = SqlParser.from_str(value)
        elif isinstance(value, int):
            value = SqlParser.from_int(value)
        elif isinstance(value, pd._libs.tslibs.timestamps.Timestamp):
            value = SqlParser.from_datetime64_ns(value)
        elif isinstance(value, datetime):
            value = SqlParser.from_datetime(value)
        elif isinstance(value, float):
            pass
        else:
            assert not f"Unsupported  type {type(value)}"

        return value

    @staticmethod
    def render_dict(dict_value: dict) -> str:
        sql = "ToDict(AsList("

        as_dict_cols = []

        key_types = set()
        value_types = set()
        for key, value in dict_value.items():
            key_types.add(type(key))
            value_types.add(type(value))

        if len(key_types) > 1:
            raise Exception(f"All key types must be of one type. Found several {key_types}")

        if len(value_types) > 1:
            raise Exception(f"All value types must be of one type. Found several {value_types}")

        for key, value in dict_value.items():
            key = SqlParser.render_value(key)
            value = SqlParser.render_value(value)

            as_dict_cols.append(f"asTuple({key}, {value})")

        sql += ",".join(as_dict_cols)
        sql += "))"

        return sql

    @staticmethod
    def render_list(list_value: list) -> str:
        sql = "AsList("

        as_list_items = []

        for value in list_value:
            value = SqlParser.render_value(value)

            as_list_items.append(str(value))

        sql += ",".join(as_list_items)
        sql += ")"

        return sql

    @staticmethod
    def render_dataframe(df: pd.DataFrame) -> str:
        sql = "AS_TABLE(AsList("

        columns = []
        for colname, _ in df.dtypes.items():
            columns.append(colname)

        as_structs = []
        for _, row in df.iterrows():
            as_struct_cols = []
            for index, (_, value) in enumerate(row.items()):
                value = SqlParser.render_value(value)

                as_struct_cols.append(f"{value} as `{columns[index]}`")

            as_struct_row = "AsStruct(" + ",".join(as_struct_cols) + ")"
            as_structs.append(as_struct_row)

        as_structs = ",".join(as_structs)
        sql += as_structs
        sql += "))"

        return sql
