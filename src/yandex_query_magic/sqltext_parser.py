import re
from datetime import datetime
import pandas as pd


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
            value = value.copy()
            value.infer_objects()
            return SqlParser.render_dataframe(value) + \
                " as `" + variable_name + "`"
        elif isinstance(value, dict):
            return SqlParser.render_dict(value) + " as `" + variable_name + "`"
        elif isinstance(value, list):
            return SqlParser.render_list(value) + " as `" + variable_name + "`"

    @staticmethod
    def from_datetime64_ns(value):
        formatted = f'DateTime::MakeDatetime(DateTime::Parse("%Y-%m-%d %H:%M:%S")("{value}"))'  # noqa
        return formatted

    @staticmethod
    def from_datetime(value):
        value_str = value.strftime("%Y-%m-%d %H:%M:%S")
        formatted = f'DateTime::MakeDatetime(DateTime::Parse("%Y-%m-%d %H:%M:%S")("{value_str}"))'  # noqa
        return formatted

    @staticmethod
    def from_str(value):
        value = value.replace('"', '\\"')
        return f"\"{value}\""

    @staticmethod
    def render_dict(dict_value: dict) -> str:
        sql = "AS_TABLE(AsList(AsStruct("

        as_struct_cols = []

        for key, value in dict_value.items():
            if isinstance(value, str):
                value = SqlParser.from_str(value)
            elif isinstance(value, datetime):
                value = SqlParser.from_datetime(value)
            elif isinstance(value, float) or isinstance(value, int):
                pass
            else:
                assert not f"Unsupported  type {type(value)}"

            as_struct_cols.append(f"{value} as `{key}`")

        sql += ",".join(as_struct_cols)
        sql += ")))"

        return sql


    @staticmethod
    def render_list(list_value: list) -> str:
        sql = "AsList("

        as_list_items = []

        for value in list_value:
            if isinstance(value, str):
                value = SqlParser.from_str(value)
            elif isinstance(value, datetime):
                value = SqlParser.from_datetime(value)
            elif isinstance(value, float) or isinstance(value, int):
                pass
            else:
                assert not f"Unsupported  type {type(value)}"

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

                if isinstance(value, float) or isinstance(value, int):
                    pass
                elif isinstance(value, str):
                    value = SqlParser.from_str(value)
                elif isinstance(value, pd._libs.tslibs.timestamps.Timestamp):
                    value = SqlParser.from_datetime64_ns(value)
                else:
                    assert not f"Unsupported object type {type(value)}"

                as_struct_cols.append(f"{value} as `{columns[index]}`")

            as_struct_row = "AsStruct(" + ",".join(as_struct_cols) + ")"
            as_structs.append(as_struct_row)

        as_structs = ",".join(as_structs)
        sql += as_structs
        sql += "))"

        return sql
