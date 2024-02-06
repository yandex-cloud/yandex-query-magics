from typing import Any, Optional
import base64
import pandas as pd
import pprint
import dateutil.parser
from datetime import datetime
import re


class YandexQueryResults:
    "Holds and formats query execution results"

    Optional_Regexp = re.compile("Optional<(.*)>")

    def __init__(self, results: list[list[Any]] | list[Any]):
        self._raw_results = results
        self._results = None

    @staticmethod
    def _convert(results: list[list[Any]] | list[Any]):
        return [YandexQueryResults._convert_single(result)
                for result in results]

    @staticmethod
    def _convert_from_base64(value: str) -> str:
        return base64.b64decode(value).decode('utf-8')

    @staticmethod
    def _convert_from_datetime(value: str) -> str:
        return dateutil.parser.isoparse(value)

    @staticmethod
    def _convert_from_date(value: str) -> datetime:
        return datetime.strptime(value, "%Y-%m-%d")

    @staticmethod
    def _extract_from_optional(type):
        "Checks if type is optional i.e. Optional<Uint16>"
        matched = YandexQueryResults.Optional_Regexp.search(type)
        if matched:
            return matched.group(1)

    @staticmethod
    def _convert_from_optional(value: list[Any]) -> Optional[Any]:

        # Optional types are encoded as [[]] objects
        # If type is Uint16, value is encoded as {"rows":[[value]]}
        # If type is Optional<Uint16>, value is encoded as {"rows":[[[value]]]}
        # If value is None than result is {"rows":[[[]]]}
        # So check if len equals 1 it means that it contains value
        # if len is 0 it means it has no value i.e. value is None
        if len(value) == 1:
            return value[0]
        elif len(value) == 0:
            return None
        else:
            print(value)
            assert False

    @staticmethod
    def _get_converter(column_type):
        "Returns converter based on column type"
        converter = None

        if column_type == "String":
            converter = YandexQueryResults._convert_from_base64
            column_type = type(str)
        elif column_type == "Datetime":
            converter = YandexQueryResults._convert_from_datetime
            column_type = type(datetime)
        elif column_type == "Date":
            converter = YandexQueryResults._convert_from_date
            column_type = type(datetime)
        elif column_type == "Timestamp":
            converter = YandexQueryResults._convert_from_datetime
            column_type = type(datetime)
        elif column_type.startswith("Optional<"):
            # If type is Optional than get base type
            converter_, column_type = YandexQueryResults._get_converter(
                YandexQueryResults._extract_from_optional(column_type))
            if converter_ is None:
                converter = YandexQueryResults._convert_from_optional
            else:
                # Remove "Optional" encoding
                # and convert resulting value as others
                def convert_from_optional(x):
                    return converter_(
                        YandexQueryResults._convert_from_optional(x))

                converter = convert_from_optional

        return (converter, column_type)

    @staticmethod
    def _convert_single(results: list[Any]) -> Any:
        converters = []
        new_column_types = []
        for column in results["columns"]:
            column_name = column["name"]
            column_type = column["type"]

            converter, column_type = YandexQueryResults._get_converter(column_type)  # noqa

            converters.append(converter)
            new_column_types.append({"name": column_name, "type": column_type})

        converted_results = []
        for row in results["rows"]:
            new_row = []
            for index, value in enumerate(row):
                converter = converters[index]
                new_row.append(
                    value if converter is None else converter(value))

            converted_results.append(new_row)

        return {"rows": converted_results, "columns": new_column_types}

    def _repr_pretty_(self, p, cycle):
        p.text(pprint.pformat(self._results))

    @property
    def results(self):
        if self._results is None:
            self._results = YandexQueryResults._convert(self.raw_results)

        return self._results

    @property
    def raw_results(self):
        return self._raw_results

    @staticmethod
    def _to_dataframe(result_set):
        return pd.DataFrame(result_set["rows"], columns=[
            column["name"] for column in result_set["columns"]])

    def to_table(self, index: Optional[int] = 0):
        if index is not None:
            return self.results[index]["rows"]
        else:
            raise ValueError("Object has multiple result sets, "
                             "please specify result set index")

    def to_dataframe(self, index: Optional[int] = 0):
        if index is not None:
            return YandexQueryResults._to_dataframe(self.results[index])
        else:
            raise ValueError("Object has multiple result sets, "
                             "please specify result set index")
