from typing import Any, Optional
from .query_results import YQResults


class YandexQueryResults:
    """Holds and formats query execution results"""

    def __init__(self, results: list[dict[str, Any]] | dict[str, Any]):
        self._raw_results = results
        self._results: Optional[list[Any]] = None
        self._parsers: Optional[list[YQResults]] = None

    def _init_results_cache(self):
        results = self._raw_results
        if not isinstance(results, list):
            results = [results]

        parsers = []
        results_converted = []
        for item in results:
            item_parser = YQResults(item)
            results_converted.append(item_parser.results)
            parsers.append(item_parser)

        self._parsers = parsers
        self._results = results_converted

    @property
    def results(self):
        if self._results is None:
            self._init_results_cache()

        return self._results

    @property
    def raw_results(self):
        return self._raw_results

    def to_table(self, index: Optional[int] = 0):
        self._init_results_cache()  # initialize internal results cache
        return self._results[index].to_table()

    def to_dataframes(self, index: Optional[int] = 0):
        self._init_results_cache()  # initialize internal results cache

        if len(self._parsers) == 0:
            return None
        elif index is not None:
            return self._parsers[index].to_dataframe()
        else:
            query_results = []
            for rs_index in range(0, len(self._parsers)):
                query_results.append(self._parsers[rs_index].to_dataframe())

            return query_results
