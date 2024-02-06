from __future__ import print_function
import time
import jwt
from urllib.parse import urljoin
import asyncio
import aiohttp
import dateutil.parser
from enum import Enum
from .query_results import YandexQueryResults
from typing import Optional, Callable, Any
from aiohttp_retry import RandomRetry, RetryClient


class YandexQueryException(Exception):
    "Specific exception for YQ query execution"
    def __init__(self, issues):
        self.issues = issues

    def __str__(self) -> str:
        return super().__str__()


class YandexQuery():
    "Execute queries in YQ"

    def __init__(self,
                 base_api_url: str = "https://api.yandex-query.cloud.yandex.net/api/",  # noqa: E501
                 base_iam_url: str = "https://iam.api.cloud.yandex.net"):
        self.service_account_key = None
        self.base_api_url = base_api_url
        self.base_iam_url = base_iam_url
        self.auth_type = YandexQuery.AuthType.VM

    # https://cloud.yandex.com/en/docs/serverless-containers/operations/sa
    async def _resolve_vm_account_key(self) -> str:
        "Resolves IAM token in current VM"
        url = 'http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token'  # noqa: E501
        headers = {'Metadata-Flavor': 'Google'}

        async with await self.create_async_session(headers=headers) as session:
            async with session.get(url, raise_for_status=True) as response:
                resp = await response.json()
                return resp["access_token"]

    # https://cloud.yandex.com/en/docs/iam/operations/iam-token/create-for-sa#get-iam-token
    async def _resolve_service_account_key(self, sa_info) -> str:
        "Resolves IAM tokey by service account key"
        async with await self.create_async_session() as session:
            api = urljoin(self.base_iam_url, "/iam/v1/tokens")

            now = int(time.time())
            payload = {
                    'aud': api,
                    'iss': sa_info["service_account_id"],
                    'iat': now,
                    'exp': now + 360}

            # Creating the JWT token
            encoded_token = jwt.encode(
                payload,
                sa_info["private_key"],
                algorithm='PS256',
                headers={'kid': sa_info["id"]})

            data = {"jwt": encoded_token}

            async with session.post(api,
                                    json=data,
                                    raise_for_status=True) as response:
                resp = await response.json()
                return resp["iamToken"]

    async def _get_iam_token(self) -> str:
        "Obtains new IAM account"
        if self.auth_type == YandexQuery.AuthType.VM:
            return await self._resolve_vm_account_key()
        else:
            return await self._resolve_service_account_key(
                self.service_account_key)

    @staticmethod
    def get_request_url_header_params(
            iam_token: Optional[str] = None,
            headers: Optional[dict[str, str]] = None) -> dict[str, str]:

        _headers = {} if headers is None else headers
        if iam_token is not None:
            _headers['Authorization'] = f"{iam_token}"

        _headers["User-Agent"] = "Jupyter yandex_query_magic"
        return _headers

    def set_service_account_key_auth(self, auth_info: str):
        "Sets auth mode as service account key file"
        self.service_account_key = auth_info
        self.auth_type = YandexQuery.AuthType.SA_KEY_FILE

    def set_vm_auth(self):
        "Sets auth mode as VM auth"
        self.service_account_key = None
        self.auth_type = YandexQuery.AuthType.VM

    class AuthType(Enum):
        "Yandex cloud futhorization type"
        SA_KEY_FILE = 1
        VM = 2

    async def start_execute_query(self,
                                  folder_id: str,
                                  query_text: Optional[str] = None,
                                  name: Optional[str] = None,
                                  description: Optional[str] = None) -> str:
        """Executing query in YQ using current settings
           :return: query id
        """

        type = "ANALYTICS"

        iam_token = await self._get_iam_token()
        async with await self.create_async_session(iam_token) as session:
            data = {"name": name,
                    "type": type,
                    "text": query_text,
                    "description": description}

            url = urljoin(self.base_api_url, f"fq/v1/queries?project={folder_id}") # noqa
            async with session.post(url,
                                    json=data,
                                    raise_for_status=True) as response:
                resp = await response.json()
                self.query_id = resp["id"]

        return self.query_id

    async def create_async_session(self,
                                   iam_token: Optional[str] = None,
                                   headers: Optional[dict[str, str]] = None)\
            -> aiohttp.ClientSession:
        "Creates retriable asyncio session"

        headers = YandexQuery.get_request_url_header_params(iam_token, headers)
        session = aiohttp.ClientSession(headers=headers,
                                        timeout=aiohttp.ClientTimeout(total=1))

        retriable_exceptions = {
            asyncio.exceptions.TimeoutError,
            asyncio.TimeoutError
            }

        retry_options = RandomRetry(attempts=3,
                                    exceptions=retriable_exceptions)

        client = RetryClient(session, retry_options=retry_options)
        return client

    async def _get_query_status(self, folder_id: str, query_id: str) -> str:
        """Retrieves the query status
           :return: status of the query
        """

        url = urljoin(self.base_api_url,
                      f"fq/v1/queries/{query_id}/status?project={folder_id}")

        iam_token = await self._get_iam_token()
        async with await self.create_async_session(iam_token) as session:
            async with session.get(url, raise_for_status=True) as response:
                resp = await response.json()
                return resp["status"]

    async def wait_results(self,
                           folder_id: str,
                           query_id: str,
                           on_status_update: Callable[[str], None],
                           on_progress_update: Callable[[int, str], None]) -> str:  # noqa
        """Wait the query to complete i.e. any status other
        than RUNNING. PENDING
        Reports current status and progress while waiting"""

        progress = 0
        query_info = await self.get_queryinfo(folder_id,
                                              query_id,
                                              await self._get_iam_token())

        started = dateutil.parser.isoparse(query_info["meta"]["started_at"])\
            .replace(tzinfo=None)

        while True:
            status = await self._get_query_status(folder_id, query_id)
            on_status_update(status)
            if status not in ["RUNNING", "PENDING"]:
                progress = 100
                on_progress_update(progress, started)
                return

            await asyncio.sleep(1)
            progress = progress+1
            on_progress_update(progress, started)
            if progress > 100:
                progress = 0

    # https://cloud.yandex.com/en/docs/query/api/methods/get-query
    async def get_queryinfo(self,
                            folder_id: str,
                            query_id: str,
                            iam_token: Optional[str] = None) -> Any:
        """ Retrieves the query execution information
            when query execution is finished already
            :return:
        """
        if iam_token is None:
            iam_token = await self._get_iam_token()

        async with await self.create_async_session(iam_token) as session:
            url = urljoin(self.base_api_url,
                          f"fq/v1/queries/{query_id}?project={folder_id}")

            async with session.get(url, raise_for_status=True) as response:
                resp = await response.json()
                return resp

    # https://cloud.yandex.com/en/docs/query/api/methods/get-query-results
    async def _query_results(self,
                             folder_id: str,
                             query_id: str,
                             result_set_count: int,
                             iam_token: str) -> YandexQueryResults:
        """Retrieves query execution results
        :result_set_count Maximum result set count to retrieve
        :return: YandexQueryResults wrapper over raw results
        """
        results = list()

        async with await self.create_async_session(iam_token) as session:

            # reading results page by page with maximum page size = 1000
            # (as YQ current limit)
            for result_index in range(0, result_set_count):
                limit = 1000
                offset = 0

                columns = None
                rows = []

                while True:
                    url = urljoin(self.base_api_url,
                                  f"fq/v1/queries/{query_id}/"
                                  f"results/{result_index}"
                                  f"?project={folder_id}&"
                                  f"limit={limit}&offset={offset}")

                    async with session.get(url, raise_for_status=True)\
                            as response:

                        qresults = await response.json()
                        if columns is None:
                            columns = qresults["columns"]

                        rows.extend(qresults["rows"])

                        if len(qresults["rows"]) != limit:
                            break
                        else:
                            offset += limit

                results.append({"rows": rows, "columns": columns})

        return YandexQueryResults(results)

    async def get_query_result(self, folder_id: str, query_id: str) -> Any:
        """Retrieves all query results"""
        iam_token = await self._get_iam_token()
        query_info = await self.get_queryinfo(folder_id, query_id, iam_token)

        if query_info["status"] in ["FAILED",
                                    "ABORTED_BY_USER",
                                    "ABORTED_BY_SYSTEM"]:
            issues = query_info.get("issues", None)

            raise YandexQueryException(issues)

        result_set_count = len(query_info["result_sets"])

        return await self._query_results(folder_id,
                                         query_id,
                                         result_set_count,
                                         iam_token)

    # https://cloud.yandex.com/en/docs/query/api/methods/stop-query
    async def stop_query(self, folder_id: str, query_id: str) -> None:
        "Stops the query"
        iam_token = await self._get_iam_token()

        async with await self.create_async_session(iam_token) as session:
            url = urljoin(self.base_api_url,
                          f"fq/v1/queries/{query_id}/stop?project={folder_id}")
            await session.post(url, raise_for_status=True)
