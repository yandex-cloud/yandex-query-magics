import pytest
from yandex_query_magic import YandexQuery, YandexQueryException
from pytest_httpserver import HTTPServer, httpserver
from werkzeug.wrappers import Response
import datetime
import time
import jwt
import json


# Test sa key to be used in tests
TEST_SA_KEY = {
  "id": "ajer7vgr0lsf92k6ftbf",
  "service_account_id": "aje5cgv2rif15p739hve",
  "created_at": "2024-02-01T08:32:16.871406224Z",
  "key_algorithm": "RSA_2048",
  "public_key": "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAvN76kfg8U4itnysDyqGm\nt6xgxxFFnWSGOMCFtGSnYjchs11h8asNHLLSdfqRWSocoHSKJw5Z598sAoUlLbYC\nOxlz/lkdXV5lU+b9jUfTtl/9Q5ior9xurhzGYQCmaYY0/9UBRMFDTSzrW9rjaM6H\n3KaSZs3WRvyB8nGqk3f6DruN6z1wR0LtrvoTMVvDs8mLzs/K4VjqTziLWGfCuh0d\n2KGmGjyWSwN+XRadjqd+DjEGA3CaVAgLzqzY25zpOYzZYYsKRAB58/zl9KEAhN3b\nFYeBSJ359RbL7SJ8KvC8jmWvZZUmT8eP77gvd6l1Xp3Kr/dB9Rxx2SfZTVKtNUDV\nlQIDAQAB\n-----END PUBLIC KEY-----\n",  # noqa
  "private_key": "PLEASE DO NOT REMOVE THIS LINE! Yandex.Cloud SA Key ID <ajer7vgr0lsf92k6ftbf>\n-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC83vqR+DxTiK2f\nKwPKoaa3rGDHEUWdZIY4wIW0ZKdiNyGzXWHxqw0cstJ1+pFZKhygdIonDlnn3ywC\nhSUttgI7GXP+WR1dXmVT5v2NR9O2X/1DmKiv3G6uHMZhAKZphjT/1QFEwUNNLOtb\n2uNozofcppJmzdZG/IHycaqTd/oOu43rPXBHQu2u+hMxW8OzyYvOz8rhWOpPOItY\nZ8K6HR3YoaYaPJZLA35dFp2Op34OMQYDcJpUCAvOrNjbnOk5jNlhiwpEAHnz/OX0\noQCE3dsVh4FInfn1FsvtInwq8LyOZa9llSZPx4/vuC93qXVencqv90H1HHHZJ9lN\nUq01QNWVAgMBAAECggEAUKKyP9fHibJ0zdvDhqN1Vj2WI+dP3V6pn1kyvE2s2NXI\n4Zcg1di9hF8kU5Jis7qy9h5LTVlnMQOq+nh14wot8aVwTEsnqlE+2Y9o+QSNcvOQ\nYWevvUVTS6qlV4y7f5n4zrDWFdCdNznSUiklpf1nK+FB5/pBXZU4tZWpycQTUm4r\nnt+Zgu9hJHe0vDDw3VQEyNswd38zFELj0NdPZWgpsEdOxBfL5eaSkNF/oXPnO8Pu\nkLNus0klOcaKG9IzoTy2wf4587n5FcyeViRi3pRl7JM+2m9LtudMiS9YrNsx0vAI\nvkahGpFWFEnjg0Bzrz09mO+yqgX+k+iw88EJcedQkQKBgQD3S7BwDEugOCprF/I3\n3clLaGgvmWxDegKu3FsWFWOROKpsdaM09NBZVTqB20nBwKURy/+JugDyLMKzCDwH\nRiC5ROsyxoISAQAmdxspGHYQ4bQEoUQURR+UPBvPOjXawx7Qczv+6+0foy2MAmYq\n+KC8gC88ov7g2Q4llIWUIc74NwKBgQDDhNd5Mt6r9d7Cq4fy+9W0sveRvbLN0G9I\nOvNPYuPKiz4M2l/Gli+zQSJodJGpRnRTxFxRyy3/qHe+rBHfnR9FRSpdMyowB7j3\n00hZK9qg3/hrSoUiyQHovwHmncO2zxpC6PwqRTLjmmKHhg+8AATqjqq/EwPn8FVi\ngf8USpcikwKBgQCX1Euub44a/4DjoZ8gN7Y36xFUcCDtSMLO8xGlfFpWNfFEh/E9\nOTWWM2KpiaY+I/X0+EebGq0sAtlDLEIWwTKkLTEuSnxfa2fZNfViBNewQ7LPyOdQ\nfqQF0eXNFFMuTe/kUWu7dsRuUTRMqshph57APP9Dflt4Vyt9XTOqIBu/fwKBgQCS\nhszg9MGyB9qA9AI3lIpNGM62t374BZxQenV76jWixoWjJkkez9FBuq+prqq3PKjT\nWlaBqg54Ce5rxBLFDcCwriwYms6kHjV97SbMRTRc2l0XM0rhjdjTb8ph5ZwWNdGN\nkYPhvehscHgk7tB96VnF21OVTQ/hU9j5sUjUES6A5wKBgHvzNT6bYhURpymppktQ\nii/wSCaW+JKB+re3HirTkQkg7XyP1tdJmjC8u0XznWW32oIUu3Yc5u3NpM2UyFhu\nvsbJmXl/Ku3Jh24HTRV+ZC3TDNcMZXHCBaiZkfTYXSpWwOVTETu/AWe9Qa1lBGTx\njIX5w9L1zoLytMGlZ3lrcCKN\n-----END PRIVATE KEY-----\n"  # noqa
}


@pytest.fixture(scope="function")
def yandex_query(yq_httpserver: HTTPServer,
                 iam_httpserver: HTTPServer) -> YandexQuery:

    base_api_url = yq_httpserver.url_for("/")
    base_iam_url = iam_httpserver.url_for("/")
    yq = YandexQuery(base_api_url=base_api_url, base_iam_url=base_iam_url)
    yq.set_service_account_key_auth(TEST_SA_KEY)
    return yq


@pytest.fixture(scope="function")
def yq_httpserver() -> HTTPServer:
    "Main YQ HTTP-server"

    server = HTTPServer()
    server.start()
    yield server
    server.stop()


@pytest.fixture(scope="function")
def iam_httpserver() -> HTTPServer:
    "IAM HTTP-server"

    server = HTTPServer()
    server.start()

    yield server
    server.stop()


@pytest.mark.asyncio
async def test_auth(iam_httpserver: HTTPServer, yandex_query: YandexQuery):
    "Tests Yandex Cloud IAM integration"

    assert yandex_query is not None
    api = iam_httpserver.url_for("/iam/v1/tokens")

    # Custom handler to check correctness of IAM data
    def handler(r):
        decode_result = jwt.decode(
            json.loads(r.data)["jwt"],
            TEST_SA_KEY["public_key"],
            algorithms=["PS256"], audience=api)

        assert decode_result["iss"] == TEST_SA_KEY["service_account_id"]

        return Response(json.dumps({"iamToken": "iamToken"}),
                        status=200,
                        content_type="application/json")

    iam_httpserver.expect_request("/iam/v1/tokens",
                                  method="POST").\
                                  respond_with_handler(handler)  # noqa

    await yandex_query._get_iam_token()


@pytest.mark.asyncio
async def test_auth_timeout(iam_httpserver: HTTPServer,
                            yandex_query: YandexQuery):
    "Tests Yandex Cloud IAM timeouts"

    assert yandex_query is not None

    def sleeping(_):
        time.sleep(2)
        return Response(json.dumps({"iamToken": "iamToken"}),
                        status=200, content_type="application/json")

    iam_httpserver.expect_oneshot_request("/iam/v1/tokens",
                                          method="POST").respond_with_handler(
                                              sleeping)

    iam_httpserver.expect_request("/iam/v1/tokens",
                                  method="POST").respond_with_json(
                                      {"iamToken": "test_iam_token"})

    await yandex_query._get_iam_token()


@pytest.mark.asyncio
async def test_user_agent(iam_httpserver: HTTPServer,
                          yq_httpserver: HTTPServer,
                          yandex_query: YandexQuery):
    "Tests happy path to create query"
    assert yandex_query is not None

    folder_id = "folder_id"
    query_id = "query_id"
    name = "name"
    description = "description"
    query_text = "select 1"

    # Set up IAM to handle all auth queries
    iam_httpserver.expect_request("/iam/v1/tokens", method="POST",
                                  headers={"User-Agent": "Jupyter yandex_query_magic"},  # noqa
                                  handler_type=httpserver.HandlerType.PERMANENT).\
        respond_with_json({"iamToken": "test_iam_token"})  # noqa

    # Checking correctness of create-query command
    yq_httpserver.expect_request("/fq/v1/queries",
                                 query_string=f"project={folder_id}",
                                 method="POST",
                                 headers={"User-Agent": "Jupyter yandex_query_magic"},  # noqa
                                 json={"name": name,
                                       "description": description,
                                       "text": query_text,
                                       "type": "ANALYTICS"}).\
        respond_with_json({"id": query_id})

    # Issue create-query command
    expected_query_id = await yandex_query.start_execute_query(folder_id,
                                                               query_text,
                                                               name,
                                                               description)

    # Checking if expected query id is the same as created
    assert expected_query_id == query_id


@pytest.mark.asyncio
async def test_query(iam_httpserver: HTTPServer,
                     yq_httpserver: HTTPServer,
                     yandex_query: YandexQuery):
    "Tests happy path to create query"
    assert yandex_query is not None

    folder_id = "folder_id"
    query_id = "query_id"
    name = "name"
    description = "description"
    query_text = "select 1"

    # Set up IAM to handle all auth queries
    iam_httpserver.expect_request("/iam/v1/tokens", method="POST",
                                  handler_type=httpserver.HandlerType.PERMANENT).\
        respond_with_json({"iamToken": "test_iam_token"})  # noqa

    # Checking correctness of create-query command
    yq_httpserver.expect_request("/fq/v1/queries",
                                 query_string=f"project={folder_id}",
                                 method="POST",
                                 json={"name": name,
                                       "description": description,
                                       "text": query_text,
                                       "type": "ANALYTICS"}).\
        respond_with_json({"id": query_id})

    # Issue create-query command
    expected_query_id = await yandex_query.start_execute_query(folder_id,
                                                               query_text,
                                                               name,
                                                               description)

    # Checking if expected query id is the same as created
    assert expected_query_id == query_id


@pytest.mark.asyncio
async def test_get_query_status(iam_httpserver: HTTPServer,
                                yq_httpserver: HTTPServer,
                                yandex_query: YandexQuery):
    "Tests get-query-status happy path command"
    assert yandex_query is not None

    folder_id = "folder_id"
    query_id = "query_id"
    status = "RUNNING"

    # Set up IAM to handle all auth queries
    iam_httpserver.expect_request("/iam/v1/tokens", method="POST",
                                  handler_type=httpserver.HandlerType.PERMANENT).\
        respond_with_json({"iamToken": "test_iam_token"})  # noqa

    yq_httpserver.expect_request(f"/fq/v1/queries/{query_id}/status",
                                 query_string=f"project={folder_id}",
                                 method="GET").\
        respond_with_json({"status": status})

    # Check if returned query status is the same as expected
    expected_status = await yandex_query._get_query_status(folder_id, query_id)
    assert expected_status == status


@pytest.mark.asyncio
async def test_get_query_info(iam_httpserver: HTTPServer,
                              yq_httpserver: HTTPServer,
                              yandex_query: YandexQuery):
    "Tests happy path for get-query-info command"
    assert yandex_query is not None

    folder_id = "folder_id"
    query_id = "query_id"
    status = "RUNNING"

    # Set up IAM to handle all auth queries
    iam_httpserver.expect_request("/iam/v1/tokens", method="POST",
                                  handler_type=httpserver.HandlerType.PERMANENT).\
        respond_with_json({"iamToken": "test_iam_token"})  # noqa

    yq_httpserver.expect_request(f"/fq/v1/queries/{query_id}",
                                 query_string=f"project={folder_id}",
                                 method="GET").\
        respond_with_json({"status": status})

    # Checks if response is the same as expected
    query_info = await yandex_query.get_queryinfo(folder_id, query_id)
    assert query_info["status"] == status


@pytest.mark.asyncio
async def test_get_query_status_timedout(iam_httpserver: HTTPServer,
                                         yq_httpserver: HTTPServer,
                                         yandex_query: YandexQuery):
    "Tests timeout for get-query-status command"

    assert yandex_query is not None

    folder_id = "folder_id"
    query_id = "query_id"
    status = "FAILED"

    def sleeping(_):
        time.sleep(2)
        return Response(json.dumps({"status": status}),
                        status=200,
                        content_type="application/json")

    # Set up IAM to handle all auth queries
    iam_httpserver.expect_request("/iam/v1/tokens", method="POST",
                                  handler_type=httpserver.HandlerType.PERMANENT).\
        respond_with_json({"iamToken": "test_iam_token"})  # noqa

    # First query responds with timeout 2 seconds
    yq_httpserver.expect_oneshot_request(f"/fq/v1/queries/{query_id}/status",
                                         query_string=f"project={folder_id}",
                                         method="GET").\
        respond_with_handler(sleeping)

    # Second query responds with HTTP 500 error code
    yq_httpserver.expect_oneshot_request(f"/fq/v1/queries/{query_id}/status",
                                         query_string=f"project={folder_id}",
                                         method="GET").\
        respond_with_response(Response(status=500))

    # Finally query responds HTTP 200 OK
    yq_httpserver.expect_request(f"/fq/v1/queries/{query_id}/status",
                                 query_string=f"project={folder_id}",
                                 method="GET").\
        respond_with_json({"status": status})

    # Query must retry all that errors
    expected_status = await yandex_query._get_query_status(folder_id, query_id)
    assert expected_status == status


@pytest.mark.asyncio
async def test_wait_results_complete(iam_httpserver: HTTPServer,
                                     yq_httpserver: HTTPServer,
                                     yandex_query: YandexQuery):
    "Tests happy path for long wait_results process"

    assert yandex_query is not None

    folder_id = "folder_id"
    query_id = "query_id"
    started_at = datetime.datetime.now().isoformat()

    def status_response():
        return {"meta": {"started_at": started_at}}

    # Set up IAM to handle all auth queries
    iam_httpserver.expect_request("/iam/v1/tokens", method="POST",
                                  handler_type=httpserver.HandlerType.PERMANENT).\
        respond_with_json({"iamToken": "test_iam_token"})  # noqa

    # Get ready to several queries in specific order
    yq_httpserver.expect_ordered_request(f"/fq/v1/queries/{query_id}",
                                         query_string=f"project={folder_id}",
                                         method="GET").\
        respond_with_json(status_response())

    yq_httpserver.expect_ordered_request(f"/fq/v1/queries/{query_id}/status",
                                         query_string=f"project={folder_id}",
                                         method="GET").\
        respond_with_json({"status": "RUNNING"})

    yq_httpserver.expect_ordered_request(f"/fq/v1/queries/{query_id}/status",
                                         query_string=f"project={folder_id}",
                                         method="GET").\
        respond_with_json({"status": "SUCCESS"})

    status = None

    def status_callback(_status):
        nonlocal status
        status = _status

    progress = 0

    def progress_callback(_progress, __):
        nonlocal progress
        progress = _progress

    await yandex_query.wait_results(folder_id,
                                    query_id,
                                    status_callback,
                                    progress_callback)

    # Checking final query status and final progress
    assert status == "SUCCESS"
    assert progress == 100


@pytest.mark.asyncio
async def test_get_query_result_bad_status(iam_httpserver: HTTPServer,
                                           yq_httpserver: HTTPServer,
                                           yandex_query: YandexQuery):
    "Checks that get-query-result throws exception if query execution failed"

    assert yandex_query is not None

    with pytest.raises(YandexQueryException):
        folder_id = "folder_id"
        query_id = "query_id"
        status = "FAILED"

        # Set up IAM to handle all auth queries
        iam_httpserver.expect_request("/iam/v1/tokens", method="POST",
                                      handler_type=httpserver.HandlerType.PERMANENT).\
            respond_with_json({"iamToken": "test_iam_token"})  # noqa

        yq_httpserver.expect_request(f"/fq/v1/queries/{query_id}",
                                     query_string=f"project={folder_id}",
                                     method="GET").\
            respond_with_json({"status": status})

        await yandex_query.get_query_result(folder_id, query_id)


@pytest.mark.asyncio
async def test_get_query_results(iam_httpserver: HTTPServer,
                                 yq_httpserver: HTTPServer,
                                 yandex_query: YandexQuery):
    "Checks corectness of simple get-query-result command"
    assert yandex_query is not None

    def result_response():
        return {
            "columns": [{"name": "a", "type": "int"}],
            "rows": [[0]]
                }

    folder_id = "folder_id"
    query_id = "query_id"

    # Set up IAM to handle all auth queries
    iam_httpserver.expect_request("/iam/v1/tokens", method="POST",
                                  handler_type=httpserver.HandlerType.PERMANENT).\
        respond_with_json({"iamToken": "test_iam_token"})  # noqa

    # Respond
    yq_httpserver.expect_request(f"/fq/v1/queries/{query_id}/results/0",
                                 query_string=f"project={folder_id}&limit=1000&offset=0",  # noqa
                                 method="GET").\
        respond_with_json(result_response())

    iam_token = await yandex_query._get_iam_token()
    result = await yandex_query._query_results(folder_id,
                                               query_id, 1,
                                               iam_token)
    assert len(result.results[0]["rows"]) == 1
    assert result.results[0]["rows"][0][0] == 0


@pytest.mark.asyncio
async def test_get_query_big_results(iam_httpserver: HTTPServer,
                                     yq_httpserver: HTTPServer,
                                     yandex_query: YandexQuery):
    "Tests multipage response and limit-offset listing of results"

    assert yandex_query is not None

    def result_response(offset):
        response = {
            "columns": [{"name": "a", "type": "int"}],
            "rows": [[value] for value in range(offset, 2500)[0:1000]]
                }
        return response

    folder_id = "folder_id"
    query_id = "query_id"

    # Set up IAM to handle all auth queries
    iam_httpserver.expect_request("/iam/v1/tokens", method="POST",
                                  handler_type=httpserver.HandlerType.PERMANENT).\
        respond_with_json({"iamToken": "test_iam_token"})  # noqa

    yq_httpserver.expect_request(f"/fq/v1/queries/{query_id}/results/0",
                                 query_string=f"project={folder_id}&limit=1000&offset=0",  # noqa
                                 method="GET").\
        respond_with_json(result_response(0))

    yq_httpserver.expect_request(f"/fq/v1/queries/{query_id}/results/0",
                                 query_string=f"project={folder_id}&limit=1000&offset=1000",  # noqa
                                 method="GET").\
        respond_with_json(result_response(1000))

    yq_httpserver.expect_request(f"/fq/v1/queries/{query_id}/results/0",
                                 query_string=f"project={folder_id}&limit=1000&offset=2000",  # noqa
                                 method="GET").\
        respond_with_json(result_response(2000))

    iam_token = await yandex_query._get_iam_token()
    result = await yandex_query._query_results(folder_id, query_id,
                                               1, iam_token)

    # Checks if results len is expected
    assert len(result.results[0]["rows"]) == 2500

    # Checks if results are as expected
    assert result.results[0]["rows"] == [[value] for value in range(0, 2500)]


@pytest.mark.asyncio
async def test_get_query_result_multiple_resultsets(iam_httpserver: HTTPServer,
                                                    yq_httpserver: HTTPServer,
                                                    yandex_query: YandexQuery):
    "Tests multipage response, limit-offset and multi resultset response"
    assert yandex_query is not None

    started_at = datetime.datetime.now().isoformat()

    def status_response():
        return {
                "meta": {
                    "started_at": started_at},
                "result_sets": [
                    {
                        "rows": 2500, "truncated": False
                    },
                    {
                        "rows": 3999, "truncated": False
                    }
                ],
                "status": "COMPLETED"}

    folder_id = "folder_id"
    query_id = "query_id"

    # Set up IAM to handle all auth queries
    iam_httpserver.expect_request("/iam/v1/tokens", method="POST",
                                  handler_type=httpserver.HandlerType.PERMANENT).\
        respond_with_json({"iamToken": "test_iam_token"})  # noqa

    yq_httpserver.expect_request(f"/fq/v1/queries/{query_id}",
                                 query_string=f"project={folder_id}",
                                 method="GET").\
        respond_with_json(status_response())

    def result_response0(offset):
        response = {
            "columns": [{"name": "a", "type": "int"}],
            "rows": [[value] for value in range(offset, 2500)[0:1000]]
                }
        return response

    def result_response1(offset):
        response = {
            "columns": [{"name": "b", "type": "int"}],
            "rows": [[value] for value in range(offset + 999, 3999+999)[0:1000] ]  # noqa
                }
        return response

    folder_id = "folder_id"
    query_id = "query_id"

    # Setting up query sequence
    yq_httpserver.expect_request(f"/fq/v1/queries/{query_id}/results/0",
                                 query_string=f"project={folder_id}&limit=1000&offset=0",  # noqa
                                 method="GET").\
        respond_with_json(result_response0(0))

    yq_httpserver.expect_request(f"/fq/v1/queries/{query_id}/results/0",
                                 query_string=f"project={folder_id}&limit=1000&offset=1000",  # noqa
                                 method="GET").\
        respond_with_json(result_response0(1000))

    yq_httpserver.expect_request(f"/fq/v1/queries/{query_id}/results/0",
                                 query_string=f"project={folder_id}&limit=1000&offset=2000",  # noqa
                                 method="GET").\
        respond_with_json(result_response0(2000))

    yq_httpserver.expect_request(f"/fq/v1/queries/{query_id}/results/1",
                                 query_string=f"project={folder_id}&limit=1000&offset=0",  # noqa
                                 method="GET").\
        respond_with_json(result_response1(0))

    yq_httpserver.expect_request(f"/fq/v1/queries/{query_id}/results/1",
                                 query_string=f"project={folder_id}&limit=1000&offset=1000",  # noqa
                                 method="GET").\
        respond_with_json(result_response1(1000))

    yq_httpserver.expect_request(f"/fq/v1/queries/{query_id}/results/1",
                                 query_string=f"project={folder_id}&limit=1000&offset=2000",  # noqa
                                 method="GET").\
        respond_with_json(result_response1(2000))

    yq_httpserver.expect_request(f"/fq/v1/queries/{query_id}/results/1",
                                 query_string=f"project={folder_id}&limit=1000&offset=3000",  # noqa
                                 method="GET").\
        respond_with_json(result_response1(3000))

    result = await yandex_query.get_query_result(folder_id, query_id)

    # Checks the results of first result set
    assert len(result.results[0]["rows"]) == 2500
    assert result.results[0]["rows"] == [[value] for value in range(0, 2500)]

    # Checks the results of second result set
    assert len(result.results[1]["rows"]) == 3999
    assert result.results[1]["rows"] == [[value] for value in range(999, 3999+999)]  # noqa
