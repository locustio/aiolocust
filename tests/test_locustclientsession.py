import asyncio
from dataclasses import dataclass

import aiohttp
import pytest
from aiohttp import ClientConnectorError, WSMsgType, web
from aiohttp.client_exceptions import ClientResponseError
from pytest_httpserver import HTTPServer

from aiolocust import events
from aiolocust.otel import configure_telemetry
from aiolocust.users.http import LocustClientSession


@dataclass(slots=True)
class Request:
    name: str
    ttlb: float
    error: str | None



requests: list[Request] = []


@pytest.fixture(scope="module", autouse=True)
def configure_test_telemetry():
    configure_telemetry()


@pytest.fixture(autouse=True)
def reset():
    events._clear_handlers()
    requests.clear()

    @events.request.add_listener
    def save_request(name: str, ttlb: float, error: str | None):
        requests.append(Request(name, ttlb, error))

    yield


async def test_name(httpserver: HTTPServer):
    httpserver.expect_request("/").respond_with_data("")

    async def _(client: LocustClientSession):
        async with client.get(httpserver.url_for("/"), name="foo") as resp:
            assert resp.status == 200
        assert len(requests) == 1
        r = requests[0]
        assert r.name == "foo"
        assert r.error is None

        async with client.get(httpserver.url_for("/doesnt_exist"), name="foo") as resp:
            pass
        r = requests[1]
        assert r.name == "foo"
        assert r.error
        assert r.error.startswith("aiohttp.client_exceptions.ClientResponseError: 500, message='INTERNAL SERVER ERROR'")

    async with LocustClientSession() as client:
        await _(client)


async def test_hard_fails_raise_and_log():
    async def _(client: LocustClientSession):
        with pytest.raises(ClientConnectorError):
            async with client.get("http://localhost:6666") as resp:
                raise Exception("This will never be reached")

    async with LocustClientSession() as client:
        await _(client)

    r = requests[0]
    assert isinstance(r.error, str)
    assert r.error.startswith("aiohttp.client_exceptions.ClientConnectorError")


async def test_timeout(httpserver: HTTPServer):
    httpserver.expect_request("/").respond_with_data("")

    async def _(client: LocustClientSession):
        async with client.get(httpserver.url_for("/"), name="foo") as resp:
            pass

    with pytest.raises(asyncio.TimeoutError):
        async with LocustClientSession(timeout=aiohttp.ClientTimeout(0.0001)) as client:
            await _(client)

    assert len(requests) == 1
    assert requests[0].name == "foo"


async def test_404(httpserver: HTTPServer):
    httpserver.expect_request("/").respond_with_data("", 404)

    async def _(client: LocustClientSession):
        async with client.get(httpserver.url_for("/")) as resp:
            pass
        assert len(requests) == 1
        r = requests[0]
        assert r.name.endswith("/")
        assert r.error
        assert "404," in r.error

    async with LocustClientSession() as client:
        await _(client)


async def test_raise_for_status(httpserver: HTTPServer):
    async def _(client: LocustClientSession):
        async with client.get(httpserver.url_for("/doesnt_exist1")) as resp:
            pass
        async with client.get(httpserver.url_for("/doesnt_exist2"), raise_for_status=True) as resp:
            pass
        async with client.get(httpserver.url_for("/this_wont_be_reached")) as resp:
            pass

    async with LocustClientSession() as client:
        with pytest.raises(ClientResponseError):
            await _(client)

    assert len(requests) == 2
    r = requests[0]
    assert r.error
    assert "doesnt_exist1" in r.error
    assert "500" in r.error
    r = requests[1]
    assert r.error
    assert "doesnt_exist2" in r.error
    assert "500" in r.error


async def test_assert(httpserver: HTTPServer):
    async def _(client: LocustClientSession):
        async with client.post(httpserver.url_for("/")) as resp:
            assert resp.status == 200, "Intentionally failed assert"
        async with client.post(httpserver.url_for("/doesnt_exist")) as resp:
            assert resp.status == 200, "Intentionally failed assert"

    with pytest.raises(AssertionError, match="Intentionally failed assert"):
        async with LocustClientSession() as client:
            await _(client)

    r = requests[0]
    assert r.error
    assert "Intentionally failed assert" in r.error  # assertion error overwrites the HTTP 500 error


async def test_handler(httpserver: HTTPServer):
    httpserver.expect_request("/").respond_with_data("")

    async def _(client: LocustClientSession):
        async with client.get(httpserver.url_for("/")) as resp:
            pass
        assert len(requests) == 1
        assert requests[0].error is None

        async with client.get(httpserver.url_for("/doesnt_exist")) as resp:
            pass

        r = requests[1]
        assert r.error
        assert "500," in r.error

        # Explicitly mark the request as successful
        async with client.get(httpserver.url_for("/doesnt_exist")) as resp:
            resp.error = False

        assert requests[2].error is None

        # Explicit error logs the request as failed, but flow continues
        async with client.get(httpserver.url_for("/")) as resp:
            text = await resp.text()
            if not text.startswith("Hello"):
                resp.error = "Response did not start with 'Hello'"
        assert requests[3].error == "Exception: Response did not start with 'Hello'"

        with pytest.raises(AssertionError):
            async with client.get("http://localhost:8081/doesnotexist", name="2") as resp:
                assert "foo" in await resp.text()

        r = requests[4]
        assert r.error
        assert "AssertionError" in r.error

    async with LocustClientSession() as client:
        await _(client)

    # assert len(requests) == 5
    # assert not requests[0].error
    # assert requests[1].error  # bad response code => error = True
    # assert not requests[2].error  # error explicitly set to False
    # assert requests[3].error == "Response did not start with 'Hello'"
    # assert isinstance(requests[4].error, AssertionError)  # assertion failure overwrites bad response code


async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    async for msg in ws:
        if msg.type == WSMsgType.TEXT:
            if msg.data == "close":
                await ws.close()
            else:
                await ws.send_str(f"reply-for-{msg.data}")
        elif msg.type == WSMsgType.ERROR:
            print(f"ws connection closed with exception {ws.exception()}")

    return ws


# async def test_websocket(aiohttp_client: pytest_aiohttp.AiohttpClient):
#     app = web.Application()
#     app.add_routes([web.get("/ws", websocket_handler)])
#     test_client = await aiohttp_client(app)

#     async def _(client: LocustClientSession):
#         async with client.ws_connect(test_client.make_url("/ws")) as ws:
#             await ws.send_str("foo")
#             events.request.fire("send foo", 0, None)
#             async for msg in ws:
#                 if msg.type == WSMsgType.TEXT:
#                     events.request.fire(f"recv {msg.data}", 0, None)
#                     await ws.send_str("close")
#                 elif msg.type == WSMsgType.ERROR:
#                     events.request.fire(f"recv {msg.data}", 0, "error-response")
#                     break

#     async with LocustClientSession() as client:
#         await _(client)
