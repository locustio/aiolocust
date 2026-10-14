import asyncio
import re
import threading
import time
from typing import cast

import aiohttp
import pytest
from opentelemetry import trace
from utils import assert_search
from websockets import ClientConnection

from aiolocust import User, rate_limit, tracer
from aiolocust.runner import LoopWorker, Runner, Stage, desired_user_count
from aiolocust.users.http import HttpUser, LocustClientSession
from aiolocust.users.websocket import WebsocketUser


@pytest.mark.parametrize("with_handshake", [False, True])
async def test_websocket_on_connect_before_loops(mocker, with_handshake):
    steps = []
    loops_started = asyncio.Event()
    websocket = mocker.AsyncMock(spec=ClientConnection)
    connect_mock = mocker.patch("aiolocust.users.websocket.traced_connect", return_value=websocket)
    runner = mocker.Mock(spec=Runner, host="ws://example.test")
    runner.wait_for_shutdown = mocker.AsyncMock(side_effect=loops_started.wait)

    class TestUser(WebsocketUser):
        async def receive(self) -> None:
            steps.append("receive")
            if "send" in steps:
                loops_started.set()
            await asyncio.Event().wait()

        async def send(self) -> None:
            steps.append("send")
            if "receive" in steps:
                loops_started.set()
            await asyncio.Event().wait()

    class HandshakeUser(TestUser):
        async def on_connect(self) -> None:
            assert self.websocket is websocket
            await self.websocket.send("handshake")
            await self.websocket.recv()
            assert steps == []
            steps.append("handshake")

    user = HandshakeUser(runner) if with_handshake else TestUser(runner)
    await asyncio.wait_for(user.run(), timeout=1)

    connect_mock.assert_awaited_once_with(runner.host, ssl=None)
    assert set(steps[-2:]) == {"send", "receive"}
    if with_handshake:
        assert steps[0] == "handshake"
        websocket.send.assert_awaited_once_with("handshake")
        websocket.recv.assert_awaited_once_with()
    websocket.close.assert_awaited_once_with()


@pytest.mark.parametrize("error_type", [ValueError, asyncio.CancelledError])
async def test_websocket_on_connect_failure_closes_connection(mocker, error_type):
    websocket = mocker.AsyncMock(spec=ClientConnection)
    mocker.patch("aiolocust.users.websocket.traced_connect", return_value=websocket)
    runner = mocker.Mock(spec=Runner, host="ws://example.test")
    receive_mock = mocker.AsyncMock()
    send_mock = mocker.AsyncMock()

    class TestUser(WebsocketUser):
        async def on_connect(self) -> None:
            raise error_type("handshake failed")

        async def receive(self) -> None:
            await receive_mock()

        async def send(self) -> None:
            await send_mock()

    with pytest.raises(error_type, match="handshake failed"):
        await TestUser(runner).run()

    receive_mock.assert_not_awaited()
    send_mock.assert_not_awaited()
    websocket.close.assert_awaited_once_with()


def test_basic(http_server, capteesys):  # noqa: ARG001
    class TestUser(HttpUser):
        async def run(self):
            async with self.client.get("http://localhost:8081/") as resp:
                pass
            async with self.client.get("http://localhost:8081/404") as resp:
                pass
            async with self.client.get("http://localhost:8081/", name="renamed") as resp:
                resp.error = "Oh no"
            async with self.client.get("http://localhost:8081/") as resp:
                assert "foo" in await resp.text()
            async with self.client.get("http://localhost:8081/") as resp:
                assert "bar" in await resp.text()

    Runner([TestUser], iterations=1).run_test()
    out, err = capteesys.readouterr()
    assert err == ""
    assert "Summary" in out
    assert_search(r" http://localhost:8081/[ ]+│[ ]+[2] .* \(50.0%\)", out)
    assert_search(r" renamed[ ]+│[ ]+1 .* \(100.0%\)", out)
    assert "Error" in out
    assert_search(r"1 .* assert 'foo' in 'OK' \(test_runner.py:\d", out)
    assert_search(r"1 .* 404,", out)
    assert_search(r"1 .* Oh no", out)
    assert "bar" not in out


def test_unhandled_exception(http_server, capteesys):  # noqa: ARG001
    class TestUser(HttpUser):
        async def run(self):
            raise Exception("an error")

    Runner([TestUser], iterations=1).run_test()
    out, err = capteesys.readouterr()
    assert err == ""
    assert "Summary" in out
    assert_search(r"1 .* an error", out)


from contextlib import asynccontextmanager


def test_timeout_catching(http_server, capteesys):  # noqa: ARG001
    class TestUser(HttpUser):
        @asynccontextmanager
        async def cm(self):
            async with LocustClientSession(
                self.runner, self.base_url, timeout=aiohttp.ClientTimeout(0.0001)
            ) as self.client:
                yield

        async def run(self):
            await asyncio.sleep(0.2)
            async with self.client.get("http://localhost:8081/") as resp:
                pass
            raise Exception("We'll never get here")

    Runner([TestUser], iterations=1).run_test()
    out, err = capteesys.readouterr()
    assert err == ""
    assert "Summary" in out
    assert "(100.0%)" in out
    assert not "We'll never get here" in out
    assert_search(r"\d .* TimeoutError", out)


def test_w_otel(http_server, capteesys):  # noqa: ARG001
    class TestUser(HttpUser):
        async def run(self):
            async with self.client.get("http://localhost:8081/") as resp:
                pass
            async with self.client.get("http://localhost:8081/404") as resp:
                pass
            async with self.client.get("http://localhost:8081/") as resp:
                assert "foo" in await resp.text()
            async with self.client.get("http://localhost:8081/") as resp:
                assert "bar" in await resp.text()

    Runner([TestUser], iterations=1).run_test()
    out, err = capteesys.readouterr()
    assert err == ""
    assert "Summary" in out
    assert_search(r" http://localhost:8081/[ ]+│[ ]+[2] .* \(50.0%\)", out)
    assert "Error" in out
    assert_search(r"1 .* assert 'foo' in 'OK'", out)
    assert_search(r"1 .* 404,", out)
    assert "bar" not in out


def test_manual_shutdown(http_server, capteesys):  # noqa: ARG001
    class TestUser(HttpUser):
        async def run(self):
            async with self.client.get("http://localhost:8081/") as resp:
                pass
            await self.runner.shutdown("foo")  # manually trigger shutdown from user code

    Runner([TestUser]).run_test()
    out, err = capteesys.readouterr()
    assert err == ""
    print(out)
    assert "Summary" in out
    assert " http://localhost:8081/ │     1 │ 0 (0.0%) " in out

    # I wish we could test this, but it isnt actually printed by the runner,
    # and it isn't worth testing at test_aiolocust-level
    # assert "foo" in out


@pytest.mark.parametrize("outcome", ["shutdown", "cancel", "stopped"])
async def test_wait_for_shutdown(outcome):
    # ai generated test case
    runner = Runner([])
    runner.running = outcome != "stopped"
    task = asyncio.create_task(runner.wait_for_shutdown())
    try:
        await asyncio.sleep(0)
        if outcome == "stopped":
            assert await asyncio.wait_for(task, 1) is None
        else:
            assert not task.done()
            assert len(runner._shutdown_waiters) == 1
            if outcome == "shutdown":
                await runner.shutdown()
                assert await asyncio.wait_for(task, 1) is None
            else:
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
        assert not runner._shutdown_waiters
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        runner.forced_shutdown_timer.cancel()


def test_iterations(http_server, capteesys):  # noqa: ARG001
    class TestUser(HttpUser):
        async def run(self):
            async with self.client.get("http://localhost:8081/") as resp:
                pass
            async with self.client.get("http://localhost:8081/") as resp:
                assert "foo" in await resp.text()

    Runner(
        [TestUser],
        user_count=2,
        iterations=30,
        event_loops=1,
        duration=8,  # ensure we dont run forever, even if the iteration limit fails
    ).run_test()
    out, err = capteesys.readouterr()
    assert err == ""
    print(out)
    assert "Summary" in out
    assert_search(r" http://localhost:8081/.* 60 .* 30 \(50.0%\)", out)
    assert "Error" in out
    assert_search(r"30 .* assert 'foo' in 'OK'", out)


def test_desired_user_count():
    stages = [
        Stage(duration=2, target=2),
        Stage(duration=2, target=2),
        Stage(duration=2, target=4),
        Stage(duration=2, target=0),
        Stage(duration=2, target=10),
    ]
    assert desired_user_count(stages, 0) == 0
    assert desired_user_count(stages, 0.1) == 1  # using math.ceil to avoid 0 users at the start of the test
    assert desired_user_count(stages, 2) == 2
    assert desired_user_count(stages, 3) == 2  # no change in second stage
    assert desired_user_count(stages, 4) == 2
    assert desired_user_count(stages, 5) == 3  # halfway through third stage
    assert desired_user_count(stages, 6) == 4
    assert desired_user_count(stages, 7) == 2  # halfway through ramp down stage
    assert desired_user_count(stages, 8) == 0  # end of ramp down stage
    assert desired_user_count(stages, 9) == 5  # can ramp up again after ramping down to 0
    assert desired_user_count(stages, 9.3) == 7  # floats are nice
    assert desired_user_count(stages, 10) == 10
    assert desired_user_count(stages, 999) is None
    assert desired_user_count([Stage(0, 100), Stage(1, 100)], 0.001) == 100  # correctly handles instant ramp up


def test_current_user_count_gauge(http_server, monkeypatch):  # noqa: ARG001
    class TestUser(HttpUser):
        async def run(self):
            await asyncio.sleep(0.01)

    gauge_values = []

    def _record_gauge(value, attributes=None, context=None):  # noqa: ARG001
        gauge_values.append(value)

    monkeypatch.setattr("aiolocust.runner.current_users_gauge.set", _record_gauge)
    Runner([TestUser], user_count=2, duration=1).run_test()

    assert gauge_values[0] == 0
    assert max(gauge_values) == 2


def test_base_url_gets_removed_from_request_name(http_server, capteesys):  # noqa: ARG001
    class TestUser(HttpUser):
        async def run(self):
            async with self.client.get("/") as resp:
                pass

    Runner([TestUser], iterations=1, host="http://localhost:8081").run_test()
    out, err = capteesys.readouterr()
    assert err == ""
    assert "Summary" in out
    assert_search(r" / .* \(0.0%\)", out)
    assert "http://localhost:8081" not in out


def test_duration_shorter_than_rampup(http_server, capteesys):  # noqa: ARG001
    class TestUser(HttpUser):
        async def run(self):
            async with self.client.get("/") as resp:
                pass

    runner = Runner([TestUser], user_count=2, duration=1, rate=0.1, host="http://localhost:8081")
    thread = threading.Thread(target=runner.run_test, daemon=True)
    thread.start()
    thread.join(timeout=5)
    assert not thread.is_alive(), "Runner.run_test() took longer than 5 seconds to complete"

    out, err = capteesys.readouterr()
    assert err == ""
    assert "Summary" in out
    assert_search(r" / .* \(0.0%\)", out)
    assert "http://localhost:8081" not in out


def test_rate_limiting(http_server, capteesys):  # noqa: ARG001
    class TestUser(HttpUser):
        @rate_limit(10)
        async def run(self):
            async with self.client.get("http://localhost:8081/") as resp:
                pass

    Runner([TestUser], 20, 3, host="http://localhost:8081", config={"stats_print_interval": 1}).run_test()
    output, err = capteesys.readouterr()
    assert err == ""
    assert "Summary" in output
    highest_rate = 0.0
    for line in output.splitlines():
        match = re.search(r"(\d*\.?\d+)/s $", line)
        if match:
            rate = float(match.group(1))
            highest_rate = max(highest_rate, rate)
    assert highest_rate > 9.0, f"request rate never reached high enough value: {highest_rate}"
    # limiting is using sliding window so slight overshoot during a clock second is normal
    assert highest_rate <= 14.0, f"rate limit exceeded: {highest_rate}"


def test_rate_limiting_doesnt_block_shutdown(http_server, capteesys):  # noqa: ARG001
    class TestUser(HttpUser):
        @rate_limit(1)
        async def run(self):
            async with self.client.get("/") as resp:
                pass

    runner = Runner([TestUser], user_count=20, duration=2, rate=20, host="http://localhost:8081")
    thread = threading.Thread(target=runner.run_test, daemon=True)
    thread.start()
    thread.join(timeout=5)
    assert not thread.is_alive(), "Runner.run_test() took longer than 5 seconds to complete"

    out, err = capteesys.readouterr()
    assert err == ""
    assert "Summary" in out
    assert_search(r" / .* \(0.0%\)", out)
    assert "http://localhost:8081" not in out


def test_futures_cleanup_on_scale_down():
    class TestUser(User):
        async def run(self):
            await asyncio.sleep(0.01)

    runner = Runner(users=[TestUser], user_count=0)
    assert len(runner.futures) == 0

    mock_worker = LoopWorker()
    mock_worker.start()

    try:
        # Scale up
        for _ in range(10):
            runner.add_user(mock_worker)
        assert len(runner.futures) == 10

        # Scale down
        for _ in range(10):
            if runner.running_users:
                runner.stop_user()

        assert len(runner.futures) == 0, "Memory leak: futures list retained references after scale-down!"

    finally:
        mock_worker.stop()


def test_multiple_user_classes():
    created = []

    class UserA(User):
        async def run(self):
            created.append("A")
            await asyncio.sleep(0.001)

    class UserB(User):
        async def run(self):
            created.append("B")
            await asyncio.sleep(0.001)

    runner = Runner(
        [UserA, UserB],
        user_count=3,
        iterations=3,
        event_loops=1,
    )
    runner.run_test()

    assert created == ["A", "B", "A"]


async def test_websocket_user(websocket_server, capteesys):
    received = asyncio.Queue()

    async def handler(ws):
        # engineIO type handshake
        await ws.send('0{"sid": "abcd123"}')
        async for message in ws:
            await received.put(message)
            if message == "40":
                await ws.send('40{"sid":"something"}')
                await asyncio.sleep(0.01)
                await ws.send("2")

    url = await websocket_server(handler)

    class MyUser(WebsocketUser):
        async def on_connect(self):
            message = cast(str, await self.websocket.recv())
            assert message.startswith('0{"sid"')
            with tracer.start_as_current_span("websocket.40", kind=trace.SpanKind.CLIENT) as span:
                span.set_attribute("network.protocol.name", "websocket")
                await self.websocket.send("40")
                message = cast(str, await self.websocket.recv())
                assert message.startswith("40")

        async def receive(self):
            async for message in self.websocket:
                s = str(message)
                if s == "2":
                    await self.websocket.send("3")  # heartbeat response
                elif s.startswith("42["):
                    pass
                else:
                    print(f"Unknown message received: {s}")

        async def send(self):
            while True:
                await self.websocket.send(f'42["ping",{{"ts":{time.time_ns()}}}]')
                await asyncio.sleep(0.1)

    runner = Runner([MyUser], user_count=20, duration=1, host=url)
    await runner.run_test_async()
    assert received.qsize() >= 10

    while True:
        try:
            message = received.get_nowait()
            assert message == "40" or message == "3" or message.startswith('42["ping",')
        except asyncio.QueueEmpty:
            break

    output, err = capteesys.readouterr()
    assert err == ""
    assert "Summary" in output
    assert_search(r" websocket.connect[ ]+│[ ]+[20].*\(0.0%\)", output)
    assert_search(r" websocket.40[ ]+│[ ]+[20].*\(0.0%\)", output)


async def test_websocket_port_closed(capteesys):
    class MyUser(WebsocketUser):
        async def receive(self):
            pass

        async def send(self):
            pass

    runner = Runner([MyUser], iterations=1, host="ws://localhost:6666")
    await runner.run_test_async()

    output, err = capteesys.readouterr()
    assert err == ""
    assert "Summary" in output
    assert_search(r" websocket.connect[ ]+│[ ]+[1].*\(100.0%\)", output)


async def test_websocket_dns_fail(capteesys):
    class MyUser(WebsocketUser):
        async def receive(self):
            pass

        async def send(self):
            pass

    runner = Runner([MyUser], iterations=1, host="ws://no_this_doesnt_exist_no_way.net")
    await runner.run_test_async()

    output, err = capteesys.readouterr()
    assert err == ""
    assert "Summary" in output
    assert_search(r" websocket.connect[ ]+│[ ]+[1].*\(100.0%\)", output)
    assert "Traceback" not in output  # no explicit callstack should be printed
