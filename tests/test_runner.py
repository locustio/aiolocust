import asyncio
import re
import threading
import time
from contextlib import asynccontextmanager
from typing import cast

import aiohttp
import pytest
from opentelemetry import trace
from utils import assert_search

from aiolocust import User, rate_limit, tracer
from aiolocust.runner import LoopWorker, Runner, Stage, desired_user_count_stage_index_reset_stats
from aiolocust.users.http import HttpUser, LocustClientSession
from aiolocust.users.websocket import WebsocketUser


def test_basic(http_server, capteesys):
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


def test_unhandled_exception(http_server, capteesys):
    class TestUser(HttpUser):
        async def run(self):
            raise Exception("an error")

    Runner([TestUser], iterations=1).run_test()
    out, err = capteesys.readouterr()
    assert err == ""
    assert "Summary" in out
    assert_search(r"1 .* an error", out)


def test_timeout_catching(http_server, capteesys):
    class TestUser(HttpUser):
        @asynccontextmanager
        async def cm(self):
            async with LocustClientSession(
                self.runner, self.host, timeout=aiohttp.ClientTimeout(0.0001)
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


def test_w_otel(http_server, capteesys):
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


def test_manual_shutdown(http_server, capteesys):
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


async def test_concurrent_shutdown_calls(monkeypatch):
    runner = Runner([])
    runner.running = True
    shutdown_event_calls = 0
    allow_shutdown = asyncio.Event()

    async def fire_shutdown_event(runner):
        nonlocal shutdown_event_calls
        shutdown_event_calls += 1
        await allow_shutdown.wait()

    monkeypatch.setattr("aiolocust.runner.events.shutdown_requested.fire", fire_shutdown_event)

    first_shutdown = asyncio.create_task(runner.shutdown())
    await asyncio.sleep(0)
    second_shutdown = asyncio.create_task(runner.shutdown())
    await asyncio.sleep(0)
    allow_shutdown.set()

    try:
        await asyncio.gather(first_shutdown, second_shutdown)
        assert shutdown_event_calls == 1
        assert not runner.running
    finally:
        runner.forced_shutdown_timer.cancel()


def test_iterations(http_server, capteesys):
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
        Stage(duration=1, target=2),
        Stage(duration=1, target=2, reset_stats=True),
        Stage(duration=1, target=4),
        Stage(duration=1, target=0, reset_stats=True),
        Stage(duration=1, target=10),
    ]
    # elapsed, expected user count, expected stage index, expected reset_stats
    expected_results = [
        (0, 0, 0, False),
        (0.1, 1, 0, False),  # ceil avoids 0 users at the start
        # user count in the second stage is flat
        (1, 2, 0, False),
        (1.5, 2, 1, False),
        (2, 2, 1, False),
        # halfway through the third stage, and we just finished a reset_stats=True stage:
        (2.5, 3, 2, True),
        (3, 4, 2, False),
        # halfway through ramp down:
        (3.5, 2, 3, False),
        (4, 0, 3, False),
        # ramp up again after reaching 0, and we just finished a reset_stats=True stage:
        (4.5, 5, 4, True),
        (4.65, 7, 4, False),
        (5, 10, 4, False),
        (5.1, None, -1, False),  # finished
    ]
    current_stage_index = 0
    for elapsed, expected_users, expected_stage_index, reset_stats in expected_results:
        result = desired_user_count_stage_index_reset_stats(stages, elapsed, current_stage_index)
        assert result == (expected_users, expected_stage_index, reset_stats)
        current_stage_index = result[1]

    # fast rampup
    assert desired_user_count_stage_index_reset_stats([Stage(0, 100, True), Stage(1, 100)], 0.001, 0) == (100, 1, True)


def test_current_user_count_gauge(http_server, monkeypatch):
    class TestUser(HttpUser):
        async def run(self):
            await asyncio.sleep(0.01)

    gauge_values = []

    def _record_gauge(value, attributes=None, context=None):
        gauge_values.append(value)

    monkeypatch.setattr("aiolocust.runner.current_users_gauge.set", _record_gauge)
    Runner([TestUser], user_count=2, duration=1).run_test()

    assert gauge_values[0] == 0
    assert max(gauge_values) == 2


def test_base_url_gets_removed_from_request_name(http_server, capteesys):
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


def test_duration_shorter_than_rampup(http_server, capteesys):
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


def test_rate_limiting(http_server, capteesys):
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


def test_rate_limiting_doesnt_block_shutdown(http_server, capteesys):
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
        async def on_start(self):
            await super().connect()
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
    assert received.qsize() >= 20

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


@pytest.mark.parametrize(("user_count", "iterations", "event_loops"), [(1, 1, 1), (2, 1, 2), (4, 3, 2)])
async def test_websocket_user_iteration_exit(websocket_server, capteesys, caplog, user_count, iterations, event_loops):
    received = asyncio.Queue()

    async def handler(ws):
        # engineIO type handshake
        await ws.send('0{"sid": "abcd123"}')
        async for message in ws:
            await received.put(message)
            if message == "40":
                await ws.send('40{"sid":"something"}')

    url = await websocket_server(handler)

    class MyUser(WebsocketUser):
        async def on_start(self):
            await super().connect()
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
            await self.websocket.send(f'42["ping",{{"ts":{time.time_ns()}}}]')
            await asyncio.sleep(0.1)

    runner = Runner(
        [MyUser], user_count=user_count, iterations=iterations, duration=4, host=url, event_loops=event_loops
    )
    start = time.monotonic()
    with caplog.at_level("INFO", logger="aiolocust.runner"):
        await runner.run_test_async()
    assert time.monotonic() - start < 3
    assert f"Shutting down (reached iteration limit ({iterations}))" in caplog.text
    assert runner.iteration_counter.value == iterations
    output, err = capteesys.readouterr()
    assert err == ""
    assert "Summary" in output

    messages = [received.get_nowait() for _ in range(received.qsize())]
    assert messages.count("40") == iterations
    assert_search(rf" websocket.connect[ ]+│[ ]+{iterations}.*\(0.0%\)", output)
    assert_search(rf" websocket.40[ ]+│[ ]+{iterations}.*\(0.0%\)", output)


async def test_websocket_iteration_limit_with_regular_user(websocket_server, caplog):
    async def handler(ws):
        async for message in ws:
            await ws.send(message)

    url = await websocket_server(handler)

    class SocketUser(WebsocketUser):
        async def receive(self):
            async for message in self.websocket:
                pass

        async def send(self):
            while True:
                await self.websocket.send("ping")
                await asyncio.sleep(0.01)

    completed = []

    class RegularUser(User):
        async def run(self):
            await asyncio.sleep(0.05)
            completed.append(True)

    runner = Runner([SocketUser, RegularUser], user_count=2, iterations=3, duration=4, host=url, event_loops=2)
    start = time.monotonic()
    with caplog.at_level("INFO", logger="aiolocust.runner"):
        await runner.run_test_async()
    assert time.monotonic() - start < 3
    assert "Shutting down (reached iteration limit (3))" in caplog.text
    assert runner.iteration_counter.value == 3
    assert len(completed) == 2


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


async def test_websocket_combo(http_server, websocket_server, capteesys):
    async def handler(ws):
        async for message in ws:
            await ws.send(message)

    url = await websocket_server(handler)

    class CombinedUser(HttpUser, WebsocketUser):
        async def on_start(self):
            await self.connect(url)

        async def receive(self):
            async for message in self.websocket:
                pass

        async def send(self):
            async with self.client.get("http://localhost:8081/") as resp:
                pass  # in a real test this probably would loop, but once is enough for us

    runner = Runner([CombinedUser], iterations=1, host=url)
    await runner.run_test_async()
    output, err = capteesys.readouterr()
    assert err == ""
    assert "http://localhost:8081/" in output


async def test_websocket_with_failure(http_server, websocket_server, caplog, capteesys):
    async def handler(ws):
        async for message in ws:
            await ws.send(message)

    url = await websocket_server(handler)

    class CombinedUser(HttpUser, WebsocketUser):
        async def receive(self):
            async for message in self.websocket:
                pass

        async def send(self):
            async with self.client.get("http://localhost:8081/") as resp:
                assert "nope" in await resp.text()

    runner = Runner([CombinedUser], iterations=1, host=url)
    with caplog.at_level("INFO", logger="aiolocust.runner"):
        await runner.run_test_async()
    print(caplog.text)
    assert "Shutting down (reached iteration limit (1))" in caplog.text
    output, err = capteesys.readouterr()
    assert "Traceback" not in output
    assert "assert 'nope' in " in output
