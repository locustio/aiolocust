import asyncio
import time
from typing import cast

import pytest
from opentelemetry.trace import StatusCode

from aiolocust import WebsocketUser


async def user_loop(user_instance):  # this is basically copied from runner.user_loop()
    async with user_instance.cm():
        await user_instance.run()


async def test_basic(websocket_server, span_exporter):
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

    class BasicUser(WebsocketUser):
        host = url

        async def on_connect(self):
            message = cast(str, await self.websocket.recv())
            assert message.startswith('0{"sid"')
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
            for _ in range(5):  # usually this is just a while True but here we want the run to terminate
                await self.websocket.send(f'42["ping",{{"ts":{time.time_ns()}}}]')
                await asyncio.sleep(0.1)

    await user_loop(BasicUser())

    spans = span_exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "websocket.connect"


async def test_websocket_user_bad_host(span_exporter):
    class BasicUser(WebsocketUser):
        host = "ws://localhost:6666"

        async def receive(self):
            pass

        async def send(self):
            pass

    with pytest.raises(OSError):
        await user_loop(BasicUser())

    spans = span_exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "websocket.connect"
    assert spans[0].status.status_code == StatusCode.ERROR
