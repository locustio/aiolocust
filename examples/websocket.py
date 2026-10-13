# Example connecting to a Engine.IO style websocket
import asyncio
import time
from typing import cast

from opentelemetry import trace

from aiolocust import tracer
from aiolocust.users.websocket import WebsocketUser


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
