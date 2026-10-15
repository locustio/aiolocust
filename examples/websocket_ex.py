# Example connecting to a Engine.IO style websocket
import asyncio
import time
from typing import cast

from opentelemetry import trace

from aiolocust import HttpUser, WebsocketUser, tracer


class MyUser(WebsocketUser, HttpUser):
    host = "ws://localhost:1234"

    # by default, connect() just connects the websocket, but if you want, you can override it too
    async def connect(self) -> None:
        async with self.client.post("http://localhost:8081/") as resp:
            js = await resp.json()
            access_token = js["access_token"]
            token_type = js["token_type"]
        self.additional_headers = {"Authorization": f"{token_type} {access_token}"}

        # now actually do the connect, using host specified with attribute or --host, and self.additional_headers
        await super().connect()

        message = cast(str, await self.websocket.recv())
        assert message.startswith('0{"sid"')

        # here's how to record a span (which in turn gets added to request metrics & command line output)
        with tracer.start_as_current_span("websocket.40", kind=trace.SpanKind.CLIENT) as span:
            span.set_attribute("network.protocol.name", "websocket")
            await self.websocket.send("40")
            message = cast(str, await self.websocket.recv())
            assert message.startswith("40")

    # receive() and send() will run concurrently until the Runner is stopped (or one of them exits)
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
