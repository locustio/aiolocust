# Example connecting to a Engine.IO style websocket
import asyncio
import ssl
import time
from typing import cast

import truststore
from opentelemetry import trace

from aiolocust import tracer
from aiolocust.users.websocket import WebsocketUser, traced_connect


class MyUser(WebsocketUser):
    async def connect(self):
        # typically we'd rely on the base class implementation, but here we want to do some initial communication
        # before starting the send loop

        assert self.runner.host  # use --host parameter please
        self.websocket = await traced_connect(
            self.runner.host,  # If you want to trust system certs for wss, install truststore
            ssl=truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT) if self.runner.host.startswith("wss://") else None,
        )
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
