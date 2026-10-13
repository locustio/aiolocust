# Example connecting to a Engine.IO style websocket
import asyncio
import logging
import ssl
import time
from typing import cast

import truststore
from opentelemetry import trace
from opentelemetry.propagate import inject
from websockets import ClientConnection, connect
from websockets.exceptions import ConnectionClosed

from aiolocust import User, tracer

logger = logging.getLogger("aiolocust")


# There's no auto instrumentation for websockets so we build it ourselves
async def traced_connect(url: str, additional_headers=None, ssl=None) -> ClientConnection:
    with tracer.start_as_current_span(
        "websocket.connect",
        kind=trace.SpanKind.CLIENT,
    ) as span:
        span.set_attribute("url.full", url)
        span.set_attribute("network.protocol.name", "websocket")
        additional_headers = dict(additional_headers) if additional_headers else {}
        inject(additional_headers)
        return await connect(url, additional_headers=additional_headers, ssl=ssl)


class MyUser(User):
    async def run(self):
        assert self.runner.host  # use --host parameter please
        websocket = await traced_connect(
            self.runner.host,  # If you want to trust system certs for wss, install truststore
            ssl=truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT) if self.runner.host.startswith("wss://") else None,
        )
        message = cast(str, await websocket.recv())
        assert message.startswith('0{"sid"')
        with tracer.start_as_current_span("websocket.40", kind=trace.SpanKind.CLIENT) as span:
            span.set_attribute("network.protocol.name", "websocket")
            await websocket.send("40")
            message = cast(str, await websocket.recv())
            assert message.startswith("40")

        async def receive():
            async for message in websocket:
                s = str(message)
                if s == "2":
                    await websocket.send("3")  # heartbeat response
                elif s.startswith("42["):
                    pass
                else:
                    logger.error(f"Unknown message received: {s}")

        async def send():
            try:
                while True:
                    await websocket.send(f'42["ping",{{"ts":{time.time_ns()}}}]')
                    await asyncio.sleep(0.1)
            except ConnectionClosed:
                return

        try:
            async with asyncio.TaskGroup() as group:
                tasks = [
                    group.create_task(receive()),
                    group.create_task(send()),
                    group.create_task(self.runner.wait_for_shutdown()),
                ]
                _done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for task in pending:
                    task.cancel()
        finally:
            await websocket.close()
