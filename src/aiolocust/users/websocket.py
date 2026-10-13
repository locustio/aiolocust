import asyncio
import ssl
from abc import abstractmethod

import truststore
from opentelemetry import trace
from opentelemetry.propagate import inject
from websockets import ClientConnection, ConnectionClosed, connect

from aiolocust import User, tracer


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


class WebsocketUser(User):
    """
    Websocket test cases are different: instead of providing the normal run() interface, we need three methods:

    connect() to establish the connection, creating self.websocket (but the default implementation is often fine)

    receive() to read messages received by the client

    send() to send messages, or do anything else you like to do concurrently with the receive loop.
    It stops automatically (without logging a ConnectionClosed) if the connection is closed by the server.

    The run() method still exists, but has a default implementation.
    """

    websocket: ClientConnection

    async def connect(self):
        assert self.runner.host
        self.websocket = await traced_connect(
            self.runner.host,
            ssl=truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT) if self.runner.host.startswith("wss://") else None,
        )

    @abstractmethod
    async def receive(self) -> None: ...

    @abstractmethod
    async def send(self) -> None: ...

    async def _send_wrapper(self) -> None:
        try:
            await self.send()
        except ConnectionClosed:
            return

    async def run(self) -> None:
        await self.connect()
        try:
            async with asyncio.TaskGroup() as group:
                tasks = [
                    group.create_task(self.receive()),
                    group.create_task(self.send()),
                    group.create_task(self.runner.wait_for_shutdown()),
                ]
                _, pending = await asyncio.wait(
                    tasks,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in pending:
                    task.cancel()
        finally:
            await self.websocket.close()
