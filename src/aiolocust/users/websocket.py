import asyncio
from abc import abstractmethod
from ssl import PROTOCOL_TLS_CLIENT, SSLContext

import truststore
from opentelemetry import trace
from opentelemetry.propagate import inject
from websockets import ClientConnection, connect

from aiolocust import User, tracer


class WebsocketUser(User):
    """
    Websocket test cases are different: instead of providing the normal run() interface, we use these methods:

    connect() to establish the connection, creating self.websocket

    on_connect() to do any initial communication before the send and receive loops start

    receive() to read messages received by the client

    send() to send messages, or do anything else you like to do concurrently with the receive loop.
    It stops automatically (without logging a ConnectionClosed) if the connection is closed by the server.

    The run() method still exists, but has a default implementation, tying these things together
    """

    websocket: ClientConnection = None  # type: ignore # just ensure the attribute exists

    async def on_start(self) -> None:
        await self.connect()

    async def connect(
        self, url: str | None = None, ssl: SSLContext | None = None, additional_headers: dict | None = None
    ) -> None:
        url = url or self.host
        assert url, "No url specified"
        if not ssl and url.startswith("wss://"):
            ssl = truststore.SSLContext(PROTOCOL_TLS_CLIENT)
        with tracer.start_as_current_span(
            "websocket.connect",
            kind=trace.SpanKind.CLIENT,
        ) as span:
            span.set_attribute("url.full", url)
            span.set_attribute("network.protocol.name", "websocket")
            additional_headers = additional_headers or {}
            inject(additional_headers)
            try:
                self.websocket = await connect(url, additional_headers=additional_headers, ssl=ssl)
            except Exception:
                span.set_status(trace.StatusCode.ERROR, "woop")
                raise

    @abstractmethod
    async def receive(self) -> None: ...

    @abstractmethod
    async def send(self) -> None: ...

    async def run(self) -> None:
        try:
            await self.on_start()
            assert self.websocket, "No websocket was created during on_start!"
            async with asyncio.TaskGroup() as group:
                tasks = [
                    group.create_task(self.receive()),
                    group.create_task(self.send()),
                ]
                if self.runner:
                    tasks.append(group.create_task(self.runner.wait_for_shutdown()))
                _, pending = await asyncio.wait(
                    tasks,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in pending:
                    task.cancel()
        finally:
            if self.websocket:
                await self.websocket.close()
