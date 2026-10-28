import asyncio
from abc import abstractmethod
from collections.abc import Mapping
from ssl import PROTOCOL_TLS_CLIENT, DER_cert_to_PEM_cert, SSLContext

import truststore
from opentelemetry import trace
from opentelemetry.propagate import inject
from opentelemetry.util.types import AnyValue
from websockets import ClientConnection, connect

from aiolocust import User, tracer
from aiolocust.runner import EXPECTED_ERRORS


class WebsocketUser(User):
    """
    Websocket test cases are different: instead of providing the normal run() interface, subclasses typically implement these methods:

    on_start() to establish the connection (if you need to do auth before connecting or some initial WS communication afterwards)

    receive() to read messages received by the client

    send() to send messages, or do anything else you like to do concurrently with the receive loop.
    It stops automatically (without logging a ConnectionClosed) if the connection is closed by the server.

    The run() method ties these things together. Unlike other User classes you don't typically override it.
    """

    websocket: ClientConnection = None  # type: ignore # just ensure the attribute exists

    async def on_start(self) -> None:
        await self.connect()

    async def connect(
        self,
        url: str | None = None,
        ssl: SSLContext | None = None,
        additional_headers: dict | None = None,
        additional_attributes: Mapping[str, AnyValue] | None = None,
        **kwargs,
    ) -> None:
        url = url or self.host
        assert url, "No url specified"
        if not ssl and url.startswith("wss://"):
            ssl = truststore.SSLContext(PROTOCOL_TLS_CLIENT)
            ssl.check_hostname = False

        with tracer.start_as_current_span(
            "websocket.connect",
            kind=trace.SpanKind.CLIENT,
        ) as span:
            span.set_attribute("url.full", url)
            span.set_attribute("network.protocol.name", "websocket")
            if additional_attributes:
                span.set_attributes(additional_attributes)
            additional_headers = additional_headers or {}
            inject(additional_headers)
            self.websocket = await connect(
                url,
                additional_headers=additional_headers,
                ssl=ssl,
                **kwargs,
            )

    @abstractmethod
    async def receive(self) -> None: ...

    @abstractmethod
    async def send(self) -> None: ...

    # Wrappers made to ignore expected errors like assertions, response errors, etc. Like in Runner.user_loop()
    async def _receive(self) -> None:
        try:
            await self.receive()
        except EXPECTED_ERRORS:
            pass

    async def _send(self) -> None:
        try:
            await self.send()
        except EXPECTED_ERRORS:
            pass

    async def run(self) -> None:
        try:
            await self.on_start()
            assert self.websocket, "No websocket was created during on_start!"
            async with asyncio.TaskGroup() as group:
                tasks = [
                    group.create_task(self._receive()),
                    group.create_task(self._send()),
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


class _CertificatePrinterClientConnection(ClientConnection):  # type: ignore
    """if debugging ssl certs, you can pass this to as create_connection parameter to connect()"""

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        super().connection_made(transport)
        ssl_object = transport.get_extra_info("ssl_object")
        if ssl_object is not None:
            certificate = ssl_object.getpeercert(binary_form=True)
            if certificate is not None:
                print(DER_cert_to_PEM_cert(certificate))
