import asyncio
import ctypes
import sys
import threading
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, Callable
from collections.abc import Coroutine as AbcCoroutine
from contextlib import asynccontextmanager
from functools import wraps
from importlib.metadata import version
from typing import Any, Concatenate, ParamSpec, TypeVar

from opentelemetry import trace
from pyrate_limiter import Duration, Limiter, Rate, StateBucket, TokenBucket

try:
    from uvloop import new_event_loop as new_limiter_event_loop
except ImportError:
    new_limiter_event_loop = asyncio.new_event_loop

tracer = trace.get_tracer("aiolocust", version("aiolocust"))


class User(ABC):
    host: str | None = None

    def __init__(self, runner: Runner | None = None) -> None:
        if runner and runner.host:
            self.host = runner.host
        self.runner: Runner = runner  # pyright: ignore[reportAttributeAccessIssue] # always set outside of unit testing
        self.running = True

    @abstractmethod
    def run(self) -> AbcCoroutine[Any, Any, None]: ...

    @asynccontextmanager
    async def cm(self) -> AsyncGenerator[None]:
        """Override this method if you need an async context manager around the run method"""
        yield


from aiolocust.runner import Runner
from aiolocust.users.http import HttpUser, LocustClientSession  # type: ignore # noqa: F401
from aiolocust.users.websocket import WebsocketUser  # type: ignore # noqa: F401


class _LimiterPortal:
    def __init__(self, rate: Rate) -> None:
        self._loop = new_limiter_event_loop()
        self._ready = threading.Event()
        self._timer_resolution_requested = False
        self._winmm: Any = getattr(ctypes, "windll").winmm if sys.platform == "win32" else None
        if self._winmm is not None:
            self._timer_resolution_lock = threading.Lock()

        self._thread = threading.Thread(
            target=self._run,
            args=(rate,),
            daemon=True,
        )
        self._thread.start()
        self._ready.wait()

    def _run(self, rate: Rate) -> None:
        asyncio.set_event_loop(self._loop)
        self._limiter = Limiter(
            StateBucket([rate], algorithm=TokenBucket()),
            buffer_ms=1,  # The default retry buffer is 50 ms, which throttles high rates.
        )
        self._shutdown = asyncio.Event()
        self._ready.set()
        self._loop.run_forever()

    async def acquire(self):
        if self._winmm is not None:
            with self._timer_resolution_lock:
                if self._winmm is not None and not self._timer_resolution_requested:
                    # Windows asyncio timers need 1ms resolution to sustain high-rate permits.
                    self._timer_resolution_requested = self._winmm.timeBeginPeriod(1) == 0
        future = asyncio.run_coroutine_threadsafe(self._acquire(), self._loop)
        return await asyncio.shield(asyncio.wrap_future(future))

    async def _acquire(self) -> bool:
        # we race these two tasks against eachother to avoid waiting for try_aquire_async
        # during shutdown, because that will take a long time if there are a lot of queued iterations
        acquire = asyncio.create_task(self._limiter.try_acquire_async("global"))
        shutdown = asyncio.create_task(self._shutdown.wait())
        done, _ = await asyncio.wait(
            {acquire, shutdown},
            return_when=asyncio.FIRST_COMPLETED,
        )

        if shutdown in done:
            acquire.cancel()
            await asyncio.gather(acquire, return_exceptions=True)
            return False

        shutdown.cancel()
        await asyncio.gather(shutdown, return_exceptions=True)
        return acquire.result()

    def request_shutdown(self) -> None:
        if self._winmm is not None:
            with self._timer_resolution_lock:
                winmm = self._winmm
                if winmm is not None and self._timer_resolution_requested:
                    winmm.timeEndPeriod(1)
                    self._timer_resolution_requested = False
                self._winmm = None
        self._loop.call_soon_threadsafe(self._shutdown.set)

    def close(self) -> None:
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join()


def rate_limit(rate: int, duration: int | Duration = Duration.SECOND, burst: int = 3):
    limiter = _LimiterPortal(Rate(rate, duration, burst))
    P = ParamSpec("P")
    R = TypeVar("R")
    UserT = TypeVar("UserT", bound="User")
    if rate / int(duration) > 1:
        print("Warning: rate_limit does not work well for very high throughputs (>1000/s)")

    def decorate(
        func: Callable[Concatenate[UserT, P], AbcCoroutine[Any, Any, R]],
    ) -> Callable[Concatenate[UserT, P], AbcCoroutine[Any, Any, R | None]]:
        @wraps(func)
        async def wrapper(self: UserT, *args: P.args, **kwargs: P.kwargs) -> R | None:
            if await limiter.acquire() and self.running:
                return await func(self, *args, **kwargs)
            limiter.request_shutdown()  # this will stop any concurrent aquire calls

        return wrapper

    return decorate
