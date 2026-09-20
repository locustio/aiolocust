from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, Callable
from collections.abc import Coroutine as AbcCoroutine
from contextlib import asynccontextmanager
from functools import wraps
from threading import Lock
from typing import TYPE_CHECKING, Any, Concatenate, ParamSpec, TypeVar

P = ParamSpec("P")
R = TypeVar("R")
UserT = TypeVar("UserT", bound="User")

from pyrate_limiter import Duration, Limiter, Rate, StateBucket, TokenBucket


class User(ABC):
    def __init__(self, runner: Runner | None = None, **kwargs: dict[str, Any]) -> None:
        self.runner: Runner = runner  # pyright: ignore[reportAttributeAccessIssue] # always set outside of unit testing
        self.running = True

    @abstractmethod
    def run(self) -> AbcCoroutine[Any, Any, None]: ...

    @asynccontextmanager
    async def cm(self) -> AsyncGenerator[None]:
        """Override this method if you need an async context manager around the run method"""
        yield


if TYPE_CHECKING:
    from aiolocust.runner import Runner
    from aiolocust.users.http import HttpUser, LocustClientSession


def __getattr__(name):
    # prevent early load of these classes, because they in turn might trigger otel setup
    if name == "HttpUser":
        from aiolocust.users.http import HttpUser

        return HttpUser
    elif name == "LocustClientSession":
        from aiolocust.users.http import LocustClientSession

        return LocustClientSession
    elif name == "Runner":
        from aiolocust.runner import Runner

        return Runner

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


lock = Lock()

import asyncio
import threading


class LimiterPortal:
    def __init__(self, rate: Rate) -> None:
        self._loop = asyncio.new_event_loop()
        self._ready = threading.Event()

        self._thread = threading.Thread(
            target=self._run,
            args=(rate,),
            daemon=True,
        )
        self._thread.start()
        self._ready.wait()

    def _run(self, rate: Rate) -> None:
        asyncio.set_event_loop(self._loop)
        self._limiter = Limiter(StateBucket([rate], algorithm=TokenBucket()))
        self._shutdown = asyncio.Event()
        self._ready.set()
        self._loop.run_forever()

    async def acquire(self):
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
        self._loop.call_soon_threadsafe(self._shutdown.set)

    def close(self) -> None:
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join()


def rate_limit(rate: int, duration: int | Duration = Duration.SECOND, burst: int = 3):
    limiter = LimiterPortal(Rate(rate, duration, burst))

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


__all__ = ["User", "HttpUser", "LocustClientSession", "Runner", "rate_limit"]
