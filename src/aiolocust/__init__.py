from abc import ABC, abstractmethod
from collections.abc import Callable
from collections.abc import Coroutine as AbcCoroutine
from contextlib import asynccontextmanager
from functools import wraps
from threading import Lock
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar, cast

P = ParamSpec("P")
R = TypeVar("R")
from pyrate_limiter import Duration, Limiter, Rate, StateBucket, TokenBucket


class User(ABC):
    def __init__(self, runner: Runner | None = None, **kwargs):
        self.runner: Runner = runner  # pyright: ignore[reportAttributeAccessIssue] # always set outside of unit testing
        self.running = True

    @abstractmethod
    def run(self) -> AbcCoroutine[Any, Any, None]: ...

    @asynccontextmanager
    async def cm(self):
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
    def __init__(self, rate: Rate):
        self._loop = asyncio.new_event_loop()
        self._ready = threading.Event()

        self._thread = threading.Thread(
            target=self._run,
            args=(rate,),
            daemon=True,
        )
        self._thread.start()
        self._ready.wait()

    def _run(self, rate: Rate):
        asyncio.set_event_loop(self._loop)

        # Limiter is created and used only on this event loop.
        self._limiter = Limiter(StateBucket([rate], algorithm=TokenBucket()))

        self._ready.set()
        self._loop.run_forever()

    async def acquire(self, key: str = "global"):
        # Schedule the actual Pyrate acquisition on the dedicated loop.
        future = asyncio.run_coroutine_threadsafe(
            self._limiter.try_acquire_async(key),
            self._loop,
        )

        # Convert concurrent.futures.Future into something
        # awaitable from the caller's event loop.
        return await asyncio.wrap_future(future)

    def close(self):
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join()


def rate_limit(rate: int, duration: int | Duration = Duration.SECOND, burst: int = 2):
    limiter = LimiterPortal(Rate(rate, duration, burst))

    def decorator(func: Callable[P, AbcCoroutine[Any, Any, R]]):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            await limiter.acquire()
            return await func(*args, **kwargs)

        return cast(Callable[P, AbcCoroutine[Any, Any, R]], async_wrapper)

    return decorator


__all__ = ["User", "HttpUser", "LocustClientSession", "Runner", "rate_limit"]
