import inspect
from abc import ABC, abstractmethod
from collections.abc import Callable
from collections.abc import Coroutine as AbcCoroutine
from contextlib import asynccontextmanager
from functools import wraps
from threading import Lock
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar, cast

P = ParamSpec("P")
R = TypeVar("R")
from pyrate_limiter import BucketAsyncWrapper, Duration, InMemoryBucket, Limiter, Rate


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


def rate_limit(rate: int, duration: Duration = Duration.SECOND):
    def decorator(func: Callable[P, AbcCoroutine[Any, Any, R]]):
        limiter = None

        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            nonlocal limiter
            with lock:
                if limiter is None:
                    limiter = Limiter(BucketAsyncWrapper(InMemoryBucket([Rate(rate, duration)])))
            await limiter.try_acquire_async(name=func.__qualname__)
            return await func(*args, **kwargs)

        return cast(Callable[P, AbcCoroutine[Any, Any, R]], async_wrapper)

    return decorator


__all__ = ["User", "HttpUser", "LocustClientSession", "Runner", "rate_limit"]
