import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, ParamSpec

from aiolocust.datatypes import Request

if TYPE_CHECKING:
    from aiolocust.runner import Runner  # noqa

P = ParamSpec("P")


class EventHook[**P]:
    def __init__(self):
        self._handlers: list[Callable[P, None]] = []
        self._logger = logging.getLogger(__name__)  # get logger here, once it has been initialized

    def add_listener(self, func: Callable[P, None]) -> Callable[P, None]:
        if func not in self._handlers:
            self._handlers.append(func)
        else:
            pass  # ignore duplicate listener registration
        return func

    def fire(self, *args: P.args, **kwargs: P.kwargs) -> None:
        for handler in self._handlers:
            try:
                handler(*args, **kwargs)
            except Exception as e:
                self._logger.exception(e)


startup = EventHook[[]]()
request = EventHook[[Request]]()
shutdown_requested = EventHook[["Runner"]]()
shutdown_completed = EventHook[["Runner"]]()


def _clear_handlers():
    global startup, request, shutdown_requested, shutdown_completed
    startup = EventHook[[]]()
    shutdown_requested = EventHook[["Runner"]]()
    shutdown_completed = EventHook[["Runner"]]()
