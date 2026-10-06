from collections import defaultdict
from threading import Lock

MAX_ERROR_KEYS = 200
error_counter: dict[str, int] = defaultdict(int)
error_counter_lock = Lock()


def record_error(message: str) -> None:
    with error_counter_lock:
        if message not in error_counter and len(error_counter) >= MAX_ERROR_KEYS:
            message = "OTHER"
        error_counter[message] += 1


def clear() -> None:
    error_counter.clear()
