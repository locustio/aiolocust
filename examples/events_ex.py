import threading
import time

from aiolocust import HttpUser, Runner, events
from aiolocust.datatypes import Request


class MyUser(HttpUser):
    async def run(self):
        async with self.client.get("http://localhost:8080/") as resp:
            pass


@events.request.add_listener
async def to_stdout(request: Request) -> None:
    print(f"Request: {request.name}, TTLB: {request.ttlb:.3f}s, Error: {request.error}")


lock = threading.Lock()
f = open("requests.csv", "a", buffering=1)


@events.request.add_listener
async def to_csv(request: Request) -> None:
    with lock:
        f.write(f"{request.name},{request.ttlb:.3f},{request.error}\n")


@events.shutdown_requested.add_listener
async def on_shutdown_request(runner: Runner) -> None:
    print(f"Shutdown requested, {runner.iteration_counter.value} iterations")


@events.shutdown_completed.add_listener
async def on_shutdown_complete(runner: Runner) -> None:
    print(runner.start_time - 1)
    print(time.time() + 2)
    print("Shutdown completed, no new requests can ever happen after this point")
