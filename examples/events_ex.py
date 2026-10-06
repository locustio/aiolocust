import threading
import time

from aiolocust import HttpUser, Runner, events


class MyUser(HttpUser):
    async def run(self):
        async with self.client.get("http://localhost:8080/"):
            pass


@events.request.add_listener
def to_stdout(name: str, ttlb: float, error: str | None) -> None:
    print(f"Request: {name}, TTLB: {ttlb:.3f}s, Error: {error}")


lock = threading.Lock()
f = open("requests.csv", "a", buffering=1)


@events.request.add_listener
def to_csv(name: str, ttlb: float, error: str | None) -> None:
    with lock:
        f.write(f"{name},{ttlb:.3f},{error}\n")


@events.shutdown_requested.add_listener
async def on_shutdown_request(runner: Runner) -> None:
    print(f"Shutdown requested, {runner.iteration_counter.value} iterations")


@events.shutdown_completed.add_listener
async def on_shutdown_complete(runner: Runner) -> None:
    print(runner.start_time - 1)
    print(time.time() + 2)
    print("Shutdown completed, no new requests can ever happen after this point")
