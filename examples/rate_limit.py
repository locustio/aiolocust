from pyrate_limiter import Duration

from aiolocust import HttpUser, rate_limit


class MyUser(HttpUser):
    @rate_limit(10, Duration.SECOND)
    async def run(self):
        async with self.client.get("http://localhost:8081/") as resp:
            pass
