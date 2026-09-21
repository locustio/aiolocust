from aiolocust import HttpUser, Runner, events


class MyUser(HttpUser):
    async def run(self):
        async with self.client.get("http://localhost/"):
            pass


@events.shutdown_completed.add_listener
async def validate_thresholds(runner: Runner) -> None:
    if runner.total_stats.error_percentage > 10:
        print(f"Total error percentage exceeded threshold: {runner.total_stats.error_percentage:.2f}% > 10%")
        runner.exit_code = 42
