import os

from typer.testing import CliRunner

from aiolocust import events
from aiolocust.main import app


def _timeout_handler(_signum, _frame):
    if bool(os.environ.get("VSCODE_CLI")):
        pass  # disable timeout when debugging
    else:
        raise TimeoutError("test timed out after 10 seconds")


def invoke(tmp_path, locustfile: str, *args, **kwargs):
    runner = CliRunner()
    with open(tmp_path / "locustfile.py", "w") as f:
        f.write(locustfile)
    output = runner.invoke(app, [str(tmp_path / "locustfile.py"), *args], **kwargs)
    print(output)
    return output


def test_main(http_server, tmp_path):  # noqa: ARG001
    result = invoke(
        tmp_path,
        """
from aiolocust import HttpUser

class MyUser(HttpUser):
    async def run(self):
        async with self.client.get("http://localhost:8081/") as resp:
            pass
    """,
        "--iterations",
        "3",
        "-u",
        "2",
    )
    assert "http://localhost:" in result.output
    assert "0 (0.0%)" in result.output
    assert result.exit_code == 0


def test_run_method(http_server, tmp_path):  # noqa: ARG001
    result = invoke(
        tmp_path,
        """
async def run(user):
    async with user.client.get("http://localhost:8081/") as resp:
        pass
""",
        "--iterations",
        "3",
        "-u",
        "2",
    )
    assert "http://localhost:" in result.output
    assert " 3 " in result.output
    assert "0 (0.0%)" in result.output
    assert result.exit_code == 0


def test_on_start_and_shutdown(tmp_path):  # noqa: ARG001
    try:
        result = invoke(
            tmp_path,
            """
from aiolocust import HttpUser, events

started = False

@events.startup.add_listener
async def on_start(runner):
    global started
    started = True
    print("foo")

@events.shutdown_requested.add_listener
async def on_shutdown_request_crashing(runner):
    raise Exception("this exception will be logged, but mustn't prevent shutdown")

@events.shutdown_requested.add_listener
async def on_shutdown_request(runner):
    print("bar")
    assert runner.running
    assert started
    print(runner.iteration_counter.value)

@events.shutdown_completed.add_listener
async def on_shutdown_complete(runner):
    assert not runner.running
    print("baz")

class MyUser(HttpUser):
    async def run(self):
        if started:
            print("xxx")
        else:
            print("on_start didn't seem to run?")
""",
            "--iterations",
            "42",
        )
    finally:
        events._clear_handlers()
    assert "xxx" in result.output
    assert not "on_start didn't" in result.output
    assert "foo" in result.output
    assert "bar" in result.output
    assert "42" in result.output
    assert "baz" in result.output
    # this will end up being logged to pytest
    # assert "this exception will be logged, but mustn't prevent shutdown" in result.output
    assert result.exit_code == 0


def test_html_report(http_server, tmp_path):  # noqa: ARG001
    result = invoke(
        tmp_path,
        """
async def run(user):
    async with user.client.get("http://localhost:8081/") as resp:
        pass
""",
        "--iterations",
        "3",
        "-u",
        "2",
        "--html-report",
        tmp_path / "reports/report.html",
    )
    assert "http://localhost:" in result.output
    assert "0 (0.0%)" in result.output
    assert result.exit_code == 0
    assert result.output.count("http://localhost:") == 1  # no accidental duplicate print
    with open(tmp_path / "reports/report.html") as report:
        html = report.read()
    assert "<!DOCTYPE html>" in html
    assert "http://localhost:" in html
    assert "target user count: 2" in html
    assert "Total" in html


def test_relative_import_in_module(tmp_path):
    runner = CliRunner()
    os.makedirs(tmp_path / "mytests")
    with open(tmp_path / "mytests/__init__.py", "w") as f:
        f.write("")
    with open(tmp_path / "mytests/helper.py", "w") as f:
        f.write(
            """
async def run(user):
pass
"""
        )
    with open(tmp_path / "mytests/my_locustfile.py", "w") as f:
        f.write(
            """
from .helper import run
"""
        )

        result = runner.invoke(app, [str(tmp_path / "mytests/my_locustfile.py"), "--iterations", "1"])
        assert result.exit_code == 0


def test_config(http_server, tmp_path):  # noqa: ARG001
    result = invoke(
        tmp_path,
        """
async def run(user):
    async with user.client.get("http://localhost:8081/") as resp:
        pass
""",
        "--config",
        '{ "stages": [{ "duration": 2, "target": 2 }] }',
    )
    assert "http://localhost:" in result.output
    assert "0 (0.0%)" in result.output
    assert result.exit_code == 0
