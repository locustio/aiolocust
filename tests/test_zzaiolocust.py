# the filename of these tests is because pytest runs in alphabetical order and we want lower level tests to run first
import asyncio
import json
import logging
import os
import re
import signal
import unittest
from asyncio.subprocess import Process
from shlex import quote, split
from tempfile import TemporaryDirectory
from weakref import proxy

from utils import assert_search

from aiolocust.otel import _weakrefAttributeFilter

if os.name == "nt":
    from subprocess import CREATE_NEW_PROCESS_GROUP

    WINDOWS_DELAY = 1
    # this is necessary because otherwise CTRL_C_EVENT is sent to all processes in the same group, including pytest itself
    creationflags = CREATE_NEW_PROCESS_GROUP
else:
    WINDOWS_DELAY = 0
    creationflags = 0


async def exec(cmd: str, **kwargs) -> Process:
    args = split(cmd, posix=os.name != "nt")
    if os.name == "nt":
        # Non-POSIX shlex keeps surrounding quotes; remove them without changing Windows backslashes.
        for index, arg in enumerate(args):
            if len(arg) >= 2 and arg[0] == arg[-1] and arg[0] in ("'", '"'):
                args[index] = arg[1:-1]
    return await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        **kwargs,
    )


def test_otel_log_filter_for_weakref():
    # When logging something in the context of a websocket, otel will sometimes log warnings.
    class Connection:
        pass

    connection = Connection()
    record = logging.LogRecord("websockets.client", logging.DEBUG, __file__, 1, "message", (), None)
    setattr(record, "websocket", proxy(connection))
    setattr(record, "ordinary_attribute", "kept")

    assert _weakrefAttributeFilter().filter(record)
    assert getattr(record, "websocket") == "weakref.ProxyType (converted)"
    assert getattr(record, "ordinary_attribute") == "kept"


async def test_websocket_example(websocket_server, http_server):
    received = asyncio.Queue()

    async def handler(ws):
        # engineIO type handshake
        await ws.send('0{"sid": "abcd123"}')
        async for message in ws:
            await received.put(message)
            if message == "40":
                await ws.send("40")

    url = await websocket_server(handler)
    proc = await exec(
        f"aiolocust examples/websocket_ex.py -d 1 -H {url} -i 1",  # iteration limit is just to reduce spam in case of errors
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=4)
    except TimeoutError:
        proc.kill()
        stdout, stderr = await proc.communicate()
        err = stderr.decode(errors="replace")
        print(err)
        output = stdout.decode(errors="replace")
        print(output)
        raise AssertionError("process never terminated") from None
    else:
        err = stderr.decode(errors="replace")
        print(err)
        assert "ERROR" not in err
        assert "Shutting down" in err
        output = stdout.decode(errors="replace")
        print(output)
        assert " websocket.connect " in output
        assert "Summary" in output
        assert_search(r" websocket\.connect +[│|] +1 +[│|] +0 \(0\.0%\) ", output)
        assert await proc.wait() == 0
        try:
            assert received.get_nowait() == "40"
        except asyncio.QueueEmpty:
            raise Exception("WS server received no messages!")
        # assert '"network.protocol.name": "websocket"' in output  # tracing stuff!
        assert not "unhandled error" in output


async def test_websocket_example_concurrent_users(websocket_server, http_server):
    received = asyncio.Queue()

    async def handler(ws):
        # engineIO type handshake
        await ws.send('0{"sid": "abcd123"}')
        async for message in ws:
            await received.put(message)
            if message == "40":
                await ws.send("40")

    url = await websocket_server(handler)
    proc = await exec(
        f"aiolocust examples/websocket_ex.py -d 2 -u 5 -H {url} -i 10"  # iteration limit is just to reduce spam in case of errors
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=4)
    except TimeoutError:
        proc.kill()
        stdout, stderr = await proc.communicate()
        err = stderr.decode(errors="replace")
        print(err)
        output = stdout.decode(errors="replace")
        print(output)
        raise AssertionError("process never terminated") from None
    else:
        err = stderr.decode(errors="replace")
        print(err)
        assert "ERROR" not in err
        assert "Shutting down" in err
        output = stdout.decode(errors="replace")
        print(output)
        assert " websocket.connect " in output
        assert "Summary" in output
        assert_search(r" websocket\.connect +[│|] +5 +[│|] +0 \(0\.0%\) ", output)
        assert await proc.wait() == 0
        try:
            assert received.get_nowait() == "40"
        except asyncio.QueueEmpty:
            raise Exception("WS server received no messages!")
        # assert '"network.protocol.name": "websocket"' in output  # tracing stuff!
        assert not "unhandled error" in output


async def test_websocket_example_bad_host(http_server):
    duration = 1 + WINDOWS_DELAY * 5
    proc = await exec(
        f"aiolocust examples/websocket_ex.py -d {duration} -i 1 -H ws://localhost:6666",
        env={
            "OTEL_TRACES_EXPORTER": "console",
            "OTEL_METRICS_EXPORTER": "none",
            "OTEL_LOGS_EXPORTER": "none",
            **os.environ,
        },
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=4 + WINDOWS_DELAY * 10)
    except TimeoutError:
        proc.kill()
        stdout, stderr = await proc.communicate()
        err = stderr.decode(errors="replace")
        print(err)
        output = stdout.decode(errors="replace")
        print(output)
        raise AssertionError("process never terminated") from None
    else:
        err = stderr.decode(errors="replace")
        print(err)
        assert "Shutting down" in err
        output = stdout.decode(errors="replace")
        print(output)
        assert " websocket.connect " in output
        assert "Summary" in output
        assert_search(r" websocket\.connect .* \(100\.0%\) ", output)
        assert await proc.wait() == 0
        # assert '"network.protocol.name": "websocket"' in output  # tracing stuff!
        assert not "unhandled error" in output
        assert any(f'"description": "{error_type}' in output for error_type in ("OSError", "ConnectionRefusedError"))


async def test_no_locustfile():
    with TemporaryDirectory() as tmp_dir:
        proc = await exec(
            "aiolocust",
            cwd=tmp_dir,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=6)
        err = stderr.decode(errors="replace")
        assert "Welcome to aiolocust!" in err
        assert "Create a locustfile.py" in err
        assert "Error" not in err
        assert await proc.wait() == 1


@unittest.skipIf(os.name == "nt", reason="otel instrumentation seems to have some issues with freethreading on Windows")
async def test_otel_autoinstrumentation(http_server):
    with TemporaryDirectory() as tmp_dir:
        script_path = os.path.join(tmp_dir, "my_script.py")

        with open(script_path, "w") as tempfile:
            tempfile.write("""
async def run(self):
    async with self.client.get("http://localhost:8081/") as resp:
        resp.span.set_attribute("custom.attribute", "example")
    async with self.client.get("http://localhost:8081/", name="foo") as resp:
        assert await resp.text() == "no way"
""")
        proc = await exec(
            f"aiolocust {tempfile.name} -u 5 --iterations 30 --instrument",
            env={
                "OTEL_TRACES_EXPORTER": "console",
                "OTEL_METRICS_EXPORTER": "none",
                "OTEL_LOGS_EXPORTER": "none",
                **os.environ,
            },
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=6)
        except TimeoutError:
            proc.terminate()
            stdout, stderr = await proc.communicate()
            err = stdout.decode(errors="replace")
            print(err)
            output = stdout.decode(errors="replace")
            print(output)
            raise AssertionError("process never terminated") from None
        else:
            err = stderr.decode(errors="replace")
            print(err)
            output = stdout.decode(errors="replace")
            print(output)
            assert " GET http://localhost:8081/ │    30 │    0 (0.0%) " in output
            assert " foo                        │    30 │ 30 (100.0%)" in output
            assert '"status_code": "UNSET"' in output
            assert '"status_code": "ERROR"' in output
            assert '"exception.type": "AssertionError"' in output
            assert '"trace_id":' in output
            assert '"custom.attribute": "example"' in output
            assert f'"filename": "{tempfile.name}"' in output  # ensure filename is included in resource attributes
            assert '"name": "GET"' in output  # not renamed
            assert '"name": "foo"' in output  # using explicit name
            assert await proc.wait() == 0


async def test_loglevel(http_server):
    with TemporaryDirectory() as tmp_dir:
        script_path = os.path.join(tmp_dir, "my_script.py")

        with open(script_path, "w") as tempfile:
            tempfile.write("""
import logging
logger = logging.getLogger(__name__)

async def run(user):
    logger.warning("warning level log message")
    logger.info("info level log message")
    async with user.client.get("http://localhost:8081/") as resp:
        pass
    async with user.client.get("http://localhost:8081/500") as resp:
        pass
    async with user.client.get("http://localhost:8081/503", raise_for_status=True) as resp:
        pass
""")
        proc = await exec(
            f"aiolocust {tempfile.name} --iterations 1 --log-level  warning",
            env={
                "OTEL_TRACES_EXPORTER": "console",
                **os.environ,
            },
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=4)
        except TimeoutError:
            proc.terminate()
            stdout, stderr = await proc.communicate()
            output = stdout.decode(errors="replace")
            print(output)
            raise AssertionError("process never terminated") from None
        else:
            err = stderr.decode(errors="replace")
            print(err)
            assert "warning level log message" in err
            assert "info level log message" not in err
            assert "Traceback" not in err  # raise_for_status=True shouldn't crash out completely
            output = stdout.decode(errors="replace")
            print(output)
            # assert "exception" not in err.lower()
            # test some span stuff while we are at it
            assert '"http.status": 200' in output
            assert '"http.status": 500' in output
            assert '"http.status": 503' in output
            assert not '"1970-' in output  # must be a bad timestamp scaling if this ever happens
            assert await proc.wait() == 0


async def test_loglevel_debug(http_server):
    with TemporaryDirectory() as tmp_dir:
        script_path = os.path.join(tmp_dir, "my_script.py")

        with open(script_path, "w") as tempfile:
            tempfile.write("""
async def run(user):
    async with user.client.get("http://localhost:8081/") as resp:
        pass
""")
        proc = await exec(
            f"aiolocust {tempfile.name} --iterations 1 --log-level debug",
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=4)
        except TimeoutError:
            proc.terminate()
            stdout, stderr = await proc.communicate()
            output = stdout.decode(errors="replace")
            print(output)
            raise AssertionError("process never terminated") from None
        else:
            err = stderr.decode(errors="replace")
            print(err)
            output = stdout.decode(errors="replace")
            assert "exception" not in err.lower()
            assert await proc.wait() == 0
            # this also tests that otel initialization didn't happen before log level setup
            assert "OTLP exporter for 'http/protobuf' is not available" in err


async def test_host_param(http_server):
    with TemporaryDirectory() as tmp_dir:
        script_path = os.path.join(tmp_dir, "my_script.py")

        with open(script_path, "w") as tempfile:
            tempfile.write("""
async def run(user):
    async with user.client.get("/?foo") as resp:
        pass
""")
        proc = await exec(
            f"aiolocust {tempfile.name} -i 1 --host http://localhost:8081",
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=4)
        except TimeoutError:
            proc.terminate()
            stdout, stderr = await proc.communicate()
            output = stdout.decode(errors="replace")
            print(output)
            raise AssertionError("process never terminated") from None
        else:
            err = stderr.decode(errors="replace")
            output = stdout.decode(errors="replace")
            print(output)
            print(err)
            assert " /?foo " in output
            assert " 0 (0.0%) " in output
            assert "Error" not in output
            assert await proc.wait() == 0


async def test_sigint(http_server):
    with TemporaryDirectory() as tmp_dir:
        script_path = os.path.join(tmp_dir, "my_script.py")

        with open(script_path, "w") as tempfile:
            tempfile.write("""
async def run(user):
    async with user.client.get("http://localhost:8081/") as resp:
        pass
""")
        proc = await exec(
            f"aiolocust {tempfile.name} --duration 10",
            creationflags=creationflags,
        )
        try:
            await asyncio.sleep(1)
            if os.name == "nt":
                proc.send_signal(signal.CTRL_C_EVENT)
            else:
                proc.send_signal(signal.SIGINT)
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=3)
        except TimeoutError:
            proc.kill()
            stdout, stderr = await proc.communicate()
            output = stdout.decode(errors="replace")
            print(output)
            raise AssertionError("process never terminated") from None
        else:
            output = stdout.decode(errors="replace")
            err = stderr.decode(errors="replace")
            print(output)
            assert "Summary" in output
            assert await proc.wait() == 0
            print(err)
            assert "Shutting down (got SIGINT/CTRL-C)" in err


async def test_sigint_doesnt_wait_for_otel_to_connect(http_server):
    with TemporaryDirectory() as tmp_dir:
        script_path = os.path.join(tmp_dir, "my_script.py")

        with open(script_path, "w") as tempfile:
            tempfile.write("""
async def run(user):
    async with user.client.get("http://localhost:8081/") as resp:
        pass
""")
        proc = await exec(
            f"aiolocust {tempfile.name} --duration 10 --instrument",
            env={
                "OTEL_METRICS_EXPORTER": "otlp",
                "OTEL_EXPORTER_OTLP_ENDPOINT": "http://www.locust.cloud:22",  # invalid endpoint to simulate connection issues
                **os.environ,
            },
            creationflags=creationflags,
        )
        try:
            await asyncio.sleep(2)
            if os.name == "nt":
                proc.send_signal(signal.CTRL_C_EVENT)
            else:
                proc.send_signal(signal.SIGINT)
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=5)
        except TimeoutError:
            proc.kill()
            stdout, stderr = await proc.communicate()
            output = stdout.decode(errors="replace")
            print(output)
            raise AssertionError("process never terminated") from None
        else:
            output = stdout.decode(errors="replace")
            err = stderr.decode(errors="replace")
            print(output)
            assert "Summary" in output
            assert await proc.wait() == 0
            print(err)
            assert "Shutting down (got SIGINT/CTRL-C)" in err


async def test_unhandled_error_logging(http_server):
    with TemporaryDirectory() as tmp_dir:
        script_path = os.path.join(tmp_dir, "my_script.py")

        with open(script_path, "w") as tempfile:
            tempfile.write("""
import asyncio

async def run(user):
    await asyncio.sleep(0.1)
    raise Exception("an error")
""")
        proc = await exec(
            f"aiolocust {tempfile.name} -i 1",
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=3)
        except TimeoutError:
            proc.kill()
            stdout, stderr = await proc.communicate()
            output = stdout.decode(errors="replace")
            print(output)
            raise AssertionError("process never terminated") from None
        else:
            err = stderr.decode(errors="replace")
            print(err)
            assert "Traceback" in err
            assert 'my_script.py", line 6, in run' in err
            assert 'raise Exception("an error")' in err
            output = stdout.decode(errors="replace")
            print(output)
            assert "Summary" in output
            assert await proc.wait() == 0
            assert_search(r"[12] .* an error", output)


async def test_config_and_stages(http_server):
    with TemporaryDirectory() as tmp_dir:
        with open(os.path.join(tmp_dir, "my_script.py"), "w") as tempfile:
            tempfile.write("""
import asyncio

async def run(user):
    async with user.client.get("http://localhost:8081/") as resp:
        pass
    if user.running:
        await asyncio.sleep(1)
""")
        with open(os.path.join(tmp_dir, "my_config.json"), "w") as configfile:
            json.dump(
                {
                    "stages": [{"duration": 3, "target": 1}, {"duration": 1, "target": 20}],
                },
                configfile,
            )

        proc = await exec(
            f"aiolocust {tempfile.name} --config {configfile.name}",
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=7 + WINDOWS_DELAY * 2)
        except TimeoutError:
            proc.kill()
            stdout, stderr = await proc.communicate()
            output = stdout.decode(errors="replace")
            print(output)
            error = stderr.decode(errors="replace")
            print(error)
            raise AssertionError("process never terminated") from None
        else:
            err = stderr.decode(errors="replace")
            print(err)
            assert "duration" in err
            output = stdout.decode(errors="replace")
            print(output)
            assert "Summary" in output
            assert await proc.wait() == 0
            assert_search(r"0|1\.[0-9]*/s", output)  # first one
            assert_search(r"[2-9]\.[0-9]*/s", output)  # last one


async def test_user_forwards_params_to_session_and_handles_timeouts(http_server):
    proc = await exec(
        "aiolocust examples/advanced_user_class_settings.py -i 3",
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=6)
    except TimeoutError:
        proc.kill()
        stdout, stderr = await proc.communicate()
        output = stdout.decode(errors="replace")
        print(output)
        raise AssertionError("process never terminated") from None
    else:
        err = stderr.decode(errors="replace")
        print(err)
        assert "Shutting down" in err
        output = stdout.decode(errors="replace")
        print(output)
        assert "Summary" in output
        assert await proc.wait() == 0
        assert_search(r"3 .* TimeoutError", output)


async def test_shutdown_timeout():
    with TemporaryDirectory() as tmp_dir:
        with open(os.path.join(tmp_dir, "my_script.py"), "w") as tempfile:
            tempfile.write("""
import asyncio

async def run(user):
    await asyncio.sleep(10)
""")
        proc = await exec(
            f"aiolocust {tempfile.name} -d 1 --config {quote('{"shutdown_timeout":0.1}')}",
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=9)
        except TimeoutError:
            proc.kill()
            stdout, stderr = await proc.communicate()
            output = stdout.decode(errors="replace")
            print(output)
            raise AssertionError("process never terminated") from None
        else:
            err = stderr.decode(errors="replace")
            print(err)
            output = stdout.decode(errors="replace")
            print(output)
            assert "Shutdown timed out" in err
            assert await proc.wait() == 124


async def test_rate_limiting(http_server):

    proc = await exec(
        f"aiolocust examples/rate_limit.py -d 4 -u 10 --config {quote('{"stats_print_interval": 1}')}",
    )
    output, err = await communicate_print_and_decode(proc, 7)
    assert "Shutting down" in err
    assert "error" not in err.lower()
    assert "Summary" in output
    assert await proc.wait() == 0
    highest_rate = 0.0
    for line in output.splitlines():
        match = re.search(r"(\d*\.?\d+)/s $", line)
        if match:
            rate = float(match.group(1))
            highest_rate = max(highest_rate, rate)
    assert highest_rate > 9.0, f"request rate never reached high enough value: {highest_rate}"
    # limiting is using sliding window so slight overshoot during a clock second is normal
    assert highest_rate <= 14.0, f"rate limit exceeded: {highest_rate}"


async def communicate_print_and_decode(proc, timeout=None):
    stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    err = stderr.decode(errors="replace")
    print(err)
    output = stdout.decode(errors="replace")
    print(output)
    return output, err


async def test_iteration_limit_log_message():
    with TemporaryDirectory() as tmp_dir:
        with open(os.path.join(tmp_dir, "my_script.py"), "w") as tempfile:
            tempfile.write("""
import asyncio

async def run(user):
    await asyncio.sleep(0.1)
""")
        proc = await exec(
            f"aiolocust {tempfile.name} -d 1 -i 1",
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=9)
        except TimeoutError:
            proc.kill()
            stdout, stderr = await proc.communicate()
            output = stdout.decode(errors="replace")
            print(output)
            raise AssertionError("process never terminated") from None
        else:
            err = stderr.decode(errors="replace")
            print(err)
            output = stdout.decode(errors="replace")
            print(output)
            assert "Shutting down (reached iteration limit" in err


async def test_iteration_limit_log_message_with_ws(websocket_server, http_server):
    received = asyncio.Queue()

    async def handler(ws):
        # engineIO type handshake
        await ws.send('0{"sid": "abcd123"}')
        async for message in ws:
            await received.put(message)
            if message == "40":
                await ws.send("40")

    url = await websocket_server(handler)

    proc = await exec(
        f"aiolocust examples/websocket_ex.py -d 4 -u 2 -i 2 -H {url}",
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=9)
    except TimeoutError:
        proc.kill()
        stdout, stderr = await proc.communicate()
        output = stdout.decode(errors="replace")
        print(output)
        raise AssertionError("process never terminated") from None
    else:
        err = stderr.decode(errors="replace")
        print(err)
        output = stdout.decode(errors="replace")
        print(output)
        err = stderr.decode(errors="replace")
        print(err)
        assert "ERROR" not in err
        assert "Shutting down (reached iteration limit" in err
        output = stdout.decode(errors="replace")
        print(output)
        assert " websocket.connect " in output
        assert "Summary" in output
        assert_search(r" websocket\.connect +[│|] +2 +[│|] +0 \(0\.0%\) ", output)
        assert await proc.wait() == 0
        try:
            assert received.get_nowait() == "40"
        except asyncio.QueueEmpty:
            raise Exception("WS server received no messages!")
        assert not "unhandled error" in output
