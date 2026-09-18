import io
import time

import pytest
from rich.console import Console
from utils import assert_search

from aiolocust.datatypes import Request
from aiolocust.otel import configure_telemetry
from aiolocust.stats import StatsFormatter, record_request


@pytest.fixture(scope="module", autouse=True)
def configure_test_telemetry():
    configure_telemetry()


async def test_get_values_and_get_table():
    f = io.StringIO()
    start_time = time.time()
    console = Console(file=f)
    sf = StatsFormatter(start_time)
    console.print(sf.get_table(sf._get_values(), start_time + 1))
    output = f.getvalue()
    f.seek(0)
    assert "Total" in output

    await record_request(Request("foo", 1, 1, None))
    await record_request(Request("foo", 1, 2, True))
    await record_request(Request("bar", 1, 1, None))
    await record_request(Request("bar", 1, 2, True))
    console.print(sf.get_table(sf._get_values(), start_time + 2))
    output = f.getvalue()
    f.seek(0)
    assert "foo" in output
    assert "bar" in output
    assert "1500.0ms" in output
    assert "1 (50.0%)" in output
    assert_search(r"foo .* 2.00/s", output)
    assert_search(r"Total .* 4.00/s", output)

    console.print(sf.get_table(sf._get_values(), start_time + 3, True))
    output = f.getvalue()
    f.seek(0)
    assert_search(r"foo .* 0.67/s", output)
    assert_search(r"Total .* 1.33/s", output)
    assert "Summary" in output
    assert "1500.0ms" in output


async def test_cumulative_printout():
    f = io.StringIO()
    console = Console(file=f)
    start_time = time.time()
    sf = StatsFormatter(start_time)

    await record_request(Request("foo", 1, 1, None))
    await record_request(Request("foo", 2, 2, None))
    await record_request(Request("bar", 3, 3, None))
    await record_request(Request("baz", 4, 4, True))

    console.print(sf.get_table(sf._get_values(), start_time + 2))
    output = f.getvalue()
    print(output)
    assert_search(r"foo .* 2 .* 1.00/s .* 1.00/s", output)
    assert_search(r"bar .* 1 .* 0.50/s .* 0.50/s", output)
    assert_search(r"baz .* 1 .* 0.50/s .* 0.50/s", output)
    assert_search(r"Total .* 4 .* 2.00/s .* 2.00/s", output)

    f.seek(0)
    f.truncate(0)
    await record_request(Request("foo", 1, 1, None))
    await record_request(Request("bar", 2, 2, None))
    await record_request(Request("baz", 3, 3, None))
    console.print(sf.get_table(sf._get_values(), start_time + 4))
    output = f.getvalue()
    print(output)
    assert_search(r"foo .* 3 .* 0.75/s .* 0.50/s", output)
    assert_search(r"bar .* 2 .* 0.50/s .* 0.50/s", output)
    assert_search(r"baz .* 2 .* 0.50/s .* 0.50/s", output)
    assert_search(r"Total .* 7 .* 1.75/s .* 1.50/s", output)

    f.seek(0)
    f.truncate(0)
    await record_request(Request("foo", 1, 1, None))
    await record_request(Request("foo", 2, 2, None))
    await record_request(Request("bar", 3, 3, None))
    console.print(sf.get_table(sf._get_values(), start_time + 5, True))
    output = f.getvalue()
    print(output)
    assert "Current rate" not in output
    assert_search(r"Total .* 10 .* 2.00/s $", output)


async def test_error_pct_summary():
    f = io.StringIO()
    console = Console(file=f)
    start_time = time.time()
    sf = StatsFormatter(start_time)
    await record_request(Request("foo", 1, 1, None))
    await record_request(Request("foo", 2, 2, None))
    await record_request(Request("bar", 3, 3, None))
    await record_request(Request("bar", 4, 4, Exception("an exception")))
    await record_request(Request("baz", 5, 5, True))
    console.print(sf.get_table(sf._get_values(), start_time + 1, True))
    console.print(sf.get_error_table())
    output = f.getvalue()
    print(output)
    assert "Summary" in output
    assert_search(r"foo .* 0 \(0.0%\)", output)
    assert_search(r"bar .* 1 \(50.0%\)", output)
    assert_search(r"baz .* 1 \(100.0%\)", output)
    assert_search(r"Total .* 2 \(40.0%\)", output)

    assert_search(r"foo .* 1500.0ms", output)
    assert_search(r"bar .* 3500.0ms", output)
    assert_search(r"Total .* 3000.0ms .* 5000.0ms", output)

    assert "Error" in output
    assert_search(r"1 .* an exception", output)


async def test_error_cardinality():
    f = io.StringIO()
    console = Console(file=f)
    sf = StatsFormatter(time.time())
    for i in range(300):
        await record_request(Request("foo", 1, 1, Exception(f"error with unique id {i}")))
    console.print(sf.get_error_table())
    output = f.getvalue()
    assert "Error" in output
    assert_search(r"1 .* error with unique id 0", output)
    assert_search(r"1 .* error with unique id 199", output)
    assert_search(r"100 .* OTHER", output)
