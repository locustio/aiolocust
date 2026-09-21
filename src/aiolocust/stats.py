import os
from collections import defaultdict
from dataclasses import dataclass
from threading import Lock
from types import TracebackType

from opentelemetry import metrics
from opentelemetry.sdk.metrics.export import HistogramDataPoint
from rich.table import Table

from aiolocust import otel
from aiolocust.datatypes import Request, RequestEntry

MAX_ERROR_KEYS = 200


meter = metrics.get_meter("locust")
ttlb_histogram = meter.create_histogram(
    "locust.client.duration", unit="s", description="Time to last byte for requests"
)
error_counter: dict[str, int] = defaultdict(int)
error_counter_lock = Lock()


@dataclass(slots=True)
class StatsRowData:
    name: str
    count: int
    current_count: int
    errorcount: int
    sum_ttlb: float
    max_ttlb: float
    rate: float
    current_rate: float

    @property
    def error_percentage(self) -> float:
        return (self.errorcount / self.count * 100.0) if self.count > 0 else 0.0

    @property
    def avg_ttlb_ms(self) -> float:
        return (self.sum_ttlb / self.count * 1000) if self.count > 0 else 0.0

    @property
    def max_ttlb_ms(self) -> float:
        return self.max_ttlb * 1000

    def asdict(self) -> dict[str, float | int | str]:
        return {
            "name": self.name,
            "count": self.count,
            "errorcount": self.errorcount,
            "sum_ttlb": self.sum_ttlb,
            "max_ttlb": self.max_ttlb,
            "rate": self.rate,
        }

    def __init__(
        self,
        name: str,
        cumulative_entry: RequestEntry,
        current_entry: RequestEntry,
        start_time: float,
        last_time: float,
        current_time: float,
    ) -> None:
        self.name = name
        self.count = cumulative_entry.count
        self.current_count = current_entry.count
        self.errorcount = cumulative_entry.errorcount
        self.sum_ttlb = cumulative_entry.sum_ttlb
        self.max_ttlb = cumulative_entry.max_ttlb
        self.rate = cumulative_entry.rate(start_time, current_time)
        self.current_rate = current_entry.rate(last_time, current_time)


def record_error(message: str) -> None:
    with error_counter_lock:
        if message not in error_counter and len(error_counter) >= MAX_ERROR_KEYS:
            message = "OTHER"
        error_counter[message] += 1


async def record_request(req: Request) -> None:
    attributes = {
        "name": req.name,
        # the rest of these remain to be implemented
        # http.method=GET,
        # http.host=localhost,
        # net.peer.name=localhost,
        # net.peer.port=8080,
        # http.status_code=200}
    }
    if req.error:
        # error.type is propagated to otel, but it also picked up when calculating command line stats table
        attributes["error.type"] = req.error.__class__.__name__
        if isinstance(req.error, AssertionError):
            tb: TracebackType = req.error.exc_tb  # type: ignore
            record_error(
                f"{str(req.error) or req.error.__class__.__name__} ({os.path.basename(tb.tb_frame.f_code.co_filename)}:{tb.tb_lineno})"
            )
        else:
            record_error(str(req.error) or req.error.__class__.__name__)
    ttlb_histogram.record(req.ttlb, attributes=attributes)


class StatsFormatter:
    def __init__(self, start_time) -> None:
        self.start_time = start_time
        self.last_time = self.start_time
        self.aggregate: dict[str, RequestEntry] = defaultdict(RequestEntry)
        # clear reader, in case this is not the first Stats object
        _ = otel.reader.get_metrics_data()
        error_counter.clear()

    def _get_entries(self) -> dict[str, RequestEntry]:
        metrics_data = otel.reader.get_metrics_data()
        entries: dict[str, RequestEntry] = defaultdict(RequestEntry)
        for resource_metric in metrics_data.resource_metrics if metrics_data else []:
            for scope_metric in resource_metric.scope_metrics:
                for metric in scope_metric.metrics:
                    if metric.name != "locust.client.duration":
                        continue
                    for point in metric.data.data_points:
                        if not point.attributes:
                            raise Exception(f"A data point had no attributes, that should never happen. Point: {point}")
                        if not isinstance(point, HistogramDataPoint):
                            raise Exception(f"Unexpected Strange datapoint type: {point}")
                        entries[str(point.attributes["name"])] += RequestEntry(
                            point.count,
                            point.count if point.attributes.get("error.type") else 0,
                            point.sum,
                            point.max,
                        )

        return entries

    def collect_stats_rows(self, current_time: float) -> list[StatsRowData]:
        current_entries = self._get_entries()
        for url, re in current_entries.items():
            self.aggregate[url] += re

        cumulative_total = RequestEntry()
        current_total = RequestEntry()

        for current_entry in current_entries.values():
            current_total += current_entry

        values: list[StatsRowData] = []
        for url, cumulative_entry in self.aggregate.items():
            cumulative_total += cumulative_entry
            current_entry = current_entries.get(url, RequestEntry())
            values.append(
                StatsRowData(url, cumulative_entry, current_entry, self.start_time, self.last_time, current_time)
            )

        values.append(
            StatsRowData("Total", cumulative_total, current_total, self.start_time, self.last_time, current_time)
        )
        self.last_time = current_time
        return values

    def get_table(self, requests: list[StatsRowData], final_summary=False) -> Table:
        table = Table(show_edge=False)
        table.add_column("Name", max_width=30)
        table.add_column("Count", justify="right")
        table.add_column("Failures", justify="right")
        table.add_column("Avg", justify="right")
        table.add_column("Max", justify="right")
        table.add_column("Rate", justify="right")

        if not final_summary:
            table.add_column("Current rate", justify="right")

        for request in requests:
            table.add_row(*self.make_row(request, final_summary))

        if final_summary:
            table.title = "Summary"
        return table

    @staticmethod
    def get_error_table() -> Table:
        error_table = Table(show_edge=False)
        error_table.add_column("Count")
        error_table.add_column("Error")

        for key, count in sorted(error_counter.items(), key=lambda item: item[1], reverse=True):
            error_table.add_row(str(count), key)

        return error_table

    def make_row(self, statsrow: StatsRowData, final_summary) -> list[str]:
        row = [
            statsrow.name,
            str(statsrow.count),
            f"{statsrow.errorcount} ({statsrow.error_percentage:2.1f}%)",
            f"{statsrow.avg_ttlb_ms:4.1f}ms",
            f"{statsrow.max_ttlb_ms:4.1f}ms",
            f"{statsrow.rate:.2f}/s",
        ]
        if not final_summary:
            row.append(f"{statsrow.current_rate:.2f}/s")
        return row
