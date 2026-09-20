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
    cumulative_entry: RequestEntry
    current_entry: RequestEntry | None


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
    def __init__(self, start_time):
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

    def _collect_stats_rows(self) -> list[StatsRowData]:
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
            values.append(StatsRowData(url, cumulative_entry, current_entry))

        values.append(StatsRowData("Total", cumulative_total, current_total))
        return values

    def get_table(self, requests: list[StatsRowData], end: float, final_summary=False):
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
            table.add_row(*self.make_row(request, end, final_summary))

        self.last_time = end

        if final_summary:
            table.title = "Summary"
        return table

    @staticmethod
    def get_error_table():
        error_table = Table(show_edge=False)
        error_table.add_column("Count")
        error_table.add_column("Error")

        for key, count in sorted(error_counter.items(), key=lambda item: item[1], reverse=True):
            error_table.add_row(str(count), key)

        return error_table

    def make_row(self, statsrow: StatsRowData, end, final_summary) -> list[str]:
        cumul_e: RequestEntry = statsrow.cumulative_entry
        curr_e: RequestEntry | None = statsrow.current_entry
        row = [
            statsrow.name,
            str(cumul_e.count),
            f"{cumul_e.errorcount} ({cumul_e.error_percentage:2.1f}%)",
            f"{cumul_e.avg_ttlb_ms:4.1f}ms",
            f"{cumul_e.max_ttlb_ms:4.1f}ms",
            f"{cumul_e.rate(self.start_time, end):.2f}/s",
        ]
        if curr_e and not final_summary:
            row.append(f"{curr_e.rate(self.last_time, end):.2f}/s")
        return row
