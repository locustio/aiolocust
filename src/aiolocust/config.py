# Used for giving easy access to command line arguments.
# If you make changes here, remember to keep it in sync with main() arguments
import os
from enum import StrEnum
from pathlib import Path


class LogLevel(StrEnum):
    debug = "debug"
    info = "info"
    warning = "warning"
    error = "error"


filename: str = "locustfile.py"
users: int = 1
duration: int | None = None
rate: float | None = None
iterations: int | None = None
host: str | None = None
instrument: bool = False
log_level: LogLevel = LogLevel.info
config: dict | None = None
event_loops: int | None = None
html_report: Path | None = None
profile: str | None = None
_version: bool = False


METRIC_ATTRIBUTES_ENV_VAR = "LOCUST_METRIC_ATTRIBUTES"


def get_metric_attributes() -> dict[str, str]:
    configured_attributes: dict[str, str] = {}
    raw_attributes = os.getenv(METRIC_ATTRIBUTES_ENV_VAR, "")
    for raw_attribute in filter(None, raw_attributes.split(",")):
        key, separator, value = raw_attribute.partition("=")
        key = key.strip()
        if not separator or not key:
            raise ValueError(f"{METRIC_ATTRIBUTES_ENV_VAR} must contain comma-separated key=value pairs")
        if key in configured_attributes:
            raise ValueError(f"{METRIC_ATTRIBUTES_ENV_VAR} cannot contain duplicate attribute {key!r}")
        configured_attributes[key] = value.strip()
    return configured_attributes
