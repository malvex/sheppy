import json
import logging
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from rich.console import Console

from sheppy.models import Task

console = Console()


class OutputFormat(str, Enum):
    table = "table"
    json = "json"


class LogLevel(str, Enum):
    """Log level enum for CLI arguments."""
    debug = "debug"
    info = "info"
    warning = "warning"
    error = "error"

    def to_logging_level(self) -> int:
        """Convert to Python logging level."""
        return getattr(logging, self.value.upper())  # type: ignore[no-any-return]


def humanize_datetime(dt: datetime | None, now: datetime | None = None) -> str:
    if not dt:
        return "N/A"

    if not now:
        now = datetime.now(timezone.utc)

    delta = (dt - now).total_seconds()

    is_past = delta < 0
    abs_delta = abs(delta)

    if abs_delta > 86400:  # 24 hours
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    hours = int(abs_delta/3600)
    hours_s = f"{hours} hour" + ("s" if hours > 1 else "")

    minutes = int((abs_delta % 3600) / 60)
    minutes_s = f"{minutes} minute" + ("s" if minutes > 1 else "")

    if abs_delta > 3600:
        time_string = hours_s

    elif abs_delta > 60:
        time_string = minutes_s
    else:
        time_string = f"{int(abs_delta)} second" + ("s" if abs_delta >= 2 else "")

    return f"{time_string} ago" if is_past else f"in {time_string}"


def print_json(data: Any) -> None:
    console.print(json.dumps(data, indent=2, default=str), markup=False, highlight=False, soft_wrap=True, crop=False)


def task_status_label(task: Task) -> str:
    if task.status == 'completed':
        return "completed"
    if task.status == 'crashed':
        return "crashed"
    if task.status == 'retrying':  # FIXME: doesn't work
        return "retrying"
    if task.exception:
        return "failed"
    if task.status == 'scheduled':
        return "scheduled"
    return task.status
