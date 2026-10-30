import json
import logging
import math
import sys
from collections.abc import Mapping, MutableMapping
from datetime import datetime, timezone
from typing import Any

from rich.logging import RichHandler
from rich.text import Text

from ._config import LogFormatType


def format_log_context(context: Mapping[Any, Any]) -> str:
    return "".join(f" {key}={value}" for key, value in context.items())


class ContextLoggerAdapter(logging.LoggerAdapter):  # type: ignore[type-arg]
    def process(self, msg: Any, kwargs: MutableMapping[str, Any]) -> tuple[Any, MutableMapping[str, Any]]:
        extra = kwargs.get("extra")
        if not isinstance(extra, Mapping):
            return msg, kwargs

        context = extra.get("log_context")
        if not isinstance(context, Mapping) or not context:
            return msg, kwargs

        message = str(msg)
        kwargs["extra"] = {**extra, "log_event": message}
        return f"{message}{format_log_context(context)}", kwargs

    def getChild(self, suffix: str) -> "ContextLoggerAdapter":
        return ContextLoggerAdapter(self.logger.getChild(suffix))


LoggerLike = logging.Logger | ContextLoggerAdapter


def _component_name(logger_name: str) -> str:
    worker_prefix = "sheppy.worker."
    if logger_name == "sheppy.worker":
        return "worker"
    if logger_name.startswith(worker_prefix):
        return logger_name[len(worker_prefix):].split(".", 1)[0]
    return logger_name.rsplit(".", 1)[-1]


class _ComponentFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        record.__dict__["component"] = _component_name(record.name)
        return super().format(record)


class _WorkerLogHandler(RichHandler):
    pass


class _JsonLogHandler(logging.StreamHandler):  # type: ignore[type-arg]
    pass


def _format_log_time(log_time: datetime) -> Text:
    timestamp = log_time.astimezone(timezone.utc).isoformat(timespec="seconds")
    return Text(timestamp.replace("+00:00", "Z"))


def _json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        timestamp = value.astimezone(timezone.utc).isoformat()
        return timestamp.replace("+00:00", "Z")
    if isinstance(value, BaseException):
        return f"{type(value).__name__}: {value}"
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        context = record.__dict__.get("log_context", {})
        if not isinstance(context, dict):
            context = {}

        payload: dict[str, Any] = {
            "timestamp": _format_log_time(datetime.fromtimestamp(record.created, timezone.utc)).plain,
            "level": record.levelname,
            "logger": record.name,
            "message": str(record.__dict__.get("log_event", record.getMessage())),
        }
        for key, value in context.items():
            key = str(key)
            if key not in payload:
                payload[key] = _json_safe(value)

        if record.exc_info:
            exc_type, exc_value, _ = record.exc_info
            payload["exception"] = {
                "type": exc_type.__name__ if exc_type is not None else "Unknown",
                "message": str(exc_value) if exc_value is not None else "",
                "traceback": self.formatException(record.exc_info),
            }
        if record.stack_info:
            payload["stack_info"] = self.formatStack(record.stack_info)

        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def configure_worker_logging(level: int | str = logging.INFO, log_format: LogFormatType = "console") -> None:
    if log_format not in ("console", "json"):
        raise ValueError(f"Unsupported log format: {log_format!r}")

    logger = logging.getLogger("sheppy")
    logger.setLevel(level)
    logger.propagate = False

    handler_type: type[logging.Handler] = _JsonLogHandler if log_format == "json" else _WorkerLogHandler
    for existing_handler in list(logger.handlers):
        if isinstance(existing_handler, (_WorkerLogHandler, _JsonLogHandler)) and not isinstance(existing_handler, handler_type):
            logger.removeHandler(existing_handler)
            existing_handler.close()

    if any(isinstance(handler, handler_type) for handler in logger.handlers):
        return

    handler: logging.Handler
    if log_format == "json":
        handler = _JsonLogHandler(sys.stdout)
        handler.setFormatter(_JsonFormatter())
    else:
        handler = _WorkerLogHandler(
            rich_tracebacks=True,
            tracebacks_show_locals=False,
            log_time_format=_format_log_time,
            omit_repeated_times=False,
            show_path=False,
        )
        handler.setFormatter(_ComponentFormatter("%(component)-9s %(message)s"))
    logger.addHandler(handler)
