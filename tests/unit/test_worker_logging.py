import io
import json
import logging
import sys
from datetime import datetime, timedelta, timezone

import pytest
from typer.testing import CliRunner

from sheppy import MemoryBackend, Queue, Worker, task
from sheppy._logging import (
    ContextLoggerAdapter,
    _ComponentFormatter,
    _format_log_time,
    _JsonFormatter,
    _JsonLogHandler,
    _WorkerLogHandler,
    configure_worker_logging,
)
from sheppy._utils.task_execution import LoggingMiddleware, TaskProcessor
from sheppy.cli.cli import app
from sheppy.cli.commands.work import _redact_backend_url


@task
def logging_add(x: int, y: int) -> int:
    return x + y


@task(retry=1, retry_delay=0)
def logging_fail() -> None:
    raise ValueError("bad input")


def logging_prestart() -> None:
    pass


@pytest.fixture(autouse=True)
def preserve_sheppy_logger(monkeypatch: pytest.MonkeyPatch) -> None:
    logger = logging.getLogger("sheppy")
    monkeypatch.setattr(logger, "handlers", logger.handlers.copy())
    monkeypatch.setattr(logger, "level", logger.level)
    monkeypatch.setattr(logger, "propagate", True)


def make_processor() -> TaskProcessor:
    logger = ContextLoggerAdapter(logging.getLogger("sheppy.worker"))
    return TaskProcessor(middleware=[LoggingMiddleware(logger)])


async def test_successful_task_logs_include_context(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="sheppy.worker")
    queue = Queue(MemoryBackend(instant_processing=False), "emails")
    queued_task = logging_add(1, 2)

    exception, processed_task = await make_processor().process_task(queued_task, queue, "worker-id")

    assert exception is None
    assert processed_task.status == "completed"
    assert [record.getMessage() for record in caplog.records] == [
        f"Task started function={__name__}:logging_add queue=emails task_id={queued_task.id}",
        f"Task completed function={__name__}:logging_add queue=emails task_id={queued_task.id}",
    ]
    payload = json.loads(_JsonFormatter().format(caplog.records[0]))
    assert payload["message"] == "Task started"
    assert payload["function"] == f"{__name__}:logging_add"
    assert payload["queue"] == "emails"
    assert payload["task_id"] == str(queued_task.id)


async def test_retry_and_final_failure_logs(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="sheppy.worker")
    queue = Queue(MemoryBackend(instant_processing=False), "emails")
    processor = make_processor()
    queued_task = logging_fail()

    exception, retrying_task = await processor.process_task(queued_task, queue, "worker-id")

    assert isinstance(exception, ValueError)
    assert retrying_task.status == "retrying"
    retry_record = caplog.records[-1]
    assert retry_record.levelno == logging.WARNING
    retry_message = retry_record.getMessage()
    assert retry_message.startswith(f"Task failed function={__name__}:logging_fail queue=emails task_id={queued_task.id}")
    assert "attempt=1/1" in retry_message
    assert f"retry_at={retrying_task.next_retry_at}" in retry_message
    assert "error=ValueError: bad input" in retry_message

    exception, failed_task = await processor.process_task(retrying_task, queue, "worker-id")

    assert isinstance(exception, ValueError)
    assert failed_task.status == "failed"
    failure_record = caplog.records[-1]
    assert failure_record.levelno == logging.ERROR
    assert failure_record.getMessage() == (
        f"Task failed function={__name__}:logging_fail queue=emails task_id={queued_task.id} "
        "retries=1/1 error=ValueError: bad input"
    )


async def test_worker_lifecycle_loggers_and_levels(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG, logger="sheppy.worker")
    worker = Worker("default", MemoryBackend(instant_processing=False))
    worker._blocking_timeout = 0.001

    await worker.work(oneshot=True, register_signal_handlers=False)

    records = {(record.name, record.levelno, record.getMessage()) for record in caplog.records}
    assert ("sheppy.worker.scheduler", logging.INFO, "Started") in records
    assert ("sheppy.worker.scheduler", logging.INFO, "Stopped") in records
    assert ("sheppy.worker.cron", logging.INFO, "Started") in records
    assert ("sheppy.worker.cron", logging.INFO, "Stopped") in records
    assert ("sheppy.worker.heartbeat", logging.DEBUG, "Started") in records
    assert ("sheppy.worker.reclaim", logging.DEBUG, "Started") in records
    assert ("sheppy.worker", logging.INFO, "Queue drained queue=default") in records
    assert ("sheppy.worker", logging.INFO, "Stopped processed=0 failed=0") in records


@pytest.mark.parametrize("log_format", ["console", "json"])
async def test_public_helper_supports_programmatic_worker(log_format: str, capsys: pytest.CaptureFixture[str]) -> None:
    logging.getLogger("sheppy").handlers.clear()
    configure_worker_logging(log_format=log_format)
    worker = Worker("default", MemoryBackend(instant_processing=False))
    worker._blocking_timeout = 0.001

    await worker.work(oneshot=True, register_signal_handlers=False)

    output = capsys.readouterr().out
    if log_format == "json":
        records = [json.loads(line) for line in output.splitlines()]
        drained = next(record for record in records if record["message"] == "Queue drained")
        assert drained["logger"] == "sheppy.worker"
        assert drained["queue"] == "default"
        assert all("component" not in record for record in records)
    else:
        assert "worker" in output
        assert "Queue drained queue=default" in output


@pytest.mark.parametrize(
    ("logger_name", "expected"),
    [
        ("sheppy.worker", "worker    Started"),
        ("sheppy.worker.scheduler", "scheduler Started"),
        ("sheppy.worker.cron", "cron      Started"),
    ],
)
def test_component_formatter(logger_name: str, expected: str) -> None:
    formatter = _ComponentFormatter("%(component)-9s %(message)s")
    record = logging.LogRecord(logger_name, logging.INFO, __file__, 1, "Started", (), None)

    assert formatter.format(record) == expected


def test_log_timestamp_is_complete_utc_iso8601() -> None:
    timestamp = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone(timedelta(hours=2)))

    assert _format_log_time(timestamp).plain == "2026-01-02T01:04:05Z"


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("memory://", "memory://"),
        ("redis://localhost:6379", "redis://localhost:6379"),
        ("redis://user:secret@localhost:6379/0", "redis://user:***@localhost:6379/0"),
        ("redis://:secret@localhost:6379", "redis://:***@localhost:6379"),
        ("rediss://user:p%40ss@[::1]:6380/2?ssl_cert_reqs=required", "rediss://user:***@[::1]:6380/2?ssl_cert_reqs=required"),
        ("redis://localhost:6379/0?password=secret&db=0", "redis://localhost:6379/0?password=***&db=0"),
        ("redis://user:secret@[::1", "redacted"),
    ],
)
def test_backend_url_redaction(url: str, expected: str) -> None:
    assert _redact_backend_url(url) == expected


def test_logging_configuration_is_idempotent_with_root_handlers(monkeypatch: pytest.MonkeyPatch) -> None:
    root_logger = logging.getLogger()
    monkeypatch.setattr(root_logger, "handlers", root_logger.handlers.copy())
    root_logger.addHandler(logging.NullHandler())

    configure_worker_logging()
    configure_worker_logging("DEBUG")

    logger = logging.getLogger("sheppy")
    handlers = [handler for handler in logger.handlers if isinstance(handler, _WorkerLogHandler)]
    assert len(handlers) == 1
    assert logger.level == logging.DEBUG
    assert logger.propagate is False
    assert isinstance(handlers[0].formatter, _ComponentFormatter)
    assert handlers[0].tracebacks_show_locals is False


def test_cli_does_not_print_backend_password(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_work(*_args, **_kwargs) -> None:
        return None

    monkeypatch.setattr(Worker, "work", fake_work)
    result = CliRunner().invoke(app, ["work", "--backend-url", "redis://user:supersecret@localhost:6379/0"])

    assert result.exit_code == 0, result.output
    assert "supersecret" not in result.output
    assert "redis://user:***@localhost:6379/0" in result.output


def test_logging_configuration_switches_formats() -> None:
    logger = logging.getLogger("sheppy")

    configure_worker_logging(log_format="console")
    configure_worker_logging(log_format="json")

    handlers = [handler for handler in logger.handlers if isinstance(handler, (_WorkerLogHandler, _JsonLogHandler))]
    assert len(handlers) == 1
    json_handler = handlers[0]
    assert isinstance(json_handler, _JsonLogHandler)
    assert isinstance(json_handler.formatter, _JsonFormatter)

    configure_worker_logging("DEBUG", "json")
    assert logger.handlers.count(json_handler) == 1
    assert logger.level == logging.DEBUG

    configure_worker_logging(log_format="console")
    handlers = [handler for handler in logger.handlers if isinstance(handler, (_WorkerLogHandler, _JsonLogHandler))]
    assert len(handlers) == 1
    assert isinstance(handlers[0], _WorkerLogHandler)
    assert json_handler not in logger.handlers


def test_configure_worker_logging_rejects_invalid_format() -> None:
    with pytest.raises(ValueError, match="Unsupported log format"):
        configure_worker_logging(log_format="yaml")  # type: ignore[arg-type]


def test_json_formatter_serializes_structured_context() -> None:
    queued_task = logging_add(1, 2)
    function = f"{__name__}:logging_add"
    record = logging.LogRecord(
        "sheppy.worker",
        logging.INFO,
        __file__,
        1,
        "Task started",
        (),
        None,
    )
    record.created = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc).timestamp()
    record.log_context = {"function": function, "queue": "emails", "task_id": queued_task.id}

    formatted = _JsonFormatter().format(record)
    payload = json.loads(formatted)

    assert "\n" not in formatted
    assert payload == {
        "timestamp": "2026-01-02T03:04:05Z",
        "level": "INFO",
        "logger": "sheppy.worker",
        "message": "Task started",
        "function": function,
        "queue": "emails",
        "task_id": str(queued_task.id),
    }


def test_json_formatter_normalizes_context_values() -> None:
    queued_task = logging_add(1, 2)
    retry_at = datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=timezone.utc)
    record = logging.LogRecord("sheppy.worker", logging.WARNING, __file__, 1, "Task failed", (), None)
    record.log_context = {
        "retry_at": retry_at,
        "task_ids": [queued_task.id],
        "error": ValueError("bad input"),
        "non_finite": float("inf"),
    }

    payload = json.loads(_JsonFormatter().format(record))

    assert payload["message"] == "Task failed"
    assert payload["retry_at"] == "2026-01-02T03:04:05.123456Z"
    assert payload["task_ids"] == [str(queued_task.id)]
    assert payload["error"] == "ValueError: bad input"
    assert payload["non_finite"] == "inf"


def test_json_handler_emits_one_line_with_exception() -> None:
    stream = io.StringIO()
    handler = _JsonLogHandler(stream)
    handler.setFormatter(_JsonFormatter())

    try:
        secret_value = "sensitive" + "-local"
        raise ValueError("bad input")
    except ValueError:
        record = logging.LogRecord(
            "sheppy.worker.scheduler",
            logging.ERROR,
            __file__,
            1,
            "Scheduling failed",
            (),
            sys.exc_info(),
        )

    handler.emit(record)
    output = stream.getvalue()
    payload = json.loads(output)

    assert output.count("\n") == 1
    assert secret_value not in output
    assert payload["message"] == "Scheduling failed"
    assert payload["logger"] == "sheppy.worker.scheduler"
    assert "component" not in payload
    assert payload["exception"]["type"] == "ValueError"
    assert payload["exception"]["message"] == "bad input"
    assert "ValueError: bad input" in payload["exception"]["traceback"]


def test_cli_json_stream_is_structured_and_redacted(monkeypatch: pytest.MonkeyPatch) -> None:
    queued_task = logging_add(1, 2)
    function = f"{__name__}:logging_add"

    async def fake_work(*_args, **_kwargs) -> None:
        logging.getLogger("sheppy.worker").info(
            "Task started",
            extra={"log_context": {"function": function, "queue": "emails", "task_id": queued_task.id}},
        )

    monkeypatch.setattr(Worker, "work", fake_work)
    result = CliRunner().invoke(app, [
        "work",
        "--backend-url", "redis://user:supersecret@localhost:6379/0",
        "--log-format", "json",
        "--queue", "emails",
        "--queue", "emails",
        "--prestart", f"{__name__}:logging_prestart",
    ])

    assert result.exit_code == 0, result.output
    lines = result.output.strip().splitlines()
    payloads = [json.loads(line) for line in lines]

    assert len(payloads) == 4
    assert payloads[0]["message"] == "Duplicate queue name ignored"
    assert payloads[0]["queue"] == "emails"
    assert payloads[1]["message"] == "Starting worker"
    assert payloads[1]["queues"] == ["emails"]
    assert payloads[1]["backend_url"] == "redis://user:***@localhost:6379/0"
    assert payloads[2]["message"] == "Running prestart hook"
    assert payloads[2]["prestart"] == f"{__name__}:logging_prestart"
    assert payloads[3]["message"] == "Task started"
    assert payloads[3]["function"] == function
    assert payloads[3]["queue"] == "emails"
    assert payloads[3]["task_id"] == str(queued_task.id)
    assert all(payload["timestamp"].endswith("Z") for payload in payloads)
    assert "supersecret" not in result.output
    assert "\x1b" not in result.output
