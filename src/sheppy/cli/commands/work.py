import asyncio
import logging
import os
import sys
from typing import Annotated
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import typer

from sheppy import Backend, Worker
from sheppy._config import LogFormatType, LogLevelType, config
from sheppy._logging import configure_worker_logging
from sheppy._queue import _create_backend_from_url
from sheppy._utils.functions import _resolve_function

from .._utils import LogFormat, LogLevel, console

worker_logger = logging.getLogger("sheppy.worker")


def _redact_backend_url(backend_url: str) -> str:
    try:
        parsed = urlsplit(backend_url)
        query_items = parse_qsl(parsed.query, keep_blank_values=True)
        if parsed.password is None and all(key.lower() != "password" for key, _ in query_items):
            return backend_url

        netloc = parsed.netloc
        if parsed.password is not None:
            userinfo, host = netloc.rsplit("@", 1)
            username = userinfo.split(":", 1)[0]
            netloc = f"{username}:***@{host}"
        query = urlencode([
            (key, "***" if key.lower() == "password" else value)
            for key, value in query_items
        ], safe="*")
    except ValueError:
        return "redacted"

    return urlunsplit(parsed._replace(netloc=netloc, query=query))


def work(
    queue: Annotated[list[str], typer.Option("--queue", "-q", help="Queue name(s). Env: SHEPPY_QUEUE")] = config.queue_list,
    backend_url: Annotated[str | None, typer.Option("--backend-url", "-u", help="Backend URL. Env: SHEPPY_BACKEND_URL")] = config.backend_url,
    max_concurrent: Annotated[int, typer.Option("--max-concurrent", "-c", help="Max concurrent tasks. Env: SHEPPY_MAX_CONCURRENT_TASKS", min=1)] = config.max_concurrent_tasks,
    max_prefetch: Annotated[int | None, typer.Option("--max-prefetch", help="Max prefetch tasks", min=1)] = None,
    oneshot: Annotated[bool, typer.Option("--oneshot", help="Process pending tasks and then exit")] = False,
    max_tasks: Annotated[int | None, typer.Option("--max-tasks", help="Maximum amount of tasks to process", min=1)] = None,
    disable_job_processing: Annotated[bool, typer.Option("--disable-job-processing", help="Disable job processing")] = False,
    disable_scheduler: Annotated[bool, typer.Option("--disable-scheduler", help="Disable scheduler")] = False,
    disable_cron_manager: Annotated[bool, typer.Option("--disable-cron-manager", help="Disable cron manager")] = False,
    log_level: Annotated[LogLevelType, typer.Option("--log-level", "-l", help="Logging level. Env: SHEPPY_LOG_LEVEL")] = config.log_level,
    log_format: Annotated[LogFormatType, typer.Option("--log-format", help="Log output format. Env: SHEPPY_LOG_FORMAT")] = config.log_format,
    shutdown_timeout: Annotated[float, typer.Option("--shutdown-timeout", help="Shutdown timeout in seconds. Env: SHEPPY_SHUTDOWN_TIMEOUT")] = config.shutdown_timeout,
    prestart: Annotated[str | None, typer.Option("--prestart", help="Prestart hook 'module:function' run before the worker starts. Env: SHEPPY_PRESTART")] = config.prestart,
) -> None:
    """Start a worker to process tasks from a queue."""

    if all([disable_job_processing, disable_scheduler, disable_cron_manager]):
        raise ValueError("At least one processing type must be enabled")

    if backend_url is None:
        backend_url = "redis://127.0.0.1:6379"
    backend_instance = _create_backend_from_url(backend_url)

    _log_level = LogLevel(log_level)
    _log_format = LogFormat(log_format)
    configure_worker_logging(_log_level.to_logging_level(), _log_format.value)

    # deduplicate queues to prevent unexpected behavior
    queues = []
    for q in queue:
        if q in queues:
            if _log_format == LogFormat.json:
                worker_logger.warning("Duplicate queue name ignored", extra={"log_context": {"queue": q}})
            else:
                console.print(f"[yellow]Warning: Queue '{q}' provided multiple times.[/yellow]")
            continue
        queues.append(q)

    # add current working directory to Python path to allow importing tasks
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    if _log_format == LogFormat.json:
        worker_logger.info("Starting worker", extra={"log_context": {
            "queues": queues,
            "backend": type(backend_instance).__name__,
            "backend_url": _redact_backend_url(backend_url),
            "job_processing_enabled": not disable_job_processing,
            "scheduler_enabled": not disable_scheduler,
            "cron_manager_enabled": not disable_cron_manager,
            "max_concurrent_tasks": max_concurrent,
            "max_prefetch_tasks": max_prefetch,
            "oneshot": oneshot,
            "max_tasks": max_tasks,
            "shutdown_timeout": shutdown_timeout,
            "prestart": prestart,
        }})
    else:
        _s = "s" if len(queues) > 1 else ""
        queue_s = "[/bold]', '[bold]".join(queues)

        _os = " [yellow]\\[oneshot]" if oneshot else ""
        _mt = f" [yellow]\\[max_tasks: {max_tasks}]" if max_tasks else ""

        console.print(f"[cyan]Starting worker for queue{_s} '[bold]{queue_s}[/bold]'[/cyan]{_os}{_mt}")
        console.print(f"  Backend: [yellow]{type(backend_instance).__name__}[/yellow] [gray0]\\[{_redact_backend_url(backend_url)}][/gray0]")
        console.print(f"  Job processing: [yellow]{not disable_job_processing}[/yellow]"
                      f"  Scheduler: [yellow]{not disable_scheduler}[/yellow]"
                      f"  Cron Manager: [yellow]{not disable_cron_manager}[/yellow]")
        console.print(f"  Max concurrent tasks: [yellow]{max_concurrent}[/yellow]")
        if prestart:
            console.print(f"  Prestart hook: [yellow]{prestart}[/yellow]")

        console.print()

    _start_worker(queues, backend_instance, max_concurrent, max_prefetch, _log_level, _log_format,
                  shutdown_timeout, disable_job_processing, disable_scheduler, disable_cron_manager,
                  oneshot, max_tasks, prestart)


def _start_worker(queues: list[str], backend: Backend, max_concurrent: int, max_prefetch_tasks: int | None,
                  log_level: LogLevel, log_format: LogFormat, shutdown_timeout: float,
                  disable_job_processing: bool, disable_scheduler: bool, disable_cron_manager: bool,
                  oneshot: bool = False, max_tasks: int | None = None, prestart: str | None = None,
                  ) -> None:

    configure_worker_logging(log_level.to_logging_level(), log_format.value)

    if prestart:
        if log_format == LogFormat.json:
            worker_logger.info("Running prestart hook", extra={"log_context": {"prestart": prestart}})
        else:
            console.print(f"[cyan]Running prestart hook '[bold]{prestart}[/bold]'[/cyan]")
        _resolve_function(prestart)()  # evil

    worker = Worker(queues, backend=backend, max_concurrent_tasks=max_concurrent,
                    max_prefetch_tasks=max_prefetch_tasks,
                    shutdown_timeout=shutdown_timeout,
                    enable_job_processing=not disable_job_processing,
                    enable_scheduler=not disable_scheduler,
                    enable_cron_manager=not disable_cron_manager)
    asyncio.run(worker.work(max_tasks=max_tasks, oneshot=oneshot))
