import asyncio
import logging
import os
import sys
from typing import Annotated

import typer
from rich.logging import RichHandler
from watchfiles import run_process

from sheppy import Backend, Worker
from sheppy._config import LogLevelType, config
from sheppy.queue import _create_backend_from_url

from ..utils import LogLevel, console


def work(
    queue: Annotated[list[str], typer.Option("--queue", "-q", help="Queue name(s). Env: SHEPPY_QUEUE")] = config.queue_list,
    backend_url: Annotated[str | None, typer.Option("--backend-url", "-u", help="Backend URL. Env: SHEPPY_BACKEND_URL")] = config.backend_url,
    max_concurrent: Annotated[int, typer.Option("--max-concurrent", "-c", help="Max concurrent tasks. Env: SHEPPY_MAX_CONCURRENT_TASKS", min=1)] = config.max_concurrent_tasks,
    max_prefetch: Annotated[int | None, typer.Option("--max-prefetch", help="Max prefetch tasks", min=1)] = None,
    autoreload: Annotated[bool, typer.Option("--reload", help="Reload worker on file changes")] = False,
    oneshot: Annotated[bool, typer.Option("--oneshot", help="Process pending tasks and then exit")] = False,
    max_tasks: Annotated[int | None, typer.Option("--max-tasks", help="Maximum amount of tasks to process", min=1)] = None,
    disable_job_processing: Annotated[bool, typer.Option("--disable-job-processing", help="Disable job processing")] = False,
    disable_scheduler: Annotated[bool, typer.Option("--disable-scheduler", help="Disable scheduler")] = False,
    disable_cron_manager: Annotated[bool, typer.Option("--disable-cron-manager", help="Disable cron manager")] = False,
    log_level: Annotated[LogLevelType, typer.Option("--log-level", "-l", help="Logging level. Env: SHEPPY_LOG_LEVEL")] = config.log_level,
    shutdown_timeout: Annotated[float, typer.Option("--shutdown-timeout", help="Shutdown timeout in seconds. Env: SHEPPY_SHUTDOWN_TIMEOUT")] = config.shutdown_timeout,
) -> None:
    """Start a worker to process tasks from a queue."""

    if all([disable_job_processing, disable_scheduler, disable_cron_manager]):
        raise ValueError("At least one processing type must be enabled")

    if backend_url is None:
        backend_url = "redis://127.0.0.1:6379"
    backend_instance = _create_backend_from_url(backend_url)

    _log_level = LogLevel(log_level)

    # deduplicate queues to prevent unexpected behavior
    queues = []
    for q in queue:
        if q in queues:
            console.print(f"[yellow]Warning: Queue '{q}' provided multiple times.[/yellow]")
            continue
        queues.append(q)

    # add current working directory to Python path to allow importing tasks
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    _s = "s" if len(queues) > 1 else ""
    queue_s = "[/bold]', '[bold]".join(queues)

    _os = " [yellow]\\[oneshot]" if oneshot and not autoreload else ""
    _mt = f" [yellow]\\[max_tasks: {max_tasks}]" if max_tasks and not autoreload else ""

    console.print(f"[cyan]Starting worker for queue{_s} '[bold]{queue_s}[/bold]'[/cyan]{_os}{_mt}")
    console.print(f"  Backend: [yellow]{type(backend_instance).__name__}[/yellow] [gray0]\\[{backend_url}][/gray0]")
    console.print(f"  Job processing: [yellow]{not disable_job_processing}[/yellow]"
                  f"  Scheduler: [yellow]{not disable_scheduler}[/yellow]"
                  f"  Cron Manager: [yellow]{not disable_cron_manager}[/yellow]")
    console.print(f"  Max concurrent tasks: [yellow]{max_concurrent}[/yellow]")
    console.print()

    if autoreload:
        if max_tasks:
            console.print("[yellow]Warning: --max-tasks is not compatible with --reload, ignoring[/yellow]")
        if oneshot:
            console.print("[yellow]Warning: --oneshot is not compatible with --reload, ignoring[/yellow]")

        run_process('.', target=_start_worker,
                    args=(queues, backend_instance, max_concurrent, max_prefetch, _log_level,
                          shutdown_timeout, disable_job_processing, disable_scheduler, disable_cron_manager),
                    callback=lambda _: console.print("Detected file changes, reloading worker..."))
    else:
        _start_worker(queues, backend_instance, max_concurrent, max_prefetch, _log_level,
                      shutdown_timeout, disable_job_processing, disable_scheduler, disable_cron_manager,
                      oneshot, max_tasks)


def _start_worker(queues: list[str], backend: Backend, max_concurrent: int, max_prefetch_tasks: int | None,
                  log_level: LogLevel, shutdown_timeout: float,
                  disable_job_processing: bool, disable_scheduler: bool, disable_cron_manager: bool,
                  oneshot: bool = False, max_tasks: int | None = None,
                  ) -> None:

    loggers = [
        logging.getLogger("sheppy.worker"),
        logging.getLogger("sheppy.middleware"),
    ]

    for logger in loggers:
        if not logger.hasHandlers():
            logger.setLevel(log_level.to_logging_level())
            logger.addHandler(RichHandler(
                rich_tracebacks=True,
                tracebacks_show_locals=True,
                log_time_format="[%X] ",
                show_path=False
            ))

    worker = Worker(queues, backend=backend, max_concurrent_tasks=max_concurrent,
                    max_prefetch_tasks=max_prefetch_tasks,
                    shutdown_timeout=shutdown_timeout,
                    enable_job_processing=not disable_job_processing,
                    enable_scheduler=not disable_scheduler,
                    enable_cron_manager=not disable_cron_manager)
    asyncio.run(worker.work(max_tasks=max_tasks, oneshot=oneshot))
