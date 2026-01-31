import asyncio
import logging
import os
import sys
from typing import Annotated

import typer
from rich.logging import RichHandler
from watchfiles import run_process

from sheppy import Backend, Worker

from ..utils import BackendType, LogLevel, console, get_backend


def work(
    queue: Annotated[list[str] | None, typer.Option("--queue", "-q", help="Name of queue to process (can be used multiple times)")] = None,
    backend: Annotated[BackendType, typer.Option("--backend", "-b", help="Queue backend type")] = BackendType.redis,
    redis_url: Annotated[str, typer.Option("--redis-url", "-r", help="Redis server URL")] = "redis://127.0.0.1:6379",
    local_backend_embedded_server: Annotated[bool, typer.Option("--local-backend-embedded-server", help="Enable embedded server (local backend)")] = False,
    local_backend_port: Annotated[int, typer.Option("--local-backend-port", help="Local backend port")] = 17420,
    max_concurrent: Annotated[int, typer.Option("--max-concurrent", "-c", help="Max concurrent tasks", min=1)] = 10,
    max_prefetch: Annotated[int | None, typer.Option("--max-prefetch", help="Max prefetch tasks", min=1)] = None,
    autoreload: Annotated[bool, typer.Option("--reload", help="Reload worker on file changes")] = False,
    oneshot: Annotated[bool, typer.Option("--oneshot", help="Process pending tasks and then exit")] = False,
    max_tasks: Annotated[int | None, typer.Option("--max-tasks", help="Maximum amount of tasks to process", min=1)] = None,
    disable_job_processing: Annotated[bool, typer.Option("--disable-job-processing", help="Disable job processing")] = False,
    disable_scheduler: Annotated[bool, typer.Option("--disable-scheduler", help="Disable scheduler")] = False,
    disable_cron_manager: Annotated[bool, typer.Option("--disable-cron-manager", help="Disable cron manager")] = False,
    log_level: Annotated[LogLevel, typer.Option("--log-level", "-l", help="Logging level")] = LogLevel.info,
) -> None:
    """Start a worker to process tasks from a queue."""

    if all([disable_job_processing, disable_scheduler, disable_cron_manager]):
        raise ValueError("At least one processing type must be enabled")

    if queue is None:
        queue = ["default"]

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

    backend_instance = get_backend(backend, redis_url, local_backend_port, local_backend_embedded_server)

    _bs = ""
    if backend == BackendType.redis:
        _bs = f" [gray0]\\[{redis_url}][/gray0]"

    _s = "s" if len(queues) > 1 else ""
    queue_s = "[/bold]', '[bold]".join(queues)

    _os = " [yellow]\\[oneshot]" if oneshot and not autoreload else ""
    _mt = f" [yellow]\\[max_tasks: {max_tasks}]" if max_tasks and not autoreload else ""

    console.print(f"[cyan]Starting worker for queue{_s} '[bold]{queue_s}[/bold]'[/cyan]{_os}{_mt}")
    console.print(f"  Backend: [yellow]{backend.value}[/yellow]{_bs}")
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
                    args=(queues, backend_instance, max_concurrent, max_prefetch, log_level,
                          disable_job_processing, disable_scheduler, disable_cron_manager),
                    callback=lambda _: console.print("Detected file changes, reloading worker..."))
    else:
        _start_worker(queues, backend_instance, max_concurrent, max_prefetch, log_level,
                      disable_job_processing, disable_scheduler, disable_cron_manager,
                      oneshot, max_tasks)


def _start_worker(queues: list[str], backend: Backend, max_concurrent: int, max_prefetch_tasks: int | None,
                  log_level: LogLevel,
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
                    enable_job_processing=not disable_job_processing,
                    enable_scheduler=not disable_scheduler,
                    enable_cron_manager=not disable_cron_manager)
    asyncio.run(worker.work(max_tasks=max_tasks, oneshot=oneshot))
