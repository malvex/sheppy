import asyncio
import logging
import os
import sys

import typer
from rich.logging import RichHandler
from watchfiles import run_process

from sheppy import Backend, Worker

from ..utils import BackendType, LogLevel, console, get_backend


def work(
    queue: str = typer.Option("default", "--queue", "-q", help="Name of queue to process"),
    backend: BackendType = typer.Option(BackendType.redis, "--backend", "-b", help="Queue backend type"),
    redis_url: str = typer.Option("redis://127.0.0.1:6379", "--redis-url", "-r", help="Redis server URL"),
    max_concurrent: int = typer.Option(10, "--max-concurrent", "-c", help="Max concurrent tasks", min=1),
    autoreload: bool = typer.Option(False, "--reload", help="Reload worker on file changes"),
    log_level: LogLevel = typer.Option(LogLevel.info, "--log-level", "-l", help="Logging level"),
) -> None:
    """Start a worker to process tasks from a queue."""

    # add current working directory to Python path to allow importing tasks
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    backend_instance = get_backend(backend, redis_url)

    _bs = ""
    if backend == BackendType.redis:
        _bs = f" [gray0]\\[{redis_url}][/gray0]"

    console.print(f"[cyan]Starting worker for queue '[bold]{queue}[/bold]'...[/cyan]")
    console.print(f"  Backend: [yellow]{backend.value}[/yellow]{_bs}")
    console.print(f"  Max concurrent tasks: [yellow]{max_concurrent}[/yellow]")
    console.print()

    if autoreload:
        run_process('.', target=_start_worker,
                    args=(queue, backend_instance, max_concurrent, log_level),
                    callback=lambda _: console.print("Detected file changes, reloading worker..."))
    else:
        _start_worker(queue, backend_instance, max_concurrent, log_level)


def _start_worker(queue: str, backend: Backend, max_concurrent: int, log_level: LogLevel) -> None:

    worker_logger = logging.getLogger("sheppy.worker")

    if not worker_logger.hasHandlers():
        worker_logger.setLevel(log_level.to_logging_level())
        worker_logger.addHandler(RichHandler(
            rich_tracebacks=True,
            tracebacks_show_locals=True,
            log_time_format="[%X] ",
            show_path=False
        ))

    worker = Worker(queue, backend=backend, max_concurrent_tasks=max_concurrent)
    asyncio.run(worker.work())
