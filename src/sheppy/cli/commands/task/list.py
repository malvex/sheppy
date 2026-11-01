import asyncio
import os
import sys
from datetime import datetime, timezone
from enum import Enum
from typing import Annotated

import typer
from rich.table import Table

from sheppy import Queue, Task
from sheppy._config import config
from sheppy._queue import _create_backend_from_url

from ..._utils import (
    OutputFormat,
    console,
    humanize_datetime,
    print_json,
    task_status_label,
)

_MIN_DATETIME = datetime.min.replace(tzinfo=timezone.utc)


class StatusFilter(str, Enum):
    all = "all"
    new = "new"
    pending = "pending"
    scheduled = "scheduled"
    processing = "processing"
    retrying = "retrying"
    completed = "completed"
    failed = "failed"
    crashed = "crashed"
    cancelled = "cancelled"
    unknown = "unknown"


class TaskSortField(str, Enum):
    created = "created"
    finished = "finished"
    scheduled = "scheduled"
    function = "function"
    status = "status"


class SortOrder(str, Enum):
    asc = "asc"
    desc = "desc"


def _matches_function(task: Task, function: str) -> bool:
    return function in (task.spec.func, task.spec.func.rsplit(":", 1)[-1])


def _sort_tasks(tasks: list[Task], sort_by: TaskSortField, order: SortOrder) -> None:
    reverse = order == SortOrder.desc

    if sort_by == TaskSortField.created:
        tasks.sort(key=lambda task: task.created_at, reverse=reverse)
    elif sort_by == TaskSortField.finished:
        tasks.sort(key=lambda task: task.finished_at or _MIN_DATETIME, reverse=reverse)
    elif sort_by == TaskSortField.scheduled:
        tasks.sort(key=lambda task: task.scheduled_at or _MIN_DATETIME, reverse=reverse)
    elif sort_by == TaskSortField.function:
        tasks.sort(key=lambda task: task.spec.func, reverse=reverse)
    else:
        tasks.sort(key=task_status_label, reverse=reverse)


def _display_status(task: Task) -> str:
    status = task_status_label(task)
    if status == "completed":
        return "[green]completed[/green]"
    if status == "crashed":
        return "[bold][red]crashed[/red][/bold]"
    if status == "retrying":
        return "[yellow]retrying[/yellow]"
    if status == "failed":
        return "[red]failed[/red]"
    if status == "scheduled":
        return "[magenta]scheduled[/magenta]"
    return status


def list_tasks(
    queue: Annotated[str, typer.Option("--queue", "-q", help="Queue name. Env: SHEPPY_QUEUE")] = config.queue_list[0],
    backend_url: Annotated[str | None, typer.Option("--backend-url", "-u", help="Backend URL. Env: SHEPPY_BACKEND_URL")] = config.backend_url,
    status_filter: Annotated[StatusFilter, typer.Option("--status", "-s", help="Filter by status")] = StatusFilter.all,
    function: Annotated[str | None, typer.Option("--function", help="Filter by fully qualified or bare function name")] = None,
    limit: Annotated[int, typer.Option("--limit", "-l", min=1, help="Maximum number of tasks to show")] = 100,
    sort_by: Annotated[TaskSortField, typer.Option("--sort-by", help="Field to sort tasks by")] = TaskSortField.created,
    order: Annotated[SortOrder, typer.Option("--order", help="Sort order")] = SortOrder.desc,
    format_output: Annotated[OutputFormat, typer.Option("--format", "-f", help="Output format")] = OutputFormat.table,
) -> None:
    """List tasks."""

    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    async def _list(backend_url: str | None) -> None:
        if backend_url is None:
            backend_url = "redis://127.0.0.1:6379"
        backend_instance = _create_backend_from_url(backend_url)
        q = Queue(backend_instance, queue)

        tasks = []

        all_backend_tasks = await q.get_all_tasks()
        tasks = [
            task for task in all_backend_tasks
            if (status_filter == StatusFilter.all or task_status_label(task) == status_filter.value)
            and (function is None or _matches_function(task, function))
        ]
        total_matching = len(tasks)

        _sort_tasks(tasks, sort_by, order)
        tasks = tasks[:limit]

        if format_output == OutputFormat.json:

            tasks_for_json = []
            for task in tasks:
                task_dict = task.model_dump(mode="json")
                task_dict["queue_status"] = task_status_label(task)
                tasks_for_json.append(task_dict)

            print_json({
                "queue": queue,
                "count": len(tasks),
                "tasks": tasks_for_json,
            })
            return

        if not tasks:
            filters: list[str] = []
            if status_filter != StatusFilter.all:
                filters.append(f"status '{status_filter.value}'")
            if function is not None:
                filters.append(f"function '{function}'")
            filter_description = f" with {' and '.join(filters)}" if filters else ""
            console.print(f"[yellow]No tasks found in queue '{queue}'{filter_description}[/yellow]")
            return

        table = Table(title=f"Tasks in [bold cyan]{queue}[/bold cyan] (showing {len(tasks)} of {total_matching})")
        table.add_column("Task ID", style="dim")
        table.add_column("Function", style="blue")
        table.add_column("Status", style="yellow")
        table.add_column("Created (UTC)", style="blue")
        table.add_column("Finished (UTC)", style="blue")
        table.add_column("Scheduled (UTC)", style="blue")

        for task in tasks:
            table.add_row(
                str(task.id),
                task.spec.func,
                _display_status(task),
                humanize_datetime(task.created_at),
                humanize_datetime(task.finished_at),
                humanize_datetime(task.scheduled_at if not task.finished_at else None),
            )

        console.print(table)

    asyncio.run(_list(backend_url))
