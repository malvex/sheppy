import asyncio
import json
import os
import sys
from datetime import datetime
from enum import Enum
from typing import Annotated

import typer
from rich.table import Table

from sheppy import Queue

from ...utils import BackendType, OutputFormat, console, get_backend, humanize_datetime


class StatusFilter(str, Enum):
    all = "all"
    pending = "pending"
    scheduled = "scheduled"
    completed = "completed"
    failed = "failed"



def list_tasks(
    queue: Annotated[str, typer.Option("--queue", "-q", help="Name of queue")] = "default",
    status_filter: Annotated[StatusFilter, typer.Option("--status", "-s", help="Filter by status")] = StatusFilter.all,
    # limit: Annotated[int, typer.Option("--limit", "-l", help="Maximum number of tasks to show")] = 100,
    backend: Annotated[BackendType, typer.Option("--backend", "-b", help="Queue backend type")] = BackendType.redis,
    redis_url: Annotated[str, typer.Option("--redis-url", "-r", help="Redis server URL")] = "redis://127.0.0.1:6379",
    format_output: Annotated[OutputFormat, typer.Option("--format", "-f", help="Output format")] = OutputFormat.table,
) -> None:
    """List all tasks."""

    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    async def _list() -> None:
        backend_instance = get_backend(backend, redis_url)
        q = Queue(backend_instance, queue)

        tasks = []

        all_backend_tasks = await q.get_all_tasks()

        #if status_filter in ["all", "completed", "failed"]:

        for task in all_backend_tasks:
            if task.completed:
                queue_status = "[green]completed[/green]"
            elif task.error:
                queue_status = "[red]failed[/red]"
            #elif str(task.id) in all_scheduled_task_ids:
            elif task.scheduled_at:
                queue_status = "[magenta]scheduled[/magenta]"
            else:
                queue_status = "pending"

            tasks.append((task, queue_status))

        tasks.sort(key=lambda x: x[0].created_at or datetime.min, reverse=True)

        if format_output == OutputFormat.json:

            tasks_for_json = []
            for task, queue_status in tasks:
                task_dict = task.model_dump(mode='json')
                task_dict["queue_status"] = queue_status
                tasks_for_json.append(task_dict)

            output = {
                "queue": queue,
                "count": len(tasks),
                "tasks": tasks_for_json
            }
            console.print(json.dumps(output, indent=2, default=str))
        else:
            if not tasks:
                console.print(f"[yellow]No tasks found in queue '{queue}' with status '{status_filter.value}'[/yellow]")
                return

            table = Table(title=f"Tasks in [bold cyan]{queue}[/bold cyan] (showing {len(tasks)} of {len(tasks)})")
            table.add_column("Task ID", style="dim")
            table.add_column("Function", style="blue")
            table.add_column("Status", style="yellow")
            table.add_column("Created (UTC)", style="blue")
            table.add_column("Finished (UTC)", style="blue")
            table.add_column("Scheduled (UTC)", style="blue")

            for (task, queue_status) in tasks:  # enumerate(tasks[:limit], 1):


                table.add_row(
                    str(task.id),
                    task.spec.func,
                    queue_status,
                    humanize_datetime(task.created_at),
                    humanize_datetime(task.finished_at),
                    humanize_datetime(task.scheduled_at if not task.finished_at else None),
                )

            console.print(table)

    asyncio.run(_list())
