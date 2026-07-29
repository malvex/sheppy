import asyncio
import os
import sys
from typing import Annotated
from uuid import UUID

import typer

from sheppy import Queue
from sheppy._config import config
from sheppy.exceptions import TaskCancellationError
from sheppy.queue import _create_backend_from_url

from ...utils import OutputFormat, console, print_json, task_status_label


def cancel(
    task_id: Annotated[str, typer.Argument(help="Task ID to cancel")],
    queue: Annotated[str, typer.Option("--queue", "-q", help="Queue name. Env: SHEPPY_QUEUE")] = config.queue_list[0],
    backend_url: Annotated[str | None, typer.Option("--backend-url", "-u", help="Backend URL. Env: SHEPPY_BACKEND_URL")] = config.backend_url,
    format_output: Annotated[OutputFormat, typer.Option("--format", help="Output format")] = OutputFormat.table,
) -> None:
    """Cancel a pending or scheduled task so it is never executed."""

    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    async def _cancel(backend_url: str | None) -> None:
        if backend_url is None:
            backend_url = "redis://127.0.0.1:6379"
        backend_instance = _create_backend_from_url(backend_url)
        q = Queue(backend_instance, queue)

        try:
            uuid_obj = UUID(task_id)
        except ValueError:
            console.print("[red]Error: Task ID must be UUID format[/red]")
            raise typer.Exit(1) from None

        try:
            task = await q.cancel(uuid_obj)
        except TaskCancellationError as e:
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1) from None

        if format_output == OutputFormat.json:
            task_dict = task.model_dump(mode='json')
            task_dict["queue"] = queue
            task_dict["queue_status"] = task_status_label(task)
            task_dict["cancelled"] = True
            print_json(task_dict)
            return

        console.print(f"[green]✓ Task {task_id} has been cancelled[/green]")
        console.print(f"  Function: [blue]{task.spec.func}[/blue]")
        console.print(f"  Status: [magenta]{task.status}[/magenta]")

    asyncio.run(_cancel(backend_url))
