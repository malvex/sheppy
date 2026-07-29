import asyncio
import os
import sys
from typing import Annotated
from uuid import UUID

import typer

from sheppy import Queue
from sheppy._config import config
from sheppy.queue import _create_backend_from_url

from ...utils import OutputFormat, console, print_json


def delete(
    task_id: Annotated[str, typer.Argument(help="Task ID to delete")],
    queue: Annotated[str, typer.Option("--queue", "-q", help="Queue name. Env: SHEPPY_QUEUE")] = config.queue_list[0],
    backend_url: Annotated[str | None, typer.Option("--backend-url", "-u", help="Backend URL. Env: SHEPPY_BACKEND_URL")] = config.backend_url,
    format_output: Annotated[OutputFormat, typer.Option("--format", help="Output format")] = OutputFormat.table,
) -> None:
    """Hard-delete a finished task's metadata (cancel pending/scheduled tasks first)."""

    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    async def _delete(backend_url: str | None) -> None:
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
            deleted = await q.delete(uuid_obj)
        except ValueError as e:
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1) from None

        if not deleted:
            console.print(f"[red]Error: Task {task_id} not found in queue '{queue}'[/red]")
            raise typer.Exit(1)

        if format_output == OutputFormat.json:
            print_json({"task_id": task_id, "queue": queue, "deleted": True})
            return

        console.print(f"[green]✓ Task {task_id} has been deleted[/green]")

    asyncio.run(_delete(backend_url))
