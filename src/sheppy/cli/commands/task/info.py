import asyncio
import os
import sys
from typing import Annotated
from uuid import UUID

import typer

from sheppy import Queue
from sheppy._config import config
from sheppy.queue import _create_backend_from_url

from ...utils import console


def info(
    task_id: Annotated[str, typer.Argument(help="Task ID to get info for")],
    queue: Annotated[str, typer.Option("--queue", "-q", help="Queue name. Env: SHEPPY_QUEUE")] = config.queue_list[0],
    backend_url: Annotated[str | None, typer.Option("--backend-url", "-u", help="Backend URL. Env: SHEPPY_BACKEND_URL")] = config.backend_url,
) -> None:
    """Get detailed information about a specific task."""

    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    async def _info(backend_url: str | None) -> None:
        if backend_url is None:
            backend_url = "redis://127.0.0.1:6379"
        backend_instance = _create_backend_from_url(backend_url)
        q = Queue(backend_instance, queue)

        try:
            uuid_obj = UUID(task_id)
        except ValueError:
            console.print("[red]Error: Task ID must be UUID format[/red]")
            raise typer.Exit(1) from None

        task = await q.get_task(uuid_obj)

        if not task:
            console.print(f"[red]Error: Task {task_id} not found in queue '{queue}'[/red]")
            raise typer.Exit(1)

        if task.status == 'completed':
            status = "[green]completed[/green]"
        elif task.error:
            status = "[red]failed[/red]"
        elif task.status == 'scheduled':
            status = "[magenta]scheduled[/magenta]"
        else:
            status = task.status

        console.print("\n[bold cyan]Task Information[/bold cyan]")
        console.print(f"  ID: [yellow]{task.id}[/yellow]")
        console.print(f"  Function: [blue]{task.spec.func or 'Unknown'}[/blue]")
        console.print(f"  Status: {status}")
        console.print(f"  Queue: [cyan]{queue}[/cyan]")
        console.print(f"  Created: [dim]{task.created_at}[/dim]")
        if task.scheduled_at:
            console.print(f"  Scheduled: [dim]{task.scheduled_at}[/dim]")
        if task.finished_at:
            console.print(f"  Finished: [dim]{task.finished_at}[/dim]")
        if task.config.retry:  # ! FIXME
            console.print(f"  Retries: [magenta]{task.retry_count}[/magenta] (max [magenta]{task.config.retry}[/magenta])")

        if task.spec.args:
            console.print("\n[bold]Arguments:[/bold]")
            for arg in task.spec.args:
                console.print(f"  - {arg}")

        if task.spec.kwargs:
            console.print("\n[bold]Keyword Arguments:[/bold]")
            for k, v in task.spec.kwargs.items():
                console.print(f"  {k}: {v}")

        if task.result is not None:
            console.print("\n[bold]Result:[/bold]")
            console.print(f"  {task.result}")

        if task.error:
            console.print("\n[bold red]Error:[/bold red]")
            console.print(f"  {task.error}")

    asyncio.run(_info(backend_url))
