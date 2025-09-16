import asyncio
import os
import sys
from typing import Annotated
from uuid import UUID

import typer

from sheppy import Queue

from ...utils import BackendType, console, get_backend


def info(
    task_id: Annotated[str, typer.Argument(help="Task ID to get info for")],
    queue: Annotated[str, typer.Option("--queue", "-q", help="Name of queue")] = "default",
    backend: Annotated[BackendType, typer.Option("--backend", "-b", help="Queue backend type")] = BackendType.redis,
    redis_url: Annotated[str, typer.Option("--redis-url", "-r", help="Redis server URL")] = "redis://127.0.0.1:6379",
) -> None:
    """Get detailed information about a specific task."""

    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    async def _info() -> None:
        backend_instance = get_backend(backend, redis_url)
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

        if task.completed:
            status = "[green]completed[/green]"
        elif task.error:
            status = "[red]failed[/red]"
        else:
            status = "pending"

        console.print("\n[bold cyan]Task Information[/bold cyan]")
        console.print(f"  ID: [yellow]{task.id}[/yellow]")
        console.print(f"  Function: [blue]{task.spec.func or 'Unknown'}[/blue]")
        console.print(f"  Status: {status}")
        console.print(f"  Queue: [cyan]{queue}[/cyan]")
        console.print(f"  Created: [dim]{task.created_at}[/dim]")
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

    asyncio.run(_info())
