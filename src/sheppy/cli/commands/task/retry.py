import asyncio
import os
import sys
from typing import Annotated
from uuid import UUID

import typer

from sheppy import Queue

from ...utils import BackendType, console, get_backend


def retry(
    task_id: Annotated[str, typer.Argument(help="Task ID to retry")],
    queue: Annotated[str, typer.Option("--queue", "-q", help="Name of queue")] = "default",
    backend: Annotated[BackendType, typer.Option("--backend", "-b", help="Queue backend type")] = BackendType.redis,
    redis_url: Annotated[str, typer.Option("--redis-url", "-r", help="Redis server URL")] = "redis://127.0.0.1:6379",
    force: Annotated[bool, typer.Option("--force", "-f", help="Force retry even if task hasn't failed")] = False,
) -> None:
    """Retry a failed task by re-queueing it."""

    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    async def _retry() -> None:
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

        is_failed = task.error and (task.finished_at or not task.completed)

        if not is_failed and not force:
            if task.completed:
                console.print(f"[yellow]Task {task_id} has already completed successfully[/yellow]")
            else:
                console.print(f"[yellow]Task {task_id} is still pending/in-progress[/yellow]")
            console.print("Use --force to retry anyway")
            raise typer.Exit(1)

        success = await q.retry(task, force=True)

        if success:
            console.print(f"[green]✓ Task {task_id} has been re-queued for retry[/green]")
            console.print(f"  Function: [blue]{task.spec.func}[/blue]")
            if task.error:
                console.print(f"  Previous error: [dim]{task.error}[/dim]")
            console.print(f"  Retry count: [magenta]{task.retry_count}[/magenta]")
        else:
            console.print(f"[red]Failed to re-queue task {task_id}[/red]")
            raise typer.Exit(1)

    asyncio.run(_retry())
