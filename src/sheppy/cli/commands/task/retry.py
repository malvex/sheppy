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


def retry(
    task_id: Annotated[str, typer.Argument(help="Task ID to retry")],
    queue: Annotated[str, typer.Option("--queue", "-q", help="Queue name. Env: SHEPPY_QUEUE")] = config.queue_list[0],
    backend_url: Annotated[str | None, typer.Option("--backend-url", "-u", help="Backend URL. Env: SHEPPY_BACKEND_URL")] = config.backend_url,
    force: Annotated[bool, typer.Option("--force", "-f", help="Force retry even if task hasn't failed")] = False,
) -> None:
    """Retry a failed task by re-queueing it."""

    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    async def _retry(backend_url: str | None) -> None:
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

        if not task.error and not force:
            if task.status == 'completed':
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

    asyncio.run(_retry(backend_url))
