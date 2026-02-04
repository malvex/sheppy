import asyncio
from typing import Annotated

import typer
from rich.table import Table

from sheppy._config import config
from sheppy.queue import _create_backend_from_url

from ...utils import console


def list_queues(
    backend_url: Annotated[str | None, typer.Option("--backend-url", "-u", help="Backend URL. Env: SHEPPY_BACKEND_URL")] = config.backend_url,
) -> None:
    """List all queues with their pending task counts."""

    async def _list(backend_url: str | None) -> None:
        if backend_url is None:
            backend_url = "redis://127.0.0.1:6379"
        backend_instance = _create_backend_from_url(backend_url)

        await backend_instance.connect()
        queues = await backend_instance.list_queues()

        if not queues:
            console.print("[yellow]No active queues found[/yellow]")
            return

        table = Table(title="Active Queues")
        table.add_column("Queue Name", style="cyan")
        table.add_column("Pending Tasks", justify="right", style="yellow")

        total_pending = 0
        for queue_name, pending_count in queues.items():
            table.add_row(queue_name, str(pending_count))
            total_pending += pending_count

        console.print(table)

        if len(queues) > 1:
            console.print(f"\n[dim]Total: {len(queues)} queues, {total_pending} pending tasks[/dim]")


    asyncio.run(_list(backend_url))
