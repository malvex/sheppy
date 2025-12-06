import asyncio
from typing import Annotated

import typer
from rich.table import Table

from ...utils import BackendType, console, get_backend


def list_queues(
    backend: Annotated[BackendType, typer.Option("--backend", "-b", help="Queue backend type")] = BackendType.redis,
    redis_url: Annotated[str, typer.Option("--redis-url", "-r", help="Redis server URL")] = "redis://127.0.0.1:6379",
    local_backend_embedded_server: bool = typer.Option(False, "--local-backend-embedded-server", help="Enable embedded server (local backend)"),
    local_backend_port: int = typer.Option("17420", "--local-backend-port", help="Local backend port"),
) -> None:
    """List all queues with their pending task counts."""

    async def _list() -> None:
        backend_instance = get_backend(backend, redis_url, local_backend_port, local_backend_embedded_server)

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


    asyncio.run(_list())
