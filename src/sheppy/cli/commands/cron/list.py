import asyncio
from typing import Annotated

import typer
from rich.table import Table

from sheppy import Queue

from ...utils import BackendType, console, get_backend, humanize_datetime


def list_crons(
    queue: Annotated[str, typer.Option("--queue", "-q", help="Name of queue")] = "default",
    backend: Annotated[BackendType, typer.Option("--backend", "-b", help="Queue backend type")] = BackendType.redis,
    redis_url: Annotated[str, typer.Option("--redis-url", "-r", help="Redis server URL")] = "redis://127.0.0.1:6379",
) -> None:
    """List all active crons."""

    async def _list() -> None:
        backend_instance = get_backend(backend, redis_url)
        q = Queue(backend_instance, queue)

        crons = await q.get_crons()

        if not crons:
            console.print(f"[yellow]No crons found in queue '{queue}'[/yellow]")
            return

        table = Table(title="Active Crons")
        table = Table(title=f"Tasks in [bold cyan]{queue}[/bold cyan] (showing {len(crons)} of {len(crons)})")
        table.add_column("Cron ID", style="dim")
        table.add_column("Function", style="blue")
        table.add_column("Args", style="blue")
        table.add_column("Kwargs", style="blue")
        table.add_column("Cron expression", style="yellow")
        # table.add_column("Last run (UTC)", style="blue")
        table.add_column("Next run (UTC)", style="blue")

        for cron in crons:
            table.add_row(
                str(cron.id),
                str(cron.spec.func),
                str(cron.spec.args),
                str(cron.spec.kwargs),
                str(cron.expression),
                # todo - last run
                humanize_datetime(cron.next_run())
            )

        console.print(table)


    asyncio.run(_list())
