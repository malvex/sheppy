import asyncio
from typing import Annotated

import typer
from rich.table import Table

from sheppy import Queue
from sheppy._config import config
from sheppy.queue import _create_backend_from_url

from ...utils import console, humanize_datetime


def list_crons(
    queue: Annotated[str, typer.Option("--queue", "-q", help="Queue name. Env: SHEPPY_QUEUE")] = config.queue_list[0],
    backend_url: Annotated[str | None, typer.Option("--backend-url", "-u", help="Backend URL. Env: SHEPPY_BACKEND_URL")] = config.backend_url,
) -> None:
    """List all active crons."""

    async def _list(backend_url: str | None) -> None:
        if backend_url is None:
            backend_url = "redis://127.0.0.1:6379"
        backend_instance = _create_backend_from_url(backend_url)
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


    asyncio.run(_list(backend_url))
