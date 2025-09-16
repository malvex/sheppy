import asyncio
import importlib
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Annotated

import typer

from sheppy import Queue, Task

from ...utils import BackendType, console, get_backend


def schedule(
    function: Annotated[str, typer.Argument(help="Function to schedule (module:function format")],
    delay: Annotated[str | None, typer.Option("--delay", "-d", help="Delay before task execution (e.g., 30s, 5m, 2h, 1d)")] = None,
    at: Annotated[str | None, typer.Option("--at", help="Execute at specific time (ISO format: 2024-01-20T15:30:00)")] = None,
    args: Annotated[str, typer.Option("--args", "-a", help="JSON array of positional arguments")] = "[]",
    kwargs: Annotated[str, typer.Option("--kwargs", "-k", help="JSON object of keyword arguments")] = "{}",
    queue: Annotated[str, typer.Option("--queue", "-q", help="Name of queue")] = "default",
    backend: Annotated[BackendType, typer.Option("--backend", "-b", help="Queue backend type")] = BackendType.redis,
    redis_url: Annotated[str, typer.Option("--redis-url", "-r", help="Redis server URL")] = "redis://127.0.0.1:6379",
) -> None:
    """Schedule a task to run at a specific time."""

    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    if not delay and not at:
        console.print("[red]Error: You must specify either --delay or --at[/red]")
        raise typer.Exit(1)

    if delay and at:
        console.print("[red]Error: Cannot specify both --delay and --at[/red]")
        raise typer.Exit(1)

    async def _schedule() -> None:
        backend_instance = get_backend(backend, redis_url)
        q = Queue(backend_instance, queue)

        try:
            # ! FIXME - better input method
            parsed_args = json.loads(args)
            parsed_kwargs = json.loads(kwargs)
        except json.JSONDecodeError as e:
            console.print(f"[red]Invalid JSON in arguments: {e}[/red]")
            raise typer.Exit(1) from None

        try:
            module_name, func_name = function.rsplit(":", 1)
            module = importlib.import_module(module_name)
            func = getattr(module, func_name)
        except (ValueError, ImportError, AttributeError) as e:
            console.print(f"[red]Could not import function '{function}': {e}[/red]")
            raise typer.Exit(1) from None

        if delay:
            try:
                if delay.endswith('s'):
                    delta = timedelta(seconds=int(delay[:-1]))
                elif delay.endswith('m'):
                    delta = timedelta(minutes=int(delay[:-1]))
                elif delay.endswith('h'):
                    delta = timedelta(hours=int(delay[:-1]))
                elif delay.endswith('d'):
                    delta = timedelta(days=int(delay[:-1]))
                else:
                    delta = timedelta(seconds=int(delay))

                schedule_time: datetime | timedelta = delta
            except (ValueError, TypeError):
                console.print(f"[red]Error: Invalid delay format '{delay}'. Use format like '30s', '5m', '2h', '1d'[/red]")
                raise typer.Exit(1) from None
        else:
            try:
                if at is None:  # this cannot happen, this is just to make mypy happy
                    return

                dt = datetime.fromisoformat(at)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                schedule_time = dt
            except ValueError:
                console.print(f"[red]Error: Invalid datetime format '{at}'. Use ISO format (e.g., 2024-01-20T15:30:00)[/red]")
                raise typer.Exit(1) from None

        try:
            task = func(*parsed_args, **parsed_kwargs)

            if not isinstance(task, Task):
                console.print(f"[red]Function '{function}' is not a task.[/red]")
                raise typer.Exit(1)
        except Exception as e:
            console.print(f"[red]Error creating task: {e}[/red]")
            raise typer.Exit(1) from None

        console.print(f"  Task ID: [yellow]{task.id}[/yellow]")
        console.print(f"  Function: [blue]{task.spec.func}[/blue]")
        console.print(f"  Queue: [cyan]{queue}[/cyan]")
        if parsed_args:
            console.print(f"  Arguments: {json.dumps(parsed_args, indent=2)}")
        if parsed_kwargs:
            console.print(f"  Keyword arguments: {json.dumps(parsed_kwargs, indent=2)}")

        success = await q.schedule(task, schedule_time)

        if success:
            console.print("[green]✓[/green] Task scheduled successfully")
            if isinstance(schedule_time, timedelta):
                run_at = datetime.now(timezone.utc) + schedule_time
                console.print(f"  Scheduled for: [magenta]{run_at.isoformat()}[/magenta] (in {delay})")
            else:
                console.print(f"  Scheduled for: [magenta]{schedule_time.isoformat()}[/magenta]")

        else:
            console.print("[red]Error: Failed to schedule task[/red]")
            raise typer.Exit(1)

    asyncio.run(_schedule())
