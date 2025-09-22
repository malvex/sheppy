import asyncio
import importlib
import json
import os
import sys
from typing import Annotated

import typer

from sheppy import Queue, Task

from ...utils import BackendType, console, get_backend


def add(
    function: Annotated[str, typer.Argument(help="Function to add (module:function format)")],
    args: Annotated[str, typer.Option("--args", "-a", help="JSON array of positional arguments")] = "[]",
    kwargs: Annotated[str, typer.Option("--kwargs", "-k", help="JSON object of keyword arguments")] = "{}",
    wait: Annotated[bool, typer.Option("--wait", "-w", help="Wait for task result")] = False,
    timeout: Annotated[float, typer.Option("--timeout", "-t", help="Timeout in seconds when waiting for result")] = 0.0,
    queue: Annotated[str, typer.Option("--queue", "-q", help="Name of queue")] = "default",
    backend: Annotated[BackendType, typer.Option("--backend", "-b", help="Queue backend type")] = BackendType.redis,
    redis_url: Annotated[str, typer.Option("--redis-url", "-r", help="Redis server URL")] = "redis://127.0.0.1:6379",
) -> None:
    """Add a new task to a queue."""

    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    async def _add() -> None:
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

        await q.add(task)

        console.print("[green]✓[/green] Task added successfully")

        if not wait:
            return

        s_timeout = f" (timeout: {timeout}s)" if timeout != 0 else ""
        console.print(f"\n[cyan]Waiting for result{s_timeout}...[/cyan]")

        try:
            result_task = await q.wait_for(task, timeout=timeout)
        except TimeoutError:
            console.print(f"\n[yellow]Timeout: Task did not complete within {timeout} seconds[/yellow]")
            console.print(f"[yellow]Task is still running in the background. Use 'sheppy task info {task.id}' to check status.[/yellow]")
            raise typer.Exit(1) from None

        if result_task and result_task.completed:
            console.print("\n[green]✓ Task completed successfully[/green]")
            console.print("\n[bold]Result:[/bold]")
            if result_task.result is None:
                console.print("[dim]None[/dim]")
            elif isinstance(result_task.result, dict | list):
                console.print(json.dumps(result_task.result, indent=2, default=str))
            else:
                console.print(str(result_task.result))
        elif result_task and result_task.error:
            console.print("\n[red]✗ Task failed[/red]")
            console.print("\n[bold red]Error:[/bold red]")
            console.print(f"  {result_task.error}")
        else:
            # should never happen
            console.print("\n[yellow]Task not completed yet (still pending)[/yellow]")

    asyncio.run(_add())
