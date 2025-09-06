import asyncio
import importlib
import json
import os
import sys
import time
from typing import Annotated

import typer

from sheppy import Task
from sheppy.utils.task_execution import TaskProcessor

from ...utils import console


def test(
    function: Annotated[str, typer.Argument(help="Function to test (module:function format)")],
    args: Annotated[str, typer.Option("--args", "-a", help="JSON array of positional arguments")] = "[]",
    kwargs: Annotated[str, typer.Option("--kwargs", "-k", help="JSON object of keyword arguments")] = "{}",
    trace: Annotated[bool, typer.Option("--trace", "-t", help="Show full execution trace")] = False,
) -> None:
    """Test run a task function without queuing it."""

    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    async def run_test() -> None:

        try:
            # ! FIXME - better input method
            parsed_args = json.loads(args)
            parsed_kwargs = json.loads(kwargs)
        except json.JSONDecodeError as e:
            console.print(f"[red]Invalid JSON in arguments: {e}[/red]")
            return

        try:
            module_name, func_name = function.rsplit(":", 1)
            module = importlib.import_module(module_name)
            func = getattr(module, func_name)
        except (ValueError, ImportError, AttributeError) as e:
            console.print(f"[red]Could not import function '{function}': {e}[/red]")
            return

        console.print(f"\n[cyan]Testing function [bold]{function}[/bold][/cyan]")

        console.print("\n[bold]Input:[/bold]")
        if parsed_args:
            console.print(f"  args: {json.dumps(parsed_args, indent=2)}")
        if parsed_kwargs:
            console.print(f"  kwargs: {json.dumps(parsed_kwargs, indent=2)}")

        console.print("\n[bold]Execution:[/bold]")
        start_time = time.time()

        try:
            task = func(*parsed_args, **parsed_kwargs)

            if not isinstance(task, Task):
                console.print(f"[red]Function '{function}' is not a task.[/red]")
                return

            exception, executed_task = await TaskProcessor.execute_task(task, "cli")

            if exception:
                raise exception

            result = executed_task.result

            end_time = time.time() - start_time

            console.print(f"\n[bold green]✓ Success[/bold green] (took {end_time:.3f}s)")
            console.print("\n[bold]Result:[/bold]")

            if result is None:
                console.print("[dim]None[/dim]")
            elif isinstance(result, dict | list):
                console.print(json.dumps(result, indent=2, default=str))
            else:
                console.print(str(result))

            console.print("\n[bold]Task Details:[/bold]")
            console.print(f"  Task ID: {task.id}")
            console.print(f"  Function: {task.spec.func}")

        except Exception as e:
            if trace:
                console.print_exception()
                return

            end_time = time.time() - start_time
            console.print(f"\n[bold red]✗ Failed[/bold red] (took {end_time:.3f}s)")
            console.print("\n[bold]Exception:[/bold]")
            console.print(f"  Type: [red]{type(e).__name__}[/red]")
            console.print(f"  Message: [red]{e!s}[/red]")


    asyncio.run(run_test())
