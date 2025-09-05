import typer
from rich import print as rprint

from sheppy import __version__

from .commands.cron.list import list_crons
from .commands.queue.list import list_queues
from .commands.task.add import add
from .commands.task.info import info
from .commands.task.list import list_tasks
from .commands.task.retry import retry
from .commands.task.schedule import schedule
from .commands.task.test import test
from .commands.work import work

app = typer.Typer(rich_markup_mode="rich", no_args_is_help=True, add_completion=False)


def version_callback(value: bool) -> None:
    if value:
        rprint(f"[green]Sheppy version: {__version__}[/green]")
        raise typer.Exit()


@app.callback()
def callback(
    version: bool = typer.Option(None, "--version", help="Show the version and exit.", callback=version_callback),
) -> None:
    """
    Sheppy - Modern Task Queue
    """
    pass


task_app = typer.Typer(help="Task management commands", no_args_is_help=True)
task_app.command(name="list")(list_tasks)
task_app.command()(info)
task_app.command()(retry)
task_app.command()(test)
task_app.command()(add)
task_app.command()(schedule)

app.add_typer(task_app, name="task")

queue_app = typer.Typer(help="Queue management commands", no_args_is_help=True)
queue_app.command(name="list")(list_queues)

app.add_typer(queue_app, name="queue")

cron_app = typer.Typer(help="Cron management commands", no_args_is_help=True)
cron_app.command(name="list")(list_crons)

app.add_typer(cron_app, name="cron")

app.command()(work)
