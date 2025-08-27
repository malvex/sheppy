import typer
from rich import print as rprint

from sheppy import __version__

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


app.command()(work)
