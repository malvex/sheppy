import asyncio
import logging

import typer
from rich.logging import RichHandler

from sheppy._localkv.server import start_server

from ..utils import LogLevel, console


def dev_server(
    host: str = typer.Option("127.0.0.1", "--host", "-H", help="IP to bind to"),
    port: int = typer.Option(17420, "--port", "-p", help="What port it should run at"),
    log_level: LogLevel = typer.Option(LogLevel.info, "--log-level", "-l", help="Logging level"),
) -> None:
    """Start a local key-value server."""

    server_logger = logging.getLogger("sheppy.localkv.server")

    if not server_logger.hasHandlers():
        server_logger.setLevel(log_level.to_logging_level())
        server_logger.addHandler(RichHandler(
            rich_tracebacks=True,
            tracebacks_show_locals=True,
            log_time_format="[%X] ",
            show_path=False
        ))

    try:
        asyncio.run(start_server(host=host, port=port))
    except Exception as e:
        console.print(str(e))
