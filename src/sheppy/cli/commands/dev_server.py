import asyncio
import logging
from typing import Annotated

import typer
from rich.logging import RichHandler

from sheppy._localkv.server import start_server

from ..utils import LogLevel, console


def dev_server(
    host: Annotated[str, typer.Option("--host", "-H", help="IP to bind to")] = "127.0.0.1",
    port: Annotated[int, typer.Option("--port", "-p", help="What port it should run at")] = 17420,
    log_level: Annotated[LogLevel, typer.Option("--log-level", "-l", help="Logging level")] = LogLevel.info,
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
