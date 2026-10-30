"""
This file contains utility functions meant for internal use only. Expect breaking changes if you use them directly.
"""

import logging
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .._logging import ContextLoggerAdapter

if sys.version_info >= (3, 11):
    import tomllib
else:  # Python 3.10
    import tomli as tomllib

logger = ContextLoggerAdapter(logging.getLogger("sheppy.worker.cron"))


@dataclass(frozen=True)
class CronDeclaration:
    task: str
    expression: str
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)
    queue: str | None = None


def _parse_entry(index: int, entry: Any) -> CronDeclaration | None:
    if not isinstance(entry, dict):
        logger.warning("Ignoring cron declaration: expected a table", extra={"log_context": {"entry": index}})
        return None

    task = entry.get("task")
    expression = entry.get("expression")

    if not isinstance(task, str) or ":" not in task:
        logger.warning("Ignoring cron declaration: 'task' must use 'module:function' format", extra={"log_context": {"entry": index}})
        return None

    if not isinstance(expression, str) or not expression.strip():
        logger.warning("Ignoring cron declaration: 'expression' must be a non-empty string", extra={"log_context": {"entry": index, "function": task}})
        return None

    args = entry.get("args", [])
    if not isinstance(args, list):
        logger.warning("Ignoring cron declaration: 'args' must be a list", extra={"log_context": {"entry": index, "function": task}})
        return None

    kwargs = entry.get("kwargs", {})
    if not isinstance(kwargs, dict):
        logger.warning("Ignoring cron declaration: 'kwargs' must be a table", extra={"log_context": {"entry": index, "function": task}})
        return None

    queue = entry.get("queue")
    if queue is not None and not isinstance(queue, str):
        logger.warning("Ignoring cron declaration: 'queue' must be a string", extra={"log_context": {"entry": index, "function": task}})
        return None

    return CronDeclaration(task=task, expression=expression, args=tuple(args), kwargs=dict(kwargs), queue=queue)


def load_cron_declarations(path: str | Path) -> list[CronDeclaration] | None:
    path = Path(path)

    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        logger.debug("Cron config file not found", extra={"log_context": {"path": path}})
        return None
    except (OSError, tomllib.TOMLDecodeError) as e:
        logger.error("Failed to parse cron config", extra={"log_context": {"path": path, "error": f"{type(e).__name__}: {e}"}})
        return None

    entries = data.get("tool", {}).get("sheppy", {}).get("cron", [])

    if not isinstance(entries, list):
        logger.error("Invalid cron config: [tool.sheppy.cron] must be an array of tables", extra={"log_context": {"path": path}})
        return None

    declarations = []
    for index, entry in enumerate(entries, 1):
        declaration = _parse_entry(index, entry)
        if declaration is not None:
            declarations.append(declaration)

    return declarations
