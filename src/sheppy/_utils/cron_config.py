"""
This file contains utility functions meant for internal use only. Expect breaking changes if you use them directly.
"""

import logging
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

if sys.version_info >= (3, 11):
    import tomllib
else:  # Python 3.10
    import tomli as tomllib

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CronDeclaration:
    task: str
    expression: str
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)
    queue: str | None = None


def _parse_entry(index: int, entry: Any) -> CronDeclaration | None:
    if not isinstance(entry, dict):
        logger.warning(f"[tool.sheppy.cron] entry #{index} is not a table, skipping")
        return None

    task = entry.get("task")
    expression = entry.get("expression")

    if not isinstance(task, str) or ":" not in task:
        logger.warning(f"[tool.sheppy.cron] entry #{index} has a missing or invalid 'task' (expected 'module:function'), skipping")
        return None

    if not isinstance(expression, str) or not expression.strip():
        logger.warning(f"[tool.sheppy.cron] entry #{index} ({task}) has a missing or invalid 'expression', skipping")
        return None

    args = entry.get("args", [])
    if not isinstance(args, list):
        logger.warning(f"[tool.sheppy.cron] entry #{index} ({task}) has non-list 'args', skipping")
        return None

    kwargs = entry.get("kwargs", {})
    if not isinstance(kwargs, dict):
        logger.warning(f"[tool.sheppy.cron] entry #{index} ({task}) has non-table 'kwargs', skipping")
        return None

    queue = entry.get("queue")
    if queue is not None and not isinstance(queue, str):
        logger.warning(f"[tool.sheppy.cron] entry #{index} ({task}) has a non-string 'queue', skipping")
        return None

    return CronDeclaration(task=task, expression=expression, args=tuple(args), kwargs=dict(kwargs), queue=queue)


def load_cron_declarations(path: str | Path) -> list[CronDeclaration] | None:
    path = Path(path)

    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        logger.debug(f"cron config file not found: {path}")
        return None
    except (OSError, tomllib.TOMLDecodeError) as e:
        logger.error(f"cannot parse cron config file {path}: {e}")
        return None

    entries = data.get("tool", {}).get("sheppy", {}).get("cron", [])

    if not isinstance(entries, list):
        logger.error(f"[tool.sheppy.cron] in {path} must be an array of tables")
        return None

    declarations = []
    for index, entry in enumerate(entries, 1):
        declaration = _parse_entry(index, entry)
        if declaration is not None:
            declarations.append(declaration)

    return declarations
