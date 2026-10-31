from typing import Any

from ..models import Task, TTLValue


def resolve_metadata_ttl(task_data: dict[str, Any], *, ttl: int | None, error_ttl: TTLValue) -> int | None:
    if task_data.get("finished_at") is None:
        return None

    config = task_data.get("config") or {}
    task_ttl: TTLValue = config.get("ttl", "inherit")
    task_error_ttl: TTLValue = config.get("error_ttl", "inherit")

    if task_data.get("status") in ("failed", "crashed", "cancelled"):
        for candidate in (task_error_ttl, task_ttl, error_ttl, ttl):
            if not isinstance(candidate, str):  # skip "inherit"
                return candidate
        return None

    return task_ttl if not isinstance(task_ttl, str) else ttl


def resolve_ttl_config(config: dict[str, Any], *, ttl: TTLValue, error_ttl: TTLValue) -> dict[str, Any]:
    resolved = dict(config)
    if resolved.get("ttl", "inherit") == "inherit":
        resolved["ttl"] = ttl
    if resolved.get("error_ttl", "inherit") == "inherit":
        resolved["error_ttl"] = error_ttl
    return resolved


def apply_task_ttl_defaults(task: Task, *, ttl: TTLValue, error_ttl: TTLValue) -> Task:
    config = task.config.model_dump()
    resolved = resolve_ttl_config(config, ttl=ttl, error_ttl=error_ttl)
    if resolved == config:
        return task
    return task.model_copy(update={"config": task.config.model_copy(update=resolved)})


def apply_ttl_defaults_to_data(task_data: dict[str, Any], *, ttl: TTLValue, error_ttl: TTLValue) -> dict[str, Any]:
    config = task_data.get("config") or {}
    resolved = resolve_ttl_config(config, ttl=ttl, error_ttl=error_ttl)
    if resolved == config:
        return task_data
    return {**task_data, "config": resolved}


def validate_backend_ttl(ttl: int | None, error_ttl: TTLValue) -> None:
    if ttl is not None and (not isinstance(ttl, int) or isinstance(ttl, bool) or ttl <= 0):
        raise ValueError("ttl must be a positive integer or None")
    if isinstance(error_ttl, str):
        if error_ttl != "inherit":
            raise ValueError("error_ttl must be a positive integer, None, or 'inherit'")
    elif error_ttl is not None and (not isinstance(error_ttl, int) or isinstance(error_ttl, bool) or error_ttl <= 0):
        raise ValueError("error_ttl must be a positive integer, None, or 'inherit'")
