"""
This file contains utility functions meant for internal use only. Expect breaking changes if you use them directly.
"""

import importlib
import inspect
import socket
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import anyio

from .argument_processing import prepare_task_arguments
from .dependency_injection import DependencyResolver

if TYPE_CHECKING:
    from ..queue import Queue
    from ..task import Task


async def execute_task(
    task: "Task",
    dependency_resolver: DependencyResolver,
    worker_id: str
) -> "Task":
    """Execute a task with dependency resolution and result handling."""
    # Resolve the function from its string representation
    try:
        if task.internal.func is None:
            raise ValueError("Task has no function specified")
        module_name, function_name = task.internal.func.split(':')
        module = importlib.import_module(module_name)
        func = getattr(module, function_name).__wrapped__

    except (ValueError, ImportError, AttributeError):
        raise ValueError(f"Cannot resolve function: {task.internal.func}")

    # Create dependency cache for this execution
    dependency_cache: dict[Any, Any] = {}

    # Resolve dependencies
    resolved_values = await dependency_resolver.solve_dependencies(
        func,
        args=tuple(task.internal.args or []),
        kwargs=task.internal.kwargs or {},
        dependency_cache=dependency_cache
    )

    # Prepare final arguments
    final_args, final_kwargs = prepare_task_arguments(task, resolved_values, func)

    # Execute the task
    if inspect.iscoroutinefunction(func):
        # Async function - run directly
        result = await func(*final_args, **final_kwargs)
    else:
        # Sync function - run via anyio's thread pool
        result = await anyio.to_thread.run_sync(lambda: func(*final_args, **final_kwargs))

    # Recreate an updated task, don't mutate the original
    updated_task = task.model_copy(deep=True)
    updated_task.__dict__["result"] = result
    updated_task.__dict__["completed"] = True
    updated_task.__dict__["error"] = None  # Clear any previous error on success

    updated_task.metadata.__dict__["worker"] = worker_id
    updated_task.metadata.__dict__["finished_datetime"] = datetime.now(timezone.utc)

    return updated_task

async def get_available_tasks(queue: "Queue", limit: int | None = None, timeout: float | None = None) -> list["Task"]:
    """Get available tasks from queue, prioritizing scheduled tasks."""
    tasks = []

    # First get all scheduled tasks
    tasks += await queue.get_scheduled()

    # If we have a limit and reached it, return early
    if limit and len(tasks) >= limit:
        # Put excess tasks back in the queue
        for task in tasks[limit:]:
            await queue.add(task)
        return tasks[:limit]

    # If no scheduled tasks (or need more), get regular tasks
    remaining = limit - len(tasks) if limit else None

    if not limit or len(tasks) < limit:
        # For blocking mode (timeout > 0), wait for at least one task
        # For non-blocking mode (timeout = None or 0), return immediately
        if timeout is not None and timeout > 0 and len(tasks) == 0:
            # Block waiting for at least one task when no scheduled tasks
            regular_task = await queue._pop(timeout=timeout)
            if regular_task:
                tasks.append(regular_task)
                remaining = remaining - 1 if remaining else None

        # Get additional tasks without blocking (up to limit)
        while remaining is None or remaining > 0:
            regular_task = await queue._pop(timeout=None)  # Non-blocking for additional tasks
            if not regular_task:
                break
            tasks.append(regular_task)
            if remaining is not None:
                remaining -= 1

    return tasks


def calculate_retry_delay(task: "Task") -> float:
    if isinstance(task.metadata.retry_delay, float):
        # Constant delay for all retries
        return task.metadata.retry_delay

    if isinstance(task.metadata.retry_delay, list):
        # Custom delays per retry
        if len(task.metadata.retry_delay) == 0:
            return 1.0  # Empty list defaults to 1 second

        if task.metadata.retry_count < len(task.metadata.retry_delay):
            return float(task.metadata.retry_delay[task.metadata.retry_count])
        else:
            # Use last delay value for remaining retries
            return float(task.metadata.retry_delay[-1])

    # This should never happen if the library is used correctly
    if isinstance(task.metadata.retry_delay, int):
        return float(task.metadata.retry_delay)

    # This should never happen if the library is used correctly
    raise ValueError(f"Invalid retry_delay type: {type(task.metadata.retry_delay).__name__}. Expected None, float, or list.")


def update_failed_task(task: "Task", exception: Exception) -> "Task":
    updated_task = task.model_copy(deep=True)

    updated_task.__dict__["completed"] = False
    updated_task.__dict__["error"] = str(exception)

    # Check if task should be retried
    if updated_task.metadata.retry_count < updated_task.metadata.retry:
        # Update retry metadata
        updated_task.metadata.__dict__["retry_count"] += 1
        updated_task.metadata.__dict__["last_retry_at"] = datetime.now(timezone.utc)

        # Calculate next retry time
        updated_task.metadata.__dict__["next_retry_at"] = datetime.now(timezone.utc) + timedelta(seconds=calculate_retry_delay(task))

        # Task will be retried
        updated_task.metadata.__dict__["finished_datetime"] = None
    else:
        # Final failure - no more retries
        updated_task.metadata.__dict__["finished_datetime"] = datetime.now(timezone.utc)

    return updated_task


def generate_unique_worker_id(prefix: str) -> str:
    return f"{prefix}-{socket.gethostname()}-{str(uuid4())[:8]}"
