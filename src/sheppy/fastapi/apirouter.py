"""
FastAPI router integration.
"""

from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Annotated, Any
from uuid import UUID

from sheppy import Backend, Queue

from ..models import Task, TaskCron
from ..utils.functions import resolve_function
from .schemas import (
    AddTaskRequest,
    CronRequest,
    RetryRequest,
    ScheduleTaskRequest,
)

try:
    import fastapi  # type: ignore[import-not-found,unused-ignore]
    FASTAPI_INSTALLED = True
except ImportError:
    fastapi = None  # type: ignore[assignment,unused-ignore]
    FASTAPI_INSTALLED = False


def create_router(backend: Backend,
                  *,
                  prefix: str = "",
                  tags: Sequence[str] | None = None,
                  **kwargs: Any,
                  ) -> Any:
    if not FASTAPI_INSTALLED:
        raise ImportError("Cannot create APIRouter because FastAPI is not installed. "
                          "Install it using pip install \"fastapi[standard]\" or "
                          "uv add \"fastapi[standard]\"")

    async def get_queue(queue: str) -> Queue:
        return Queue(backend, queue)

    router_tags: Sequence[str] = tags if tags is not None else ["sheppy"]

    router = fastapi.APIRouter(prefix=prefix, tags=list(router_tags), **kwargs)

    @router.get("/queues")  # type: ignore[untyped-decorator,unused-ignore]
    async def list_queues() -> dict[str, int]:
        # temp hack
        if not backend.is_connected:
            await backend.connect()

        return await backend.list_queues()

    @router.get("/{queue}/size")  # type: ignore[untyped-decorator,unused-ignore]
    async def get_queue_size(q: Annotated[Queue, fastapi.Depends(get_queue)]) -> int:
        return await q.size()

    @router.get("/{queue}/tasks")  # type: ignore[untyped-decorator,unused-ignore]
    async def list_tasks(q: Annotated[Queue, fastapi.Depends(get_queue)]) -> list[Task]:
        return await q.get_all_tasks()

    @router.get("/{queue}/tasks/pending")  # type: ignore[untyped-decorator,unused-ignore]
    async def list_pending_tasks(q: Annotated[Queue, fastapi.Depends(get_queue)], limit: int = fastapi.Query(default=100, ge=1)) -> list[Task]:
        return await q.get_pending(limit)

    @router.get("/{queue}/tasks/scheduled")  # type: ignore[untyped-decorator,unused-ignore]
    async def list_scheduled_tasks(q: Annotated[Queue, fastapi.Depends(get_queue)]) -> list[Task]:
        return await q.get_scheduled()

    @router.get("/{queue}/tasks/{task_id}")  # type: ignore[untyped-decorator,unused-ignore]
    async def get_task(task_id: UUID, q: Annotated[Queue, fastapi.Depends(get_queue)]) -> Task:
        task = await q.get_task(task_id)
        if task is not None:
            return task

        raise fastapi.HTTPException(404, f"Task {task_id} not found")

    @router.post("/{queue}/tasks/{task_id}/retry")  # type: ignore[untyped-decorator,unused-ignore]
    async def retry_task(task_id: UUID, q: Annotated[Queue, fastapi.Depends(get_queue)], retry: RetryRequest | None = None) -> str:
        if not retry:
            retry = RetryRequest()

        try:
            success = await q.retry(task_id, at=timedelta(seconds=retry.delay_seconds), force=retry.force)
        except ValueError as e:
            raise fastapi.HTTPException(400, str(e)) from e

        if success:
            return f"Task {task_id} queued for retry"

        raise fastapi.HTTPException(400, f"Failed to retry task {task_id}")

    @router.post("/{queue}/tasks")  # type: ignore[untyped-decorator,unused-ignore]
    async def add_task(task: AddTaskRequest, q: Annotated[Queue, fastapi.Depends(get_queue)]) -> Task:
        try:
            func = resolve_function(task.func, wrapped=False)
        except Exception as e:
            raise fastapi.HTTPException(400, f"Could not import function '{task.func}': {e}") from e

        try:
            task_fn = func(*task.args, **task.kwargs)
        except Exception as e:
            raise fastapi.HTTPException(400, f"Error creating task: {e}") from e

        if not isinstance(task_fn, Task):
            raise fastapi.HTTPException(400, f"Function '{task.func}' did not return a Task")

        success = await q.add(task_fn)
        if not success:
            raise fastapi.HTTPException(500, "Failed to add task to queue")

        return task_fn

    @router.post("/{queue}/tasks/schedule")  # type: ignore[untyped-decorator,unused-ignore]
    async def schedule_task(schedule: ScheduleTaskRequest, q: Annotated[Queue, fastapi.Depends(get_queue)]) -> Task:
        schedule_at: datetime | timedelta
        if schedule.delay_seconds is not None:
            schedule_at = timedelta(seconds=schedule.delay_seconds)

        elif schedule.at is not None:
            schedule_at = schedule.at

        else:
            raise fastapi.HTTPException(400, "Either 'delay_seconds' or 'at' must be provided")

        try:
            func = resolve_function(schedule.func, wrapped=False)
        except Exception as e:
            raise fastapi.HTTPException(400, f"Could not import function '{schedule.func}': {e}") from e

        try:
            task_fn = func(*schedule.args, **schedule.kwargs)
        except Exception as e:
            raise fastapi.HTTPException(400, f"Error creating task: {e}") from e

        if not isinstance(task_fn, Task):
            raise fastapi.HTTPException(400, f"Function '{schedule.func}' did not return a Task")

        success = await q.schedule(task_fn, schedule_at)
        if not success:
            raise fastapi.HTTPException(500, "Failed to schedule task")

        return task_fn

    @router.post("/{queue}/tasks/clear")  # type: ignore[untyped-decorator,unused-ignore]
    async def clear_tasks(q: Annotated[Queue, fastapi.Depends(get_queue)]) -> int:
        return await q.clear()

    @router.get("/{queue}/crons")  # type: ignore[untyped-decorator,unused-ignore]
    async def list_crons(q: Annotated[Queue, fastapi.Depends(get_queue)]) -> list[TaskCron]:
        return await q.get_crons()

    @router.post("/{queue}/crons")  # type: ignore[untyped-decorator,unused-ignore]
    async def add_cron(request: CronRequest, q: Annotated[Queue, fastapi.Depends(get_queue)]) -> bool:
        try:
            func = resolve_function(request.func, wrapped=False)
        except Exception as e:
            raise fastapi.HTTPException(400, f"Could not import function '{request.func}': {e}") from e

        try:
            task = func(*request.args, **request.kwargs)
        except Exception as e:
            raise fastapi.HTTPException(400, f"Error creating task: {e}") from e

        if not isinstance(task, Task):
            raise fastapi.HTTPException(400, f"Function '{request.func}' did not return a Task")

        success = await q.add_cron(task, request.expression)
        if not success:
            raise fastapi.HTTPException(500, "Failed to add cron job")

        return True

    @router.delete("/{queue}/crons")  # type: ignore[untyped-decorator,unused-ignore]
    async def delete_cron(request: CronRequest, q: Annotated[Queue, fastapi.Depends(get_queue)]) -> str:
        try:
            func = resolve_function(request.func, wrapped=False)
        except Exception as e:
            raise fastapi.HTTPException(400, f"Could not import function '{request.func}': {e}") from e

        try:
            task = func(*request.args, **request.kwargs)
        except Exception as e:
            raise fastapi.HTTPException(400, f"Error creating task: {e}") from e

        if not isinstance(task, Task):
            raise fastapi.HTTPException(400, f"Function '{request.func}' did not return a Task")

        success = await q.delete_cron(task, request.expression)
        if not success:
            raise fastapi.HTTPException(404, "Cron job not found or could not be deleted")

        return "Cron job deleted"

    return router
