import asyncio
import threading
from collections.abc import Coroutine
from datetime import datetime, timedelta
from typing import Any, TypeVar, overload
from uuid import UUID

from ._workflow import Workflow, WorkflowResult
from .models import Task, TaskCron
from .queue import Queue

T = TypeVar("T")


class SyncQueue:
    """Synchronous wrapper around `Queue` for codebases without asyncio.

    A dedicated asyncio event loop runs in a background daemon thread.
    Every method submits the corresponding `Queue` coroutine to that loop
    via `asyncio.run_coroutine_threadsafe` and blocks the calling thread
    until the result is ready. The wrapped `Queue` and its backend always
    run on the same loop, so the instance can be shared between threads;
    long waits (e.g. `wait_for` with no timeout) block the calling
    thread, not the loop.

    Call `close()` when done to stop the loop and join its thread. The
    thread is a daemon, so it does not block interpreter exit.

    Unlike `Queue`, the backend can only be given as a URL string (or via
    the `SHEPPY_BACKEND_URL` environment variable); the constructor does
    not accept a `Backend` instance.

    Args:
        backend: Backend URL string, e.g. "redis://localhost:6379" or
                 "memory://". If None, uses `SHEPPY_BACKEND_URL` env var.
        name: Name of the queue. Defaults to `SHEPPY_QUEUE` env var or "default".

    Example:
        ```python
        q = SyncQueue("redis://localhost:6379")

        success = q.add(task)
        assert success is True

        q.close()
        ```
    """
    def __init__(self, backend: str | None = None, name: str | None = None):
        self._backend = backend
        self._name = name

        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        self._closed = False

        self._queue: Queue = asyncio.run_coroutine_threadsafe(self._create_queue(), self._loop).result()

    async def _create_queue(self) -> Queue:
        return Queue(self._backend, self._name)

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def close(self) -> None:
        """Stop the background event loop and join its thread.

        Safe to call more than once.
        """
        if self._closed:
            return
        self._closed = True
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5)

    def _run_coro(self, coro: Coroutine[Any, Any, T]) -> T:
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result()

    # def __enter__(self) -> "SyncQueue":
    #     return self

    # def __exit__(self, *args: object) -> None:
    #     self.close()

    @overload
    def add(self, task: Task) -> bool: ...

    @overload
    def add(self, task: list[Task]) -> list[bool]: ...

    def add(self, task: Task | list[Task]) -> bool | list[bool]:
        """Add task into the queue. Accept list of tasks for batch add.

        Args:
            task: Instance of a Task, or list of Task instances for batch mode.

        Returns:
            Success boolean, or list of booleans in batch mode.
        """
        if isinstance(task, list):  # mypy needs this
            return self._run_coro(self._queue.add(task))
        return self._run_coro(self._queue.add(task))

    @overload
    def get_task(self, task: Task | UUID | str) -> Task | None: ...

    @overload
    def get_task(self, task: list[Task | UUID | str]) -> dict[UUID, Task]: ...

    def get_task(self, task: Task | UUID | str | list[Task | UUID | str]) -> Task | None | dict[UUID, Task]:
        """Get task by id.

        Args:
            task: Instance of a Task or its ID, or list of Task instances/IDs for batch mode.

        Returns:
            Instance of a Task or None if not found.<br>In *batch mode*, returns Dictionary of Task IDs to Task instances.
        """
        return self._run_coro(self._queue.get_task(task))

    def get_all_tasks(self) -> list[Task]:
        """Get all tasks, including completed/failed ones.

        Returns:
            List of all tasks
        """
        return self._run_coro(self._queue.get_all_tasks())

    def get_pending(self, count: int = 1) -> list[Task]:
        """List pending tasks.

        Args:
            count: Number of pending tasks to retrieve.

        Returns:
            List of pending tasks
        """
        return self._run_coro(self._queue.get_pending(count))

    def schedule(self, task: Task, at: datetime | timedelta) -> bool:
        """Schedule task to be processed after certain time.

        Args:
            task: Instance of a Task
            at: When to process the task.<br>
                If timedelta is provided, it will be added to current time.<br>
                *Note: datetime must be offset-aware (i.e. have timezone info).*

        Returns:
            Success boolean
        """
        return self._run_coro(self._queue.schedule(task, at))

    def get_scheduled(self) -> list[Task]:
        """List scheduled tasks.

        Returns:
            List of scheduled tasks
        """
        return self._run_coro(self._queue.get_scheduled())

    def retry(self, task: Task | UUID | str, at: datetime | timedelta | None = None, force: bool = False) -> bool:
        """Retry failed task.

        Args:
            task: Instance of a Task or its ID
            at: When to retry the task.<br>
                - If None (default), retries immediately.<br>
                - If timedelta is provided, it will be added to current time.<br>
                *Note: datetime must be offset-aware (i.e. have timezone info).*
            force: If True, allows retrying even if task has completed successfully. Defaults to False.

        Returns:
            Success boolean

        Raises:
            ValueError: If task has already completed successfully and force is not set to True.
            TypeError: If provided datetime is not offset-aware.
        """
        return self._run_coro(self._queue.retry(task, at, force))

    def cancel(self, task: Task | UUID | str) -> Task:
        """Cancel a pending or scheduled task.

        Args:
            task: Instance of a Task or its ID.

        Returns:
            The updated Task instance with status 'cancelled'.

        Raises:
            TaskCancellationError: If the task cannot be cancelled - either it
                was already claimed by a worker, it already finished, or it
                does not exist.
        """
        return self._run_coro(self._queue.cancel(task))

    def delete(self, task: Task | UUID | str) -> bool:
        """Hard-delete a finished task's stored metadata.

        Only tasks in a terminal state (completed, failed, crashed, cancelled) can be deleted.

        Args:
            task: Instance of a Task or its ID.

        Returns:
            True if the task existed and was deleted, False if it was not found.

        Raises:
            ValueError: If the task has not finished yet.
        """
        return self._run_coro(self._queue.delete(task))

    def size(self) -> int:
        """Get number of pending tasks in the queue.

        Returns:
            Number of pending tasks
        """
        return self._run_coro(self._queue.size())

    def clear(self) -> int:
        """Clear all tasks, including completed ones.

        Returns:
            Number of stored task entries removed.
        """
        return self._run_coro(self._queue.clear())

    def add_cron(self, task: Task, cron: str) -> bool:
        """Add a cron job to run a task on a schedule.

        Args:
            task: Instance of a Task
            cron: Cron expression string (e.g. "*/5 * * * *" to run every 5 minutes)

        Returns:
            Success boolean
        """
        return self._run_coro(self._queue.add_cron(task, cron))

    def delete_cron(self, task: Task, cron: str) -> bool:
        """Delete a cron job.

        Args:
            task: Instance of a Task
            cron: Cron expression string used when adding the cron job

        Returns:
            Success boolean
        """
        return self._run_coro(self._queue.delete_cron(task, cron))

    def get_crons(self) -> list[TaskCron]:
        """List all cron jobs.

        Returns:
            List of TaskCron instances
        """
        return self._run_coro(self._queue.get_crons())

    @overload
    def wait_for(self, task: Task | UUID | str, timeout: float = 0) -> Task | None: ...

    @overload
    def wait_for(self, task: list[Task | UUID | str], timeout: float = 0) -> dict[UUID, Task]: ...

    def wait_for(self, task: Task | UUID | str | list[Task | UUID | str], timeout: float = 0) -> Task | None | dict[UUID, Task]:
        """Wait for task to complete and return updated task instance.

        Blocks the calling thread until the task finishes; use `timeout`
        to bound the wait.

        Args:
            task: Instance of a Task or its ID, or list of Task instances/IDs for batch mode.
            timeout: Maximum time to wait in seconds. Default is 0 (wait indefinitely).<br>
                     If timeout is reached, returns None (or partial results in batch mode).<br>
                     In batch mode, this is the maximum time to wait for all tasks to complete.<br>
                     Note: In non-batch mode, if timeout is reached and no task is found, a TimeoutError is raised.

        Returns:
            Instance of a Task or None if not found or timeout reached.<br>In batch mode, returns dictionary of Task IDs to Task instances (partial results possible on timeout).

        Raises:
            TimeoutError: If timeout is reached and no task is found (only in non-batch mode).
        """
        return self._run_coro(self._queue.wait_for(task, timeout))

    def add_workflow(self, workflow: Workflow) -> WorkflowResult:
        """Add a workflow into the queue."""
        return self._run_coro(self._queue.add_workflow(workflow))

    @overload
    def wait_for_workflow(self, workflow: Workflow | UUID | str, timeout: float = 0) -> Workflow | None: ...

    @overload
    def wait_for_workflow(self, workflow: list[Workflow | UUID | str], timeout: float = 0) -> dict[UUID, Workflow]: ...

    def wait_for_workflow(self, workflow: Workflow | UUID | str | list[Workflow | UUID | str], timeout: float = 0) -> Workflow | None | dict[UUID, Workflow]:
        """Wait for workflow to finish (complete or fail) and return the updated workflow.

        Args:
            workflow: Instance of a Workflow or its ID, or list of Workflow instances/IDs for batch mode.
            timeout: Maximum time to wait in seconds. Default is 0 (wait indefinitely).

        Returns:
            The finished Workflow, or None if not found.<br>In *batch mode*, returns a dictionary of Workflow IDs to finished Workflow instances.

        Raises:
            TimeoutError: If a positive timeout expires before all given workflows have finished.
        """
        return self._run_coro(self._queue.wait_for_workflow(workflow, timeout))

    @overload
    def get_workflow(self, workflow: Workflow | UUID | str) -> Workflow | None: ...

    @overload
    def get_workflow(self, workflow: list[Workflow | UUID | str]) -> dict[UUID, Workflow]: ...

    def get_workflow(self, workflow: Workflow | UUID | str | list[Workflow | UUID | str]) -> Workflow | None | dict[UUID, Workflow]:
        """Get workflow by id.

        Args:
            workflow: Instance of a Workflow or its ID, or list of Workflow instances/IDs for batch mode.

        Returns:
            Instance of a Workflow or None if not found.<br>In *batch mode*, returns Dictionary of Workflow IDs to Workflow instances.
        """
        return self._run_coro(self._queue.get_workflow(workflow))

    def get_all_workflows(self) -> list[Workflow]:
        """Get all stored workflows, including completed/failed ones.

        Returns:
            List of all Workflow instances
        """
        return self._run_coro(self._queue.get_all_workflows())

    def get_pending_workflows(self) -> list[Workflow]:
        """Get workflows that are neither completed nor errored.

        Returns:
            List of pending Workflow instances
        """
        return self._run_coro(self._queue.get_pending_workflows())

    def delete_workflow(self, workflow: Workflow | UUID | str) -> bool:
        """Delete a workflow.

        Args:
            workflow: Instance of a Workflow or its ID

        Returns:
            True if the workflow existed and was deleted.
        """
        return self._run_coro(self._queue.delete_workflow(workflow))
