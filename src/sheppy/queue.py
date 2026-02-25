from datetime import datetime, timedelta, timezone
from typing import overload
from urllib.parse import urlparse
from uuid import UUID

from ._config import config
from ._workflow import Workflow, WorkflowResult, WorkflowRunner
from .backend.base import Backend
from .models import Task, TaskCron
from .task_factory import TaskFactory


def _create_backend_from_url(url: str) -> Backend:
    """Create a backend instance from a URL string."""
    try:
        parsed = urlparse(url)
    except Exception as e:
        raise ValueError(f"Invalid backend URL: {url}") from e

    if not parsed.scheme:
        raise ValueError(f"Invalid backend URL: {url}")

    scheme = parsed.scheme.lower()

    if scheme in ("redis", "rediss"):
        # circular import
        from .backend.redis import RedisBackend  # noqa: PLC0415
        return RedisBackend(url=url)

    if scheme == "local":
        from .backend.local import LocalBackend  # noqa: PLC0415
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or 17420
        return LocalBackend(host=host, port=port)

    if scheme == "memory":
        from .backend.memory import MemoryBackend  # noqa: PLC0415
        return MemoryBackend()

    raise ValueError(f"Unsupported backend protocol: {scheme}")


class Queue:
    """
    `Queue` class provides an easy way to manage task queue.

    Parameters:
        backend: An instance of task backend (e.g. `sheppy.RedisBackend`),<br>
                 or a URL string to automatically infer a backend:<br>
                 - `redis://host:port` or `rediss://host:port` for RedisBackend<br>
                 - `local://host:port` for LocalBackend<br>
                 - `memory://` for MemoryBackend<br>
                 If not provided, uses `SHEPPY_BACKEND_URL` environment variable.
        name: Name of the queue. Defaults to `SHEPPY_QUEUE` env var or "default".
    """

    def __init__(self, backend: Backend | str | None = None, name: str | None = None):
        self.name = name if name is not None else config.queue

        if backend is None:
            # done like this to make mypy happy
            backend = config.backend_url
            if backend is None:
                raise ValueError("No backend provided. Either pass a backend instance/URL or set SHEPPY_BACKEND_URL environment variable.")

        if isinstance(backend, str):
            self.backend = _create_backend_from_url(backend)
        else:
            self.backend = backend

    @overload
    async def add(self, task: Task) -> bool: ...

    @overload
    async def add(self, task: list[Task]) -> list[bool]: ...

    async def add(self, task: Task | list[Task]) -> bool | list[bool]:
        """
        Add task into the queue. Accept list of tasks for batch add.

        Args:
            task: Instance of a Task, or list of Task instances for batch mode.

        Returns:
            Success boolean, or list of booleans in batch mode.

        Example:
            ```python
            q = Queue(...)
            success = await q.add(task)
            assert success is True

            # batch mode
            success = await q.add([task1, task2])
            assert success == [True, True]  # returns list of booleans in batch mode
            ```
        """
        await self.__ensure_backend_is_connected()

        if isinstance(task, list):
            batch_mode = True
            tasks = [t.model_dump(mode='json') for t in task]
        else:
            batch_mode = False
            tasks = [task.model_dump(mode='json')]

        success = await self.backend.append(self.name, tasks)

        return success if batch_mode else success[0]

    @overload
    async def get_task(self, task: Task | UUID | str) -> Task | None: ...

    @overload
    async def get_task(self, task: list[Task | UUID | str]) -> dict[UUID, Task]: ...

    async def get_task(self, task: Task | UUID | str | list[Task | UUID | str]) -> dict[UUID, Task] | Task | None:
        """Get task by id.

        Args:
            task: Instance of a Task or its ID, or list of Task instances/IDs for batch mode.

        Returns:
            Instance of a Task or None if not found.<br>In *batch mode*, returns Dictionary of Task IDs to Task instances.
        """
        await self.__ensure_backend_is_connected()

        task_ids, batch_mode = self._get_task_ids(task)
        task_results = await self.backend.get_tasks(self.name, task_ids)

        if batch_mode:
            return {UUID(t_id): Task.model_validate(t) for t_id, t in task_results.items()}

        td = task_results.get(task_ids[0])

        return Task.model_validate(td) if td else None

    async def get_all_tasks(self) -> list[Task]:
        """Get all tasks, including completed/failed ones.

        Returns:
            List of all tasks
        """
        await self.__ensure_backend_is_connected()
        tasks_data = await self.backend.get_all_tasks(self.name)
        return [Task.model_validate(t) for t in tasks_data]

    async def get_pending(self, count: int = 1) -> list[Task]:
        """List pending tasks.

        Args:
            count: Number of pending tasks to retrieve.

        Returns:
            List of pending tasks
        """
        if count <= 0:
            raise ValueError("Value must be larger than zero")

        await self.__ensure_backend_is_connected()

        return [Task.model_validate(t) for t in await self.backend.get_pending(self.name, count)]

    async def schedule(self, task: Task, at: datetime | timedelta) -> bool:
        """Schedule task to be processed after certain time.

        Args:
            task: Instance of a Task
            at: When to process the task.<br>
                If timedelta is provided, it will be added to current time.<br>
                *Note: datetime must be offset-aware (i.e. have timezone info).*

        Returns:
            Success boolean

        Example:
            ```python
            from datetime import datetime, timedelta

            q = Queue(...)
            # schedule task to be processed after 10 minutes
            await q.schedule(task, timedelta(minutes=10))

            # ... or at specific time
            await q.schedule(task, datetime.fromisoformat("2026-01-01 00:00:00 +00:00"))
            ```
        """
        await self.__ensure_backend_is_connected()

        if isinstance(at, timedelta):
            at = datetime.now(timezone.utc) + at

        if not at.tzinfo:
            raise TypeError("provided datetime must be offset-aware")

        task.__dict__["scheduled_at"] = at
        task.__dict__["status"] = "scheduled"

        return await self.backend.schedule(self.name, task.model_dump(mode="json"), at)

    async def get_scheduled(self) -> list[Task]:
        """List scheduled tasks.

        Returns:
            List of scheduled tasks
        """
        await self.__ensure_backend_is_connected()
        return [Task.model_validate(t) for t in await self.backend.get_scheduled(self.name)]

    @overload
    async def wait_for(self, task: Task | UUID | str, timeout: float = 0) -> Task | None: ...

    @overload
    async def wait_for(self, task: list[Task | UUID | str], timeout: float = 0) -> dict[UUID, Task]: ...

    async def wait_for(self, task: Task | UUID | str | list[Task | UUID | str], timeout: float = 0) -> dict[UUID, Task] | Task | None:
        """Wait for task to complete and return updated task instance.

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

        Example:
            ```python
            q = Queue(...)

            # wait indefinitely for task to complete
            updated_task = await q.wait_for(task)
            assert updated_task.status == 'completed'

            # wait up to 5 seconds for task to complete
            try:
                updated_task = await q.wait_for(task, timeout=5)
                if updated_task:
                    assert updated_task.status == 'completed'
                else:
                    print("Task not found or still pending after timeout")
            except TimeoutError:
                print("Task did not complete within timeout")

            # batch mode
            updated_tasks = await q.wait_for([task1, task2, task3], timeout=10)

            for task_id, task in updated_tasks.items():
                print(f"Task {task_id} status: {task.status}")

            # Note: updated_tasks may contain only a subset of tasks if timeout is reached
            ```
        """
        await self.__ensure_backend_is_connected()

        task_ids, batch_mode = self._get_task_ids(task)
        task_results = await self.backend.get_results(self.name, task_ids, timeout)

        if batch_mode:
            return {UUID(t_id): Task.model_validate(t) for t_id, t in task_results.items()}

        td = task_results.get(task_ids[0])

        return Task.model_validate(td) if td else None

    async def retry(self, task: Task | UUID | str, at: datetime | timedelta | None = None, force: bool = False) -> bool:
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

        Example:
            ```python
            q = Queue(...)

            # retry task immediately
            success = await q.retry(task)
            assert success is True

            # or retry after 5 minutes
            await q.retry(task, at=timedelta(minutes=5))

            # or at specific time
            await q.retry(task, at=datetime.fromisoformat("2026-01-01 00:00:00 +00:00"))

            # force retry even if task is completed (= finished successfully)
            await q.retry(task, force=True)
            ```
        """
        _task = await self.get_task(task)  # ensure_backend_is_connected is called in get_task already
        if not _task:
            return False

        if not force and _task.status == 'completed':
            raise ValueError("Task has already completed successfully, use force to retry anyways")

        needs_update = False  # temp hack
        if _task.finished_at:
            needs_update = True
            _task.__dict__["last_retry_at"] = datetime.now(timezone.utc)
            _task.__dict__["next_retry_at"] = datetime.now(timezone.utc)
            _task.__dict__["finished_at"] = None

        if at:
            if isinstance(at, timedelta):
                at = datetime.now(timezone.utc) + at

            if not at.tzinfo:
                raise TypeError("provided datetime must be offset-aware")

            if needs_update:
                _task.__dict__["next_retry_at"] = at
                _task.__dict__["scheduled_at"] = at
                _task.__dict__["status"] = "scheduled"

            return await self.backend.schedule(self.name, _task.model_dump(mode="json"), at, unique=False)

        success = await self.backend.append(self.name, [_task.model_dump(mode="json")], unique=False)
        return success[0]

    async def size(self) -> int:
        """Get number of pending tasks in the queue.

        Returns:
            Number of pending tasks

        Example:
            ```python
            q = Queue(...)

            await q.add(task)

            count = await q.size()
            assert count == 1
            ```
        """
        await self.__ensure_backend_is_connected()
        return await self.backend.size(self.name)

    async def clear(self) -> int:
        """Clear all tasks, including completed ones."""
        await self.__ensure_backend_is_connected()
        return await self.backend.clear(self.name)

    async def add_cron(self, task: Task, cron: str) -> bool:
        """Add a cron job to run a task on a schedule.

        Args:
            task: Instance of a Task
            cron: Cron expression string (e.g. "*/5 * * * *" to run every 5 minutes)

        Returns:
            Success boolean

        Example:
            ```python
            q = Queue(...)

            @task
            async def say_hello(to: str) -> str:
                print(f"[{datetime.now()}] Hello, {to}!")

            # schedule task to run every minute
            await q.add_cron(say_hello("World"), "* * * * *")
            ```
        """
        await self.__ensure_backend_is_connected()
        task_cron = TaskFactory.create_cron_from_task(task, cron)
        return await self.backend.add_cron(self.name, str(task_cron.deterministic_id), task_cron.model_dump(mode="json"))

    async def delete_cron(self, task: Task, cron: str) -> bool:
        """Delete a cron job.

        Args:
            task: Instance of a Task
            cron: Cron expression string used when adding the cron job

        Returns:
            Success boolean

        Example:
            ```python
            q = Queue(...)

            # delete previously added cron job
            success = await q.delete_cron(say_hello("World"), "* * * * *")
            assert success is True
            ```
        """
        await self.__ensure_backend_is_connected()
        task_cron = TaskFactory.create_cron_from_task(task, cron)
        return await self.backend.delete_cron(self.name, str(task_cron.deterministic_id))

    async def get_crons(self) -> list[TaskCron]:
        """List all cron jobs.

        Returns:
            List of TaskCron instances

        Example:
            ```python
            q = Queue(...)

            crons = await q.get_crons()

            for cron in crons:
                print(f"Cron ID: {cron.id}, Expression: {cron.expression}, TaskSpec: {cron.spec}")
            ```
        """
        await self.__ensure_backend_is_connected()
        return [TaskCron.model_validate(tc) for tc in await self.backend.get_crons(self.name)]

    async def _pop_pending(self, limit: int = 1, timeout: float | None = None) -> list[Task]:
        """Get next task to process. Internal method used by workers.

        Args:
            limit: Maximum number of tasks to return
            timeout: Timeout for waiting for tasks

        Returns:
            List of popped tasks
        """
        await self.__ensure_backend_is_connected()

        if limit <= 0:
            raise ValueError("Pop limit must be greater than zero.")

        tasks_data = await self.backend.pop(self.name, limit, timeout)
        tasks = []

        for task_data in tasks_data:
            task = Task.model_validate(task_data)

            if task.config.rate_limit:
                rl = task.config.rate_limit
                key = rl.get("key", task.spec.func)

                wait = await self.backend.acquire_rate_limit(
                    self.name,
                    key,
                    rl["max_rate"],
                    rl["rate_period"],
                    strategy=rl.get("strategy", "sliding_window"),
                    task_id=str(task.id),
                )

                if wait is not None:
                    scheduled_at = datetime.now(timezone.utc) + timedelta(seconds=wait)
                    task.__dict__["status"] = "scheduled"
                    task.__dict__["scheduled_at"] = scheduled_at
                    await self.backend.schedule(self.name, task.model_dump(mode='json'), scheduled_at, unique=False)
                    continue

            tasks.append(task)

        return tasks

    async def _store_result(self, task: Task) -> bool:
        """Store task result. Internal method used by workers.

        Args:
            task: Instance of a Task

        Returns:
            Success boolean
        """
        await self.__ensure_backend_is_connected()
        return await self.backend.store_result(self.name, task.model_dump(mode='json'))

    async def _enqueue_scheduled(self, now: datetime | None = None) -> list[Task]:
        """Enqueue scheduled tasks that are ready to be processed. Internal method used by workers.

        Args:
            now: Current time for scheduling

        Returns:
            List of enqueued tasks
        """
        await self.__ensure_backend_is_connected()

        tasks_data = await self.backend.pop_scheduled(self.name, now)
        for i in range(len(tasks_data)):
            tasks_data[i]['status'] = "pending"

        tasks = [Task.model_validate(t) for t in tasks_data]
        await self.backend.append(self.name, tasks_data, unique=False)

        return tasks

    async def __ensure_backend_is_connected(self) -> None:
        """Automatically connects backend on first async call."""
        if not self.backend.is_connected:
            await self.backend.connect()

    def _get_task_ids(self, task: list[Task | UUID | str] | Task | UUID | str) -> tuple[list[str], bool]:
        batch_mode = True
        if not isinstance(task, list):
            task = [task]
            batch_mode = False

        # set to deduplicate task ids
        task_ids = list({str(t.id if isinstance(t, Task) else t) for t in task})

        return task_ids, batch_mode

    async def add_workflow(self, workflow: Workflow) -> WorkflowResult:
        await self.__ensure_backend_is_connected()

        runner = WorkflowRunner(workflow)
        result = runner.run()

        await self.backend.store_workflow(self.name, result.workflow.model_dump(mode='json'))

        if result.pending_tasks:
            await self.add(result.pending_tasks)

        return result

    async def resume_workflow(self, workflow: Workflow | UUID | str, task_results: dict[UUID, Task] | None = None) -> WorkflowResult:
        await self.__ensure_backend_is_connected()

        if isinstance(workflow, (UUID, str)):
            w_id = str(workflow)
            wf_data = await self.backend.get_workflows(self.name, [w_id])
            if not wf_data or not wf_data[w_id]:
                raise ValueError(f"Workflow not found: {workflow}")
            workflow = Workflow.model_validate(wf_data[w_id])

        if task_results is None:
            incomplete_ids = workflow.get_incomplete_task_ids()
            if incomplete_ids:
                task_results_dict = await self.get_task(incomplete_ids)  # type: ignore
                task_results = {
                    tid: task for tid, task in task_results_dict.items()
                    if task.status == 'completed' or task.error
                }
            else:
                task_results = {}

        runner = WorkflowRunner(workflow, task_results=task_results)
        result = runner.run()

        await self.backend.store_workflow(self.name, result.workflow.model_dump(mode='json'))

        if result.pending_tasks:
            await self.add(result.pending_tasks)

        return result

    @overload
    async def get_workflow(self, workflow: Workflow | UUID | str) -> Workflow | None: ...

    @overload
    async def get_workflow(self, workflow: list[Workflow | UUID | str]) -> dict[UUID, Workflow]: ...

    async def get_workflow(self, workflow: Workflow | UUID | str | list[Workflow | UUID | str]) -> dict[UUID, Workflow] | Workflow | None:
        await self.__ensure_backend_is_connected()

        batch_mode = isinstance(workflow, list)
        if not batch_mode:
            workflow = [workflow]  # type: ignore

        workflow_ids = [str(w.id if isinstance(w, Workflow) else w) for w in workflow]  # type: ignore

        results = await self.backend.get_workflows(self.name, workflow_ids)

        if batch_mode:
            return {UUID(wf_id): Workflow.model_validate(wf) for wf_id, wf in results.items()}

        if workflow_ids[0] in results:
            return Workflow.model_validate(results[workflow_ids[0]])
        return None

    async def get_all_workflows(self) -> list[Workflow]:
        await self.__ensure_backend_is_connected()

        workflows_data = await self.backend.get_all_workflows(self.name)
        return [Workflow.model_validate(wf) for wf in workflows_data]

    async def get_pending_workflows(self) -> list[Workflow]:
        await self.__ensure_backend_is_connected()

        workflows_data = await self.backend.get_pending_workflows(self.name)
        return [Workflow.model_validate(wf) for wf in workflows_data]

    async def delete_workflow(self, workflow: Workflow | UUID | str) -> bool:
        await self.__ensure_backend_is_connected()

        workflow_id = str(workflow.id if isinstance(workflow, Workflow) else workflow)
        return await self.backend.delete_workflow(self.name, workflow_id)

    async def _mark_workflow_task_complete(self, workflow_id: UUID | str, task_id: UUID | str) -> int:
        await self.__ensure_backend_is_connected()

        return await self.backend.mark_workflow_task_complete(self.name, str(workflow_id), str(task_id))
