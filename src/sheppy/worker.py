import asyncio
import logging
import signal
from functools import partial

from pydantic import BaseModel

from .backend.base import Backend
from .models import Task, TaskCron
from .queue import Queue
from .utils.task_execution import (
    TaskProcessor,
    generate_unique_worker_id,
)

logger = logging.getLogger(__name__)


class WorkerStats(BaseModel):
    processed: int = 0
    failed: int = 0


WORKER_PREFIX = "<Worker> "
SCHEDULER_PREFIX = "<Scheduler> "
CRON_MANAGER_PREFIX = "<CronManager> "


class Worker:
    """Worker that processes tasks from the queue.

    The Worker monitors the specified queue(s) for pending tasks and processes them asynchronously. It uses blocking pop operations to efficiently wait for new tasks. The worker can handle multiple tasks concurrently, up to a specified limit.
    It also handles scheduled tasks and cron jobs.

    Args:
        queue_name (str | list[str]): Name of the queue or list of queue names to process tasks from.
        backend (Backend): Instance of the backend to use for storing and retrieving tasks.
        shutdown_timeout (float): Time in seconds to wait for active tasks to complete during shutdown. Default is 30.0 seconds.
        max_concurrent_tasks (int): Maximum number of tasks to process concurrently. Default is 10.
        enable_job_processing (bool): If True, enables job processing. Default is True.
        enable_scheduler (bool): If True, enables the scheduler to enqueue scheduled tasks. Default is True.
        enable_cron_manager (bool): If True, enables the cron manager to handle cron jobs. Default is True.

    Attributes:
        queues (list[Queue]): List of Queue instances corresponding to the specified queue names.
        worker_id (str): Unique identifier for the worker instance.
        stats (WorkerStats): Statistics about processed and failed tasks.
        enable_job_processing (bool): Indicates if job processing is enabled.
        enable_scheduler (bool): Indicates if the scheduler is enabled.
        enable_cron_manager (bool): Indicates if the cron manager is enabled.

    Raises:
        ValueError: If none of the processing types (job processing, scheduler, cron manager) are enabled.

    Example:
        ```python
        import asyncio
        from sheppy import Worker, RedisBackend

        async def main():
            backend = RedisBackend()
            worker = Worker(queue_name="default", backend=backend)

            await worker.work()

        if __name__ == "__main__":
            asyncio.run(main())
        ```
    """

    def __init__(
        self,
        queue_name: str | list[str],
        backend: Backend,
        shutdown_timeout: float = 30.0,
        max_concurrent_tasks: int = 10,
        max_prefetch_tasks: int | None = None,
        enable_job_processing: bool = True,
        enable_scheduler: bool = True,
        enable_cron_manager: bool = True,
    ):
        if not any([enable_job_processing, enable_scheduler, enable_cron_manager]):
            raise ValueError("At least one processing type must be enabled")

        self._backend = backend

        if not isinstance(queue_name, list|tuple):
            queue_name = [str(queue_name)]
        self.queues = [Queue(backend, q) for q in queue_name]

        self.shutdown_timeout = shutdown_timeout
        self.worker_id = generate_unique_worker_id("worker")
        self.stats = WorkerStats()

        self._task_processor = TaskProcessor()
        self._task_semaphore = asyncio.Semaphore(max_concurrent_tasks)
        self._max_prefetch_tasks = max_prefetch_tasks
        self._shutdown_event = asyncio.Event()
        self._ctrl_c_counter = 0

        self._blocking_timeout = 5
        self._scheduler_polling_interval = 1.0
        self._cron_polling_interval = 10.0

        self._active_tasks: dict[str, dict[asyncio.Task[Task], Task]] = {queue.name: {} for queue in self.queues}

        self.enable_job_processing = enable_job_processing
        self.enable_scheduler = enable_scheduler
        self.enable_cron_manager = enable_cron_manager

        self._work_queue_tasks: list[asyncio.Task[None]] = []
        self._scheduler_task: asyncio.Task[None] | None = None
        self._cron_manager_task: asyncio.Task[None] | None = None

        self._tasks_to_process: int | None = None
        self._empty_queues: list[str] = []

    async def work(self, max_tasks: int | None = None, oneshot: bool = False, register_signal_handlers: bool = True) -> None:
        """Start worker to process tasks from the queue.

        Args:
            max_tasks (int | None): Maximum number of tasks to process before shutting down. If None, process indefinitely.
            oneshot (bool): If True, process tasks until the queue is empty, then shut down.
            register_signal_handlers (bool): If True, register SIGINT and SIGTERM signal handlers for graceful shutdown. Default is True.

        Returns:
            None

        Raises:
            BackendError: If there is an issue connecting to the backend.

        Note:
            - The worker can be gracefully shut down by sending a SIGINT or SIGTERM signal (e.g., pressing CTRL+C).
            - If the worker is already shutting down, pressing CTRL+C multiple times (default 3) will force an immediate shutdown.
            - The worker will attempt to complete active tasks before shutting down, up to the specified shutdown timeout.
            - If there are still active tasks after the timeout, they will be cancelled.
        """
        # register signals
        loop = asyncio.get_event_loop()
        if register_signal_handlers:
            self.__register_signal_handlers(loop)

        self._tasks_to_process = max_tasks
        self._empty_queues.clear()

        # reset state (likely relevant only for tests)
        self._shutdown_event.clear()
        self._ctrl_c_counter = 0

        # test connection
        await self._verify_connection(self._backend)

        # start scheduler
        if self.enable_scheduler:
            self._scheduler_task = asyncio.create_task(self._run_scheduler(self._scheduler_polling_interval))

        # start cron manager
        if self.enable_cron_manager:
            self._cron_manager_task = asyncio.create_task(self._run_cron_manager(self._cron_polling_interval))

        # start job processing
        if self.enable_job_processing:
            for queue in self.queues:
                self._work_queue_tasks.append(asyncio.create_task(self._run_worker_loop(queue, oneshot)))

        # blocking wait for created asyncio tasks
        _futures = self._work_queue_tasks
        _futures += [self._scheduler_task] if self._scheduler_task else []
        _futures += [self._cron_manager_task] if self._cron_manager_task else []
        await asyncio.gather(*_futures, return_exceptions=True)
        self._shutdown_event.set()

        # this is starting to feel like Perl
        remaining_tasks = {k: v for inner_dict in self._active_tasks.values() for k, v in inner_dict.items()}

        # attempt to exit cleanly
        if remaining_tasks:
            logger.info(WORKER_PREFIX + f"Waiting for {len(remaining_tasks)} active tasks to complete...")
            try:
                await asyncio.wait_for(
                    asyncio.gather(*remaining_tasks.keys(), return_exceptions=True),
                    timeout=self.shutdown_timeout
                )
            except asyncio.TimeoutError:
                logger.warning("Some tasks did not complete within shutdown timeout")

                # ! FIXME - what should we do here with the existing tasks? (maybe DLQ?)

                for task_future in remaining_tasks:
                    if not task_future.done():
                        task_future.cancel()

                        # ! FIXME - should we try reqeueue here or just store state?
                        # task = remaining_tasks[task_future]
                        # try:
                        #     await queue.add(task)
                        # except Exception as e:
                        #     logger.error(f"Failed to requeue task {task.id}: {e}")

        # unregister signals
        if register_signal_handlers:
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.remove_signal_handler(sig)

        logger.info(f"Worker stopped. Processed: {self.stats.processed}, Failed: {self.stats.failed}")

    async def _run_scheduler(self, poll_interval: float) -> None:
        logger.info(SCHEDULER_PREFIX + "started")

        while not self._shutdown_event.is_set():
            try:
                for queue in self.queues:
                    tasks = await queue.enqueue_scheduled()

                    if tasks:
                        _l = len(tasks)
                        _task_s = ", ".join([str(task.id) for task in tasks])
                        logger.info(SCHEDULER_PREFIX + f"Enqueued {_l} scheduled task{'s' if _l > 1 else ''} for processing: {_task_s}")

            except asyncio.CancelledError:
                logger.warning(SCHEDULER_PREFIX + "cancelled")
                break

            except Exception as e:
                logger.exception(SCHEDULER_PREFIX + f"Scheduling failed with error: {e}")
                self._shutdown_event.set()
                break

            await asyncio.sleep(poll_interval)  # TODO: replace polling with notifications when worker notifications are implemented

        logger.info(SCHEDULER_PREFIX + "stopped")

    async def _run_cron_manager(self, poll_interval: float) -> None:
        logger.info(CRON_MANAGER_PREFIX + "started")

        while not self._shutdown_event.is_set():
            try:
                for queue in self.queues:
                    for cron_data in await queue.get_crons():
                        cron = TaskCron.model_validate(cron_data)

                        _next_run = None
                        for _ in range(3):
                            _next_run = cron.next_run(_next_run)
                            task = cron.create_task(_next_run)
                            success = await queue.schedule(task, at=_next_run)
                            if success:
                                logger.info(CRON_MANAGER_PREFIX + f"Cron {cron.id} ({cron.spec.func}) scheduled at {_next_run}")

            except asyncio.CancelledError:
                logger.warning(CRON_MANAGER_PREFIX + "cancelled")
                break

            except Exception as e:
                logger.exception(CRON_MANAGER_PREFIX + f"failed with error: {e}")
                self._shutdown_event.set()
                break

            await asyncio.sleep(poll_interval)  # TODO: replace polling with notifications when worker notifications are implemented

        logger.info(CRON_MANAGER_PREFIX + "stopped")

    async def _run_worker_loop(self, queue: Queue, oneshot: bool = False) -> None:
        while not self._shutdown_event.is_set():

            if self._tasks_to_process is not None and self._tasks_to_process <= 0:
                self._shutdown_event.set()
                break

            # clean up completed tasks
            completed = [t for t in self._active_tasks[queue.name] if t.done()]
            for t in completed:
                del self._active_tasks[queue.name][t]

            if self._task_semaphore._value == 0:
                # hacky way to wait until there is an available slot
                async with self._task_semaphore:
                    continue

            # how many tasks to get
            capacity = self._task_semaphore._value
            if self._max_prefetch_tasks:
                capacity = min(capacity, self._max_prefetch_tasks)
            if self._tasks_to_process is not None:
                capacity = min(capacity, self._tasks_to_process)

            try:
                available_tasks = await queue.pop_pending(timeout=self._blocking_timeout,
                                                limit=capacity)

                if oneshot and not available_tasks:
                    logger.info(WORKER_PREFIX + f"Queue '{queue.name}' emptied")
                    self._empty_queues.append(queue.name)
                    if len(self._empty_queues) == len(self.queues):
                        self._shutdown_event.set()
                    break

                for task in available_tasks:
                    logger.info(WORKER_PREFIX + f"Processing task {task.id} ({task.spec.func})")
                    task_future = asyncio.create_task(self.process_task_semaphore_wrap(queue, task))
                    self._active_tasks[queue.name][task_future] = task

                    if self._tasks_to_process is not None:
                        self._tasks_to_process -= 1

            except asyncio.CancelledError:
                logger.warning(WORKER_PREFIX + "cancelled")
                break

            except Exception as e:
                logger.exception(WORKER_PREFIX + f"failed with error: {e}")
                self._shutdown_event.set()
                break

    async def process_task_semaphore_wrap(self, queue: Queue, task: Task) -> Task:
        async with self._task_semaphore:
            task = await self.process_task(task)
            await self._store_result(queue, task)

            # schedule the task for retry
            if task.error and task.should_retry and task.next_retry_at is not None:
                await queue.retry(task, task.next_retry_at)

            return task

    async def process_task(self, task: Task) -> Task:

        exception, task = await self._task_processor.execute_task(task, self.worker_id)

        if task.completed:
            self.stats.processed += 1
            logger.info(WORKER_PREFIX + f"Task {task.id} completed successfully")
        else:
            self.stats.failed += 1

        # non retriable task
        if task.error and not task.is_retriable:
            logger.error(WORKER_PREFIX + f"Task {task.id} failed: {exception}")

        # retriable task - final failure
        if task.error and task.is_retriable and not task.should_retry:
            logger.error(WORKER_PREFIX + f"Task {task.id} failed after {task.retry_count} retries: {exception}")

        # retriable task - reschedule
        if task.error and task.should_retry:
            logger.warning(WORKER_PREFIX + f"Task {task.id} failed (attempt {task.retry_count}/{task.config.retry}), scheduling retry at {task.next_retry_at}")

        return task

    async def _store_result(self, queue: Queue, task: Task) -> None:
        try:
            await queue.backend.store_result(queue.name, task.model_dump(mode='json'))  # TODO
        except Exception:
            logger.exception(f"Failed to store result for task {task.id}")

    def __register_signal_handlers(self, loop: asyncio.AbstractEventLoop) -> None:
        CTRL_C_THRESHOLD = 3
        def signal_handler(sig: signal.Signals) -> None:
            if self._shutdown_event.is_set():
                if self._ctrl_c_counter == CTRL_C_THRESHOLD:
                    logger.warning("Forcing shutdown...")
                    # cancel all tasks on shutdown
                    _futures = self._work_queue_tasks
                    _futures += [
                        k for inner_dict in self._active_tasks.values()  # type: ignore[misc]
                          for k, v in inner_dict.items()]
                    _futures += [self._scheduler_task] if self._scheduler_task else []
                    _futures += [self._cron_manager_task] if self._cron_manager_task else []
                    for future in _futures:
                        future.cancel()
                    # we cancelled active tasks, so clear all dicts as well
                    for d in self._active_tasks.values():
                        d.clear()
                    return

                logger.info(f"Press CTRL+C {CTRL_C_THRESHOLD - self._ctrl_c_counter} more times to force shutdown...")
                self._ctrl_c_counter += 1

            else:
                logger.info(f"Received signal {sig.name}, initiating graceful shutdown...")
                self._shutdown_event.set()

        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, partial(signal_handler, sig))

    async def _verify_connection(self, backend: Backend) -> bool:
        if not backend.is_connected:
            # TODO: implement backend.ping()
            await backend.connect()

        return True
