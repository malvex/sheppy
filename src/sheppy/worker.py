import asyncio
import logging
import signal
from functools import partial

from pydantic import BaseModel

from .backend.base import Backend
from .queue import Queue
from .task import Task, TaskCron
from .utils.task_execution import (
    TaskStatus,
    TaskProcessor,
    generate_unique_worker_id,
)

logger = logging.getLogger(__name__)


class WorkerStats(BaseModel):
    processed: int = 0
    failed: int = 0


# ! FIXME - do this differently
WORKER_PREFIX = "<Worker> "
SCHEDULER_PREFIX = "<Scheduler> "
CRON_MANAGER_PREFIX = "<CronManager> "


class Worker:
    def __init__(
        self,
        queue_name: str,
        backend: Backend,
        shutdown_timeout: float = 30.0,
        max_concurrent_tasks: int = 10
    ):
        self.queue_name = queue_name
        self.queue = Queue(queue_name, backend)
        self.shutdown_timeout = shutdown_timeout

        self.worker_id = generate_unique_worker_id("worker")
        self.stats = WorkerStats()

        self._task_processor = TaskProcessor()
        self._shutdown_event = asyncio.Event()
        self._active_tasks: dict[asyncio.Task[Task], Task] = {}
        self._task_semaphore = asyncio.Semaphore(max_concurrent_tasks)
        self._blocking_timeout = 5
        self._scheduler_task: asyncio.Task = None
        self._cron_manager_task: asyncio.Task = None

    async def work(self, max_tasks: int | None = None) -> None:
        loop = asyncio.get_event_loop()
        self.__register_signal_handlers(loop)

        try:
            self._scheduler_task = asyncio.create_task(self._run_scheduler())
            self._cron_manager_task = asyncio.create_task(self._run_cron_manager())
            await self._run_worker_loop(max_tasks)

        except asyncio.CancelledError:
            logger.info("Cancelled")

        except Exception as e:
            logger.error(f"Error in worker loop: {e}", exc_info=True)

        if self._scheduler_task:
            self._scheduler_task.cancel()

        # attempt to exit cleanly
        if self._active_tasks:
            logger.info(WORKER_PREFIX + f"Waiting for {len(self._active_tasks)} active tasks to complete...")
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._active_tasks.keys(), return_exceptions=True),
                    timeout=self.shutdown_timeout
                )
            except asyncio.TimeoutError:
                logger.warning("Some tasks did not complete within shutdown timeout")

                # ! FIXME - what should we do here with the existing tasks? (maybe DLQ?)

                for task_future in self._active_tasks:
                    if not task_future.done():
                        task_future.cancel()

                        # ! FIXME - should we try reqeueue here or just store state?
                        # task = self._active_tasks[task_future]
                        # try:
                        #     await self.queue.add(task)
                        # except Exception as e:
                        #     logger.error(f"Failed to requeue task {task.id}: {e}", exc_info=True)

        # signal cleanup
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.remove_signal_handler(sig)

        logger.info(f"Worker stopped. Processed: {self.stats.processed}, Failed: {self.stats.failed}")

    async def _run_scheduler(self, poll_interval: float = 1.0):
        logger.info(SCHEDULER_PREFIX + "started")

        while not self._shutdown_event.is_set():
            try:
                tasks = await self.queue.enqueue_scheduled()

                if tasks:
                    _l = len(tasks)
                    _task_s = ", ".join([str(task.id) for task in tasks])
                    logger.info(SCHEDULER_PREFIX + f"Enqueued {_l} scheduled task{"s" if _l > 1 else ""} for processing: {_task_s}")

            except asyncio.CancelledError:
                break

            except Exception as e:
                logger.exception(SCHEDULER_PREFIX + f"Scheduling failed with error: {e}", exc_info=True)

            await asyncio.sleep(poll_interval)  # TODO: replace polling with notifications when worker notifications are implemented

        logger.info(SCHEDULER_PREFIX + "stopped")

    async def _run_cron_manager(self, poll_interval: float = 10.0):
        logger.info(CRON_MANAGER_PREFIX + "started")

        while not self._shutdown_event.is_set():
            try:
                for cron_data in await self.queue.backend.list_crons(self.queue_name):
                    cron = TaskCron.model_validate(cron_data)

                    _next_run = None
                    for _ in range(3):
                        _next_run = cron.next_run(_next_run)
                        task = cron.create_task(_next_run)
                        success = await self.queue.schedule(task, at=_next_run)
                        if success:
                            logger.info(CRON_MANAGER_PREFIX + f"Cron {cron.id} ({cron.func}) scheduled at {_next_run}")

                # if tasks:
                #     _l = len(tasks)
                #     _task_s = ", ".join([str(task.id) for task in tasks])
                #     logger.info(f"Enqueued {_l} scheduled task{"s" if _l > 1 else ""} for processing: {_task_s}")

            except asyncio.CancelledError:
                break

            except Exception as e:
                logger.exception(CRON_MANAGER_PREFIX + f"failed with error: {e}", exc_info=True)

            await asyncio.sleep(poll_interval)  # TODO: replace polling with notifications when worker notifications are implemented

        logger.info(CRON_MANAGER_PREFIX + "stopped")


    async def _run_worker_loop(self, max_tasks: int | None = None) -> None:
        tasks_to_process = max_tasks

        while not self._shutdown_event.is_set():

            if tasks_to_process is not None and tasks_to_process <= 0:
                break

            # clean up completed tasks
            completed = [t for t in self._active_tasks if t.done()]
            for t in completed:
                del self._active_tasks[t]

            if self._task_semaphore._value == 0:
                # hacky way to wait until there is an available slot
                async with self._task_semaphore:
                    continue

            # how many tasks to get
            capacity = self._task_semaphore._value
            if tasks_to_process is not None:
                capacity = min(capacity, tasks_to_process)

            available_tasks = await self.queue._pop(timeout=self._blocking_timeout,
                                                    limit=capacity)

            for task in available_tasks:
                logger.info(WORKER_PREFIX + f"Processing task {task.id} ({task.internal.func})")
                task_future = asyncio.create_task(self.process_task_semaphore_wrap(task))
                self._active_tasks[task_future] = task

                if tasks_to_process is not None:
                    tasks_to_process -= 1

    async def process_task_semaphore_wrap(self, task: Task) -> Task:
        async with self._task_semaphore:
            task = await self.process_task(task)
            await self._store_result(task)

            return task

    async def process_task(self, task: Task) -> Task:

        task_status, exception, task = await self._task_processor.execute_task(task, self.worker_id)

        if task_status == TaskStatus.SUCCESS:
            self.stats.processed += 1
            logger.info(WORKER_PREFIX + f"Task {task.id} completed successfully")
        else:
            self.stats.failed += 1

        # non retriable task
        if task_status == TaskStatus.FAILED_NO_RETRY:
            logger.error(WORKER_PREFIX + f"Task {task.id} failed: {exception}", exc_info=True)

        # retriable task - final failure
        if task_status == TaskStatus.FAILED_OUT_OF_RETRY:
            logger.error(WORKER_PREFIX + f"Task {task.id} failed after {task.metadata.retry_count} retries: {exception}", exc_info=True)

        # retriable task - reschedule
        if task_status == TaskStatus.FAILED_SHOULD_RETRY:
            logger.warning(WORKER_PREFIX + f"Task {task.id} failed (attempt {task.metadata.retry_count}/{task.metadata.retry}), scheduling retry at {task.metadata.next_retry_at}")

            # schedule the task for retry
            if task.metadata.next_retry_at is not None:
                await self.queue.schedule(task, task.metadata.next_retry_at)

        return task

    async def _store_result(self, task: Task) -> None:
        try:
            await self.queue.backend.store_result(self.queue_name, task.model_dump(mode='json'))  # TODO
        except Exception:
            logger.exception(f"Failed to store result for task {task.id}", exc_info=True)

    def __register_signal_handlers(self, loop: asyncio.AbstractEventLoop) -> None:
        def signal_handler(sig: signal.Signals) -> None:
            logger.info(f"Received signal {sig}, initiating graceful shutdown...")
            self._shutdown_event.set()

        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, partial(signal_handler, sig))
