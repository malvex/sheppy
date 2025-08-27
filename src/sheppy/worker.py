import asyncio
import logging
import signal
from functools import partial

from pydantic import BaseModel

from .backend.base import Backend
from .queue import Queue
from .task import Task
from .utils.dependency_injection import DependencyResolver
from .utils.task_execution import (
    execute_task,
    generate_unique_worker_id,
    get_available_tasks,
    update_failed_task,
)

logger = logging.getLogger(__name__)


class WorkerStats(BaseModel):
    processed: int = 0
    failed: int = 0


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
        self.max_concurrent_tasks = max_concurrent_tasks

        self.worker_id = generate_unique_worker_id("worker")
        self.stats = WorkerStats()

        # Worker internals
        self._dependency_resolver = DependencyResolver()
        self._shutdown_event = asyncio.Event()
        self._active_tasks: dict[asyncio.Task[Task], Task] = {}
        self._task_semaphore = asyncio.Semaphore(max_concurrent_tasks)  # Limit maximum amount of tasks to process
        self._poll_interval = 0.5

    async def process_task(self, task: Task) -> Task:
        async with self._task_semaphore:
            try:
                # Execute the task
                task = await execute_task(task, self._dependency_resolver, self.worker_id)

                logger.info(f"Task {task.id} completed successfully")
                self.stats.processed += 1

            except Exception as e:
                # Handle task failure
                task = update_failed_task(task, e)

                # Final failure
                if task.metadata.finished_datetime:
                    if task.metadata.retry > 0 and task.metadata.retry_count > 0:
                        logger.error(
                            f"Task {task.id} failed after {task.metadata.retry_count} retries: {e}",
                            exc_info=True
                        )
                    else:
                        logger.error(f"Task {task.id} failed: {e}", exc_info=True)

                    self.stats.failed += 1
                else:
                    logger.info(
                        f"Task {task.id} failed (attempt {task.metadata.retry_count}/{task.metadata.retry}), "
                        f"scheduling retry at {task.metadata.next_retry_at}"
                    )

                    # Schedule the task for retry
                    if task.metadata.next_retry_at is not None:
                        await self.queue.schedule(task, task.metadata.next_retry_at)

            # Store the result in backend for later retrieval
            try:
                await self.queue.backend.store_result(self.queue_name, task.model_dump(mode='json'))  # TODO
            except Exception as e:
                logger.warning(f"Failed to store result for task {task.id}: {e}")
                # Don't fail the task processing if result storage fails

            await self.queue._acknowledge(task.id)

            return task

    async def work(self, max_tasks: int | None = None) -> None:
        # Set up signal handlers for graceful shutdown
        loop = asyncio.get_event_loop()

        def signal_handler(sig: signal.Signals) -> None:
            logger.info(f"Received signal {sig}, initiating graceful shutdown...")
            self._shutdown_event.set()

        # Register signal handlers
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, partial(signal_handler, sig))

        try:
            tasks_to_process = max_tasks

            while not self._shutdown_event.is_set():

                # Check if we've processed enough tasks
                if max_tasks is not None and tasks_to_process is not None and tasks_to_process <= 0:
                    break

                try:
                    # Clean up completed tasks first
                    completed = [t for t in self._active_tasks if t.done()]
                    for t in completed:
                        del self._active_tasks[t]

                    # Only fetch new tasks if we have capacity
                    if self._task_semaphore._value == 0:
                        await asyncio.sleep(self._poll_interval)
                        continue

                    # Determine how many tasks to get
                    capacity = self._task_semaphore._value
                    if max_tasks and tasks_to_process is not None:
                        capacity = min(capacity, tasks_to_process)

                    # Get available tasks up to our capacity
                    available_tasks = await get_available_tasks(
                        self.queue,
                        limit=capacity,
                        timeout=self._poll_interval
                    )

                    # No tasks available after timeout - continue loop
                    if not available_tasks:
                        continue

                    # Process tasks
                    for task in available_tasks:
                        logger.info(f"Processing task {task.id}")
                        task_future = asyncio.create_task(self.process_task(task))
                        self._active_tasks[task_future] = task

                        if max_tasks and tasks_to_process is not None:
                            tasks_to_process -= 1

                except asyncio.CancelledError:
                    # Shutdown requested
                    break
                except Exception as e:
                    logger.error(f"Error in worker loop: {e}", exc_info=True)
                    # Continue processing unless shutdown requested
                    if not self._shutdown_event.is_set():
                        await asyncio.sleep(1)

        finally:
            # Wait for active tasks to complete
            if self._active_tasks:
                logger.info(f"Waiting for {len(self._active_tasks)} active tasks to complete...")
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*self._active_tasks.keys(), return_exceptions=True),
                        timeout=self.shutdown_timeout
                    )
                except asyncio.TimeoutError:
                    logger.warning("Some tasks did not complete within shutdown timeout")
                    # Cancel remaining tasks
                    for task_future in self._active_tasks:
                        if not task_future.done():
                            task_future.cancel()
                            # Requeue the task
                            try:
                                await self.queue.add(self._active_tasks[task_future])
                            except Exception as e:
                                logger.error(f"Failed to requeue task {task.id}: {e}", exc_info=True)

            # Remove signal handlers
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.remove_signal_handler(sig)

            logger.info(f"Worker stopped. Processed: {self.stats.processed}, Failed: {self.stats.failed}")
