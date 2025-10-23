"""Tests for Dead Letter Queue (DLQ) functionality."""

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

from sheppy import Queue, Task, Worker, task


@task
def non_retriable_failing_task(message: str = "Task failed") -> None:
    """A non-retriable task that always fails."""
    raise ValueError(message)


@task(retry=2, retry_delay=0)
async def retriable_failing_task(message: str = "Async task failed") -> None:
    """A retriable task that always fails."""
    await asyncio.sleep(0.001)
    raise ValueError(message)


async def test_dlq_handler_called_for_non_retriable_task(queue: Queue, worker_backend) -> None:
    """Test that DLQ handler is called when a non-retriable task fails."""
    # Create a mock handler
    mock_handler = AsyncMock()
    
    # Create worker with DLQ handler
    worker = Worker(queue.name, worker_backend, on_failure=mock_handler)
    worker._blocking_timeout = 0.01
    
    # Add a failing task
    t = non_retriable_failing_task("Test failure")
    await queue.add(t)
    
    # Process the task
    await worker.work(max_tasks=1)
    
    # Verify handler was called
    mock_handler.assert_called_once()
    called_task = mock_handler.call_args[0][0]
    assert isinstance(called_task, Task)
    assert called_task.id == t.id
    assert called_task.error is not None
    assert "Test failure" in called_task.error
    assert called_task.completed is False


async def test_dlq_handler_called_after_retry_exhausted(queue: Queue, worker_backend) -> None:
    """Test that DLQ handler is called after all retries are exhausted."""
    # Create a mock handler
    mock_handler = AsyncMock()
    
    # Create worker with DLQ handler
    worker = Worker(queue.name, worker_backend, on_failure=mock_handler)
    worker._blocking_timeout = 0.01
    
    # Add a retriable failing task (will fail 3 times: initial + 2 retries)
    t = retriable_failing_task("Retriable failure")
    await queue.add(t)
    
    # Process the task 3 times (initial + 2 retries)
    await worker.work(max_tasks=3)
    
    # Verify handler was called only once (after all retries exhausted)
    mock_handler.assert_called_once()
    called_task = mock_handler.call_args[0][0]
    assert isinstance(called_task, Task)
    assert called_task.id == t.id
    assert called_task.error is not None
    assert "Retriable failure" in called_task.error
    assert called_task.retry_count == 2
    assert called_task.should_retry is False


async def test_dlq_handler_not_called_for_successful_task(queue: Queue, worker_backend) -> None:
    """Test that DLQ handler is not called when a task succeeds."""
    # Create a mock handler
    mock_handler = AsyncMock()
    
    @task
    async def successful_task() -> str:
        await asyncio.sleep(0.001)
        return "success"
    
    # Create worker with DLQ handler
    worker = Worker(queue.name, worker_backend, on_failure=mock_handler)
    worker._blocking_timeout = 0.01
    
    # Add a successful task
    t = successful_task()
    await queue.add(t)
    
    # Process the task
    await worker.work(max_tasks=1)
    
    # Verify handler was not called
    mock_handler.assert_not_called()


async def test_dlq_handler_sync_function(queue: Queue, worker_backend) -> None:
    """Test that DLQ handler works with synchronous functions."""
    # Create a mock synchronous handler
    mock_handler = Mock()
    
    # Create worker with DLQ handler
    worker = Worker(queue.name, worker_backend, on_failure=mock_handler)
    worker._blocking_timeout = 0.01
    
    # Add a failing task
    t = non_retriable_failing_task("Sync handler test")
    await queue.add(t)
    
    # Process the task
    await worker.work(max_tasks=1)
    
    # Verify handler was called
    mock_handler.assert_called_once()
    called_task = mock_handler.call_args[0][0]
    assert isinstance(called_task, Task)
    assert called_task.id == t.id


async def test_dlq_handler_exception_does_not_crash_worker(queue: Queue, worker_backend) -> None:
    """Test that exceptions in DLQ handler don't crash the worker."""
    # Create a handler that raises an exception
    async def failing_handler(task: Task) -> None:
        raise RuntimeError("Handler failed")
    
    # Create worker with failing DLQ handler
    worker = Worker(queue.name, worker_backend, on_failure=failing_handler)
    worker._blocking_timeout = 0.01
    
    # Add a failing task
    t = non_retriable_failing_task("Handler exception test")
    await queue.add(t)
    
    # Process the task - should not crash despite handler failure
    await worker.work(max_tasks=1)
    
    # Verify task was processed (failed)
    result_task = await queue.get_task(t)
    assert result_task is not None
    assert result_task.error is not None


async def test_worker_without_dlq_handler(queue: Queue, worker_backend) -> None:
    """Test that worker works normally without DLQ handler."""
    # Create worker without DLQ handler
    worker = Worker(queue.name, worker_backend)
    worker._blocking_timeout = 0.01
    
    # Add a failing task
    t = non_retriable_failing_task("No handler test")
    await queue.add(t)
    
    # Process the task - should work normally
    await worker.work(max_tasks=1)
    
    # Verify task was processed (failed)
    result_task = await queue.get_task(t)
    assert result_task is not None
    assert result_task.error is not None


async def test_dlq_handler_receives_updated_task(queue: Queue, worker_backend) -> None:
    """Test that DLQ handler receives the task with updated metadata."""
    received_tasks = []
    
    async def capture_handler(task: Task) -> None:
        received_tasks.append(task)
    
    # Create worker with capturing handler
    worker = Worker(queue.name, worker_backend, on_failure=capture_handler)
    worker._blocking_timeout = 0.01
    
    # Add a retriable failing task
    t = retriable_failing_task("Metadata test")
    await queue.add(t)
    
    # Process the task through all retries
    await worker.work(max_tasks=3)
    
    # Verify handler received the task with correct metadata
    assert len(received_tasks) == 1
    failed_task = received_tasks[0]
    assert failed_task.id == t.id
    assert failed_task.retry_count == 2
    assert failed_task.finished_at is not None
    assert failed_task.error is not None
