import pytest

from sheppy.utils.task_execution import (
    calculate_retry_delay,
    update_failed_task,
)
from sheppy.task import Task, TaskMetadata


class TestCalculateRetryDelay:
    """Test calculate_retry_delay function."""

    @pytest.mark.parametrize("retry_delay,retry_count,expected_delay", [
        # No retry_delay configured
        (0, 0, 0),
        (0, 5, 0),

        # Constant delay
        (5, 0, 5.0),
        (5, 3, 5.0),

        # Custom delays per retry
        ([1, 2, 5, 10], 0, 1.0),
        ([1, 2, 5, 10], 1, 2.0),
        ([1, 2, 5, 10], 2, 5.0),
        ([1, 2, 5, 10], 3, 10.0),
        ([1, 2, 5, 10], 4, 10.0),  # Uses last value for extra retries
        ([1, 2, 5, 10], 10, 10.0),

        # Empty list
        ([], 0, 1.0),
        ([], 5, 1.0),
    ])
    def test_calculate_retry_delay(self, retry_delay, retry_count, expected_delay):
        task = Task(
            metadata=TaskMetadata(
                retry_delay=retry_delay,
                retry_count=retry_count
            )
        )

        delay = calculate_retry_delay(task)
        assert delay == expected_delay


class TestUpdateFailedTask:
    """Test update_failed_task function."""

    def test_retry_when_available(self):
        task = Task(
            metadata=TaskMetadata(
                retry_count=1,
                retry=3,
                retry_delay=5.0,
                last_retry_at=None,
                next_retry_at=None,
                finished_datetime=None,
            )
        )

        exception = ValueError("Test error")
        updated_task = update_failed_task(task, exception)

        assert not updated_task.metadata.finished_datetime
        assert updated_task.error == "Test error"
        assert updated_task.completed is False
        assert updated_task.metadata.retry_count == 2
        assert updated_task.metadata.last_retry_at is not None
        assert updated_task.metadata.next_retry_at is not None
        assert updated_task.metadata.finished_datetime is None

    def test_no_retry_when_max_reached(self):
        task = Task(
            metadata=TaskMetadata(
                retry_count=3,
                retry=3,
                finished_datetime=None,
            )
        )

        exception = ValueError("Final error")
        updated_task = update_failed_task(task, exception)

        assert updated_task.metadata.finished_datetime
        assert updated_task.error == "Final error"
        assert updated_task.completed is False
        assert updated_task.metadata.finished_datetime is not None
