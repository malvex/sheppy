from datetime import datetime, timedelta

from sheppy import InMemoryBackend, Queue, Worker
from tests.dependencies import simple_async_task, simple_sync_task


async def test_cron(queue: Queue):

    crons = [
        (simple_async_task(1, 2), '*/5 * * * *'),
        (simple_async_task(1, 2), '0 * * * *'),  # different cron schedule
        (simple_async_task(2, 2), '*/5 * * * *'),  # different input
        (simple_sync_task(1, 2), '*/5 * * * *'),  # different function
    ]

    for task, schedule in crons:
        for i in range(5):
            # Only first run should be successful, because cron doesn't exist yet.
            # Exactly same cron definitions (same task, same input, same cron schedule)
            # should not be added (would be duplicated crons)
            should_succeed = True if i == 0 else False  # noqa: SIM210

            created_cron = await queue.add_cron(task, schedule)
            assert (created_cron is not None) is should_succeed

    all_crons = await queue.get_crons()
    assert len(all_crons) == 4

    for task, schedule in crons:
        for i in range(5):
            # same for deleting them - only first should be successful
            should_succeed = True if i == 0 else False  # noqa: SIM210

            success = await queue.delete_cron(task, schedule)
            assert success is should_succeed

    all_crons = await queue.get_crons()
    assert len(all_crons) == 0


async def test_delete_nonexistent_cron(queue: Queue):
    success = await queue.delete_cron(simple_async_task(5, 6), "1 * * * *")
    assert not success


async def test_process_cron(datetime_now: datetime, queue: Queue, worker: Worker):
    t = simple_async_task(1, 2)
    expression = '* * * * *'

    created_cron = await queue.add_cron(t, expression)
    assert created_cron is not None

    duplicate_cron = await queue.add_cron(t, expression)
    assert duplicate_cron is None

    all_crons = await queue.get_crons()
    assert len(all_crons) == 1

    next_run = all_crons[0].next_run(datetime_now - timedelta(minutes=2))
    calculated_next_run = datetime_now - timedelta(minutes=1, seconds=datetime_now.second, microseconds=datetime_now.microsecond)
    assert next_run == calculated_next_run

    scheduled_task = all_crons[0].create_task(next_run)
    assert scheduled_task.scheduled_at is None  # TaskCron.create_task doesn't set scheduled_at! It is set by Queue.schedule()

    await queue.schedule(scheduled_task, at=next_run)
    assert await queue.size() == 0  # scheduled means no queued (pending) task yet

    await worker.work(1)
    assert await queue.size() == 0  # should be processed now

    completed_task = await queue.get_task(scheduled_task.id)

    assert completed_task.status == 'completed'
    assert completed_task.scheduled_at == next_run
    assert completed_task.result == 3


async def test_cron_generates_different_tasks(datetime_now: datetime, queue: Queue):
    t = simple_async_task(1, 2)
    expression = '* * * * *'

    created_cron = await queue.add_cron(t, expression)
    assert created_cron is not None

    all_crons = await queue.get_crons()
    assert len(all_crons) == 1

    normalized_dt = datetime_now - timedelta(seconds=datetime_now.second, microseconds=datetime_now.microsecond)

    next_run1 = all_crons[0].next_run(datetime_now + timedelta(hours=1))
    next_run2 = all_crons[0].next_run(next_run1)

    calculated_next_run = normalized_dt + timedelta(hours=1, minutes=1)
    assert next_run1 == calculated_next_run
    assert next_run2 == calculated_next_run + timedelta(minutes=1)

    t1 = all_crons[0].create_task(next_run1)
    t2 = all_crons[0].create_task(next_run2)

    assert t1 != t2
    assert t1.id != t2.id
    assert t1.spec == t2.spec
    assert t1.config == t2.config


async def test_cron_ttl_defaults_preserve_identity():
    backend = InMemoryBackend(ttl=5, error_ttl=50)
    queue = Queue(backend, "cron-ttl")
    created = simple_sync_task(1, 2)
    expression = "*/5 * * * *"

    created_cron = await queue.add_cron(created, expression)
    assert created_cron is not None
    assert created_cron.expression == expression
    assert await queue.add_cron(created, expression) is None

    cron = (await queue.get_crons())[0]
    assert cron.config.ttl == 5
    assert cron.config.error_ttl == 50
    assert created.config.ttl == "inherit"
    assert created.config.error_ttl == "inherit"

    scheduled = cron.create_task(cron.next_run())
    assert scheduled.config.ttl == 5
    assert scheduled.config.error_ttl == 50
    assert await queue.delete_cron(created, expression) is True
