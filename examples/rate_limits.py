import asyncio
from datetime import datetime
from sheppy import Queue, task

queue = Queue("redis://127.0.0.1:6379")


@task(rate_limit={"max_rate": 2, "rate_period": 5})
async def do_work(queued_time: datetime) -> int:
    execution_time = datetime.now()
    delay = (execution_time - queued_time).total_seconds()
    return int(delay)


async def main():
    t1 = do_work(datetime.now())
    t2 = do_work(datetime.now())
    t3 = do_work(datetime.now())
    await queue.add([t1, t2, t3])

    # await the task completion
    update_tasks = await queue.wait_for([t1, t2, t3])
    t1 = update_tasks[t1.id]
    t2 = update_tasks[t2.id]
    t3 = update_tasks[t3.id]

    assert all([t1.status == 'completed', t2.status == 'completed', t3.status == 'completed'])

    # two tasks will be executed immediately and result will be 0
    # one task will be executed after 5 seconds because of rate limit and returns 5
    # the order is not guaranteed, any task might run first
    print("t1.result:", t1.result)
    print("t2.result:", t2.result)
    print("t3.result:", t3.result)


if __name__ == "__main__":
    asyncio.run(main())
