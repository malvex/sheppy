import asyncio

from sheppy import MemoryBackend, Queue, Worker, task, workflow


@task
async def say_hello(name: str) -> str:
    return f"Hello, {name}!"


@workflow
def greetings_workflow(names: list[str]):
    t1 = yield say_hello("Alice")
    t2 = yield say_hello("Bob")
    tx = yield [say_hello(name) for name in names]  # fan-out style

    return "\n".join([t1.result, t2.result] + [t.result for t in tx])


queue = Queue(MemoryBackend(instant_processing=False))


async def main():
    # start worker in background
    worker = Worker("default", backend=queue.backend)
    worker_process = asyncio.create_task(worker.work())

    result = await queue.add_workflow(greetings_workflow(["Alex", "John"]))

    # wait for the workflow to finish
    wf = await queue.wait_for_workflow(result.workflow.id)

    # stop worker
    worker_process.cancel()

    if wf is None or wf.error:
        print(f"Workflow failed: {wf.error if wf else 'not found'}")
    else:
        print(wf.final_result)


if __name__ == "__main__":
    asyncio.run(main())
