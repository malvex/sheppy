import asyncio

from sheppy import Queue, task
from sheppy._workflow import workflow

ADMIN_EMAILS = [
    "admin1@example.com",
    "admin2@example.com",
    "admin3@example.com",
]

@task
async def say_hello(name: str) -> str:
    return f"Hello, {name}!"


@workflow
def example_workflow(names: list[str]):
    t1 = yield say_hello("Alice")
    t2 = yield say_hello("Bob")
    tx = yield [say_hello(name) for name in names]  # fan-out style

    return "\n".join([t1.result, t2.result] + [t.result for t in tx])


async def main():
    queue = Queue("redis://")
    wf = example_workflow(["Alex", "John"])
    await queue.add_workflow(wf)


if __name__ == "__main__":
    asyncio.run(main())
