import asyncio
from collections.abc import Generator
from typing import Any

from pydantic import BaseModel

from sheppy import Queue, task, workflow


class MyModel(BaseModel):
    name: str
    x: int
    y: int


@task
def returns_dict() -> dict:
    return {"hello": "world", "test": 3}


@task
def task1(y: int):
    return MyModel(name="task1", x="3", y=y)

@task
def task2(y: int) -> MyModel:
    return MyModel(name="task2", x="3", y=y)

@task
def task3(y: int):
    m = MyModel(name="task3", x="3", y=y)
    return {"model": m}

@task
def task4(y: int) -> dict:
    m = MyModel(name="task4", x="3", y=y)
    return {"model": m}

@task
def task5(y: int) -> dict[str, MyModel]:
    m = MyModel(name="task5", x="3", y=y)
    return {"model": m}


@workflow
def my_workflow(y: int):
    yield returns_dict()  # just to make this generator

    return MyModel(name="wf", x=1, y=y)


@workflow
def my_workflow2(y: int) -> Generator[Any, Any, MyModel]:
    yield returns_dict()  # just to make this generator

    return MyModel(name="wf", x=1, y=y)


async def test_tasks(queue2: Queue, worker_process_factory):
    worker_process_factory()

    tasks = [task1(5), task2(5), task3(5), task4(5), task5(5)]

    await queue2.add(tasks)

    done = await queue2.wait_for(tasks)

    #assert done[tasks[0].id].result == MyModel(name="task1", x=1, y=5)  # doesnt work because no type hint specified
    assert done[tasks[0].id].result == {"name": "task1", "x": 3, "y": 5}  # .. so it must be like this
    assert done[tasks[1].id].result == MyModel(name="task2", x=3, y=5)  # this one works because there is typehint
    assert done[tasks[2].id].result == {"model": {"name": "task3", "x": 3, "y": 5}}  # again, no typehint at all, so it outputs json
    assert done[tasks[3].id].result == {"model": {"name": "task4", "x": 3, "y": 5}}  # only "dict" typehint, no typehint what's inside
    assert done[tasks[4].id].result == {"model": MyModel(name="task5", x=3, y=5)}  # works fine


async def test_workflow(queue2: Queue, worker_process_factory):
    worker_process_factory()

    await queue2.add_workflow(w := my_workflow(5))

    await asyncio.sleep(1)

    ret = await queue2.get_workflow(w)

    assert ret.error is None
    assert ret.completed is True

    # assert ret.final_result == MyModel(name="wf", x=1, y=5)  # this doesn't work but again, it is understandable because no type hint
    assert ret.final_result == {"name": "wf", "x": 1, "y": 5}  # this works


    await queue2.add_workflow(w2 := my_workflow2(5))

    await asyncio.sleep(1)

    ret2 = await queue2.get_workflow(w2)

    assert ret2.error is None
    assert ret2.completed is True

    assert ret2.final_result == MyModel(name="wf", x=1, y=5)  # reconstructed from the Generator return type hint, same pattern as tasks
