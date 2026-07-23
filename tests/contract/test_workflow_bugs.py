import asyncio
from collections.abc import Generator
from typing import Any

import pytest

from sheppy import Queue, Task, task, workflow


@task
def add(x: int, y: int) -> int:
    return x + y


@task
def division(x: int, y: int) -> float:
    return x / y

@task
def returns_dict() -> dict:
    return {"hello": "world", "test": 3}


@task
def inner_pop(inp: dict) -> tuple[dict, str]:
    item = inp.pop("hello")
    assert item == "world"

    return inp, item


@workflow
def my_workflow(y: int) -> Generator[Any, Any, tuple[int, Task, Task, Task, Task, list[Task]]]:

    if y == -2:
        raise Exception("exception test")

    assert y > 0, "y assert fail test"


    returned_dict_task = yield returns_dict()  # type: dict
    assert returned_dict_task.status == "completed"
    assert returned_dict_task.result == {"hello": "world", "test": 3}, "dict has been modified!"

    returned_dict = returned_dict_task.result

    # this must not change on workflow reruns
    assert returned_dict == {"hello": "world", "test": 3}

    # bug 1
    popped_item = returned_dict.pop("test")

    returned_add = yield add(popped_item, y)
    assert returned_add.status == "completed", 1
    assert returned_add.result == popped_item + y, 2

    assert returned_dict == {"hello": "world"}  # should still be this

    division_by_zero = yield division(popped_item, 0)

    if division_by_zero.status != "failed":
        pytest.fail("division by zero did not fail")

    returned_inner_pop = yield inner_pop(returned_dict)

    assert returned_inner_pop.status == "completed", 3
    assert returned_inner_pop.result == ({}, "world"), 4

    assert returned_dict ==  {"hello": "world"}, 5

    returned_inner_pop_many = yield inner_pop(returned_dict), inner_pop(returned_dict)  # tuple yield

    assert returned_inner_pop_many[0].status == "completed"
    assert returned_inner_pop_many[0].result == ({}, "world")
    assert returned_inner_pop_many[1].status == "completed"
    assert returned_inner_pop_many[1].result == ({}, "world")

    assert returned_dict ==  {"hello": "world"}

    return y, returned_dict_task, returned_add, division_by_zero, returned_inner_pop, returned_inner_pop_many


async def test_memory_backend():
    queue = Queue("memory://")

    await queue.add_workflow(w := my_workflow(5))

    i = 0
    ok = False
    ret = None

    while i < 3:
        i += 1

        ret = await queue.get_workflow(w)

        if ret.completed or ret.error:
            ok = True
            break

        await asyncio.sleep(1)

    if not ok:
        pytest.fail("workflow did not completed in time")

    assert ret.error is None
    assert ret.completed is True

    assert ret.final_result[0] == 5

    assert ret.final_result[1].status == "completed"
    assert ret.final_result[1].result == {"hello": "world"}

    assert ret.final_result[2].status == "completed"
    assert ret.final_result[2].result == 8

    assert ret.final_result[3].status == "failed"
    assert ret.final_result[3].result is None
    assert "division by zero" in ret.final_result[3].error

    assert ret.final_result[4].status == "completed"
    assert ret.final_result[4].result == ({}, "world")

    assert ret.final_result[5][0].status == "completed"
    assert ret.final_result[5][0].result == ({}, "world")
    assert ret.final_result[5][1].status == "completed"
    assert ret.final_result[5][1].result == ({}, "world")


async def test_quick_error_exception_redis_backend(queue2: Queue, worker_process_factory):
    worker_process_factory()

    await queue2.add_workflow(w := my_workflow(-2))

    await asyncio.sleep(0.5)
    ret = await queue2.get_workflow(w)

    assert ret.completed is False
    assert ret.error == "Exception: exception test"
    assert ret.final_result is None


async def test_quick_error_assert_redis_backend(queue2: Queue, worker_process_factory):
    worker_process_factory()

    await queue2.add_workflow(w := my_workflow(-1))

    await asyncio.sleep(0.5)
    ret = await queue2.get_workflow(w)

    assert ret.completed is False
    assert ret.error == "AssertionError: y assert fail test\nassert -1 > 0"
    assert ret.final_result is None


async def test_redis_backend(queue2: Queue, worker_process_factory):
    worker_process_factory()

    await queue2.add_workflow(w := my_workflow(5))

    i = 0
    ok = False
    ret = None

    while i < 3:
        i += 1

        ret = await queue2.get_workflow(w)

        if ret.completed or ret.error:
            ok = True
            break

        await asyncio.sleep(1)

    if not ok:
        pytest.fail("workflow did not completed in time")

    # print(ret.model_dump_json())

    assert ret.error is None
    assert ret.completed is True

    assert ret.final_result[0] == 5

    assert ret.final_result[1].status == "completed"
    assert ret.final_result[1].result == {"hello": "world"}

    assert ret.final_result[2].status == "completed"
    assert ret.final_result[2].result == 8

    assert ret.final_result[3].status == "failed"
    assert ret.final_result[3].result is None
    assert "division by zero" in ret.final_result[3].error

    assert ret.final_result[4].status == "completed"
    assert ret.final_result[4].result == ({}, "world")

    assert ret.final_result[5][0].status == "completed"
    assert ret.final_result[5][0].result == ({}, "world")
    assert ret.final_result[5][1].status == "completed"
    assert ret.final_result[5][1].result == ({}, "world")
