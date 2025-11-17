from sheppy import TestQueue, task
from sheppy.testqueue import (
    assert_is_completed,
    assert_is_failed,
    assert_is_new
)

@task
def add(x: int, y: int) -> int:
    return x + y

@task
def divide(x: int, y: int) -> float:
    return x / y

def test_fail_once():
    q = TestQueue()

    t1 = add(1, 2)
    t2 = divide(1, 0)

    # add both tasks to the test queue
    q.add([t1, t2])

    # process all tasks in the queue
    processed = q.process_all()

    # quick assertions to verify original tasks weren't modified
    # (note: this is always the case, we never modify original tasks)
    assert_is_new(t1)
    assert_is_new(t2)

    # quick assertions using helper functions
    assert_is_completed(processed[0])
    assert_is_failed(processed[1])

    # check results manually as well
    assert processed[0].result == 3
    assert processed[1].error == "ZeroDivisionError: division by zero"
