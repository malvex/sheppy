from sheppy import TestQueue, task


@task
def divide(x: int, y: int) -> float:
    return x / y


def test_divide_by_zero():
    q = TestQueue()

    # instantiate two tasks
    t1 = divide(1, 2)
    t2 = divide(1, 0)

    # add both tasks to the test queue
    q.add([t1, t2])

    # process all tasks in the queue (processed in order)
    processed_tasks = q.process_all()

    # verify the first task result
    assert processed_tasks[0].completed is True
    assert processed_tasks[0].error is None
    assert processed_tasks[0].result == 0.5

    # verify the second task result (should fail)
    assert processed_tasks[1].completed is False
    assert processed_tasks[1].error == "ZeroDivisionError: division by zero"
