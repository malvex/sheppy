from sheppy import TestQueue, task

FAIL_COUNTER = 0

@task(retry=2, retry_delay=0)
def fail_once() -> str:
    global FAIL_COUNTER

    if FAIL_COUNTER < 1:
        FAIL_COUNTER += 1
        raise ValueError("task failed")

    return "success"

def test_fail_once():
    q = TestQueue()

    # instantiate the task
    t = fail_once()

    # add the task to the test queue
    q.add(t)

    assert q.size() == 1  # one task in the queue

    # process all tasks in the queue
    processed = q.process_all()

    # there should be two processed tasks: the original + one retry
    assert len(processed) == 2

    # verify the task result
    assert processed[0].completed is False
    assert processed[0].error == "task failed"

    # retry should succeed
    assert processed[1].completed is True
    assert processed[1].error is None
    assert processed[1].result == "success"

    # both processed tasks should have the same id
    assert processed[0].id == processed[1].id
