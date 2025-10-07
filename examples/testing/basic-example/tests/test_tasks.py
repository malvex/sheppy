from sheppy import TestQueue
from tasks import add

def test_add():
    q = TestQueue()

    # instantiate task
    t = add(1, 2)

    # add to the test queue
    q.add(t)

    # process the next task in the queue
    processed_task = q.process_next()

    # verify the task result
    assert processed_task.completed is True
    assert processed_task.error is None
    assert processed_task.result == 3
