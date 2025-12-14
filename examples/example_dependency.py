from sheppy import Depends, TestQueue, task


def get_db():
    return "PROD DATABASE"



@task
def my_task(db: str = Depends(get_db)):
    print("my database is: ", db)
    return db


def test_dependency_override():
    def alternative_get_db():
        return "TESTING DATABASE"

    q = TestQueue(dependency_overrides={get_db: alternative_get_db})

    q.add(my_task())

    processed = q.process_next()

    assert processed.status == 'completed'
    assert processed.result == "TESTING DATABASE"
