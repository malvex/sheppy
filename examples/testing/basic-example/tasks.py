from sheppy import task

@task
async def add(x: int, y: int) -> int:
    return x + y
