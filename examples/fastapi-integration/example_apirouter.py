from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sheppy import MemoryBackend, Queue, task
from sheppy.fastapi import create_router


@task
def add(x: int, y: int) -> int:
    if y == 30:
        raise Exception("failed to do something!")
    return x + y


backend = MemoryBackend()
queue = Queue(backend)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins="*",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(create_router(backend), prefix="/sheppy")

# run this with `fastapi dev examples/fastapi-integration/example_apirouter.py`
