from fastapi import Depends, FastAPI
from sheppy import RedisBackend, Queue

from tasks import Email, Status, send_email_task

backend = RedisBackend("redis://127.0.0.1:6379")

# FastAPI dependency injection
def get_queue() -> Queue:
    return Queue(backend)


app = FastAPI(title="Fancy Website")


@app.post("/send-email", status_code=200)
async def send_email(email: Email, queue: Queue = Depends(get_queue)) -> Status:

    t = send_email_task(email)
    await queue.add(t)

    processed = await queue.wait_for(t, timeout=5)

    if processed.error:
        raise Exception(f"Task failed: {processed.error}")

    return processed.result
