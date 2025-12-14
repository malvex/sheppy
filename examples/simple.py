import asyncio
from datetime import datetime

from pydantic import BaseModel

from sheppy import MemoryBackend, Queue, Worker, task


class Email(BaseModel):
    to: str
    subject: str
    body: str


@task
async def send_email(email: Email) -> dict[str, str]:
    print(f"[{datetime.now()}] Sent email to {email.to}, Subject: {email.subject}, Body: {email.body}")
    return {"status": "sent"}


queue = Queue(MemoryBackend(instant_processing=False))


async def run_worker():
    w = Worker("default", backend=queue.backend)
    await w.work()


async def main():
    # start worker in background
    worker_process = asyncio.create_task(run_worker())

    welcome_email = Email(to="user1@example.com",
                          subject="Registration Successful!",
                          body="Your account has been created!")

    # send welcome email immediately
    task = send_email(welcome_email)
    await queue.add(task)

    # wait for results to verify welcome email was sent
    task = await queue.wait_for(task)

    assert task.result == {"status": "sent"}
    assert task.status == 'completed'
    assert not task.error

    # stop worker
    worker_process.cancel()

if __name__ == "__main__":
    asyncio.run(main())
