import asyncio
from datetime import datetime, timedelta

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


queue = Queue(MemoryBackend())


async def run_worker():
    w = Worker("default", backend=queue.backend)
    await w.work()


async def main():
    # start worker in background
    worker_process = asyncio.create_task(run_worker())

    welcome_email = Email(to="user1@example.com",
                          subject="Registration Successful!",
                          body="Your account has been created!")

    survey_email = Email(to="user1@example.com",
                         subject="Feedback survey",
                         body="How do you like our new website?")


    # send welcome email immediately
    task = send_email(welcome_email)
    await queue.add(task)

    # schedule feedback email to be delivered after a few weeks (2 seconds in this example)
    survey_email_task = send_email(survey_email)
    await queue.schedule(survey_email_task, at=timedelta(seconds=2))

    # wait for results to verify welcome email was sent
    task = await queue.wait_for_result(task)
    assert task.result.get("status", None) == "sent"
    assert task.completed
    assert not task.error

    # confirm scheduled task wasn't sent yet
    survey_email_task = await queue.get_task(survey_email_task)
    assert not survey_email_task.completed

    # wait for scheduled email to happen (for demo purposes)
    print("sleeping for 3 seconds...")
    await asyncio.sleep(3)

    # verify the email was sent (for demo purposes)
    survey_email_task = await queue.get_task(survey_email_task)
    assert survey_email_task.completed
    assert not survey_email_task.error
    assert survey_email_task.result.get("status") == "sent"

    # stop worker
    worker_process.cancel()

if __name__ == "__main__":
    asyncio.run(main())
