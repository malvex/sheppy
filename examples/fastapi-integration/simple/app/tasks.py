import asyncio
from datetime import datetime
from pydantic import BaseModel

from sheppy import task


class Email(BaseModel):
    to: str
    subject: str
    body: str


class Status(BaseModel):
    ok: bool


@task
async def send_email_task(email: Email) -> Status:
    print(f"Sending email to {email.to} with subject '{email.subject}'")
    await asyncio.sleep(1)  # simulate sending email
    print(f"Email sent to {email.to}")
    return Status(ok=True)
