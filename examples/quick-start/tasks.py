from sheppy import task


@task
async def send_email(recipient: str) -> str:
    print(f"Sending email to {recipient}")
    return "sent"
