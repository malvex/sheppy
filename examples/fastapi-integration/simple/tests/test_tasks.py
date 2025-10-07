from sheppy import TestQueue
from tasks import Status, send_email_task


def test_send_email_task():
    q = TestQueue()

    email_data = {
        "to": "test@example.com",
        "subject": "Test Email",
        "body": "This is a test email."
    }

    t = send_email_task(email_data)
    q.add(t)

    processed_task = q.process_next()

    assert processed_task.completed is True
    assert processed_task.error is None
    assert processed_task.result == Status(ok=True)
