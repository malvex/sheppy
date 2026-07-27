# Task scheduling

`queue.schedule()` stores a task until its time comes; the scheduler inside every running worker then moves it into the pending queue for execution.

## Schedule with a delay

Pass a `timedelta` to run a task relative to now:

```python
from datetime import timedelta

await queue.schedule(send_email(email), at=timedelta(minutes=30))
```

## Schedule at an absolute time

Pass a timezone-aware `datetime`:

```python
from datetime import datetime, timezone

await queue.schedule(send_email(email), at=datetime(2027, 1, 1, tzinfo=timezone.utc))
```

!!! note
    Datetime must have timezone included. A naive `datetime` (one without `tzinfo`) raises `TypeError`.

## What happens next

- The task is stored with status `scheduled`. `queue.get_scheduled()` lists all scheduled tasks.
- Every running worker checks for due tasks once per second and moves them into the pending queue. Expect execution up to about a second after the scheduled time. This is not a real-time scheduler.
- If no worker is running, the task waits in Redis until one starts.

## Full example

`examples/simple_scheduled.py` schedules a follow-up email two seconds after a welcome email, using Redis backend (don't forget to start the worker with `sheppy work` in a separate terminal):

```python title="examples/simple_scheduled.py"
--8<-- "examples/simple_scheduled.py"
```

Output should look something like this:

```bash
$ python examples/simple_scheduled.py
[2026-07-19 14:56:01.127391] Sent email to user1@example.com, Subject: Registration Successful!, Body: Your account has been created!
sleeping for 3 seconds...
[2026-07-19 14:56:03.139961] Sent email to user1@example.com, Subject: Feedback survey, Body: How do you like our new website?
```

## Schedule from the CLI

```bash
sheppy task schedule myapp.tasks:send_email --delay 30m --kwargs '{"to": "alice@example.com"}'
sheppy task schedule myapp.tasks:send_email --at 2027-01-01T09:00:00
```

`--delay` accepts values like `30s`, `5m`, `2h`, or `1d`; `--at` takes an ISO 8601 datetime, and naive values are treated as UTC. The two options are mutually exclusive.

## Related

- Retry a failed task at a later time with `queue.retry(task, at=timedelta(hours=1))` (see [Error handling](error-handling.md)).
- Recurring tasks: [Cron jobs](cron.md).
