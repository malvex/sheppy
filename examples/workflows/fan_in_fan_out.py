import asyncio

from sheppy import Queue, RedisBackend, task
from sheppy._workflow import workflow

ADMIN_EMAILS = [
    "admin1@example.com",
    "admin2@example.com",
    "admin3@example.com",
]

@task
async def cleanup_old_data(days: int = 7):
    if days > 7:  # deterministic "random" failure
        raise Exception("some random failure happened")

    return "everything ok!"

@task
async def some_cleanup_at_the_end():
    return True

@task
async def rollback_changes():
    return True

@task
async def send_notification(to: str, subject: str):
    print(f"Sending email to {to}, subject {subject}")


@workflow
def daily_cleanup(days_to_clean: int):
    result_task = yield cleanup_old_data(days=days_to_clean)

    if result_task.error:
        yield rollback_changes()
        yield [send_notification(email, "Oh no, daily cleanup failed!") for email in ADMIN_EMAILS]

        raise Exception("Cleanup failed, notifications were sent")  # fail the workflow

    if result_task.status == 'completed':
        yield some_cleanup_at_the_end()
        yield send_notification("devteam@example.com", "Cleanup finished")

        return "Daily cleanup finished successfully"

    raise Exception("not sure what happened!")


async def main():
    queue = Queue(RedisBackend())
    # await queue.add_workflow(daily_cleanup(7))
    await queue.add_workflow(daily_cleanup(30))


if __name__ == "__main__":
    asyncio.run(main())
