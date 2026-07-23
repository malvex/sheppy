import asyncio

from sheppy import MemoryBackend, Queue, Worker, task, workflow

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

    yield some_cleanup_at_the_end()
    yield send_notification("devteam@example.com", "Cleanup finished")

    return "Daily cleanup finished successfully"


queue = Queue(MemoryBackend(instant_processing=False))


async def main():
    # start worker in background
    worker = Worker("default", backend=queue.backend)
    worker_process = asyncio.create_task(worker.work())

    result = await queue.add_workflow(daily_cleanup(30))

    # wait for the workflow to finish (successfully or not)
    wf = await queue.wait_for_workflow(result.workflow.id)

    # stop worker
    worker_process.cancel()

    if wf is None:
        print("Workflow not found")
    elif wf.error:
        print(f"Workflow failed as expected: {wf.error}")
    else:
        print(f"Workflow completed: {wf.final_result}")


if __name__ == "__main__":
    asyncio.run(main())
