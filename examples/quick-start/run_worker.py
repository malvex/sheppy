import asyncio
import logging
from sheppy import Worker
from main import backend

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


async def run_worker():
    worker = Worker(queue_name="email-queue", backend=backend)
    await worker.work()


if __name__ == "__main__":
    asyncio.run(run_worker())
