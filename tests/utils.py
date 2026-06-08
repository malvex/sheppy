import asyncio
import multiprocessing
from contextlib import asynccontextmanager

from sheppy import Worker
from sheppy.queue import _create_backend_from_url


def _start_process_worker(queue: str | list[str], backend_string: str, shutdown_timeout: float | None = None):
    worker = Worker(queue, _create_backend_from_url(backend_string))
    worker.heartbeat_timeout = 0.2
    worker.heartbeat_interval = 0.1
    worker.reclaim_interval = 0.1
    worker._blocking_timeout = 0.001
    worker._scheduler_polling_interval = 0.001
    worker._cron_polling_interval = 0.001

    if shutdown_timeout is not None:
        worker.shutdown_timeout = shutdown_timeout

    return asyncio.run(worker.work())


class WorkerInstanceProcess:
    def __init__(self, queue: str | list[str], backend: str, shutdown_timeout: float | None = None,):
        self._queue = queue
        self._backend = backend
        self._shutdown_timeout = shutdown_timeout
        self._process: multiprocessing.Process | None = None

    @property
    def is_alive(self):
        return self._process.is_alive() if self._process else False

    @property
    def exitcode(self) -> int | None:
        return self._process.exitcode if self._process else None

    @property
    def pid(self) -> int | None:
        return self._process.pid if self._process else None

    def start(self):
        self._process = multiprocessing.Process(target=_start_process_worker, args=(self._queue, self._backend, self._shutdown_timeout), daemon=True)
        self._process.start()

    def kill(self):
        self._process.kill()
        self._process.join(timeout=5)
        if self._process.is_alive():
            raise RuntimeError(f"Failed to kill worker process {self._process.pid}")

    def graceful_shutdown(self):
        # hack to make this work for python 3.13 and lower
        # I should just check python version but I can't remember how to do it
        # and it doesn't really matter as sigint is handled exactly the same as sigterm
        if hasattr(self._process, "interrupt"):
            self._process.interrupt()
        else:
            self._process.terminate()

    def terminate(self):
        self._process.terminate()

    @asynccontextmanager
    async def run(self):
        self.start()
        try:
            yield self
        finally:
            self.kill()
