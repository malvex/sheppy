"""
Minimal entrypoint to run worker without requiring typer/rich installed.

Usage:
    python -m sheppy redis://localhost:6379
    python -m sheppy redis://localhost:6379 -q myqueue -c 5
    python -m sheppy --help
"""

import argparse
import asyncio
import logging
import os
import sys

from .queue import _create_backend_from_url
from .worker import Worker


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m sheppy",
        description="Start a Sheppy worker to process tasks from a queue.",
    )

    parser.add_argument(
        "backend_url",
        nargs="?",
        default="redis://127.0.0.1:6379",
        help="Backend URL (default: redis://127.0.0.1:6379)",
    )
    parser.add_argument(
        "-q", "--queue",
        action="append",
        dest="queues",
        default=None,
        help="Queue name to process (can be used multiple times, default: 'default')",
    )
    parser.add_argument(
        "-c", "--max-concurrent",
        type=int,
        default=10,
        help="Max concurrent tasks (default: 10)",
    )
    parser.add_argument(
        "--max-prefetch",
        type=int,
        default=None,
        help="Max prefetch tasks",
    )
    parser.add_argument(
        "--oneshot",
        action="store_true",
        help="Process pending tasks and exit",
    )
    parser.add_argument(
        "--max-tasks",
        type=int,
        default=None,
        help="Maximum number of tasks to process",
    )
    parser.add_argument(
        "--disable-job-processing",
        action="store_true",
        help="Disable job processing",
    )
    parser.add_argument(
        "--disable-scheduler",
        action="store_true",
        help="Disable scheduler",
    )
    parser.add_argument(
        "--disable-cron-manager",
        action="store_true",
        help="Disable cron manager",
    )
    parser.add_argument(
        "-l", "--log-level",
        choices=["debug", "info", "warning", "error"],
        default="info",
        help="Logging level (default: info)",
    )

    args = parser.parse_args()

    if args.disable_job_processing and args.disable_scheduler and args.disable_cron_manager:
        print("Error: At least one processing type must be enabled", file=sys.stderr)  # noqa: T201
        sys.exit(1)

    queues = args.queues or ["default"]

    # deduplicate queues
    seen = set()
    unique_queues = []
    for q in queues:
        if q in seen:
            print(f"Warning: Queue '{q}' provided multiple times.", file=sys.stderr)  # noqa: T201
            continue
        seen.add(q)
        unique_queues.append(q)

    # add cwd to path for importing tasks
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    backend = _create_backend_from_url(args.backend_url)

    # setup logging
    log_level = getattr(logging, args.log_level.upper())
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("sheppy.worker").setLevel(log_level)
    logging.getLogger("sheppy.middleware").setLevel(log_level)

    queue_str = ", ".join(unique_queues)
    print(f"Starting worker for queue(s): {queue_str}")  # noqa: T201
    print(f"  Backend: {args.backend_url}")  # noqa: T201
    print(f"  Max concurrent: {args.max_concurrent}")  # noqa: T201

    worker = Worker(
        queue_name=unique_queues,
        backend=backend,
        max_concurrent_tasks=args.max_concurrent,
        max_prefetch_tasks=args.max_prefetch,
        enable_job_processing=not args.disable_job_processing,
        enable_scheduler=not args.disable_scheduler,
        enable_cron_manager=not args.disable_cron_manager,
    )

    asyncio.run(worker.work(max_tasks=args.max_tasks, oneshot=args.oneshot))


if __name__ == "__main__":
    main()
