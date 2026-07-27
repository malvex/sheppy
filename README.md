# Sheppy

[![PyPI version](https://img.shields.io/pypi/v/sheppy)](https://pypi.org/project/sheppy/)
[![CI](https://github.com/malvex/sheppy/actions/workflows/test.yml/badge.svg)](https://github.com/malvex/sheppy/actions/workflows/test.yml)
[![Python versions](https://img.shields.io/pypi/pyversions/sheppy)](https://pypi.org/project/sheppy/)

Sheppy is an async-native task queue for Python. Decorate a function with `@task`, add it to a `Queue`, and a worker process executes it. Arguments are validated with Pydantic, tasks are stored in Redis (and soon Postgres), and workers wait on blocking reads instead of polling.

## Install

```bash
pip install sheppy              # Python 3.10+
pip install "sheppy[fastapi]"   # with the FastAPI integration
```

## Example

```python
import asyncio

from sheppy import Queue, task


@task
async def send_email(to: str, subject: str) -> str:
    return f"sent '{subject}' to {to}"


async def main() -> None:
    queue = Queue("memory://")  # use "redis://localhost:6379" in production

    t = send_email("alice@example.com", "Welcome!")  # returns a Task model, nothing runs yet
    await queue.add(t)

    done = await queue.wait_for(t)
    print(done.result)


if __name__ == "__main__":
    asyncio.run(main())
```

```bash
$ python example.py
sent 'Welcome!' to alice@example.com
```

The `memory://` backend executes tasks in-process, so this runs with no external services. Against Redis the application code is identical, but a separate worker process executes the tasks:

```bash
docker run -d -p 6379:6379 redis:7   # task storage
python example.py                    # enqueues the task and waits for the result
sheppy work                          # run in a second terminal: picks up and executes it
```

Scale out by starting more workers: every `sheppy work` process adds capacity.

## Features

- Scheduling and cron: `queue.schedule(t, at=timedelta(minutes=30))` or an absolute datetime, `queue.add_cron(t, "0 9 * * *")` for recurring tasks
- Retries: `@task(retry=3, retry_delay=[1, 10, 60])` waits 1s, then 10s, then 60s between attempts. Timeouts and worker crashes can trigger retries too
- Rate limiting: `@task(rate_limit={"max_rate": 2, "rate_period": 5})` delays tasks past the limit (sliding or fixed window) instead of dropping them
- Pydantic validation: arguments are checked when you call the task function, before anything is queued. Results are validated against the return annotation
- `TestQueue` runs tasks synchronously in unit tests without a separate worker or redis
- FastAPI integration: `Depends` injection inside task functions and a monitoring ApiRouter

## Requirements

- Python 3.10+
- Redis 6.2+

## Links

- Documentation: <https://docs.sheppy.org>
- Changelog: <https://docs.sheppy.org/changelog/>
- Issues: <https://github.com/malvex/sheppy/issues>

## Development

```bash
git clone https://github.com/malvex/sheppy.git
cd sheppy
uv sync --dev

docker compose -f docker-compose.test.yml up -d
uv run pytest tests/
uv run mypy --strict src/sheppy/
uv run ruff check src/ tests/
```

## License

MIT. See [LICENSE](LICENSE).
