# Rate limiting

Set `rate_limit` on a task to cap how often it runs. Over-limit tasks are automatically delayed by rescheduling them for the next available slot by the worker.

## Define a limit

```python
@task(rate_limit={"max_rate": 2, "rate_period": 5})
async def do_work(queued_time: datetime) -> int:
    ...
```

At most 2 instances of `do_work` may start within any 5-second window; the rest wait.

The `rate_limit` dict has four keys:

- `max_rate` (int, required): number of runs allowed per period.
- `rate_period` (float, required): length of the period in seconds.
- `key` (str, optional): the bucket the limit counts against. Defaults to the task's function name; set an explicit key to share one limit across different tasks (for example, per API endpoint or per customer).
- `strategy` (optional): `"sliding_window"` (default) or `"fixed_window"`. Sliding counts runs within the last `rate_period` seconds; fixed counts within wall-clock intervals.

## Example

`examples/rate_limits.py` queues three tasks at once under a 2-per-5-seconds limit:

```python title="examples/rate_limits.py"
--8<-- "examples/rate_limits.py"
```

Each task returns how many seconds it waited. Two run immediately, the third is held back five seconds:

```bash
$ python examples/rate_limits.py   # needs Redis and a worker (sheppy work)
t1.result: 0
t2.result: 0
t3.result: 5
```

The order is not guaranteed. Any of the three tasks can be the one that waits.

## Notes

- The limit is enforced when a worker pops the task, not when you add it. `queue.add()` always succeeds, and the delay shows up as a later execution time.
- Limits are per queue and per `key`, shared by all workers using the same backend.
