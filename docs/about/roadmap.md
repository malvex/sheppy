# Roadmap

Sheppy is under active development. Here is what's planned for future releases.

## Upcoming Features

- **Redis Cluster Support** - High availability and horizontal scaling with Redis Cluster
- **Task Chaining (DAG)** - Chain tasks together to create workflows (The concept is ready - now it needs polish and thorough testing)
- **Dead Letter Queue** - Automatic handling of permanently failed tasks. Tasks that exhaust retries move to a dead letter queue for investigation and/or manual intervention.
- **Task Timeouts** - Set maximum execution time per task. Tasks exceeding the timeout are marked as failed and retried according to their retry policy.

### Additional Planned Backends

- **PostgreSQL** - A reliable SQL-based backend for those who prefer relational databases
- **Kafka** - For high-throughput, distributed task processing
- **RabbitMQ** - For everyone who cannot bother with Kafka

!!! note
    PostgreSQL is surprisingly well-suited for task queues because of NOTIFY/LISTEN support, which allows blocking waits instead of polling. Other SQL databases unfortunately don't have this feature, so Postgres might be the only supported SQL backend in the near future.

## Ongoing Side Quests

- Improving documentation, test coverage, adding more examples ...
- Benchmarks & Reliability Tests - reproducible benchmarks and reliability testing. **Reliability is priority number one.** Performance matters, but correctness matters more

## Want to Contribute?

Have an idea for Sheppy? [Open an issue on GitHub](https://github.com/malvex/sheppy/issues) to discuss it. Some areas where help is especially welcome:

- Real-world use cases and pain points
- Performance bottlenecks you've encountered
- Missing features that would make Sheppy more useful
- Backend implementations for other datastores
