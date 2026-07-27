# Roadmap

Sheppy is under active development. Here is what's planned for future releases.

## Upcoming Features

- **Observability**: metrics, logs, traces
- **Task Concurrency**: limit the number of tasks that can run concurrently
- **Improved CLI**: more available commands, better interface, etc
- **PostgreSQL backend**: new Postgres backend for those who prefer relational databases or already run one
- **Custom backend support**: a stable python interface (protocol) that allows plugging-in custom-made backends
- ... and more!

!!! note
    PostgreSQL is surprisingly well-suited for task queues because of NOTIFY/LISTEN support, which allows blocking waits instead of polling. Other SQL databases unfortunately don't have this feature, so Postgres might be the only supported SQL backend in the near future.


## Want to Contribute?

Have an idea for Sheppy? [Open an issue on GitHub](https://github.com/malvex/sheppy/issues) to discuss it. Some areas where help is especially welcome:

- Real-world use cases and pain points
- Performance bottlenecks you've encountered
- Missing features that would make Sheppy more useful
- Backend implementations for other datastores
