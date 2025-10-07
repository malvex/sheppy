# About Sheppy

## Why Another Task Queue?

Over the last 8 years, I have tried many Python task queue libraries. They were non-intuitive and had confusing documentation, or only showed their scaling limits after I got them into production. Some locked me into Redis with no way out without complete rewrite. When async Python became the norm, I hit another wall - most libraries either retrofitted async support awkwardly or didn't support it at all.

So I decided to build Sheppy to fix these issues. It's designed for modern async Python from the ground up, uses type hints everywhere, integrates with amazing Pydantic library, and rethinks how background tasks should work.

The design is inspired by FastAPI and uses similar coding patterns. The goal is to be "simple, yet powerful". Sheppy is designed to be have minimal API interface with just a few simple concepts to learn, while implementing industry best practices. No complex abstractions, no unnecessary wrappers. Just functions (tasks) and queues.

## Project Status

Sheppy is brand new and under active development. The core features are stable and ready for use, but the library is still evolving based on real-world needs and feedback.

## Contributing

Sheppy is open source and welcomes contributions. Found a bug? Have a feature idea? [Open an issue on GitHub](https://github.com/malvex/sheppy/issues).

## Support

- **Documentation**: You're reading it
- **Issues**: Report bugs and request features [here](https://github.com/malvex/sheppy/issues)
- **Source Code**: Available on [GitHub](https://github.com/malvex/sheppy) under the MIT license
