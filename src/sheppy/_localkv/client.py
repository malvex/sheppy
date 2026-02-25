"""
This file contains utility functions meant for internal use only. Expect breaking changes if you use them directly.
"""

import asyncio
import json
from typing import Any


class KVClient:
    def __init__(self, host: str = "127.0.0.1", port: int = 17420) -> None:
        self.host = host
        self.port = port
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        self._reader, self._writer = await asyncio.open_connection(self.host, self.port)

    async def close(self) -> None:
        if self._writer:
            self._writer.close()
            await self._writer.wait_closed()
            self._writer = None
            self._reader = None

    @property
    def is_connected(self) -> bool:
        return self._writer is not None

    async def _call(self, cmd: str, **args: Any) -> dict[str, Any]:
        async with self._lock:
            if not self._writer or not self._reader:
                raise RuntimeError("Not connected")

            request = {"cmd": cmd, "args": args}
            self._writer.write(json.dumps(request).encode() + b"\n")  # jsonl
            await self._writer.drain()

            # shield the read from cancellation - once we've sent a request,
            # we must consume the response to keep the connection in sync
            line = await asyncio.shield(self._reader.readline())

            response: dict[str, Any] = json.loads(line.decode())

            if not response.get("ok"):
                raise RuntimeError(response.get("error", "Unknown error"))

            return response

    # general
    async def ping(self) -> None:
        await self._call("ping")

    async def clear(self, prefix: str | None = None) -> None:
        if prefix:
            await self._call("clear", prefix=prefix)
        else:
            await self._call("clear")

    # key-value
    async def get(self, keys: list[str]) -> dict[str, str | None]:
        r = await self._call("get", keys=keys)
        return r["values"]  # type:ignore[no-any-return]

    async def set(self, items: dict[str, str]) -> None:
        await self._call("set", items=items)

    async def create(self, key: str, value: str) -> bool:
        r = await self._call("create", key=key, value=value)
        return r["created"]  # type:ignore[no-any-return]

    async def delete(self, keys: list[str]) -> int:
        r = await self._call("delete", keys=keys)
        return r["count"]  # type:ignore[no-any-return]

    async def keys(self, prefix: str = "") -> list[str]:
        r = await self._call("keys", prefix=prefix)
        return r["keys"]  # type:ignore[no-any-return]

    async def len(self, prefix: str = "") -> int:
        r = await self._call("len", prefix=prefix)
        return r["count"]  # type:ignore[no-any-return]

    # list (queue)
    async def list_push(self, key: str, value: str) -> None:
        await self._call("list_push", key=key, value=value)

    async def list_pop(self, key: str) -> str | None:
        r = await self._call("list_pop", key=key)
        return r.get("value")

    async def list_get(self, key: str, count: int | None = None) -> list[str]:
        if count is not None:
            r = await self._call("list_get", key=key, count=count)
        else:
            r = await self._call("list_get", key=key)
        return r["values"]  # type:ignore[no-any-return]

    async def list_len(self, key: str) -> int:
        r = await self._call("list_len", key=key)
        return r["count"]  # type:ignore[no-any-return]

    async def list_remove(self, key: str, value: str) -> bool:
        r = await self._call("list_remove", key=key, value=value)
        return r["removed"]  # type:ignore[no-any-return]

    # sorted list
    async def sorted_push(self, key: str, position: float, value: str) -> None:
        await self._call("sorted_push", key=key, position=position, value=value)

    async def sorted_pop(self, key: str, max_position: float) -> list[tuple[float, str]]:
        r = await self._call("sorted_pop", key=key, max_position=max_position)
        return [(item[0], item[1]) for item in r["items"]]

    async def sorted_get(self, key: str) -> list[tuple[float, str]]:
        r = await self._call("sorted_get", key=key)
        return [(item[0], item[1]) for item in r["items"]]

    async def sorted_len(self, key: str) -> int:
        r = await self._call("sorted_len", key=key)
        return r["count"]  # type:ignore[no-any-return]

    # context manager
    async def __aenter__(self) -> "KVClient":
        await self.connect()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()
