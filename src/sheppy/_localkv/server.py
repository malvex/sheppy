"""
This file contains utility functions meant for internal use only. Expect breaking changes if you use them directly.
"""

import asyncio
import bisect
import json
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass(order=True)
class SortedItem:
    position: float
    value: str = field(compare=False)


@dataclass
class Store:
    kv: dict[str, str] = field(default_factory=dict)
    lists: defaultdict[str, list[str]] = field(default_factory=lambda: defaultdict(list))
    sorted_list: defaultdict[str, list[SortedItem]] = field(default_factory=lambda: defaultdict(list))


store = Store()


def handle_command(cmd: str, args: dict[str, Any]) -> dict[str, Any]:
    match cmd:
        # general
        case "ping":
            return {"ok": True}

        case "clear":
            prefix = args.get("prefix", "")
            if prefix:
                for container in (store.kv, store.lists, store.sorted_list):
                    to_delete = [k for k in container if k.startswith(prefix)]
                    for k in to_delete:
                        del container[k]
            else:
                store.kv.clear()
                store.lists.clear()
                store.sorted_list.clear()
            return {"ok": True}

        # key-value
        case "get":
            values = {k: store.kv.get(k) for k in args["keys"]}
            return {"ok": True, "values": values}

        case "set":
            for k, v in args["items"].items():
                store.kv[k] = v
            return {"ok": True}

        case "create":
            key = args["key"]
            if key in store.kv:
                return {"ok": True, "created": False}
            store.kv[key] = args["value"]
            return {"ok": True, "created": True}

        case "delete":
            count = 0
            for k in args["keys"]:
                if store.kv.pop(k, None) is not None:
                    count += 1
                if k in store.lists:
                    del store.lists[k]
                    count += 1
                if k in store.sorted_list:
                    del store.sorted_list[k]
                    count += 1
            return {"ok": True, "count": count}

        case "keys":
            prefix = args.get("prefix", "")
            keys = [k for k in store.kv if k.startswith(prefix)]
            return {"ok": True, "keys": keys}

        case "len":
            prefix = args.get("prefix", "")
            count = sum(1 for k in store.kv if k.startswith(prefix))
            return {"ok": True, "count": count}

        # list (queue)
        case "list_push":
            store.lists[args["key"]].append(args["value"])
            return {"ok": True}

        case "list_pop":
            lst = store.lists[args["key"]]
            value = lst.pop(0) if lst else None
            return {"ok": True, "value": value}

        case "list_get":
            lst = store.lists[args["key"]]
            _count = args.get("count")
            _values = list(lst) if _count is None else lst[:_count]
            return {"ok": True, "values": _values}

        case "list_len":
            return {"ok": True, "count": len(store.lists[args["key"]])}

        case "list_remove":
            lst = store.lists[args["key"]]
            value = args["value"]
            if value in lst:
                lst.remove(value)
                return {"ok": True, "removed": True}
            return {"ok": True, "removed": False}

        # sorted list
        case "sorted_push":
            bisect.insort(store.sorted_list[args["key"]], SortedItem(args["position"], args["value"]))
            return {"ok": True}

        case "sorted_pop":
            ss = store.sorted_list[args["key"]]
            max_pos = args["max_position"]
            popped = []
            while ss and ss[0].position <= max_pos:
                item = ss.pop(0)
                popped.append((item.position, item.value))
            return {"ok": True, "items": popped}

        case "sorted_get":
            items = [(item.position, item.value) for item in store.sorted_list[args["key"]]]
            return {"ok": True, "items": items}

        case "sorted_len":
            return {"ok": True, "count": len(store.sorted_list[args["key"]])}

        case _:
            return {"ok": False, "error": f"unknown command: {cmd}"}


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    addr = writer.get_extra_info("peername")
    logger.debug(f"Connected: {addr}")

    try:
        while True:
            line = await reader.readline()
            if not line:
                break

            try:
                request = json.loads(line.decode())
                response = handle_command(request.get("cmd", ""), request.get("args", {}))
            except json.JSONDecodeError as e:
                response = {"ok": False, "error": f"invalid JSON: {e}"}
            except Exception as e:
                response = {"ok": False, "error": str(e)}

            writer.write(json.dumps(response).encode() + b"\n")
            await writer.drain()

    except Exception as e:
        logger.exception(f"Error: {e}")
    finally:
        logger.debug(f"Disconnected: {addr}")
        writer.close()
        await writer.wait_closed()


async def start_server(host: str = "127.0.0.1", port: int = 17420) -> None:
    server = await asyncio.start_server(handle_client, host, port)
    addr = server.sockets[0].getsockname()
    logger.info(f"Server listening on {addr[0]}:{addr[1]}")

    async with server:
        await server.serve_forever()
