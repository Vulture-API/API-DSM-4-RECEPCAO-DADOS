#!/usr/bin/env python3
"""Publish a configurable MQTT load and print achieved throughput."""

from __future__ import annotations

import argparse
import asyncio
import json
import time

import aiomqtt


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--topic", default="estacoes/benchmark/dados")
    parser.add_argument("--messages", type=int, default=100_000)
    parser.add_argument("--workers", type=int, default=20)
    parser.add_argument("--username")
    parser.add_argument("--password")
    return parser.parse_args()


async def run(args: argparse.Namespace) -> None:
    next_index = 0
    lock = asyncio.Lock()

    async with aiomqtt.Client(
        hostname=args.host,
        port=args.port,
        username=args.username,
        password=args.password,
    ) as client:
        started = time.perf_counter()

        async def worker() -> None:
            nonlocal next_index
            while True:
                async with lock:
                    if next_index >= args.messages:
                        return
                    index = next_index
                    next_index += 1
                payload = json.dumps(
                    {
                        "estacao_id": f"benchmark-{index % 1000}",
                        "unix_time": int(time.time()),
                        "sequence": index,
                        "temperature": 20.5,
                    },
                    separators=(",", ":"),
                )
                await client.publish(args.topic, payload=payload, qos=1)

        await asyncio.gather(*(worker() for _ in range(args.workers)))
        elapsed = time.perf_counter() - started

    print(
        json.dumps(
            {
                "messages": args.messages,
                "elapsed_seconds": round(elapsed, 3),
                "messages_per_second": round(args.messages / elapsed, 2),
            }
        )
    )


if __name__ == "__main__":
    asyncio.run(run(parse_args()))

