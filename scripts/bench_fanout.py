"""Runtime.gather vs asyncio.gather + Semaphore on local HTTP.

Not a CI gate. Measures control-plane cost on real sockets, not model latency.

    python scripts/bench_fanout.py
    python scripts/bench_fanout.py --n 400 --concurrency 16 --delay 0.02
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from pathlib import Path

from aiohttp.test_utils import TestServer

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rust_python_lab import OnFull, Runtime, Task  # noqa: E402
from tests.http_harness import Counters, create_app  # noqa: E402
import httpx  # noqa: E402


async def _get(client: httpx.AsyncClient, url: str) -> str:
    response = await client.get(url)
    response.raise_for_status()
    return response.text


async def _bench_runtime(
    url: str, n: int, concurrency: int
) -> tuple[float, int, int]:
    async with httpx.AsyncClient() as client:
        async with Runtime(
            max_concurrency=concurrency,
            queue_capacity=max(concurrency, n),
            on_full=OnFull.WAIT,
        ) as rt:
            started = time.perf_counter()
            results = await rt.gather([Task(_get, client, url) for _ in range(n)])
            elapsed = time.perf_counter() - started
            errors = sum(1 for item in results if item != "ok")
            stats = rt.stats()
    return elapsed, errors, stats["completed"]


async def _bench_asyncio(
    url: str, n: int, concurrency: int
) -> tuple[float, int]:
    sem = asyncio.Semaphore(concurrency)
    async with httpx.AsyncClient() as client:

        async def one() -> str:
            async with sem:
                return await _get(client, url)

        started = time.perf_counter()
        results = await asyncio.gather(*[one() for _ in range(n)])
        elapsed = time.perf_counter() - started
    errors = sum(1 for item in results if item != "ok")
    return elapsed, errors


async def _run(n: int, concurrency: int, delay: float) -> None:
    counters = Counters()
    async with TestServer(create_app(counters)) as server:
        url = f"{str(server.make_url('')).rstrip('/')}/work?delay={delay}"
        print(f"n={n} concurrency={concurrency} delay={delay}")

        elapsed, errors, completed = await _bench_runtime(url, n, concurrency)
        print(
            f"runtime gather: {elapsed:.3f}s  completed={completed}  "
            f"peak_in_flight={counters.peak_in_flight}  errors={errors}"
        )

    counters = Counters()
    async with TestServer(create_app(counters)) as server:
        url = f"{str(server.make_url('')).rstrip('/')}/work?delay={delay}"
        elapsed, errors = await _bench_asyncio(url, n, concurrency)
        print(
            f"asyncio gather+sem: {elapsed:.3f}s  "
            f"peak_in_flight={counters.peak_in_flight}  errors={errors}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--n", type=int, default=200)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--delay", type=float, default=0.02)
    args = parser.parse_args()
    asyncio.run(_run(args.n, args.concurrency, args.delay))


if __name__ == "__main__":
    main()
