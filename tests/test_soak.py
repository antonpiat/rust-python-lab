from __future__ import annotations

import asyncio
import os

import httpx

from rust_python_lab import OnFull, Runtime, Task
from tests.http_harness import Harness


def _soak_n() -> int:
    return 20_000 if os.environ.get("SOAK") == "1" else 2_000


async def _get(client: httpx.AsyncClient, url: str) -> str:
    response = await client.get(url)
    response.raise_for_status()
    return response.text


async def test_soak_http_drains(http_harness: Harness):
    n = _soak_n()
    url = http_harness.url("/work?delay=0")
    before = {task for task in asyncio.all_tasks() if not task.done()}

    async with httpx.AsyncClient() as client:
        async with Runtime(
            max_concurrency=32,
            queue_capacity=64,
            on_full=OnFull.WAIT,
        ) as rt:
            results = await rt.gather([Task(_get, client, url) for _ in range(n)])
            stats = rt.stats()

    assert results == ["ok"] * n
    assert stats["in_flight"] == 0
    assert stats["queued"] == 0
    assert stats["completed"] == n
    assert stats["succeeded"] == n
    assert http_harness.counters.work_hits == n
    assert http_harness.counters.in_flight == 0
    assert http_harness.counters.peak_in_flight <= 32

    await asyncio.sleep(0.05)
    after = {task for task in asyncio.all_tasks() if not task.done()}
    leaked = after - before
    assert not leaked, f"leaked asyncio tasks: {leaked}"
