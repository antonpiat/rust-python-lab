from __future__ import annotations

import asyncio
import time

import pytest

from rust_python_lab import Runtime
from tests.http_harness import Harness, http_get


async def _wait_until(predicate, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.02)
    raise AssertionError("timed out waiting for condition")


async def wait_handle(handle):
    return await handle


async def test_http_concurrency_cap(http_harness: Harness):
    rt = Runtime(max_concurrency=8)
    urls = [http_harness.url("/work") for _ in range(40)]
    handles = [rt.submit(http_get, url) for url in urls]
    results = await asyncio.gather(*[wait_handle(h) for h in handles])
    assert results == ["ok"] * 40
    assert http_harness.counters.work_hits == 40
    assert http_harness.counters.peak_in_flight <= 8
    assert http_harness.counters.in_flight == 0


async def test_http_timeout_cancels_handler(http_harness: Harness):
    rt = Runtime(max_concurrency=8)
    handle = rt.submit(http_get, http_harness.url("/slow?delay=5"), timeout=0.2)
    with pytest.raises(TimeoutError):
        await handle
    assert handle.status() == "timed_out"
    await _wait_until(lambda: http_harness.counters.slow_exited >= 1)
    assert http_harness.counters.slow_completed == 0
    assert http_harness.counters.in_flight == 0


async def test_http_cancel_aborts_handler(http_harness: Harness):
    rt = Runtime(max_concurrency=8)
    handle = rt.submit(http_get, http_harness.url("/slow?delay=5"))
    await _wait_until(lambda: http_harness.counters.slow_started >= 1)
    handle.cancel()
    with pytest.raises(asyncio.CancelledError):
        await handle
    assert handle.status() == "cancelled"
    await _wait_until(lambda: http_harness.counters.slow_exited >= 1)
    assert http_harness.counters.slow_completed == 0
    assert http_harness.counters.in_flight == 0


async def test_default_timeout(http_harness: Harness):
    rt = Runtime(max_concurrency=4, default_timeout=0.2)
    with pytest.raises(TimeoutError):
        await rt.submit(http_get, http_harness.url("/slow?delay=5"))
