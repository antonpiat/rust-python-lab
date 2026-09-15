from __future__ import annotations

import asyncio
import time

import pytest

from rust_python_lab import OnFull, Runtime, RuntimeClosed
from tests.http_harness import Harness, http_get


async def _wait_until(predicate, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.02)
    raise AssertionError("timed out waiting for condition")


async def test_submit_after_shutdown_raises():
    rt = Runtime()
    await rt.shutdown()
    assert rt.closed
    with pytest.raises(RuntimeClosed):
        rt.submit(asyncio.sleep, 0)
    with pytest.raises(RuntimeClosed):
        rt.gather([])
    with pytest.raises(RuntimeClosed):
        rt.stream(asyncio.sleep, 0)


async def test_shutdown_cancels_inflight_http(http_harness: Harness):
    rt = Runtime(max_concurrency=8)
    handle = rt.submit(http_get, http_harness.url("/slow?delay=5"))
    await _wait_until(lambda: http_harness.counters.slow_started >= 1)
    await rt.shutdown(timeout=0.2)
    with pytest.raises(asyncio.CancelledError):
        await handle
    assert handle.status() == "cancelled"
    await _wait_until(lambda: http_harness.counters.slow_exited >= 1)
    assert http_harness.counters.slow_completed == 0
    assert http_harness.counters.in_flight == 0
    with pytest.raises(RuntimeClosed):
        rt.submit(http_get, http_harness.url("/work"))


async def test_shutdown_waits_when_timeout_allows(http_harness: Harness):
    rt = Runtime(max_concurrency=4)
    handle = rt.submit(http_get, http_harness.url("/work?delay=0.15"))
    await rt.shutdown(timeout=2.0)
    assert await handle == "ok"
    assert handle.status() == "succeeded"


async def test_async_with_runtime(http_harness: Harness):
    async with Runtime(max_concurrency=4) as rt:
        result = await rt.submit(http_get, http_harness.url("/work"))
        assert result == "ok"
    assert rt.closed
    with pytest.raises(RuntimeClosed):
        rt.submit(http_get, http_harness.url("/work"))


async def test_idle_ttl_closes_admission():
    rt = Runtime(idle_ttl=0.15)
    await rt.submit(asyncio.sleep, 0)
    await asyncio.sleep(0.35)
    assert rt.closed
    with pytest.raises(RuntimeClosed):
        rt.submit(asyncio.sleep, 0)


async def test_idle_ttl_stays_open_while_busy():
    rt = Runtime(idle_ttl=0.2)
    handle = rt.submit(asyncio.sleep, 0.4)
    await asyncio.sleep(0.25)
    assert not rt.closed
    await handle
    await asyncio.sleep(0.35)
    assert rt.closed


async def test_wait_admission_aborts_on_shutdown():
    rt = Runtime(max_concurrency=1, queue_capacity=0, on_full=OnFull.WAIT)
    first = rt.submit(asyncio.sleep, 5)
    second = rt.submit(asyncio.sleep, 5)
    await rt.shutdown(timeout=0)
    with pytest.raises(asyncio.CancelledError):
        await first
    with pytest.raises(asyncio.CancelledError):
        await second
    with pytest.raises(RuntimeClosed):
        rt.submit(asyncio.sleep, 0)


async def test_shutdown_is_idempotent():
    rt = Runtime()
    await rt.submit(asyncio.sleep, 0)
    await rt.shutdown()
    await rt.shutdown(timeout=0)
    assert rt.closed
