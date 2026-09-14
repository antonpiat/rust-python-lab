from __future__ import annotations

import asyncio

import pytest

from rust_python_lab import OnFull, QueueFull, RetryPolicy, Runtime
from tests.http_harness import Harness, http_get
from tests.test_limits_http import _wait_until, wait_handle


async def test_flaky_retries_until_success(http_harness: Harness):
    rt = Runtime(
        max_concurrency=4,
        retry=RetryPolicy(max_attempts=3, backoff_ms=10, jitter=False),
    )
    body = await rt.submit(http_get, http_harness.url("/flaky?fail_times=2"))
    assert body == "ok"
    assert http_harness.counters.flaky_hits == 3
    stats = rt.stats()
    assert stats["in_flight"] == 0
    assert stats["queued"] == 0
    assert stats["succeeded"] == 1
    assert stats["completed"] == 1


async def test_flaky_gives_up_without_retry(http_harness: Harness):
    rt = Runtime(max_concurrency=4)
    with pytest.raises(Exception):
        await rt.submit(http_get, http_harness.url("/flaky?fail_times=2"))
    assert http_harness.counters.flaky_hits == 1
    assert rt.stats()["failed"] == 1


async def test_queue_full_reject(http_harness: Harness):
    rt = Runtime(max_concurrency=1, queue_capacity=2, on_full=OnFull.REJECT)
    slow = http_harness.url("/slow?delay=2")
    first = rt.submit(http_get, slow)
    await _wait_until(lambda: http_harness.counters.slow_started >= 1)

    queued = [rt.submit(http_get, slow) for _ in range(2)]
    with pytest.raises(QueueFull):
        rt.submit(http_get, slow)

    stats = rt.stats()
    assert stats["in_flight"] == 1
    assert stats["queued"] == 2

    first.cancel()
    for handle in queued:
        handle.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    for handle in queued:
        with pytest.raises(asyncio.CancelledError):
            await wait_handle(handle)

    drained = rt.stats()
    assert drained["in_flight"] == 0
    assert drained["queued"] == 0
    assert drained["cancelled"] == 3


async def test_queue_wait_then_run(http_harness: Harness):
    rt = Runtime(max_concurrency=1, queue_capacity=0, on_full=OnFull.WAIT)
    first = rt.submit(http_get, http_harness.url("/work"))
    second = rt.submit(http_get, http_harness.url("/work"))
    results = await asyncio.gather(wait_handle(first), wait_handle(second))
    assert results == ["ok", "ok"]
    assert http_harness.counters.work_hits == 2
    assert http_harness.counters.peak_in_flight <= 1
    stats = rt.stats()
    assert stats["in_flight"] == 0
    assert stats["succeeded"] == 2
