from __future__ import annotations

import asyncio

import pytest

from rust_python_lab import Runtime, Task
from tests.http_harness import Harness, http_get, sse_tokens


async def test_gather_mixed_http(http_harness: Harness):
    rt = Runtime(max_concurrency=8)
    results = await rt.gather(
        [
            Task(http_get, http_harness.url("/work?delay=0.05")),
            Task(http_get, http_harness.url("/flaky?fail_times=1")),
            Task(http_get, http_harness.url("/work?delay=0.02")),
        ],
        return_exceptions=True,
    )
    assert results[0] == "ok"
    assert isinstance(results[1], Exception)
    assert results[2] == "ok"


async def test_as_completed_out_of_order(http_harness: Harness):
    rt = Runtime(max_concurrency=8)
    order = []
    async for event in rt.as_completed(
        [
            Task(http_get, http_harness.url("/work?delay=0.2")),
            Task(http_get, http_harness.url("/work?delay=0.02")),
        ]
    ):
        order.append(event.index)
        assert event.ok
        assert event.value == "ok"
    assert order[0] == 1
    assert sorted(order) == [0, 1]


async def test_stream_sse_tokens(http_harness: Harness):
    rt = Runtime(max_concurrency=4)
    tokens = []
    async for token in rt.stream(sse_tokens, http_harness.url("/sse")):
        tokens.append(token)
    assert tokens == [f"token-{i}" for i in range(8)]
    assert http_harness.counters.sse_sent == 8


async def test_stream_backpressure():
    produced = []

    async def gen():
        for i in range(10):
            produced.append(i)
            yield i

    rt = Runtime()
    stream = rt.stream(gen, buffer=1)
    first = await stream.__anext__()
    assert first == 0
    await asyncio.sleep(0.05)
    assert produced[-1] <= 2
    rest = [item async for item in stream]
    assert [first, *rest] == list(range(10))


async def test_gather_raises_without_return_exceptions(http_harness: Harness):
    rt = Runtime(max_concurrency=4)
    with pytest.raises(Exception):
        await rt.gather(
            [
                Task(http_get, http_harness.url("/flaky?fail_times=9")),
                Task(http_get, http_harness.url("/work?delay=0.02")),
            ]
        )
