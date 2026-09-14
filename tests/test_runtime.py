import asyncio

import pytest

from rust_python_lab import Runtime


async def add(a, b):
    await asyncio.sleep(0)
    return a + b


async def boom():
    raise ValueError("nope")


async def test_submit_sleep():
    rt = Runtime()
    result = await rt.submit(asyncio.sleep, 0)
    assert result is None


async def test_submit_returns_value():
    rt = Runtime()
    assert await rt.submit(add, 1, 2) == 3


async def test_submit_propagates_error():
    rt = Runtime()
    with pytest.raises(ValueError, match="nope"):
        await rt.submit(boom)


async def test_handle_status_succeeded():
    rt = Runtime()
    handle = rt.submit(asyncio.sleep, 0)
    assert handle.status() in {"queued", "running"}
    await handle
    assert handle.status() == "succeeded"
    assert handle.done()


async def test_handle_status_failed():
    rt = Runtime()
    handle = rt.submit(boom)
    with pytest.raises(ValueError, match="nope"):
        await handle
    assert handle.status() == "failed"


async def test_concurrent_submits():
    rt = Runtime()
    h1 = rt.submit(asyncio.sleep, 0)
    h2 = rt.submit(add, 2, 3)

    async def wait(handle):
        return await handle

    none, total = await asyncio.gather(wait(h1), wait(h2))
    assert none is None
    assert total == 5


def test_submit_requires_running_loop():
    rt = Runtime()
    with pytest.raises(RuntimeError):
        rt.submit(asyncio.sleep, 0)
