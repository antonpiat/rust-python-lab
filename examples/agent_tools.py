"""Agent-shaped fan-out against the local HTTP harness.

Several tools are ordinary HTTP calls; the "LLM" is an SSE token stream.
Run from the repo root after `maturin develop --extras dev`:

    python examples/agent_tools.py
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

from aiohttp.test_utils import TestServer

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rust_python_lab import RetryPolicy, Runtime, Task  # noqa: E402
from tests.http_harness import Counters, create_app, http_get, sse_tokens  # noqa: E402


async def search_docs(base: str, query: str) -> str:
    _ = query
    return await http_get(f"{base}/work?delay=0.05")


async def fetch_user(base: str, user_id: str) -> str:
    _ = user_id
    return await http_get(f"{base}/work?delay=0.05")


async def flaky_write(base: str, note: str) -> str:
    _ = note
    return await http_get(f"{base}/flaky?fail_times=2")


async def main() -> None:
    counters = Counters()
    async with TestServer(create_app(counters)) as server:
        base = str(server.make_url("")).rstrip("/")
        retry = RetryPolicy(max_attempts=3, backoff_ms=20, jitter=False)
        async with Runtime(max_concurrency=8, retry=retry) as rt:
            tool_results = await rt.gather(
                [
                    Task(search_docs, base, "runtime limits"),
                    Task(fetch_user, base, "u-1"),
                    Task(flaky_write, base, "remember the last tool result"),
                ]
            )
            tokens = [token async for token in rt.stream(sse_tokens, f"{base}/sse")]
            stats = rt.stats()

    print("tools:", tool_results)
    print("llm tokens:", " ".join(tokens))
    print("stats:", stats)
    print("server peak in-flight:", counters.peak_in_flight)
    assert tool_results == ["ok", "ok", "ok"]
    assert tokens == [f"token-{i}" for i in range(8)]
    assert stats["in_flight"] == 0
    assert counters.flaky_hits == 3


if __name__ == "__main__":
    asyncio.run(main())
