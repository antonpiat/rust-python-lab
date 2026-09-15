# rust-python-lab

A Tokio-backed **execution controller** for Python AI/agent apps. Python stays the authoring surface (`await handle`, `async with`, `async for`). Rust owns admission, concurrency, timeouts, cancellation, retries, streaming, and shutdown. Python's asyncio event loop still executes coroutine bodies — LLM SDKs, HTTP clients, DB drivers, and tools.

This is not a LangChain/LangGraph rewrite and not a Rust HTTP/LLM client. Rust waits on bridged asyncio futures; it does not step Python bytecode.

## Execution split

| Layer | Owns |
| --- | --- |
| **Rust** | queues, permits, when a task may start, timeout, cancel, retry/backoff, gather/stream aggregation, graceful shutdown, in-memory journal |
| **Python asyncio** | coroutine bodies, sockets, SDK calls, object lifetimes, GIL |
| **Bridge** | `pyo3-async-runtimes`: capture `TaskLocals`, convert awaitables both ways, never hold the GIL across `.await` |

Cancel and timeout must cancel the **asyncio Task**, not only drop a Tokio waiter. Retries re-invoke the Python callable to get a new coroutine.

## Setup

Requires Rust (stable) and Python 3.12. Local development uses 3.12.3.

```bash
python -m venv .venv
source .venv/bin/activate
pip install maturin
maturin develop --extras dev
```

## API

```python
from rust_python_lab import OnFull, RetryPolicy, Runtime, Task

async with Runtime(
    max_concurrency=32,
    queue_capacity=128,
    on_full=OnFull.REJECT,  # or OnFull.WAIT
    default_timeout=30.0,
    retry=RetryPolicy(max_attempts=3, backoff_ms=100),
    idle_ttl=None,
) as rt:
    handle = rt.submit(call_llm, "hello", timeout=10.0)
    result = await handle
    handle.cancel()

    results = await rt.gather(
        [Task(call_llm, prompt) for prompt in prompts],
        return_exceptions=True,
    )
    async for event in rt.as_completed(tasks):
        ...
    async for token in rt.stream(llm_tokens, "hello"):
        ...

    print(rt.stats())
    await rt.shutdown(timeout=5.0)
```

After close, `submit` raises `RuntimeClosed`. A full queue with `OnFull.REJECT` raises `QueueFull`.

## Test

CI and local default: **localhost HTTP only**, no API keys, no public internet.

```bash
pytest
```

The suite drives a local `aiohttp` server (`tests/http_harness.py`) through **httpx** so policy is proven against real sockets:

- concurrency cap, timeout, and cancel against `/work` and `/slow`
- retries against `/flaky`
- `gather` / `as_completed` / SSE `stream` against mixed latencies and `/sse`
- shutdown cancels in-flight `/slow` and rejects later submits

Short soak (2k tiny GETs; `SOAK=1` runs 20k):

```bash
pytest tests/test_soak.py
SOAK=1 pytest tests/test_soak.py
```

Agent-shaped example (tools + SSE “LLM”) against the same harness:

```bash
python examples/agent_tools.py
```

Control-plane baseline (not a CI gate): `Runtime.gather` vs `asyncio.gather` + `asyncio.Semaphore` on local HTTP.

```bash
python scripts/bench_fanout.py
```

### Optional live LLM

Skipped unless a key is present. CI never sets keys. Cheap smoke: 2–3 short prompts, `as_completed`, one token stream, one cancelled handle.

```bash
python examples/live_llm.py
```

Uses `OPENAI_API_KEY` (preferred) or `ANTHROPIC_API_KEY`. Optional `OPENAI_MODEL` / `ANTHROPIC_MODEL`. To also run it under pytest: `LIVE_LLM=1 pytest tests/test_examples.py`. Do not log secrets.

## What this does not prove

- Rust is not faster at token generation. The model and Python SDK still dominate latency. Benchmarks measure **scheduler** cost on local HTTP, not provider SLA.
- No crash-durable journal replay, no multi-process supervisor, no sandbox for tool code.
- Live tests need a key, cost a few tokens, and will be rate-limited. They are a smoke, not a soak.
- No built-in Rust HTTP or LLM adapters in this lab. Coroutine bodies stay on asyncio on purpose.
