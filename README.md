# rust-python-lab

Keep writing agents in Python. Let **Rust run the scheduler**.

`rust_python_lab` is a Tokio-backed execution controller for asyncio apps. You `submit`, `gather`, `stream`, and `await` as usual. Rust owns concurrency, backpressure, timeouts, cancellation, retries, and shutdown — without rewriting your HTTP, DB, or LLM calls, and without locking you into LangChain or LangGraph.

Python still steps coroutine bodies. Rust does not execute Python bytecode. That is the point: the control plane stays off the asyncio thread, so a slow or greedy task cannot stall admission, timers, or cancel.

## Why this exists

- **Python-native API.** `await handle`, `async with Runtime`, `async for` completions and tokens. `submit` is synchronous so you can `cancel()` before the first `await`.
- **Real cancel and timeout.** Dropping a Tokio waiter is not enough. The runtime cancels the **asyncio Task** the bridge created, so httpx connections and tool calls actually stop.
- **Limits that hold under load.** `max_concurrency` is a Tokio semaphore. The admission queue is bounded. `OnFull.REJECT` raises `QueueFull`; `OnFull.WAIT` blocks until a slot exists (and aborts on shutdown).
- **Retries that work.** Rust re-invokes the Python callable for a **new** coroutine. A consumed coroutine object is never replayed.
- **Fan-out without a Python control-plane.** `gather`, `as_completed`, and bounded `stream()` keep in-flight state in Rust. `stats()` is O(1) counters, not a walk of Python objects.
- **Clean shutdown.** `await rt.shutdown(timeout=…)` stops new work, drains, then cancels the rest. `async with` does this on exit. Optional `idle_ttl` closes admission after quiet. Further `submit` raises `RuntimeClosed`.
- **Use the libraries you already have.** httpx, OpenAI/Anthropic SDKs, databases, tools — they stay asyncio. Several `Runtime` instances can apply different policies (for example an LLM pool of 8 and a DB pool of 64).
- **Proven on sockets, not `asyncio.sleep`.** CI drives a local HTTP/SSE server through httpx: concurrency caps, handler abort on timeout/cancel, retries, backpressure, streams, shutdown, and a short soak.

## Execution split

| Layer | Owns |
| --- | --- |
| **Rust** | queues, permits, when a task may start, timeout, cancel, retry/backoff, gather/stream aggregation, graceful shutdown, in-memory journal |
| **Python asyncio** | coroutine bodies, sockets, SDK calls, object lifetimes, GIL |
| **Bridge** | `pyo3-async-runtimes`: capture `TaskLocals`, convert awaitables both ways, never hold the GIL across `.await` |

## Setup

Rust (stable) and Python 3.12+.

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

    slow = rt.submit(call_llm, "long job")
    slow.cancel()

    results = await rt.gather(
        [Task(call_llm, prompt) for prompt in prompts],
        return_exceptions=True,
    )
    async for event in rt.as_completed(tasks):
        ...
    async for token in rt.stream(llm_tokens, "hello"):
        ...

    print(rt.stats())  # queued, in_flight, succeeded, failed, cancelled, timed_out, completed
```

`Handle.status()` is `queued`, `running`, `succeeded`, `failed`, `cancelled`, or `timed_out`. After close, `submit` raises `RuntimeClosed`. A full queue with `OnFull.REJECT` raises `QueueFull`. Without `async with`, call `await rt.shutdown(timeout=5.0)` yourself.

## Test

CI and the default local run use **localhost HTTP only** — no API keys, no public internet.

```bash
pytest
```

The suite uses `tests/http_harness.py` (aiohttp on `127.0.0.1`) and **httpx** task bodies:

- `/work` — peak in-flight ≤ `max_concurrency`
- `/slow` — timeout and `handle.cancel()` abort the server handler
- `/flaky` — retries re-call the callable (extra server hits)
- `/sse` — `rt.stream` plus bounded-buffer backpressure
- mixed latencies in `gather` / `as_completed`
- shutdown cancels in-flight `/slow`; later `submit` is `RuntimeClosed`

Soak (2k tiny GETs; `SOAK=1` runs 20k):

```bash
pytest tests/test_soak.py
SOAK=1 pytest tests/test_soak.py
```

Agent-shaped example (parallel tools + SSE “LLM”) against the same harness:

```bash
python examples/agent_tools.py
```

Scheduler baseline (not a CI gate): `Runtime.gather` vs `asyncio.gather` + `asyncio.Semaphore` on local HTTP.

```bash
python scripts/bench_fanout.py
```

### Optional live LLM

Skipped unless a key is set. CI never sets keys. Cheap smoke: two completions via `as_completed`, one token stream, one cancelled handle. Uses **httpx** only (no official SDK, no LangChain).

```bash
cp examples/.env.example .env
# uncomment and set OPENAI_API_KEY or ANTHROPIC_API_KEY
python examples/live_llm.py
```

Keys come from the process environment, then from `.env` in the repo root or `examples/.env`. Already-set env vars win. Optional `OPENAI_MODEL` / `ANTHROPIC_MODEL`. To also run it under pytest: `LIVE_LLM=1 pytest tests/test_examples.py`. Do not log secrets.

These are deliberate, not missing checkboxes:

- Rust does not make the model faster. Token generation and the Python SDK still dominate latency. `scripts/bench_fanout.py` measures **scheduler** cost on local HTTP, not provider SLA.
- No crash-durable journal replay, no multi-process supervisor, no sandbox for tool code.
- Live LLM is an opt-in smoke. It needs a key, spends a few tokens, and will be rate-limited.
- No built-in Rust HTTP or LLM client. Coroutine bodies stay on asyncio so existing Python libraries keep working.
