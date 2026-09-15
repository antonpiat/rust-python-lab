"""Optional OpenAI / Anthropic smoke. Skips unless a key is set.

Never required for CI. Uses httpx (no official SDKs, no LangChain).

    python examples/live_llm.py
"""

from __future__ import annotations

import asyncio
import json
import os
import sys

import httpx

from rust_python_lab import Runtime, Task

OPENAI_URL = "https://api.openai.com/v1/chat/completions"
ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
PROMPTS = (
    "Reply with the single word ping.",
    "Reply with the single word pong.",
    "Reply with the single word ok.",
)


def _provider() -> str | None:
    if os.environ.get("OPENAI_API_KEY"):
        return "openai"
    if os.environ.get("ANTHROPIC_API_KEY"):
        return "anthropic"
    return None


async def openai_complete(prompt: str) -> str:
    key = os.environ["OPENAI_API_KEY"]
    model = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 32,
    }
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            OPENAI_URL,
            headers={"Authorization": f"Bearer {key}"},
            json=payload,
        )
        response.raise_for_status()
        data = response.json()
    return data["choices"][0]["message"]["content"]


async def openai_tokens(prompt: str):
    key = os.environ["OPENAI_API_KEY"]
    model = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 32,
        "stream": True,
    }
    async with httpx.AsyncClient(timeout=30.0) as client:
        async with client.stream(
            "POST",
            OPENAI_URL,
            headers={"Authorization": f"Bearer {key}"},
            json=payload,
        ) as response:
            response.raise_for_status()
            async for line in response.aiter_lines():
                if not line.startswith("data: "):
                    continue
                data = line[6:]
                if data == "[DONE]":
                    return
                delta = json.loads(data)["choices"][0].get("delta", {})
                text = delta.get("content")
                if text:
                    yield text


async def anthropic_complete(prompt: str) -> str:
    key = os.environ["ANTHROPIC_API_KEY"]
    model = os.environ.get("ANTHROPIC_MODEL", "claude-3-haiku-20240307")
    payload = {
        "model": model,
        "max_tokens": 32,
        "messages": [{"role": "user", "content": prompt}],
    }
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            ANTHROPIC_URL,
            headers={
                "x-api-key": key,
                "anthropic-version": "2023-06-01",
            },
            json=payload,
        )
        response.raise_for_status()
        data = response.json()
    return "".join(
        block.get("text", "")
        for block in data.get("content", [])
        if block.get("type") == "text"
    )


async def anthropic_tokens(prompt: str):
    key = os.environ["ANTHROPIC_API_KEY"]
    model = os.environ.get("ANTHROPIC_MODEL", "claude-3-haiku-20240307")
    payload = {
        "model": model,
        "max_tokens": 32,
        "stream": True,
        "messages": [{"role": "user", "content": prompt}],
    }
    async with httpx.AsyncClient(timeout=30.0) as client:
        async with client.stream(
            "POST",
            ANTHROPIC_URL,
            headers={
                "x-api-key": key,
                "anthropic-version": "2023-06-01",
            },
            json=payload,
        ) as response:
            response.raise_for_status()
            async for line in response.aiter_lines():
                if not line.startswith("data: "):
                    continue
                event = json.loads(line[6:])
                if event.get("type") == "content_block_delta":
                    text = event.get("delta", {}).get("text")
                    if text:
                        yield text


async def main() -> int:
    provider = _provider()
    if provider is None:
        print("skip: set OPENAI_API_KEY or ANTHROPIC_API_KEY to run the live LLM smoke")
        return 0

    complete = openai_complete if provider == "openai" else anthropic_complete
    tokens = openai_tokens if provider == "openai" else anthropic_tokens
    print(f"provider: {provider}")

    async with Runtime(max_concurrency=3, default_timeout=30.0) as rt:
        order: list[int] = []
        async for event in rt.as_completed(
            [Task(complete, prompt) for prompt in PROMPTS[:2]]
        ):
            if not event.ok:
                raise RuntimeError(f"completion {event.index} failed")
            order.append(event.index)
            print(f"completed[{event.index}]: {event.value!r}")

        streamed = []
        async for piece in rt.stream(tokens, PROMPTS[2]):
            streamed.append(piece)
        print("streamed chars:", len("".join(streamed)))

        handle = rt.submit(complete, PROMPTS[0])
        handle.cancel()
        try:
            await handle
        except asyncio.CancelledError:
            print("cancelled: ok")

    print("as_completed order:", order)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(main()))
    except httpx.HTTPError as exc:
        print(f"live llm request failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
