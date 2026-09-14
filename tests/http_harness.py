from __future__ import annotations

import asyncio
from dataclasses import dataclass

import httpx
from aiohttp import web


@dataclass
class Counters:
    in_flight: int = 0
    peak_in_flight: int = 0
    work_hits: int = 0
    slow_started: int = 0
    slow_completed: int = 0
    slow_exited: int = 0
    cancelled_handlers: int = 0
    flaky_hits: int = 0


def _enter(counters: Counters) -> None:
    counters.in_flight += 1
    if counters.in_flight > counters.peak_in_flight:
        counters.peak_in_flight = counters.in_flight


def create_app(counters: Counters) -> web.Application:
    async def work(_request: web.Request) -> web.Response:
        counters.work_hits += 1
        _enter(counters)
        try:
            await asyncio.sleep(0.15)
            return web.Response(text="ok")
        finally:
            counters.in_flight -= 1

    async def slow(request: web.Request) -> web.Response:
        delay = float(request.query.get("delay", "5"))
        counters.slow_started += 1
        _enter(counters)
        try:
            await asyncio.sleep(delay)
            counters.slow_completed += 1
            return web.Response(text="done")
        except asyncio.CancelledError:
            counters.cancelled_handlers += 1
            raise
        finally:
            counters.in_flight -= 1
            counters.slow_exited += 1

    async def flaky(request: web.Request) -> web.Response:
        fail_times = int(request.query.get("fail_times", "0"))
        counters.flaky_hits += 1
        if counters.flaky_hits <= fail_times:
            return web.Response(status=500, text="fail")
        return web.Response(text="ok")

    app = web.Application()
    app.router.add_get("/work", work)
    app.router.add_get("/slow", slow)
    app.router.add_get("/flaky", flaky)
    return app


@dataclass
class Harness:
    base_url: str
    counters: Counters

    def url(self, path: str) -> str:
        return f"{self.base_url}{path}"


async def http_get(url: str) -> str:
    async with httpx.AsyncClient() as client:
        response = await client.get(url, timeout=None)
        response.raise_for_status()
        return response.text
