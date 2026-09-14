from collections.abc import AsyncIterator

import pytest

from tests.http_harness import Counters, Harness, create_app
from aiohttp.test_utils import TestServer


@pytest.fixture
async def http_harness() -> AsyncIterator[Harness]:
    counters = Counters()
    async with TestServer(create_app(counters)) as server:
        yield Harness(str(server.make_url("")).rstrip("/"), counters)
