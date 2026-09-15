from collections.abc import Awaitable, Callable, Generator, Iterable
from typing import Any, final

class QueueFull(RuntimeError): ...
class RuntimeClosed(RuntimeError): ...

@final
class OnFull:
    REJECT: OnFull
    WAIT: OnFull
    name: str
    value: int
    def __eq__(self, other: object) -> bool: ...

@final
class RetryPolicy:
    max_attempts: int
    backoff_ms: int
    backoff_multiplier: float
    max_backoff_ms: int
    jitter: bool
    def __init__(
        self,
        max_attempts: int = 3,
        backoff_ms: int = 100,
        backoff_multiplier: float = 2.0,
        max_backoff_ms: int = 5000,
        jitter: bool = True,
    ) -> None: ...

@final
class Task:
    def __init__(
        self,
        func: Callable[..., Any],
        *args: Any,
        timeout: float | None = None,
    ) -> None: ...

@final
class Handle(Awaitable[Any]):
    def __await__(self) -> Generator[Any, None, Any]: ...
    def cancel(self) -> None: ...
    def cancelled(self) -> bool: ...
    def status(self) -> str: ...
    def done(self) -> bool: ...

@final
class Completion:
    index: int
    ok: bool
    value: Any

@final
class CompletionStream:
    def __aiter__(self) -> CompletionStream: ...
    def __anext__(self) -> Awaitable[Completion]: ...

@final
class ItemStream:
    def __aiter__(self) -> ItemStream: ...
    def __anext__(self) -> Awaitable[Any]: ...
    def cancel(self) -> None: ...

@final
class Runtime:
    def __init__(
        self,
        max_concurrency: int = 32,
        queue_capacity: int = 128,
        on_full: OnFull = ...,
        default_timeout: float | None = None,
        retry: RetryPolicy | None = None,
        idle_ttl: float | None = None,
    ) -> None: ...
    def submit(
        self,
        func: Callable[..., Any],
        *args: Any,
        timeout: float | None = None,
    ) -> Handle: ...
    def gather(
        self,
        tasks: Iterable[Task],
        return_exceptions: bool = False,
    ) -> Awaitable[list[Any]]: ...
    def as_completed(self, tasks: Iterable[Task]) -> CompletionStream: ...
    def stream(
        self,
        func: Callable[..., Any],
        *args: Any,
        timeout: float | None = None,
        buffer: int = 8,
    ) -> ItemStream: ...
    def stats(self) -> dict[str, int]: ...
    def shutdown(self, timeout: float | None = None) -> Awaitable[None]: ...
    @property
    def closed(self) -> bool: ...
    def __aenter__(self) -> Awaitable[Runtime]: ...
    def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: Any,
    ) -> Awaitable[None]: ...

__all__ = [
    "Completion",
    "Handle",
    "OnFull",
    "QueueFull",
    "RetryPolicy",
    "Runtime",
    "RuntimeClosed",
    "Task",
]
