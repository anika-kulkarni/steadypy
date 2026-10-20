# More examples

Short recipes for common situations. Complete runnable programs are in
[`examples/`](../examples/). For the reasoning behind the behavior, see
[concepts.md](concepts.md).

- [Retry only server errors](#retry-only-server-errors)
- [A hard time limit around retries](#a-hard-time-limit-around-retries)
- [Retry and a circuit breaker together](#retry-and-a-circuit-breaker-together)
- [Retry a blocking function](#retry-a-blocking-function)
- [One rate limiter per host](#one-rate-limiter-per-host)
- [Crawl with results in completion order](#crawl-with-results-in-completion-order)
- [A TCP server with one supervised task per connection](#a-tcp-server-with-one-supervised-task-per-connection)
- [A health watchdog that triggers shutdown](#a-health-watchdog-that-triggers-shutdown)
- [Let a second Ctrl-C interrupt a slow shutdown](#let-a-second-ctrl-c-interrupt-a-slow-shutdown)
- [Testing your own code with FakeClock](#testing-your-own-code-with-fakeclock)

## Retry only server errors

`retry_on` selects exception types; `retry_if` looks at the exception itself.

```python
from steadypy import retry


class HTTPError(Exception):
    def __init__(self, status: int) -> None:
        super().__init__(f"HTTP {status}")
        self.status = status


async def get_user(user_id: str) -> dict: ...


user = await retry(
    lambda: get_user("42"),
    attempts=4,
    retry_on=(HTTPError, ConnectionError),
    retry_if=lambda exc: not isinstance(exc, HTTPError) or exc.status >= 500,
)
```

A `404` is raised immediately; a `503` or a dropped connection is retried.

## A hard time limit around retries

`deadline=` stops *starting* new attempts. To also interrupt an attempt that
is in progress, wrap the whole call in `asyncio.timeout()`, and give each
attempt its own timeout too:

```python
import asyncio

from steadypy import retry


async def fetch_once() -> bytes:
    async with asyncio.timeout(2):  # per attempt
        return await download()


async with asyncio.timeout(10):  # whole operation, including backoff sleeps
    data = await retry(fetch_once, attempts=5, retry_on=(ConnectionError, TimeoutError))
```

A `TimeoutError` raised by the per-attempt timeout is retried. When the outer
timeout expires, it cancels the retry loop, which is never retried, and
surfaces as `TimeoutError` outside the block.

## Retry and a circuit breaker together

Put the breaker inside the retried operation and leave `CircuitOpenError` out
of `retry_on`, so an open circuit fails fast instead of being retried:

```python
from steadypy import CircuitBreaker, CircuitOpenError, retry

inventory_breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=30, name="inventory")


async def check_stock(sku: str) -> int:
    async def attempt() -> int:
        async with inventory_breaker:
            return await inventory_client.stock(sku)

    try:
        return await retry(attempt, attempts=3, retry_on=(ConnectionError, TimeoutError))
    except CircuitOpenError:
        return cached_stock(sku)
```

## Retry a blocking function

`retry` needs a callable that returns an awaitable, and
`asyncio.to_thread` provides one for synchronous code:

```python
import asyncio

from steadypy import retry

report = await retry(lambda: asyncio.to_thread(legacy_client.fetch_report, "2026-01"))
```

## One rate limiter per host

```python
from collections import defaultdict
from urllib.parse import urlsplit

from steadypy import RateLimiter, bounded_map

limiters: defaultdict[str, RateLimiter] = defaultdict(lambda: RateLimiter(rate=5, per=1.0))


async def polite_fetch(url: str) -> bytes:
    async with limiters[urlsplit(url).hostname or ""]:
        return await http_get(url)


pages = await bounded_map(polite_fetch, urls, concurrency=20)
```

Twenty requests may be in flight in total, but no host sees more than five
per second.

## Crawl with results in completion order

In unordered mode results arrive as soon as they are ready. Return the input
alongside the result if you need to know which is which, and use
`return_exceptions=True` to keep going past failures:

```python
from steadypy import bounded_stream


async def fetch_with_url(url: str) -> tuple[str, bytes]:
    return url, await http_get(url)


async with bounded_stream(
    fetch_with_url, urls, concurrency=10, ordered=False, return_exceptions=True
) as results:
    async for outcome in results:
        if isinstance(outcome, Exception):
            print(f"failed: {outcome!r}")
            continue
        url, body = outcome
        print(f"{url}: {len(body)} bytes")
```

## A TCP server with one supervised task per connection

Each connection becomes a named child of the supervisor. Stopping the
supervisor closes every connection. A crash in one handler is logged and
collected, and does not take down the server (`fail_fast=False`).

```python
import asyncio

from steadypy import ShutdownSignal, TaskSupervisor


async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while line := await reader.readline():
            writer.write(line.upper())
            await writer.drain()
    finally:
        writer.close()


async def main() -> None:
    shutdown = ShutdownSignal()
    shutdown.install_signal_handlers()
    async with TaskSupervisor(fail_fast=False) as supervisor:

        def on_connect(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            host, port = writer.get_extra_info("peername")[:2]
            supervisor.start(handle, reader, writer, name=f"conn-{host}:{port}")

        server = await asyncio.start_server(on_connect, "127.0.0.1", 8888)
        await shutdown.wait()
        server.close()  # stop accepting new connections
        supervisor.stop()  # cancel the handlers, which close their connections
        await server.wait_closed()  # on Python 3.12+ this waits for open connections
    shutdown.remove_signal_handlers()


asyncio.run(main())
```

`supervisor.tasks` now doubles as a list of open connections.

## A health watchdog that triggers shutdown

```python
import asyncio

from steadypy import HealthRegistry, ShutdownSignal


async def watchdog(health: HealthRegistry, shutdown: ShutdownSignal, grace: float = 60) -> None:
    unhealthy_for = 0.0
    while True:
        await asyncio.sleep(5)
        report = health.status()
        unhealthy_for = 0.0 if report.healthy else unhealthy_for + 5
        if unhealthy_for >= grace:
            shutdown.trigger(f"unhealthy for {grace:g}s: {', '.join(report.unhealthy)}")
            return
```

Run it under the supervisor like any other task. When the process exits, your
process manager (systemd, Kubernetes) restarts it.

## Let a second Ctrl-C interrupt a slow shutdown

While steadypy's handlers are installed, repeated signals are ignored. Remove
them as soon as shutdown starts to get the default behavior back:

```python
reason = await shutdown.wait()
shutdown.remove_signal_handlers()  # a second Ctrl-C now behaves as usual
supervisor.stop()
```

## Testing your own code with FakeClock

Anything that accepts `clock=` can be driven from a test without real
sleeps. Here a client is rate limited to one call per second:

```python
import asyncio

from steadypy import RateLimiter
from steadypy.testing import FakeClock


class Client:
    def __init__(self, clock: FakeClock | None = None) -> None:
        self.limiter = RateLimiter(rate=1, clock=clock)
        self.sent: list[float] = []

    async def send(self, clock: FakeClock) -> None:
        async with self.limiter:
            self.sent.append(clock.monotonic())


async def test_client_sends_at_most_one_message_per_second() -> None:
    clock = FakeClock()
    client = Client(clock)
    tasks = [asyncio.create_task(client.send(clock)) for _ in range(3)]
    await clock.advance(10)
    await asyncio.gather(*tasks)
    assert client.sent == [0, 1, 2]
```
