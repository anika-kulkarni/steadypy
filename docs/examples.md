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

Put the breaker inside the retried operation and the per-call timeout inside
the breaker. Make sure `retry_on` does not include `CircuitOpenError` (the
default, `Exception`, would), so that an open circuit fails fast instead of
being retried:

```python
import asyncio

from steadypy import CircuitBreaker, CircuitOpenError, retry

inventory_breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=30, name="inventory")


async def check_stock(sku: str) -> int:
    async def attempt() -> int:
        async with inventory_breaker:
            async with asyncio.timeout(2):  # a hang counts as a breaker failure
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

report = await retry(
    lambda: asyncio.to_thread(legacy_client.fetch_report, "2026-01"),
    retry_on=(ConnectionError, TimeoutError),
)
```

Passing the synchronous function itself (`retry(legacy_client.fetch_report)`)
raises a `TypeError` straight away rather than retrying.

## One rate limiter per host

```python
from collections import defaultdict
from urllib.parse import urlsplit

from steadypy import RateLimiter, bounded_map

limiters: defaultdict[str, RateLimiter] = defaultdict(lambda: RateLimiter(rate=5, per=1.0, burst=1))


async def polite_fetch(url: str) -> bytes:
    async with limiters[urlsplit(url).hostname or ""]:
        return await http_get(url)


pages = await bounded_map(polite_fetch, urls, concurrency=20)
```

Up to twenty requests are in flight in total. Each host gets requests spaced
at least 0.2 s apart (five per second). With `burst=1`, any one-second window
holds at most six requests to a host. Without it, the bucket's initial burst
would allow up to ten in the first second. If the server's limit is strict,
leave yourself a margin.

## Crawl with results in completion order

In unordered mode results arrive as soon as they are ready. Return the input
alongside the result, and catch the errors you expect *inside* the function,
so that a failure still tells you which input it belongs to:

```python
from steadypy import bounded_stream


async def fetch_with_url(url: str) -> tuple[str, bytes | Exception]:
    try:
        return url, await http_get(url)
    except (ConnectionError, TimeoutError) as exc:
        return url, exc


async with bounded_stream(fetch_with_url, urls, concurrency=10, ordered=False) as results:
    async for url, outcome in results:
        if isinstance(outcome, Exception):
            print(f"{url}: failed with {outcome!r}")
        else:
            print(f"{url}: {len(outcome)} bytes")
```

## A TCP server with one supervised task per connection

Each connection becomes a named child of the supervisor. Stopping the
supervisor closes every connection. `retain_finished=False` drops closed
connections from `supervisor.tasks`, so memory stays flat however many
clients come and go. Errors you expect from clients are handled inside the
handler. With `fail_fast=False`, an unexpected crash in one handler is logged
and collected without taking down the server.

```python
import asyncio

from steadypy import ShutdownSignal, TaskSupervisor


async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while line := await reader.readline():
            writer.write(line.upper())
            await writer.drain()
    except ConnectionError:
        pass  # the client went away; not a server failure
    finally:
        writer.close()


async def main() -> None:
    shutdown = ShutdownSignal()
    shutdown.install_signal_handlers()
    try:
        async with TaskSupervisor(fail_fast=False, retain_finished=False) as supervisor:

            def on_connect(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
                host, port = writer.get_extra_info("peername")[:2]
                supervisor.start(handle, reader, writer, name=f"conn-{host}:{port}")

            server = await asyncio.start_server(on_connect, "127.0.0.1", 8888)
            await shutdown.wait()
            server.close()  # stop accepting new connections
            supervisor.stop()  # cancel the handlers, which close their connections
            await server.wait_closed()  # on Python 3.12+ this waits for open connections
    finally:
        shutdown.remove_signal_handlers()


asyncio.run(main())
```

With `retain_finished=False`, `supervisor.tasks` lists exactly the open
connections.

## A health watchdog that triggers shutdown

Components are unhealthy until their first heartbeat, so the watchdog only
starts counting once the service has been healthy at least once. Otherwise
a slow start would be mistaken for a failure:

```python
import asyncio

from steadypy import HealthRegistry, ShutdownSignal


async def watchdog(health: HealthRegistry, shutdown: ShutdownSignal, grace: float = 60) -> None:
    while not health.is_healthy():  # wait for startup to finish
        await asyncio.sleep(1)
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
them as soon as shutdown starts to get `asyncio.run()`'s usual behavior back:

```python
reason = await shutdown.wait()
shutdown.remove_signal_handlers()
supervisor.stop()
```

The next Ctrl-C cancels the main task, so the supervisor cancels its tasks
again. That ends cleanup code that is merely slow. A further Ctrl-C raises
`KeyboardInterrupt`.

## Testing your own code with FakeClock

Write production code against the `steadypy.Clock` protocol, defaulting to
the real clock, and inject a `FakeClock` in tests. Here a client is limited
to one message per second. The test uses
[pytest-asyncio](https://pypi.org/project/pytest-asyncio/):

```python
import asyncio

import pytest

from steadypy import Clock, RateLimiter, SystemClock
from steadypy.testing import FakeClock


class Client:
    def __init__(self, clock: Clock | None = None) -> None:
        self.clock = clock or SystemClock()
        self.limiter = RateLimiter(rate=1, clock=self.clock)
        self.sent_at: list[float] = []

    async def send(self, message: str) -> None:
        async with self.limiter:
            self.sent_at.append(self.clock.monotonic())


@pytest.mark.asyncio
async def test_client_sends_at_most_one_message_per_second() -> None:
    clock = FakeClock()
    client = Client(clock)
    tasks = [asyncio.create_task(client.send(f"msg {n}")) for n in range(3)]
    await clock.advance(10)
    await asyncio.gather(*tasks)
    assert client.sent_at == [0, 1, 2]
```
