# steadypy

Reliable primitives for production asyncio services.

[![Tests](https://github.com/anika-kulkarni/steadypy/actions/workflows/tests.yml/badge.svg)](https://github.com/anika-kulkarni/steadypy/actions/workflows/tests.yml)
[![Quality](https://github.com/anika-kulkarni/steadypy/actions/workflows/lint.yml/badge.svg)](https://github.com/anika-kulkarni/steadypy/actions/workflows/lint.yml)

asyncio has excellent concurrency primitives, but real long-running
applications keep needing the same reliability patterns on top of them:

- supervising background tasks, and restarting the ones that crash
- propagating cancellation correctly, so shutdown actually shuts things down
- retrying transient failures without hammering whatever is failing
- preventing cascading failures when a dependency goes down
- bounding concurrency when there are more jobs than you should run at once
- respecting rate limits
- tracking whether each part of the service is still alive
- shutting down cleanly on `SIGTERM`

steadypy packages these patterns into one small library: zero runtime
dependencies, fully typed, and careful about cancellation.

```python
from steadypy import (
    TaskSupervisor,
    retry,
    async_retry,
    CircuitBreaker,
    RateLimiter,
    HealthRegistry,
    ShutdownSignal,
    bounded_map,
)
```

> **Status: alpha.** Version 0.1.0 is tagged on GitHub but not yet published
> to PyPI. The API is small and tested, but may still change before 1.0.

## Contents

- [Installation](#installation)
- [Quick start](#quick-start)
- [TaskSupervisor](#tasksupervisor): run, restart and cancel long-running tasks
- [Retry](#retry): exponential backoff with jitter
- [Circuit breaker](#circuit-breaker): stop calling a dependency that is down
- [Bounded concurrency and rate limiting](#bounded-concurrency-and-rate-limiting)
- [Health](#health): heartbeats and readiness
- [Graceful shutdown](#graceful-shutdown)
- [Threads, event loops and logging](#threads-event-loops-and-logging)
- [Why not just use asyncio directly?](#why-not-just-use-asyncio-directly)
- [Design principles](#design-principles)
- [Testing](#testing)
- [Contributing](#contributing) · [Roadmap](#roadmap) · [Related projects](#related-projects)

## Installation

steadypy requires Python 3.11 or newer and has no runtime dependencies. CI
tests Python 3.11, 3.12 and 3.13 on Linux, plus 3.13 on macOS; Windows is
not tested yet.

It is not on PyPI yet. Install a tagged version from GitHub:

```bash
pip install "git+https://github.com/anika-kulkarni/steadypy@v0.1.0"
```

## Quick start

A worker that polls a flaky feed, retries failed fetches, is restarted if it
crashes anyway, and stops cleanly on Ctrl-C or `SIGTERM`:

```python
import asyncio
import random

from steadypy import ShutdownSignal, TaskSupervisor, retry


async def fetch_feed() -> list[str]:
    if random.random() < 0.3:  # stand-in for a flaky network call
        raise ConnectionError("feed unavailable")
    return ["item"]


async def poll_feed() -> None:
    while True:
        items = await retry(fetch_feed, attempts=5, retry_on=ConnectionError)
        print(f"fetched {len(items)} item(s)")
        await asyncio.sleep(1)


async def main() -> None:
    shutdown = ShutdownSignal()
    shutdown.install_signal_handlers()  # SIGINT / SIGTERM request a shutdown
    try:
        async with TaskSupervisor() as supervisor:
            # Restart on crashes, but give up after 5 restarts within a minute.
            supervisor.start(poll_feed, restart=True, max_restarts=5, restart_window=60)
            await shutdown.wait()
            supervisor.stop()  # cancel the workers; the block exits once they finish
    finally:
        shutdown.remove_signal_handlers()


asyncio.run(main())
```

Three runnable programs in [`examples/`](examples/) go further. None of them
needs network access or credentials:

| Example | Shows |
|---|---|
| [`resilient_worker.py`](examples/resilient_worker.py) | polling a simulated API with retry, per-attempt timeouts, a rate limit and `bounded_map` |
| [`supervised_service.py`](examples/supervised_service.py) | producers, consumers and a crash-looping connection under one supervisor |
| [`graceful_app.py`](examples/graceful_app.py) | supervisor + heartbeats + circuit breaker + signal-driven shutdown with a grace period |

## TaskSupervisor

`TaskSupervisor` is a structured-concurrency container like
[`asyncio.TaskGroup`](https://docs.python.org/3/library/asyncio-task.html#task-groups):
no task outlives its `async with` block. It adds the policies long-running
services need: named tasks, restart policies with exponential backoff and
limits, a choice between fail-fast and isolated failures, and live state.

```python
from steadypy import TaskSupervisor

async with TaskSupervisor(fail_fast=False) as supervisor:
    ws = supervisor.start(
        # A callable plus its arguments, so it can be called again on restart.
        websocket_listener,
        url,
        name="websocket",
        restart=True,  # restart when it raises,
        max_restarts=5,  # at most 5 times
        restart_window=60,  # per minute
    )
    supervisor.start(flush_metrics, name="metrics", restart="always")
    supervisor.start(warm_cache(), name="warmup")  # a coroutine object runs exactly once

    await shutdown.wait()
    print(ws.state, ws.restarts, ws.last_exception)
    supervisor.stop()
```

How it behaves:

- **Exiting the block waits** for every task, like `TaskGroup`. Call
  `supervisor.stop()` to cancel them instead. Cancelled tasks are not errors.
- **Restarts** wait `restart_delay` (0.5 s by default), doubling each time, up
  to `max_restart_delay` (30 s by default). With `restart_window`, only
  restarts within the window count toward `max_restarts` and toward the
  backoff. **Without a window, `max_restarts` is a lifetime limit**, and the
  backoff never resets.
- **`restart=True`** restarts on exceptions. **`restart="always"`** also
  restarts a task that returns, for workers that should never stop.
  Cancellation never causes a restart. A `CancelledError` that nobody asked
  for (the task awaited a future that something else cancelled) counts as a
  failed run, so a restartable task is restarted rather than stopping
  silently.
- **Failures.** A task *fails for good* when it raises and won't be
  restarted. With `fail_fast=True` (the default, matching `TaskGroup`), that
  cancels every other task *and* the body of the block. With
  `fail_fast=False`, the other tasks keep running, and failures are visible
  in `supervisor.errors`. Either way, failures are raised together as an
  `ExceptionGroup` when the block exits. Each exception carries a note naming
  its task, so you can handle them with
  [`except*`](https://docs.python.org/3/library/exceptions.html#exception-groups).
  `KeyboardInterrupt` and `SystemExit` always stop the supervisor and are
  re-raised on their own.
- **Cancellation of the surrounding task** cancels every child, waits for
  their cleanup, and propagates. The supervisor's own internal cancellation
  (used to interrupt the body in fail-fast mode) is withdrawn with
  [`Task.uncancel()`](https://docs.python.org/3/library/asyncio-task.html#asyncio.Task.uncancel),
  so it composes correctly with `asyncio.timeout()`. One exception, shared
  with `TaskGroup`: if tasks *failed*, the `ExceptionGroup` is raised instead
  of `CancelledError`. On Python 3.13+ the cancellation then stays pending and
  arrives at the next `await`. On 3.11 and 3.12 it is absorbed into the group.
  Both match `TaskGroup` on the same version.
- **Inspection.** `start()` returns a `SupervisedTask` handle with `state`,
  `restarts`, `failures`, `last_exception`, `result()`, `exception()`,
  `cancel()` and `wait()`. `supervisor.tasks` maps names to handles, finished
  tasks included. For one task per connection or per message, pass
  `retain_finished=False` so that finished tasks are dropped and memory stays
  flat.

> **Why a callable for restartable tasks?** A coroutine object such as
> `worker()` can only be awaited once, so it cannot be restarted. Pass the
> function, `start(worker, arg, restart=True)`, and steadypy calls it again
> for each run. Passing a coroutine object together with a restart policy
> raises a `TypeError` that explains this.

## Retry

```python
from steadypy import retry, async_retry

result = await retry(
    fetch,  # a callable: called again for every attempt
    attempts=5,
    base_delay=0.25,
    max_delay=5,
    retry_on=(ConnectionError, TimeoutError),
)


@async_retry(attempts=5, retry_on=(ConnectionError, TimeoutError))
async def fetch_profile(user_id: str) -> dict: ...
```

- **Defaults:** `attempts=3`, `base_delay=0.1`, `max_delay=10` (or
  `base_delay`, if larger), `multiplier=2`, `jitter=1.0`, `retry_on=Exception`.
- **Pass `retry_on` explicitly.** The default retries *every* `Exception`,
  including programming errors such as `KeyError` and a `CircuitOpenError`
  from a circuit breaker.
- The delay before retry *n* is `base_delay * multiplier ** (n - 1)`, capped
  at `max_delay`, then randomized by `jitter` (1.0 is "full jitter"; see
  [Exponential Backoff And Jitter](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/)).
- `retry_if` can veto a retry after `retry_on` matched, e.g.
  `retry_if=lambda exc: not isinstance(exc, HTTPError) or exc.status >= 500`.
  An exception raised by `retry_if` itself propagates.
- `deadline=10` stops retrying once the next attempt would start after 10
  seconds. For a hard limit that also interrupts an in-flight attempt, wrap
  the call in `asyncio.timeout()`.
- `on_retry=` receives a `RetryAttempt` (attempt number, exception, delay,
  elapsed) and may be sync or async. Use it for logging or metrics.
- When retries run out, **the same exception object your code raised** is
  re-raised, with a note such as `steadypy.retry: gave up after 5 attempts`.
- **Cancellation is never retried.** `CancelledError` propagates immediately
  from an attempt or from a backoff sleep. If an attempt swallows a
  cancellation request (catches `CancelledError` and raises something else),
  steadypy sees the pending request through
  [`Task.cancelling()`](https://docs.python.org/3/library/asyncio-task.html#asyncio.Task.cancelling)
  and does not retry.

## Circuit breaker

```python
import asyncio

from steadypy import CircuitBreaker, CircuitOpenError

breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=30, name="payments")

try:
    async with breaker:
        async with asyncio.timeout(5):  # inside the breaker: a hang counts as a failure
            await call_external_service()
except CircuitOpenError as exc:
    serve_cached_response()  # fail fast; exc.retry_after says when a probe is allowed

# or: result = await breaker.call(call_external_service, order_id)
```

After `failure_threshold` consecutive failures the circuit **opens** and calls
fail immediately with `CircuitOpenError`. After `recovery_timeout` seconds it
becomes **half-open** and admits `half_open_max_calls` probe calls (1 by
default): a successful probe **closes** it, a failed one opens it again.

- `failure_exceptions=` selects what counts as a failure. Other exceptions
  propagate but count as successes, because the dependency did respond.
  Cancellation counts as neither, so **put timeouts inside the protected
  block**. A timeout outside it cancels the call, and a hanging dependency
  would never trip the breaker.
- A probe that has not finished within `recovery_timeout` is considered lost
  and its slot is given to the next caller, so one hung probe cannot keep the
  circuit half-open forever.
- `breaker.state`, `breaker.stats` (successes, failures, rejections, current
  streak), `breaker.retry_after`, `breaker.reset()`, and an `on_state_change`
  callback are available for inspection and alerting.
- Safe to share between tasks: each call remembers the state *generation* it
  was admitted in, so a slow call that fails after the circuit already opened
  cannot extend the open period, and a stale success cannot close it.
- `async with breaker:` must be exited in the task that entered it. Around a
  `yield` in an async generator, which may be closed from another task, use
  `async with breaker.guard():` instead.

## Bounded concurrency and rate limiting

```python
from steadypy import RateLimiter, bounded_map, bounded_stream

pages = await bounded_map(fetch, urls, concurrency=10)  # results in input order

async with bounded_stream(fetch, urls, concurrency=10, ordered=False) as results:
    async for page in results:  # in completion order
        ...

limiter = RateLimiter(rate=10, per=1.0)  # 10 per second on average

async with limiter:  # the same as `await limiter.acquire()`
    await request()
```

- `bounded_map` and `bounded_stream` pull items **lazily**, one per free slot,
  so a generator of a million URLs never becomes a million tasks. An async
  input is read in the background, so a slow input never holds back results
  that are already done.
- The first failure cancels the remaining work and is raised, or, with
  `return_exceptions=True`, each failure takes the place of its result.
  Cancelling the caller cancels and awaits every in-flight call.
- `bounded_stream` yields in input order by default. One slow item then holds
  back the results behind it, and no new calls start until it finishes. Pass
  `ordered=False` to keep every slot busy.
- `bounded_stream` is used with `async with` so an early `break` reliably
  cleans up in-flight calls. A bare async generator cannot guarantee that.
- `RateLimiter` is a [token bucket](https://en.wikipedia.org/wiki/Token_bucket)
  on a monotonic clock. The bucket starts full, so **any window of *W*
  seconds admits at most `burst + rate × W` calls**. With the defaults, up to
  20 calls can fit in the first second after an idle period. Lower `burst`
  (down to 1) for smoother spacing. Waiters are served first-come,
  first-served, a cancelled waiter takes no tokens, and `try_acquire()` never
  jumps the queue.
- A rate limiter limits how *often* something starts, not how many run at
  once. Exiting `async with limiter` releases nothing. Use `bounded_map` or
  `asyncio.Semaphore` to limit concurrency.

## Health

```python
from steadypy import HealthRegistry

health = HealthRegistry(default_timeout=30)
health.register("database")
health.register("cache", critical=False)

health.heartbeat("database", details={"pool_size": 10})  # from the worker, as it makes progress
health.mark_unhealthy("cache", "connection refused")  # explicit failure, until the next heartbeat

report = health.status()
report.healthy  # True when every critical component is healthy
report.degraded  # healthy overall, but a non-critical component is not
report.to_dict()  # JSON-ready; serve it from whatever HTTP framework you use
```

A component is healthy if it sent a heartbeat within its timeout. Before its
first heartbeat it is unhealthy. That suits a **readiness** check, but a
**liveness** check (one that restarts the process) should allow for a slow
start. A registry with no critical components reports healthy. steadypy does
not run an HTTP server; it gives you the data.

## Graceful shutdown

```python
from steadypy import ShutdownSignal

shutdown = ShutdownSignal()
shutdown.install_signal_handlers()  # SIGINT and SIGTERM by default
try:
    reason = await shutdown.wait()  # e.g. "received SIGTERM"
finally:
    shutdown.remove_signal_handlers()  # restores the previous handlers
```

`trigger(reason)` works the same everywhere: from an admin endpoint, a failed
health check, or a test. Signal handlers use
[`loop.add_signal_handler`](https://docs.python.org/3/library/asyncio-eventloop.html#unix-signals)
where available (Unix) and fall back to `signal.signal` otherwise, so nothing
else in the library depends on the platform. Handlers that were set with
`signal.signal` are restored on removal. Installing over a handler that was
registered with `loop.add_signal_handler` is refused, because that one could
not be restored.

**How long can shutdown take?** `supervisor.stop()` cancels every task and
then waits for each one's cleanup to finish. That is what makes shutdown
clean, but a task that ignores cancellation will keep the process waiting.
[`graceful_app.py`](examples/graceful_app.py) shows a grace period, built
with `asyncio.timeout(None)` and `reschedule()`, that cancels slow workers
again when it expires. Nothing in-process can force-stop a task that keeps
ignoring cancellation. The hard limit is your process manager's `SIGKILL`
(for example, Kubernetes' `terminationGracePeriodSeconds`).

## Threads, event loops and logging

- **One event loop, one thread.** Every steadypy object is meant to be used
  from the event loop's thread. None of them is thread-safe. From another
  thread (for example, a worker started with `asyncio.to_thread`), hand calls
  to the loop: `loop.call_soon_threadsafe(health.heartbeat, "db")`.
- `RateLimiter` and `ShutdownSignal` contain asyncio primitives that bind to
  the first event loop that uses them. Create them per loop, not as
  module-level globals shared between loops (or between tests that each run
  their own loop). A `TaskSupervisor` is entered once, by one task.
- **Logging** goes through standard `logging`. steadypy never configures
  handlers.

  | Logger | Level | When |
  |---|---|---|
  | `steadypy.supervisor` | WARNING | a task is restarted, or reaches its restart limit |
  | `steadypy.supervisor` | ERROR (with traceback) | a task fails for good |
  | `steadypy.circuit_breaker` | WARNING | the circuit opens or re-opens; a probe is lost; `async with` exits in another task |
  | `steadypy.circuit_breaker` | INFO | half-open, closed |
  | `steadypy.shutdown` | INFO | shutdown requested, with the reason |
  | `steadypy.retry` | DEBUG | each retry |

## Why not just use asyncio directly?

You should, wherever it is enough. steadypy does not replace asyncio. It
builds reusable *policies* on top of asyncio's own primitives (`TaskGroup`-style
structured concurrency, `asyncio.timeout`, `Event`, `Lock`) and works
alongside them. You can put a `TaskSupervisor` inside an `asyncio.timeout()`,
or run `bounded_map` inside a supervised task.

What the standard library deliberately leaves to you:

- `TaskGroup` has no restart policies, no "keep the other tasks running"
  mode, and no per-task state to inspect.
- `gather()` and `TaskGroup` start every task up front. Bounding concurrency
  over a large or lazy input, and cleaning up on the first error, takes
  careful code.
- Correct retry, circuit-breaker and rate-limit logic is short but easy to
  get subtly wrong: retrying through a cancellation, letting a stale result
  flip a circuit's state, waking rate-limit waiters out of order.
- Since Python 3.11, cancellation has bookkeeping (`Task.cancelling()`,
  `Task.uncancel()`) that a supervisor needs to get right so that it composes
  with `asyncio.timeout()`.

If one `TaskGroup` and a `try/except` cover your service, you don't need this
library. When you find yourself writing the same restart loop, backoff
function and shutdown handler again, steadypy is the tested version.

## Design principles

- **Cancellation is respected.** `CancelledError` is never retried, never
  turned into a restart, and never swallowed. Every task steadypy starts is
  awaited before control returns to you. The one place a cancellation is
  outranked is the `TaskGroup` rule above: a supervisor whose tasks failed
  raises their `ExceptionGroup` instead.
- **Structured.** Supervised tasks, `bounded_map` calls and stream workers
  never outlive the block or call that started them.
- **Your exceptions, not wrappers.** steadypy re-raises the same exception
  objects your code raised, adding
  [notes](https://docs.python.org/3/library/exceptions.html#BaseException.add_note)
  for context. It defines its own exceptions only where it is the one
  refusing something (`CircuitOpenError`).
- **Deterministic time.** Every timing decision goes through an injectable
  `Clock`, so behavior can be tested exactly, without sleeping.
- **Small and dependency-free.** No runtime dependencies, no background
  threads, no global state, no configuration files.
- **Typed.** Ships `py.typed`; the source passes `mypy --strict`.

## Testing

Every primitive with a notion of time accepts `clock=`. In your own tests,
pass `steadypy.testing.FakeClock` and move time forward explicitly. This
example uses [pytest-asyncio](https://pypi.org/project/pytest-asyncio/):

```python
import asyncio

import pytest

from steadypy import retry
from steadypy.testing import FakeClock


async def always_fails() -> None:
    raise ConnectionError("still down")


@pytest.mark.asyncio
async def test_gives_up_after_three_attempts() -> None:
    clock = FakeClock()
    task = asyncio.create_task(retry(always_fails, attempts=3, clock=clock))
    await clock.advance(60)  # every backoff sleep completes instantly
    with pytest.raises(ConnectionError):
        await task
```

To run steadypy's own checks:

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest                    # the full suite runs in about a second, no real sleeps
ruff check . && ruff format --check .
mypy src
```

## Contributing

Bug reports, questions and pull requests are welcome. Please read
[CONTRIBUTING.md](CONTRIBUTING.md) first. In short: behavior changes come with
tests, especially around cancellation, and tests use `FakeClock` rather than
real sleeps. Please report security issues privately as described in
[SECURITY.md](SECURITY.md).

## Roadmap

Ideas under consideration, not promises:

- A first PyPI release once the API has had some outside use.
- Windows in CI. The signal-handling fallback is unit-tested, but the suite
  does not run on Windows yet.
- A callback when a task fails for good, so isolated (`fail_fast=False`)
  supervisors can react without polling `supervisor.errors`.
- Hooks for metrics (restarts, state changes, rejections) that can feed
  Prometheus or OpenTelemetry without depending on them.
- Optional jitter for supervisor restart backoff.
- A "one-for-all" restart strategy (restart every task when one fails), as in
  Erlang/OTP supervisors.

## Related projects

steadypy borrows well-known ideas and does not claim to have invented any of
them. Supervision trees come from Erlang/OTP; the circuit breaker comes from
Michael Nygard's *Release It!*, popularized by
[Martin Fowler](https://martinfowler.com/bliki/CircuitBreaker.html);
structured concurrency comes from
[Trio](https://vorpus.org/blog/notes-on-structured-concurrency-or-go-statement-considered-harmful/)
and Python's own `TaskGroup`. If you only need one piece, these mature
libraries are worth a look:

- [tenacity](https://github.com/jd/tenacity) and [stamina](https://github.com/hynek/stamina): retrying
- [aiolimiter](https://github.com/mjpieters/aiolimiter): rate limiting for asyncio
- [aiometer](https://github.com/florimondmanca/aiometer): running many async jobs with concurrency and rate limits
- [AnyIO](https://github.com/agronholm/anyio): structured concurrency on top of asyncio or Trio

## License

[MIT](LICENSE)
