# Concepts

This page explains the ideas steadypy is built on and the exact behavior you
can rely on. None of these ideas are new. Where a concept comes from
somewhere else, the original source is linked.

- [Structured concurrency](#structured-concurrency)
- [Cancellation](#cancellation)
- [Supervision](#supervision)
- [Retry vs restart](#retry-vs-restart)
- [Circuit breaker behavior](#circuit-breaker-behavior)
- [Rate limiting](#rate-limiting)
- [Health and heartbeats](#health-and-heartbeats)
- [Shutdown](#shutdown)
- [Time and testing](#time-and-testing)

## Structured concurrency

With `asyncio.create_task()` alone, a task can outlive the code that started
it. Its exceptions can go unnoticed, and nothing guarantees it is cleaned up
at shutdown. *Structured concurrency* fixes this by tying every task to a
scope (a block of code) that does not exit until all of its tasks have
finished. The idea is laid out in Nathaniel J. Smith's
[Notes on structured concurrency](https://vorpus.org/blog/notes-on-structured-concurrency-or-go-statement-considered-harmful/)
and is how Python's own
[`asyncio.TaskGroup`](https://docs.python.org/3/library/asyncio-task.html#task-groups)
works.

steadypy applies the same rule everywhere it starts tasks:

| Primitive | Scope | On exit |
|---|---|---|
| `TaskSupervisor` | the `async with` block | waits for every task; `stop()` cancels them first |
| `bounded_map` | the call | every started call has finished (or was cancelled and awaited) |
| `bounded_stream` | the `async with` block | calls still in flight are cancelled and awaited |

"Awaited" matters. A cancelled task may still be running its `finally:`
blocks, closing connections or flushing buffers. steadypy waits for that
cleanup instead of abandoning the task.

## Cancellation

asyncio cancels a task by throwing
[`asyncio.CancelledError`](https://docs.python.org/3/library/asyncio-task.html#task-cancellation)
into it at its current `await`. Since Python 3.8 `CancelledError` derives
from `BaseException`, so `except Exception:` does not catch it by accident.
Python 3.11 added bookkeeping for cancellation *requests*:

- [`Task.cancelling()`](https://docs.python.org/3/library/asyncio-task.html#asyncio.Task.cancelling)
  counts the requests pending on a task.
- [`Task.uncancel()`](https://docs.python.org/3/library/asyncio-task.html#asyncio.Task.uncancel)
  withdraws one. `asyncio.timeout()` and `TaskGroup` use it to tell their own
  cancellations apart from someone else's.

steadypy follows these rules:

1. **Cancellation is never retried or restarted.** A requested cancellation
   always propagates. It is never retried, never counted as a
   circuit-breaker failure, and never triggers a supervisor restart.
2. **A swallowed cancellation still counts.** If code inside a retry attempt
   or a supervised task catches `CancelledError` and raises or returns
   something else, the request is still pending (`cancelling() > 0`).
   steadypy notices and does not retry or restart.
3. **Internal cancellations are withdrawn.** In fail-fast mode the supervisor
   cancels the body of its `async with` block to stop it; when the block
   exits, it calls `uncancel()` for exactly that request. A cancellation from
   outside, such as an enclosing `asyncio.timeout()`, is left alone and
   propagates.
4. **Failures outrank a cancellation.** This follows `TaskGroup`. If
   supervised tasks failed, the supervisor raises their `ExceptionGroup` even
   when a cancellation was requested, so an enclosing `asyncio.timeout()` sees
   the group rather than a `TimeoutError`. On Python 3.13+ the supervisor
   re-arms the request (`uncancel()` + `cancel()`, as `TaskGroup` does there),
   so a cancellation nobody else will withdraw still arrives at the next
   `await`. On 3.11 and 3.12 that is not safe: `uncancel()` there cannot take
   back a re-armed request, which would hit the next `await` even after a
   timeout withdrew it. So, like `TaskGroup` on those versions, the request is
   absorbed into the group.
5. **An unrequested `CancelledError` is not a cancellation.** A task can end
   with `CancelledError` although nobody cancelled it, for example by awaiting
   a future that other code cancelled. For a supervised task with a restart
   policy, that counts as a failed run and the task is restarted.
6. **Cleanup is awaited, even under repeated cancellation.** If the caller is
   cancelled again while steadypy is waiting for children to finish their
   cleanup, it keeps waiting and re-raises the cancellation afterwards, as
   `TaskGroup` does.

What this asks of your code is what asyncio asks of all code: let
`CancelledError` propagate (clean up in `finally:` or re-raise it from an
`except` block), and do not block cancellation indefinitely. A task that
never finishes its cleanup will keep its supervisor waiting.

## Supervision

`TaskSupervisor` takes the basic shape of `TaskGroup` and adds ideas from
[Erlang/OTP supervisors](https://www.erlang.org/doc/system/sup_princ.html):

**Restart modes.** Each task has a restart mode:

| `restart=` | Restarted when the task... |
|---|---|
| `False` / `"never"` | never |
| `True` / `"on-failure"` | raises an exception |
| `"always"` | raises or returns |

Cancellation never causes a restart, and nothing is restarted once the
supervisor is stopping.

**Restart intensity.** A task that fails over and over is usually broken,
not unlucky. `max_restarts` limits how many restarts are allowed and
`restart_window` turns that into a rate ("5 restarts per 60 seconds"),
similar to OTP's *intensity* and *period*. Once the limit is hit, the next
exit is final. Without a window the limit counts every restart in the task's
lifetime, which suits batch jobs better than services.

**Backoff.** Restarts wait `restart_delay`, doubling each time up to
`max_restart_delay`. With a window, the backoff exponent counts only recent
restarts, so a task that has been stable for longer than the window starts
again from the shortest delay.

**Failure policy.** A task *fails for good* when it raises and will not be
restarted. With `fail_fast=True` (the default), that cancels its siblings and
the supervisor's body, like `TaskGroup`. With `fail_fast=False` the failure is
recorded and the other tasks keep running. In both modes the failures are
raised together as an
[`ExceptionGroup`](https://docs.python.org/3/library/exceptions.html#exception-groups)
when the block exits, each annotated with the failing task's name (see
[PEP 654](https://peps.python.org/pep-0654/) for `except*`).

**Memory.** A supervisor keeps a handle for each task it started, so final
states can be inspected after the block. Finished tasks release their
callable and arguments, but the handles themselves accumulate. For an
unbounded stream of short-lived tasks (one per connection), use
`retain_finished=False`, and catch expected errors inside the task, since
failures are kept until the block exits.

**Why restartable tasks are callables.** A coroutine object can only be
awaited once. To run a task again, steadypy must create a new coroutine,
which means calling a function. `start(worker(), restart=True)` is therefore
rejected with an explanation; `start(worker, restart=True)` works.

## Retry vs restart

Both re-run something that failed, but at different levels:

| | `retry` | supervisor restart |
|---|---|---|
| Re-runs | one operation (a request, a query) | a whole long-running task (a consumer loop, a connection) |
| Typical delays | milliseconds to seconds | seconds to tens of seconds |
| On exhaustion | raises to the caller, who decides what to do | the task fails for good; the failure policy applies |

Note that `retry` retries every `Exception` unless you pass `retry_on`:
programming errors included. Narrow it to the transient errors you expect.

They compose. Retry individual operations inside a worker, and let the
supervisor restart the worker if something escapes. Be careful when stacking
retries across layers: three layers that each make 5 attempts can produce up
to 125 calls against a dependency that is already struggling. Retry at one
layer and fail fast at the others, or put a circuit breaker in front of the
dependency.

## Circuit breaker behavior

The pattern is from Michael Nygard's *Release It!*, described in Martin
Fowler's [CircuitBreaker](https://martinfowler.com/bliki/CircuitBreaker.html):

```text
            failure_threshold consecutive failures
  CLOSED ───────────────────────────────────────────▶ OPEN
    ▲                                                   │
    │ probe succeeds                                    │ recovery_timeout elapses
    │                                                   ▼
    └───────────────────────────────────────────── HALF_OPEN
                 probe fails ──▶ back to OPEN (timer restarts)
```

How a call's outcome is counted:

| The protected call... | Counts as | Effect |
|---|---|---|
| returns | success | resets the failure streak; a successful probe closes the circuit |
| raises one of `failure_exceptions` | failure | extends the streak; may open the circuit; a failed probe re-opens it |
| raises any other `Exception` | success | the dependency responded; your exception still propagates |
| is cancelled | neither | no counters change; a probe slot is released |

Because cancellation is neutral, a timeout must sit *inside* the protected
block to count. With `async with breaker: async with asyncio.timeout(5): ...`,
a hanging dependency raises `TimeoutError`, which counts as a failure. With
the timeout outside, the breaker only sees a cancellation, and a dependency
that always hangs never trips it.

While open, calls are rejected with `CircuitOpenError` without running.
`exc.retry_after` gives the seconds until a probe is admitted. While
half-open, at most `half_open_max_calls` probes run at once; other callers
are rejected with `retry_after == 0`. A probe slot is a lease: a probe that
has not reported back within `recovery_timeout` is considered lost, and its
slot goes to the next caller. A single hung probe therefore cannot keep the
circuit half-open forever.

**Entering and exiting in different tasks.** `async with breaker:` tracks
calls per task, so it must be exited in the task that entered it. An async
generator suspended inside the block breaks that when it is closed from
another task, for example by the event loop's cleanup. For such code use
`async with breaker.guard():`, a single-use context manager that carries its
own state. (If `async with breaker:` is exited in another task anyway, the
outcome is treated as neutral and logged, and any probe slot it held is
reclaimed by the lease.)

**Generations.** Every state transition increments a generation counter, and
each admitted call remembers the generation it was admitted in. Only results
from the current generation can change the state. This prevents classic races
between concurrent callers. When ten slow calls fail after the circuit has
already opened, the open period is not extended ten times. A success from a
call admitted before the circuit opened cannot close it. Calls in flight
during `reset()` do not undo the reset.

**With retry.** Put the breaker *inside* the retried operation, and make sure
`retry_on` does not include `CircuitOpenError`. The default,
`retry_on=Exception`, does include it. Rejected calls then fail fast instead
of being retried against an open circuit.

## Rate limiting

`RateLimiter` is a [token bucket](https://en.wikipedia.org/wiki/Token_bucket).
The bucket holds up to `burst` tokens (default: `rate`, at least 1) and refills
continuously at `rate / per` tokens per second. Each `acquire(n)` takes `n`
tokens, waiting for them if needed. The bucket starts full, so a service may
burst immediately and is then held to the average rate.

The precise guarantee: in any window of *W* seconds, at most
`burst + rate / per × W` tokens are taken. `RateLimiter(rate=5)` therefore
admits up to 10 calls in the first second after an idle period, and 5 per
second after that. If the API you call enforces "at most *N* in any one
second" strictly, set `burst=1`, so calls are evenly spaced, and leave
yourself some margin.

- Waiters are served in arrival order, so a request for many tokens is not
  starved by a stream of small requests.
- Cancelling a waiter takes no tokens and lets the next waiter proceed.
- `try_acquire()` never waits, and returns `False` when other tasks are
  already waiting, rather than jumping the queue.
- Time comes from a monotonic clock, so wall-clock adjustments (an NTP step,
  someone setting the system time) never release or withhold tokens.

This limits *your* process. It cannot stand in for the server-side limits of
the API you call. For limits shared across processes, use something shared,
such as Redis.

## Health and heartbeats

A heartbeat says "this component is alive *and making progress*". The worker
calls `health.heartbeat(name)` each time it completes a unit of work, so a
component that is running but stuck (blocked on a dead connection, say)
stops sending heartbeats and becomes unhealthy after its `timeout`.

- **Readiness vs liveness.** Components are unhealthy until their first
  heartbeat, so a freshly started service is not reported *ready* before it
  has done anything. A *liveness* check, one that restarts the process when
  it fails, should not use that same signal during startup. Give it a grace
  period, or only start acting on it after the first healthy report.
  Otherwise a slow start leads to a restart loop.
- **No critical components** means overall health is `True`.
- **Critical vs non-critical.** Overall health considers only critical
  components. An unhealthy non-critical component (an optional cache) makes
  the report `degraded` instead.
- **Explicit failures.** `mark_unhealthy(name, reason)` reports a known
  failure right away, until the next heartbeat.
- **Clocks.** Staleness is measured on the monotonic clock. The
  `last_heartbeat` timestamp is wall-clock UTC, for display only.

## Shutdown

A typical shutdown sequence:

1. `SIGTERM` arrives (from systemd, Kubernetes, `docker stop`, or Ctrl-C as `SIGINT`).
2. `ShutdownSignal` is triggered and records the reason; every `wait()` returns.
3. The service calls `supervisor.stop()`. The supervisor cancels its tasks and
   waits for their cleanup.
4. Optionally, a grace period: an outer `asyncio.timeout()`, armed with
   `reschedule()` when shutdown began (see `examples/graceful_app.py`). When
   it expires, the supervisor cancels its tasks *again*, which interrupts
   cleanup code that is awaiting something, and the block raises
   `TimeoutError`.

What no in-process mechanism can do is force-stop a task that keeps catching
and ignoring `CancelledError`. The supervisor will wait for it, because
abandoning it would break structured concurrency. The real upper bound on
shutdown time is the process manager's `SIGKILL` after its own grace period
(for example, Kubernetes' `terminationGracePeriodSeconds`).

Signal handlers are installed with
[`loop.add_signal_handler`](https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.loop.add_signal_handler)
on event loops that support it (Unix). Other loops fall back to
`signal.signal`, with a handler that hands the work to the loop via
`call_soon_threadsafe`. `remove_signal_handlers()` restores whatever handler
was set before with `signal.signal`. An event loop keeps handlers registered
with `loop.add_signal_handler()` to itself, so they could not be restored.
Installing over one, whether the application's own or another
`ShutdownSignal`'s, is therefore refused with a `RuntimeError`.

**Second Ctrl-C.** While steadypy's handlers are installed, further signals
are ignored: shutdown is already under way. Call
`shutdown.remove_signal_handlers()` as soon as `wait()` returns to restore
the previous handler. Under `asyncio.run()` the next Ctrl-C then cancels the
main task, which makes a supervisor cancel its tasks again; that ends cleanup
code that is merely slow. A further Ctrl-C raises `KeyboardInterrupt`, which
is `asyncio.run()`'s usual behavior.

## Time and testing

Timing decisions in steadypy (backoff sleeps, deadlines, the circuit
recovery timeout, token refills, heartbeat staleness) all go through a
`Clock`:

```python
class Clock(Protocol):
    def monotonic(self) -> float: ...
    async def sleep(self, delay: float) -> None: ...
```

The default `SystemClock` uses `time.monotonic()` and `asyncio.sleep()`.
`steadypy.testing.FakeClock` only moves when the test calls
`await clock.advance(seconds)`. Sleepers wake in deadline order, and the event
loop gets a chance to run between wake-ups. steadypy's own test suite uses it
throughout, which is why the suite has no real sleeps and finishes in about a
second.
