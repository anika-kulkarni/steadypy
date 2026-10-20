from __future__ import annotations

import asyncio
import functools
import importlib
import random

import pytest

from steadypy import RetryAttempt, async_retry, retry
from steadypy.testing import FakeClock


class Transient(Exception):
    pass


class Fatal(Exception):
    pass


class Flaky:
    """Fails ``failures`` times with ``exc``, then returns ``value``."""

    def __init__(self, failures: int, value: str = "ok", exc: type[Exception] = Transient) -> None:
        self.failures = failures
        self.value = value
        self.exc = exc
        self.calls = 0

    async def __call__(self) -> str:
        self.calls += 1
        await asyncio.sleep(0)
        if self.calls <= self.failures:
            raise self.exc(f"failure {self.calls}")
        return self.value


async def run_with_clock(clock: FakeClock, coro: object, advance: float = 1_000) -> object:
    task = asyncio.ensure_future(coro)  # type: ignore[arg-type]
    await clock.advance(advance)
    return await task


async def test_first_success_returns_without_sleeping(clock: FakeClock) -> None:
    func = Flaky(0)
    assert await retry(func, clock=clock) == "ok"
    assert func.calls == 1
    assert clock.monotonic() == 0


async def test_retries_with_exponential_backoff(clock: FakeClock) -> None:
    func = Flaky(3)
    seen: list[RetryAttempt] = []
    result = await run_with_clock(
        clock,
        retry(func, attempts=5, base_delay=0.5, jitter=0, on_retry=seen.append, clock=clock),
    )
    assert result == "ok"
    assert func.calls == 4
    assert [a.attempt for a in seen] == [1, 2, 3]
    assert [a.delay for a in seen] == [0.5, 1.0, 2.0]
    assert [a.elapsed for a in seen] == [0, 0.5, 1.5]
    assert all(isinstance(a.exception, Transient) for a in seen)


async def test_delays_are_capped_by_max_delay(clock: FakeClock) -> None:
    seen: list[RetryAttempt] = []
    await run_with_clock(
        clock,
        retry(
            Flaky(5),
            attempts=6,
            base_delay=1,
            max_delay=3,
            multiplier=3,
            jitter=0,
            on_retry=seen.append,
            clock=clock,
        ),
    )
    assert [a.delay for a in seen] == [1, 3, 3, 3, 3]


async def test_gives_up_after_the_last_attempt_with_a_note(clock: FakeClock) -> None:
    func = Flaky(10)
    with pytest.raises(Transient, match="failure 3") as info:
        await run_with_clock(clock, retry(func, attempts=3, jitter=0, clock=clock))
    assert func.calls == 3
    assert "steadypy.retry: gave up after 3 attempts" in info.value.__notes__


async def test_single_attempt_never_retries(clock: FakeClock) -> None:
    func = Flaky(1)
    with pytest.raises(Transient) as info:
        await retry(func, attempts=1, clock=clock)
    assert func.calls == 1
    assert info.value.__notes__ == ["steadypy.retry: gave up after 1 attempt"]


@pytest.mark.parametrize("jitter", [0.25, 0.5, 1.0])
async def test_jitter_stays_within_bounds(clock: FakeClock, jitter: float) -> None:
    rng = random.Random(1234)
    seen: list[RetryAttempt] = []
    # The package exports a function named ``retry``, which shadows the
    # submodule attribute, so fetch the module itself to patch its RNG.
    retry_module = importlib.import_module("steadypy.retry")
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(retry_module.random, "random", rng.random)
        await run_with_clock(
            clock,
            retry(
                Flaky(8),
                attempts=9,
                base_delay=1,
                max_delay=1,
                jitter=jitter,
                on_retry=seen.append,
                clock=clock,
            ),
        )
    delays = [a.delay for a in seen]
    assert all(1 - jitter <= d <= 1 for d in delays)
    assert len(set(delays)) > 1  # actually randomized


async def test_unlisted_exceptions_propagate_immediately(clock: FakeClock) -> None:
    func = Flaky(1, exc=Fatal)
    with pytest.raises(Fatal):
        await retry(func, retry_on=Transient, clock=clock)
    assert func.calls == 1


async def test_retry_on_accepts_a_tuple(clock: FakeClock) -> None:
    func = Flaky(2, exc=Fatal)
    result = await run_with_clock(clock, retry(func, retry_on=(Transient, Fatal), clock=clock))
    assert result == "ok"


async def test_retry_if_can_veto_a_retry(clock: FakeClock) -> None:
    func = Flaky(3)
    with pytest.raises(Transient, match="failure 2"):
        await run_with_clock(
            clock,
            retry(func, attempts=5, retry_if=lambda exc: "2" not in str(exc), clock=clock),
        )
    assert func.calls == 2


async def test_deadline_stops_retrying_before_it_is_exceeded(clock: FakeClock) -> None:
    func = Flaky(10)
    with pytest.raises(Transient) as info:
        await run_with_clock(
            clock,
            retry(func, attempts=10, base_delay=1, jitter=0, deadline=5, clock=clock),
        )
    # Sleeps of 1 + 2 bring us to t=3; the next sleep of 4 would end at t=7 > 5.
    assert func.calls == 3
    assert clock.monotonic() >= 3
    assert any("5s deadline" in note for note in info.value.__notes__)


async def test_async_on_retry_callback_is_awaited(clock: FakeClock) -> None:
    calls: list[int] = []

    async def on_retry(attempt: RetryAttempt) -> None:
        await asyncio.sleep(0)
        calls.append(attempt.attempt)

    await run_with_clock(clock, retry(Flaky(2), on_retry=on_retry, clock=clock))
    assert calls == [1, 2]


async def test_on_retry_errors_stop_retrying(clock: FakeClock) -> None:
    def on_retry(attempt: RetryAttempt) -> None:
        raise RuntimeError("callback failed")

    func = Flaky(2)
    with pytest.raises(RuntimeError, match="callback failed"):
        await retry(func, on_retry=on_retry, clock=clock)
    assert func.calls == 1


async def test_timeout_inside_an_attempt_is_retried(clock: FakeClock) -> None:
    calls = 0

    async def slow_then_fast() -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            async with asyncio.timeout(0):
                await asyncio.Event().wait()
        return "fast"

    # asyncio.timeout cancels and then un-cancels the task, so the TimeoutError
    # must be treated as an ordinary retryable failure, not as a cancellation.
    result = await run_with_clock(clock, retry(slow_then_fast, clock=clock))
    assert result == "fast"
    assert calls == 2


# -- cancellation -------------------------------------------------------------


async def test_cancellation_during_backoff_propagates_immediately(clock: FakeClock) -> None:
    func = Flaky(10)
    task = asyncio.create_task(
        retry(func, attempts=10, base_delay=60, max_delay=60, jitter=0, clock=clock)
    )
    await clock.advance(1)
    assert func.calls == 1
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await clock.advance(1_000)
    assert func.calls == 1


async def test_cancellation_during_an_attempt_is_never_retried(clock: FakeClock) -> None:
    started = asyncio.Event()
    calls = 0

    async def hangs() -> None:
        nonlocal calls
        calls += 1
        started.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(retry(hangs, attempts=5, clock=clock))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert calls == 1


async def test_swallowed_cancellation_is_not_retried(clock: FakeClock) -> None:
    started = asyncio.Event()
    calls = 0

    async def converts_cancellation() -> None:
        nonlocal calls
        calls += 1
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            raise Transient("connection closed during shutdown") from None

    task = asyncio.create_task(retry(converts_cancellation, attempts=5, clock=clock))
    await started.wait()
    task.cancel()
    with pytest.raises(Transient):
        await task
    assert calls == 1
    assert clock.pending_sleeps == 0


# -- argument validation ------------------------------------------------------


async def test_coroutine_objects_are_rejected_and_closed() -> None:
    coro = Flaky(0)()
    with pytest.raises(TypeError, match="not `fetch\\(\\)`"):
        await retry(coro)  # type: ignore[arg-type]
    assert coro.cr_frame is None


async def test_non_callables_are_rejected() -> None:
    with pytest.raises(TypeError, match="must be callable"):
        await retry(42)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("kwargs", "error"),
    [
        ({"attempts": 0}, ValueError),
        ({"attempts": True}, ValueError),
        ({"base_delay": -1}, ValueError),
        ({"base_delay": 2, "max_delay": 1}, ValueError),
        ({"multiplier": 0.5}, ValueError),
        ({"jitter": 1.5}, ValueError),
        ({"deadline": 0}, ValueError),
        ({"retry_on": asyncio.CancelledError}, TypeError),
        ({"retry_on": (Transient, "nope")}, TypeError),
    ],
)
async def test_invalid_arguments_are_rejected(kwargs: dict[str, object], error: type) -> None:
    with pytest.raises(error):
        await retry(Flaky(0), **kwargs)  # type: ignore[arg-type]


# -- decorator ----------------------------------------------------------------


async def test_decorator_with_arguments(clock: FakeClock) -> None:
    calls: list[tuple[int, str]] = []

    @async_retry(attempts=3, jitter=0, clock=clock)
    async def fetch(item: int, *, label: str) -> str:
        """Fetch an item."""
        calls.append((item, label))
        if len(calls) < 2:
            raise Transient
        return f"{label}-{item}"

    assert fetch.__name__ == "fetch"
    assert fetch.__doc__ == "Fetch an item."
    assert await run_with_clock(clock, fetch(7, label="x")) == "x-7"
    assert calls == [(7, "x"), (7, "x")]


async def test_bare_decorator_uses_defaults() -> None:
    calls = 0

    @async_retry
    async def fetch() -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise Transient
        return calls

    assert await fetch() == 2


async def test_each_call_of_a_decorated_function_retries_independently(clock: FakeClock) -> None:
    flaky = Flaky(1)

    @async_retry(attempts=2, clock=clock)
    async def fetch() -> str:
        return await flaky()

    assert await run_with_clock(clock, fetch()) == "ok"
    flaky.calls, flaky.failures = 0, 1
    assert await run_with_clock(clock, fetch()) == "ok"


async def test_decorator_validates_eagerly() -> None:
    with pytest.raises(ValueError, match="attempts"):
        async_retry(attempts=0)
    with pytest.raises(TypeError, match="only decorate callables"):
        async_retry()(42)  # type: ignore[arg-type]


async def test_partial_binds_arguments(clock: FakeClock) -> None:
    async def add(a: int, b: int) -> int:
        return a + b

    assert await retry(functools.partial(add, 2, 3), clock=clock) == 5


async def test_max_delay_defaults_to_at_least_the_base_delay(clock: FakeClock) -> None:
    seen: list[RetryAttempt] = []
    await run_with_clock(
        clock, retry(Flaky(2), base_delay=30, jitter=0, on_retry=seen.append, clock=clock)
    )
    assert [a.delay for a in seen] == [30, 30]
