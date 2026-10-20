from __future__ import annotations

import asyncio
import gc
import logging
import weakref
from collections.abc import AsyncIterator

import pytest

from steadypy import CircuitBreaker, CircuitOpenError, CircuitState, SteadyError
from steadypy.testing import FakeClock


class Unavailable(Exception):
    pass


async def ok() -> str:
    await asyncio.sleep(0)
    return "ok"


async def broken() -> str:
    await asyncio.sleep(0)
    raise Unavailable("down")


async def trip(breaker: CircuitBreaker, times: int) -> None:
    for _ in range(times):
        with pytest.raises(Unavailable):
            await breaker.call(broken)


async def test_closed_breaker_passes_calls_through(clock: FakeClock) -> None:
    breaker = CircuitBreaker(clock=clock)
    assert await breaker.call(ok) == "ok"
    async with breaker:
        await ok()
    stats = breaker.stats
    assert stats.state is CircuitState.CLOSED
    assert (stats.successes, stats.failures, stats.rejections) == (2, 0, 0)


async def test_opens_after_consecutive_failures_and_rejects_fast(clock: FakeClock) -> None:
    breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=30, name="db", clock=clock)
    await trip(breaker, 3)
    assert breaker.state is CircuitState.OPEN

    called = False

    async def never() -> None:
        nonlocal called
        called = True

    await clock.advance(10)
    with pytest.raises(CircuitOpenError) as info:
        await breaker.call(never)
    assert not called
    assert info.value.name == "db"
    assert info.value.retry_after == pytest.approx(20)
    assert "circuit 'db' is open" in str(info.value)
    assert isinstance(info.value, SteadyError)
    assert breaker.stats.rejections == 1


async def test_success_resets_the_consecutive_failure_count(clock: FakeClock) -> None:
    breaker = CircuitBreaker(failure_threshold=3, clock=clock)
    await trip(breaker, 2)
    await breaker.call(ok)
    await trip(breaker, 2)
    assert breaker.state is CircuitState.CLOSED
    assert breaker.stats.consecutive_failures == 2


async def test_only_configured_exceptions_count_as_failures(clock: FakeClock) -> None:
    breaker = CircuitBreaker(failure_threshold=1, failure_exceptions=Unavailable, clock=clock)
    for _ in range(3):
        with pytest.raises(ValueError, match="bad input"):
            async with breaker:
                raise ValueError("bad input")
    assert breaker.state is CircuitState.CLOSED
    assert breaker.stats.successes == 3


async def test_becomes_half_open_after_the_recovery_timeout(clock: FakeClock) -> None:
    breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=5, clock=clock)
    await trip(breaker, 1)
    await clock.advance(4.9)
    assert breaker.state is CircuitState.OPEN
    assert breaker.retry_after == pytest.approx(0.1)
    await clock.advance(0.1)
    assert breaker.state is CircuitState.HALF_OPEN
    assert breaker.retry_after == 0.0


async def test_successful_probe_closes_the_circuit(clock: FakeClock) -> None:
    breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=5, clock=clock)
    await trip(breaker, 1)
    await clock.advance(5)
    assert await breaker.call(ok) == "ok"
    assert breaker.state is CircuitState.CLOSED
    assert breaker.stats.consecutive_failures == 0


async def test_failed_probe_reopens_and_restarts_the_timer(clock: FakeClock) -> None:
    breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=5, clock=clock)
    await trip(breaker, 1)
    await clock.advance(7)
    await trip(breaker, 1)
    assert breaker.state is CircuitState.OPEN
    assert breaker.retry_after == pytest.approx(5)
    assert breaker.stats.consecutive_failures == 2


async def test_reset_closes_and_keeps_lifetime_counters(clock: FakeClock) -> None:
    breaker = CircuitBreaker(failure_threshold=2, clock=clock)
    await trip(breaker, 2)
    breaker.reset()
    stats = breaker.stats
    assert stats.state is CircuitState.CLOSED
    assert stats.consecutive_failures == 0
    assert stats.failures == 2
    assert await breaker.call(ok) == "ok"


async def test_call_forwards_arguments(clock: FakeClock) -> None:
    async def greet(name: str, *, punctuation: str) -> str:
        return f"hello {name}{punctuation}"

    breaker = CircuitBreaker(clock=clock)
    assert await breaker.call(greet, "ada", punctuation="!") == "hello ada!"


async def test_nested_use_in_one_task_records_each_call(clock: FakeClock) -> None:
    breaker = CircuitBreaker(failure_threshold=5, clock=clock)
    with pytest.raises(Unavailable):
        async with breaker:
            async with breaker:
                await ok()
            raise Unavailable
    stats = breaker.stats
    assert (stats.successes, stats.failures) == (1, 1)


async def test_state_change_callback_sees_every_transition(
    clock: FakeClock, caplog: pytest.LogCaptureFixture
) -> None:
    transitions: list[tuple[CircuitState, CircuitState]] = []

    def on_change(old: CircuitState, new: CircuitState) -> None:
        transitions.append((old, new))
        raise RuntimeError("buggy callback")

    caplog.set_level(logging.INFO, logger="steadypy.circuit_breaker")
    breaker = CircuitBreaker(
        failure_threshold=1, recovery_timeout=1, on_state_change=on_change, clock=clock
    )
    await trip(breaker, 1)
    await clock.advance(1)
    await breaker.call(ok)
    assert transitions == [
        (CircuitState.CLOSED, CircuitState.OPEN),
        (CircuitState.OPEN, CircuitState.HALF_OPEN),
        (CircuitState.HALF_OPEN, CircuitState.CLOSED),
    ]
    assert sum("callback" in r.getMessage() for r in caplog.records) == 3


async def test_repr_shows_name_and_state(clock: FakeClock) -> None:
    breaker = CircuitBreaker(name="api", clock=clock)
    assert breaker.name == "api"
    assert "'api' state=closed" in repr(breaker)


@pytest.mark.parametrize(
    ("kwargs", "error"),
    [
        ({"failure_threshold": 0}, ValueError),
        ({"recovery_timeout": -1}, ValueError),
        ({"half_open_max_calls": 0}, ValueError),
        ({"failure_exceptions": KeyboardInterrupt}, TypeError),
    ],
)
def test_invalid_arguments_are_rejected(kwargs: dict[str, object], error: type) -> None:
    with pytest.raises(error):
        CircuitBreaker(**kwargs)  # type: ignore[arg-type]


# -- concurrency --------------------------------------------------------------


async def test_half_open_admits_a_single_probe_among_concurrent_callers(clock: FakeClock) -> None:
    breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=1, clock=clock)
    await trip(breaker, 1)
    await clock.advance(1)

    release = asyncio.Event()
    admitted = 0

    async def caller() -> str:
        nonlocal admitted
        async with breaker:
            admitted += 1
            await release.wait()
        return "probe"

    tasks = [asyncio.create_task(caller()) for _ in range(10)]
    await asyncio.sleep(0)
    assert admitted == 1
    release.set()
    results = await asyncio.gather(*tasks, return_exceptions=True)
    assert results.count("probe") == 1
    rejected = [r for r in results if isinstance(r, CircuitOpenError)]
    assert len(rejected) == 9
    assert all(r.retry_after == 0.0 for r in rejected)
    assert breaker.state is CircuitState.CLOSED


async def test_half_open_max_calls_limits_parallel_probes(clock: FakeClock) -> None:
    breaker = CircuitBreaker(
        failure_threshold=1, recovery_timeout=1, half_open_max_calls=3, clock=clock
    )
    await trip(breaker, 1)
    await clock.advance(1)
    release = asyncio.Event()

    async def caller() -> None:
        async with breaker:
            await release.wait()

    tasks = [asyncio.create_task(caller()) for _ in range(5)]
    await asyncio.sleep(0)
    release.set()
    results = await asyncio.gather(*tasks, return_exceptions=True)
    assert sum(isinstance(r, CircuitOpenError) for r in results) == 2


async def test_cancelled_probe_frees_its_slot(clock: FakeClock) -> None:
    breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=1, clock=clock)
    await trip(breaker, 1)
    await clock.advance(1)

    async def hangs() -> None:
        async with breaker:
            await asyncio.Event().wait()

    probe = asyncio.create_task(hangs())
    await asyncio.sleep(0)
    with pytest.raises(CircuitOpenError):
        await breaker.call(ok)
    probe.cancel()
    with pytest.raises(asyncio.CancelledError):
        await probe
    assert breaker.state is CircuitState.HALF_OPEN
    assert await breaker.call(ok) == "ok"
    assert breaker.state is CircuitState.CLOSED


async def test_cancellation_while_closed_is_neutral(clock: FakeClock) -> None:
    breaker = CircuitBreaker(failure_threshold=1, clock=clock)

    async def hangs() -> None:
        await breaker.call(asyncio.Event().wait)

    task = asyncio.create_task(hangs())
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    stats = breaker.stats
    assert stats.state is CircuitState.CLOSED
    assert (stats.successes, stats.failures) == (0, 0)


async def test_concurrent_failures_open_the_circuit_exactly_once(clock: FakeClock) -> None:
    transitions: list[CircuitState] = []
    breaker = CircuitBreaker(
        failure_threshold=3,
        recovery_timeout=10,
        on_state_change=lambda old, new: transitions.append(new),
        clock=clock,
    )
    early, late = asyncio.Event(), asyncio.Event()

    async def failing_call(release: asyncio.Event) -> None:
        async with breaker:
            await release.wait()
            raise Unavailable

    tasks = [asyncio.create_task(failing_call(early)) for _ in range(3)]
    tasks += [asyncio.create_task(failing_call(late)) for _ in range(5)]
    await asyncio.sleep(0)
    early.set()
    await clock.advance(4)
    assert breaker.state is CircuitState.OPEN
    late.set()  # five more failures from calls admitted before the circuit opened
    results = await asyncio.gather(*tasks, return_exceptions=True)

    assert all(isinstance(r, Unavailable) for r in results)
    assert transitions == [CircuitState.OPEN]
    assert breaker.stats.failures == 8
    # The late failures did not push the reopen time back from t=0 to t=4.
    assert breaker.retry_after == pytest.approx(6)


async def test_stale_success_cannot_close_an_open_circuit(clock: FakeClock) -> None:
    breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=10, clock=clock)
    release_slow = asyncio.Event()

    async def slow_success() -> str:
        async with breaker:
            await release_slow.wait()
        return "late"

    slow = asyncio.create_task(slow_success())
    await asyncio.sleep(0)
    await trip(breaker, 1)  # admitted after the slow call; opens the circuit
    release_slow.set()
    assert await slow == "late"
    assert breaker.state is CircuitState.OPEN
    assert breaker.stats.successes == 1


async def test_calls_in_flight_during_reset_do_not_affect_state(clock: FakeClock) -> None:
    breaker = CircuitBreaker(failure_threshold=1, clock=clock)
    release = asyncio.Event()

    async def fails_later() -> None:
        async with breaker:
            await release.wait()
            raise Unavailable

    task = asyncio.create_task(fails_later())
    await asyncio.sleep(0)
    breaker.reset()
    release.set()
    with pytest.raises(Unavailable):
        await task
    assert breaker.state is CircuitState.CLOSED


# -- calls that leave their task, hang, or time out ---------------------------------


async def test_guard_records_the_outcome_when_exited_in_another_task(clock: FakeClock) -> None:
    breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=1, clock=clock)
    await trip(breaker, 1)
    await clock.advance(1)

    async def paginate() -> AsyncIterator[str]:
        async with breaker.guard():
            yield "page 1"  # suspended inside the protected block

    pages = paginate()
    assert await anext(pages) == "page 1"
    await asyncio.create_task(pages.aclose())  # exits the block in another task
    # The probe slot was released, so the next call is admitted as a probe.
    assert await breaker.call(ok) == "ok"
    assert breaker.state is CircuitState.CLOSED


async def test_async_with_exited_in_another_task_neither_raises_nor_jams(
    clock: FakeClock, caplog: pytest.LogCaptureFixture
) -> None:
    breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=1, clock=clock)
    await trip(breaker, 1)
    await clock.advance(1)

    async def paginate() -> AsyncIterator[str]:
        async with breaker:
            yield "page 1"

    suspended: list[AsyncIterator[str]] = []

    async def enter_in_another_task() -> None:
        pages = paginate()
        await anext(pages)
        suspended.append(pages)

    entering = asyncio.create_task(enter_in_another_task())
    await entering
    await asyncio.create_task(suspended[0].aclose())  # no KeyError
    assert "different task" in caplog.text

    # The lost probe's slot is reclaimed once recovery_timeout has passed...
    with pytest.raises(CircuitOpenError):
        await breaker.call(ok)
    await clock.advance(1.5)
    assert await breaker.call(ok) == "ok"
    # ...and the breaker does not keep the entering task alive.
    task_ref = weakref.ref(entering)
    del entering
    gc.collect()
    assert task_ref() is None


async def test_a_hanging_probe_does_not_keep_the_circuit_half_open(clock: FakeClock) -> None:
    breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=5, clock=clock)
    await trip(breaker, 1)
    await clock.advance(5)
    hung = asyncio.create_task(breaker.call(asyncio.Event().wait))  # a probe with no timeout
    await asyncio.sleep(0)
    with pytest.raises(CircuitOpenError):
        await breaker.call(ok)
    await clock.advance(5.5)
    assert await breaker.call(ok) == "ok"  # the hung probe's slot was given away
    assert breaker.state is CircuitState.CLOSED
    hung.cancel()
    with pytest.raises(asyncio.CancelledError):
        await hung


async def test_a_guard_can_only_be_used_once(clock: FakeClock) -> None:
    guard = CircuitBreaker(clock=clock).guard()
    async with guard:
        pass
    with pytest.raises(RuntimeError, match="only be used once"):
        async with guard:
            pass


async def test_a_timeout_counts_as_a_failure_only_inside_the_breaker() -> None:
    outside = CircuitBreaker(failure_threshold=2)
    inside = CircuitBreaker(failure_threshold=2)
    for _ in range(2):
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0), outside:  # cancellation reaches the breaker
                await asyncio.Event().wait()
        with pytest.raises(TimeoutError):
            async with inside, asyncio.timeout(0):  # TimeoutError reaches the breaker
                await asyncio.Event().wait()
    assert outside.state is CircuitState.CLOSED
    assert inside.state is CircuitState.OPEN
