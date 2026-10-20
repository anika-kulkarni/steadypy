from __future__ import annotations

import asyncio

import pytest

from steadypy import SystemClock
from steadypy.testing import FakeClock


async def test_system_clock_is_monotonic_and_sleeps() -> None:
    clock = SystemClock()
    before = clock.monotonic()
    await clock.sleep(0)
    assert clock.monotonic() >= before


async def test_fake_clock_only_moves_when_advanced(clock: FakeClock) -> None:
    assert clock.monotonic() == 0.0
    await asyncio.sleep(0)
    assert clock.monotonic() == 0.0
    await clock.advance(2.5)
    assert clock.monotonic() == 2.5


async def test_fake_clock_wakes_sleepers_in_deadline_order(clock: FakeClock) -> None:
    woke: list[tuple[str, float]] = []

    async def sleeper(label: str, delay: float) -> None:
        await clock.sleep(delay)
        woke.append((label, clock.monotonic()))

    tasks = [
        asyncio.create_task(sleeper("late", 3)),
        asyncio.create_task(sleeper("early", 1)),
        asyncio.create_task(sleeper("middle", 2)),
    ]
    await clock.advance(1.5)
    assert woke == [("early", 1.0)]
    await clock.advance(10)
    assert woke == [("early", 1.0), ("middle", 2.0), ("late", 3.0)]
    assert clock.monotonic() == 11.5
    await asyncio.gather(*tasks)


async def test_fake_clock_handles_chained_sleeps_within_one_advance(clock: FakeClock) -> None:
    ticks: list[float] = []

    async def ticker() -> None:
        for _ in range(3):
            await clock.sleep(1)
            ticks.append(clock.monotonic())

    task = asyncio.create_task(ticker())
    await clock.advance(5)
    assert ticks == [1.0, 2.0, 3.0]
    await task


async def test_cancelled_sleep_is_discarded(clock: FakeClock) -> None:
    task = asyncio.create_task(clock.sleep(5))
    await clock.advance(0)
    assert clock.pending_sleeps == 1
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert clock.pending_sleeps == 0
    assert clock.next_deadline() is None
    await clock.advance(10)


async def test_zero_sleep_yields_without_registering(clock: FakeClock) -> None:
    await clock.sleep(0)
    assert clock.pending_sleeps == 0


async def test_fake_clock_rejects_invalid_arguments(clock: FakeClock) -> None:
    with pytest.raises(ValueError, match="backwards"):
        await clock.advance(-1)
    with pytest.raises(ValueError, match="settle_iterations"):
        FakeClock(settle_iterations=0)
