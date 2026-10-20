from __future__ import annotations

import asyncio

import pytest

from steadypy import RateLimiter
from steadypy.testing import FakeClock


async def test_bucket_starts_full_and_allows_a_burst(clock: FakeClock) -> None:
    limiter = RateLimiter(rate=5, clock=clock)
    for _ in range(5):
        await limiter.acquire()
    assert clock.monotonic() == 0
    assert limiter.available == pytest.approx(0)


async def test_waits_exactly_until_a_token_is_refilled(clock: FakeClock) -> None:
    limiter = RateLimiter(rate=2, per=1.0, clock=clock)
    await limiter.acquire(2)
    task = asyncio.create_task(limiter.acquire())
    await clock.advance(0.49)
    assert not task.done()
    await clock.advance(0.01)
    assert task.done()
    await task


async def test_refill_never_exceeds_capacity(clock: FakeClock) -> None:
    limiter = RateLimiter(rate=3, clock=clock)
    await clock.advance(3_600)
    assert limiter.available == 3
    assert limiter.try_acquire(3)
    assert not limiter.try_acquire()


async def test_explicit_burst_and_period(clock: FakeClock) -> None:
    limiter = RateLimiter(rate=5, per=10, burst=2, clock=clock)
    assert limiter.rate == 0.5
    assert limiter.capacity == 2
    assert limiter.try_acquire(2)
    await clock.advance(2)
    assert limiter.available == pytest.approx(1)


async def test_slow_rates_default_to_a_capacity_of_one(clock: FakeClock) -> None:
    limiter = RateLimiter(rate=1, per=60, clock=clock)
    assert limiter.capacity == 1
    await limiter.acquire()
    task = asyncio.create_task(limiter.acquire())
    await clock.advance(59)
    assert not task.done()
    await clock.advance(1)
    await task


async def test_fractional_and_multi_token_requests(clock: FakeClock) -> None:
    limiter = RateLimiter(rate=4, clock=clock)
    await limiter.acquire(2.5)
    assert limiter.available == pytest.approx(1.5)
    task = asyncio.create_task(limiter.acquire(3))
    await clock.advance(0.375)  # 1.5 more tokens at 4/s
    await task
    assert limiter.available == pytest.approx(0)


async def test_concurrent_waiters_are_served_in_arrival_order(clock: FakeClock) -> None:
    limiter = RateLimiter(rate=1, clock=clock)
    await limiter.acquire()
    finished: list[tuple[int, float]] = []

    async def worker(n: int) -> None:
        await limiter.acquire()
        finished.append((n, clock.monotonic()))

    tasks = [asyncio.create_task(worker(n)) for n in range(4)]
    await clock.advance(0)
    assert limiter.waiting == 4
    await clock.advance(10)
    await asyncio.gather(*tasks)
    assert finished == [(0, 1.0), (1, 2.0), (2, 3.0), (3, 4.0)]


async def test_a_large_request_is_not_starved_by_small_ones(clock: FakeClock) -> None:
    limiter = RateLimiter(rate=4, clock=clock)
    await limiter.acquire(4)
    order: list[str] = []

    async def take(label: str, tokens: float) -> None:
        await limiter.acquire(tokens)
        order.append(label)

    big = asyncio.create_task(take("big", 4))
    await clock.advance(0)
    small = [asyncio.create_task(take(f"small{n}", 1)) for n in range(3)]
    await clock.advance(5)
    await asyncio.gather(big, *small)
    assert order[0] == "big"


async def test_cancelled_waiter_takes_no_tokens(clock: FakeClock) -> None:
    limiter = RateLimiter(rate=1, clock=clock)
    await limiter.acquire()
    first = asyncio.create_task(limiter.acquire())
    second = asyncio.create_task(limiter.acquire())
    await clock.advance(0.5)
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    await clock.advance(0.5)
    await second  # gets the token the cancelled waiter would have taken
    assert clock.monotonic() == 1.0
    assert limiter.waiting == 0


async def test_try_acquire_never_jumps_the_queue(clock: FakeClock) -> None:
    limiter = RateLimiter(rate=1, burst=2, clock=clock)
    assert limiter.try_acquire()
    await limiter.acquire()
    waiter = asyncio.create_task(limiter.acquire())
    await clock.advance(0)
    await clock.advance(0.99)
    assert not limiter.try_acquire()  # a waiter is queued
    await clock.advance(0.01)
    await waiter
    await clock.advance(1)
    assert limiter.try_acquire()


async def test_async_with_takes_one_token(clock: FakeClock) -> None:
    limiter = RateLimiter(rate=2, clock=clock)
    async with limiter as entered:
        assert entered is limiter
    assert limiter.available == pytest.approx(1)
    assert "rate=2/s" in repr(limiter)


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [({"rate": 0}, "rate"), ({"rate": 1, "per": 0}, "per"), ({"rate": 1, "burst": 0}, "burst")],
)
def test_constructor_validates_arguments(kwargs: dict[str, float], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        RateLimiter(**kwargs)


async def test_impossible_requests_are_rejected(clock: FakeClock) -> None:
    limiter = RateLimiter(rate=2, clock=clock)
    with pytest.raises(ValueError, match="at most 2"):
        await limiter.acquire(3)
    with pytest.raises(ValueError, match="> 0"):
        limiter.try_acquire(0)
