"""Helpers for testing code that uses steadypy.

The main export is :class:`FakeClock`, a manually advanced clock. Pass it as
``clock=`` to any steadypy primitive and drive time from the test with
``await clock.advance(seconds)``; no real time passes.
"""

from __future__ import annotations

import asyncio
import heapq
import itertools

__all__ = ["FakeClock"]


class FakeClock:
    """A deterministic clock that only moves when told to.

    ``sleep()`` registers a wake-up deadline and suspends; ``advance()`` moves
    time forward and wakes sleepers in deadline order, yielding to the event
    loop after each wake-up so that woken tasks run (and can register new
    sleeps) before time moves further.

    Example::

        clock = FakeClock()
        task = asyncio.create_task(retry(fetch, attempts=3, clock=clock))
        await clock.advance(10)   # every backoff sleep completes instantly
        result = await task

    Args:
        start: The initial value returned by :meth:`monotonic`.
        settle_iterations: How many times :meth:`advance` yields to the event
            loop after each wake-up. The default is ample for steadypy's own
            primitives; raise it if your code needs many loop iterations
            between a wake-up and its next sleep.
    """

    def __init__(self, start: float = 0.0, *, settle_iterations: int = 20) -> None:
        if settle_iterations < 1:
            raise ValueError("settle_iterations must be at least 1")
        self._now = float(start)
        self._settle_iterations = settle_iterations
        self._sleepers: list[tuple[float, int, asyncio.Future[None]]] = []
        self._sequence = itertools.count()

    def monotonic(self) -> float:
        """Return the current fake time."""
        return self._now

    async def sleep(self, delay: float) -> None:
        """Suspend until :meth:`advance` moves time past ``now + delay``.

        A non-positive delay yields to the event loop once, like
        ``asyncio.sleep(0)``.
        """
        if delay <= 0:
            await asyncio.sleep(0)
            return
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        heapq.heappush(self._sleepers, (self._now + delay, next(self._sequence), future))
        await future

    @property
    def pending_sleeps(self) -> int:
        """The number of tasks currently suspended in :meth:`sleep`."""
        return sum(1 for _, _, future in self._sleepers if not future.done())

    def next_deadline(self) -> float | None:
        """The earliest pending wake-up time, or ``None`` if nobody is sleeping."""
        self._discard_finished()
        return self._sleepers[0][0] if self._sleepers else None

    async def advance(self, seconds: float) -> None:
        """Move time forward by ``seconds``, waking sleepers along the way.

        ``advance(0)`` is useful on its own: it lets ready tasks run and wakes
        anything whose deadline has already passed.
        """
        if seconds < 0:
            raise ValueError("cannot move a clock backwards")
        target = self._now + seconds
        await self._settle()
        while (deadline := self.next_deadline()) is not None and deadline <= target:
            self._now = max(self._now, deadline)
            while self._sleepers and self._sleepers[0][0] <= self._now:
                _, _, future = heapq.heappop(self._sleepers)
                if not future.done():
                    future.set_result(None)
            await self._settle()
        self._now = target
        await self._settle()

    def _discard_finished(self) -> None:
        # Sleeps cancelled before their deadline leave finished futures behind.
        while self._sleepers and self._sleepers[0][2].done():
            heapq.heappop(self._sleepers)

    async def _settle(self) -> None:
        for _ in range(self._settle_iterations):
            await asyncio.sleep(0)

    def __repr__(self) -> str:
        return f"FakeClock(now={self._now!r}, pending_sleeps={self.pending_sleeps})"
