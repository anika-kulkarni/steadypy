"""The clock abstraction every time-dependent primitive is built on.

steadypy never reads the time or sleeps directly. Retry backoff, restart
backoff, circuit-breaker recovery, rate limiting and heartbeat staleness all go
through a :class:`Clock`. Production code uses :class:`SystemClock` (the
default everywhere); tests pass :class:`steadypy.testing.FakeClock` to control
time deterministically instead of sleeping for real.
"""

from __future__ import annotations

import asyncio
import time
from typing import Protocol

__all__ = ["Clock", "SystemClock"]


class Clock(Protocol):
    """A monotonic time source plus a matching sleep.

    ``monotonic()`` must never go backwards and is only meaningful for
    measuring intervals, exactly like :func:`time.monotonic`.
    """

    def monotonic(self) -> float:
        """Return the current monotonic time in seconds."""
        ...

    async def sleep(self, delay: float) -> None:
        """Suspend the calling task for ``delay`` seconds of this clock's time."""
        ...


class SystemClock:
    """The real clock: :func:`time.monotonic` and :func:`asyncio.sleep`."""

    def monotonic(self) -> float:
        return time.monotonic()

    async def sleep(self, delay: float) -> None:
        await asyncio.sleep(delay)

    def __repr__(self) -> str:
        return "SystemClock()"


_SYSTEM_CLOCK = SystemClock()


def resolve_clock(clock: Clock | None) -> Clock:
    """Return ``clock``, or the shared :class:`SystemClock` when it is ``None``."""
    return _SYSTEM_CLOCK if clock is None else clock
