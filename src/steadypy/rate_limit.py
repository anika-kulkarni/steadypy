"""An async token-bucket rate limiter.

Example::

    limiter = RateLimiter(rate=10, per=1.0)   # 10 operations per second

    async with limiter:
        await request()

The bucket holds up to ``burst`` tokens (``rate`` by default) and refills
continuously at ``rate / per`` tokens per second, so short bursts are allowed
while the long-run average never exceeds the configured rate. Waiters are
served first-come, first-served.
"""

from __future__ import annotations

import asyncio
from types import TracebackType
from typing import Self

from steadypy.clock import Clock, resolve_clock

__all__ = ["RateLimiter"]

# Refill arithmetic is in floating point; treat amounts this close as equal so
# that a waiter never sleeps again for a rounding residue.
_EPSILON = 1e-9


class RateLimiter:
    """Limit how often an operation may happen, using a token bucket.

    Args:
        rate: Number of tokens added per ``per`` seconds.
        per: The period, in seconds, that ``rate`` refers to.
        burst: Bucket capacity: the most tokens that can be taken at once
            after an idle period. Defaults to ``max(1, rate)``, allowing one
            period's worth of operations in a burst.
        clock: Time source for refills and waiting; see :mod:`steadypy.testing`.

    The bucket starts full. A limiter is safe to share between tasks on one
    event loop (it is not thread-safe).
    """

    def __init__(
        self,
        rate: float,
        per: float = 1.0,
        *,
        burst: float | None = None,
        clock: Clock | None = None,
    ) -> None:
        if rate <= 0:
            raise ValueError("rate must be > 0")
        if per <= 0:
            raise ValueError("per must be > 0")
        capacity = max(1.0, float(rate)) if burst is None else float(burst)
        if capacity <= 0:
            raise ValueError("burst must be > 0")
        self._fill_rate = rate / per
        self._capacity = capacity
        self._clock = resolve_clock(clock)
        self._tokens = capacity
        self._updated_at = self._clock.monotonic()
        self._lock = asyncio.Lock()
        self._waiting = 0

    @property
    def rate(self) -> float:
        """Tokens added per second."""
        return self._fill_rate

    @property
    def capacity(self) -> float:
        """The maximum number of tokens the bucket holds."""
        return self._capacity

    @property
    def available(self) -> float:
        """Tokens that could be taken right now without waiting."""
        self._refill()
        return self._tokens

    @property
    def waiting(self) -> int:
        """Number of tasks currently waiting in :meth:`acquire`."""
        return self._waiting

    async def acquire(self, tokens: float = 1.0) -> None:
        """Wait until ``tokens`` are available, then take them.

        Callers are served in arrival order, so a large request is not starved
        by a stream of small ones. Cancelling a waiting caller takes no tokens
        and lets the next waiter proceed.

        Raises:
            ValueError: If ``tokens`` is not positive or exceeds the capacity,
                since such a request could never be satisfied.
        """
        self._check(tokens)
        self._waiting += 1
        try:
            async with self._lock:
                while True:
                    self._refill()
                    if self._tokens + _EPSILON >= tokens:
                        self._tokens = max(0.0, self._tokens - tokens)
                        return
                    await self._clock.sleep((tokens - self._tokens) / self._fill_rate)
        finally:
            self._waiting -= 1

    def try_acquire(self, tokens: float = 1.0) -> bool:
        """Take ``tokens`` if available right now; never waits.

        Returns ``False`` when tokens are short *or* other tasks are already
        waiting, so it never jumps the queue.
        """
        self._check(tokens)
        if self._waiting:
            return False
        self._refill()
        if self._tokens + _EPSILON >= tokens:
            self._tokens = max(0.0, self._tokens - tokens)
            return True
        return False

    async def __aenter__(self) -> Self:
        await self.acquire()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        return None

    def __repr__(self) -> str:
        return (
            f"<RateLimiter rate={self._fill_rate:g}/s capacity={self._capacity:g} "
            f"available={self.available:.3g} waiting={self._waiting}>"
        )

    def _refill(self) -> None:
        now = self._clock.monotonic()
        elapsed = now - self._updated_at
        if elapsed > 0:
            self._tokens = min(self._capacity, self._tokens + elapsed * self._fill_rate)
            self._updated_at = now

    def _check(self, tokens: float) -> None:
        if tokens <= 0:
            raise ValueError("tokens must be > 0")
        if tokens > self._capacity:
            raise ValueError(
                f"cannot acquire {tokens:g} tokens from a bucket that holds at most "
                f"{self._capacity:g}"
            )
