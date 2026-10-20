"""Exponential backoff arithmetic shared by retry and supervisor restarts."""

from __future__ import annotations


def exponential_delay(
    attempt: int,
    *,
    base: float,
    maximum: float,
    multiplier: float = 2.0,
) -> float:
    """Return ``base * multiplier ** attempt``, capped at ``maximum``.

    ``attempt`` is zero-based: attempt 0 waits ``base`` seconds. Very large
    attempt numbers saturate at ``maximum`` instead of overflowing.
    """
    if base <= 0:
        return 0.0
    try:
        delay = base * multiplier**attempt
    except OverflowError:
        return maximum
    return min(maximum, delay)
