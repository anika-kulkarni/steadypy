"""Exceptions raised by steadypy itself.

steadypy re-raises your own exceptions unchanged wherever it can (a retry that
gives up raises the last exception your function raised, a supervised task's
failure is surfaced as-is inside an :class:`ExceptionGroup`). The classes here
cover the few situations where steadypy is the one refusing to do something.
"""

from __future__ import annotations

__all__ = ["CircuitOpenError", "SteadyError"]


class SteadyError(Exception):
    """Base class for every exception defined by steadypy."""


class CircuitOpenError(SteadyError):
    """Raised when a :class:`~steadypy.CircuitBreaker` rejects a call.

    Attributes:
        name: The breaker's name, if it was given one.
        retry_after: Seconds until the breaker will admit a probe call. ``0.0``
            when the breaker is already half-open but all probe slots are taken.
    """

    def __init__(self, message: str, *, name: str | None, retry_after: float) -> None:
        super().__init__(message)
        self.name = name
        self.retry_after = retry_after
