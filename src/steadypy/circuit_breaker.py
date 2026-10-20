"""A circuit breaker for calls to a dependency that may be failing.

When a dependency keeps failing, continuing to call it wastes time, holds
resources, and adds load to something that is already struggling. A circuit
breaker counts consecutive failures; past a threshold it *opens* and rejects
calls immediately with :class:`~steadypy.CircuitOpenError`. After a recovery
timeout it lets a limited number of *probe* calls through (*half-open*): a
successful probe closes the circuit again, a failed one re-opens it.

Example::

    breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=30)

    async with breaker:
        await call_external_service()

The pattern is described in Michael Nygard's *Release It!* and in Martin
Fowler's article "CircuitBreaker" (https://martinfowler.com/bliki/CircuitBreaker.html).
"""

from __future__ import annotations

import asyncio
import enum
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from types import TracebackType
from typing import Any, ParamSpec, Self, TypeVar

from steadypy.clock import Clock, resolve_clock
from steadypy.exceptions import CircuitOpenError

__all__ = ["CircuitBreaker", "CircuitState", "CircuitStats"]

logger = logging.getLogger("steadypy.circuit_breaker")

T = TypeVar("T")
P = ParamSpec("P")


class CircuitState(enum.Enum):
    """The three states of a :class:`CircuitBreaker`."""

    CLOSED = "closed"
    """Calls pass through; consecutive failures are counted."""

    OPEN = "open"
    """Calls are rejected until the recovery timeout has elapsed."""

    HALF_OPEN = "half_open"
    """A limited number of probe calls may test whether the dependency recovered."""


@dataclass(frozen=True, slots=True)
class CircuitStats:
    """A snapshot of a breaker's state and counters.

    Attributes:
        state: The current state.
        consecutive_failures: Failures since the last success while closed.
        successes: Lifetime count of calls that did not fail.
        failures: Lifetime count of calls that failed.
        rejections: Lifetime count of calls rejected without being made.
    """

    state: CircuitState
    consecutive_failures: int
    successes: int
    failures: int
    rejections: int


@dataclass(slots=True)
class _Ticket:
    """An admitted call: the state generation it was admitted in."""

    generation: int
    probe: bool


class CircuitBreaker:
    """An async circuit breaker.

    Use it as an async context manager around the protected call, or call
    :meth:`call`. One breaker is meant to be shared by every caller of the
    same dependency.

    How outcomes are counted:
        * An exception matching ``failure_exceptions`` is a **failure**.
        * Returning normally, or raising any other :class:`Exception`, is a
          **success**: the dependency responded, even if your code then
          rejected the response. The exception still propagates.
        * :class:`asyncio.CancelledError` (and other non-``Exception``
          ``BaseException`` types) are **neutral**: they change no counters and
          no state, but a cancelled probe does free its half-open slot.

    Concurrency:
        State transitions happen synchronously between ``await`` points, so a
        breaker is safe to share between tasks on one event loop (it is not
        thread-safe). Every admitted call remembers the state *generation* it
        was admitted in, and an outcome only affects the state if no
        transition has happened since. A slow call that fails after the
        circuit has already opened does not extend the open period, and a
        success from before the circuit opened cannot close it.

    Args:
        failure_threshold: Consecutive failures that open the circuit.
        recovery_timeout: Seconds the circuit stays open before admitting a probe.
        half_open_max_calls: Probe calls allowed in flight while half-open.
        failure_exceptions: Exception class or tuple of classes that count as
            failures.
        name: Optional name for log messages and errors.
        on_state_change: Optional callback ``(old_state, new_state)`` invoked
            after every transition. Exceptions it raises are logged, not
            propagated.
        clock: Time source for the recovery timeout; see :mod:`steadypy.testing`.
    """

    def __init__(
        self,
        *,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        half_open_max_calls: int = 1,
        failure_exceptions: type[Exception] | tuple[type[Exception], ...] = Exception,
        name: str | None = None,
        on_state_change: Callable[[CircuitState, CircuitState], object] | None = None,
        clock: Clock | None = None,
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        if recovery_timeout < 0:
            raise ValueError("recovery_timeout must be >= 0")
        if half_open_max_calls < 1:
            raise ValueError("half_open_max_calls must be >= 1")
        types = (
            failure_exceptions if isinstance(failure_exceptions, tuple) else (failure_exceptions,)
        )
        for exc_type in types:
            if not (isinstance(exc_type, type) and issubclass(exc_type, Exception)):
                raise TypeError(
                    f"failure_exceptions must be Exception subclasses, not {exc_type!r}"
                )
        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._half_open_max_calls = half_open_max_calls
        self._failure_exceptions = types
        self._name = name
        self._on_state_change = on_state_change
        self._clock = resolve_clock(clock)

        self._state = CircuitState.CLOSED
        self._generation = 0
        self._consecutive_failures = 0
        self._opened_at = 0.0
        self._probes_in_flight = 0
        self._successes = 0
        self._failures = 0
        self._rejections = 0
        self._tickets: dict[asyncio.Task[Any] | None, list[_Ticket]] = {}

    # -- inspection ----------------------------------------------------------

    @property
    def name(self) -> str | None:
        """The breaker's name, if it was given one."""
        return self._name

    @property
    def state(self) -> CircuitState:
        """The current state. An open circuit whose timeout elapsed reads as half-open."""
        self._refresh()
        return self._state

    @property
    def retry_after(self) -> float:
        """Seconds until an open circuit admits a probe; ``0.0`` when not open."""
        self._refresh()
        if self._state is not CircuitState.OPEN:
            return 0.0
        return max(0.0, self._opened_at + self._recovery_timeout - self._clock.monotonic())

    @property
    def stats(self) -> CircuitStats:
        """A :class:`CircuitStats` snapshot."""
        return CircuitStats(
            state=self.state,
            consecutive_failures=self._consecutive_failures,
            successes=self._successes,
            failures=self._failures,
            rejections=self._rejections,
        )

    def reset(self) -> None:
        """Close the circuit and clear the consecutive-failure count.

        Lifetime counters in :attr:`stats` are kept. Calls in flight when the
        breaker is reset no longer affect its state.
        """
        self._transition(CircuitState.CLOSED)
        self._generation += 1
        self._consecutive_failures = 0

    # -- protecting calls ------------------------------------------------------

    async def call(
        self, func: Callable[P, Awaitable[T]], /, *args: P.args, **kwargs: P.kwargs
    ) -> T:
        """Call ``func(*args, **kwargs)`` through the breaker and return its result.

        Raises:
            CircuitOpenError: If the call is rejected.
        """
        ticket = self._admit()
        try:
            result = await func(*args, **kwargs)
        except BaseException as exc:
            self._record(ticket, exc)
            raise
        self._record(ticket, None)
        return result

    async def __aenter__(self) -> Self:
        ticket = self._admit()
        self._tickets.setdefault(asyncio.current_task(), []).append(ticket)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        key = asyncio.current_task()
        stack = self._tickets[key]
        ticket = stack.pop()
        if not stack:
            del self._tickets[key]
        self._record(ticket, exc)

    def __repr__(self) -> str:
        label = f" {self._name!r}" if self._name else ""
        return (
            f"<CircuitBreaker{label} state={self.state.value} "
            f"consecutive_failures={self._consecutive_failures}>"
        )

    # -- state machine ---------------------------------------------------------

    def _refresh(self) -> None:
        if (
            self._state is CircuitState.OPEN
            and self._clock.monotonic() - self._opened_at >= self._recovery_timeout
        ):
            self._transition(CircuitState.HALF_OPEN)

    def _admit(self) -> _Ticket:
        self._refresh()
        if self._state is CircuitState.CLOSED:
            return _Ticket(self._generation, probe=False)
        if self._state is CircuitState.OPEN:
            self._rejections += 1
            retry_after = self.retry_after
            raise CircuitOpenError(
                f"{self._label()} is open; retry after {retry_after:.3g}s",
                name=self._name,
                retry_after=retry_after,
            )
        if self._probes_in_flight >= self._half_open_max_calls:
            self._rejections += 1
            raise CircuitOpenError(
                f"{self._label()} is half-open and its probe calls are in flight",
                name=self._name,
                retry_after=0.0,
            )
        self._probes_in_flight += 1
        return _Ticket(self._generation, probe=True)

    def _record(self, ticket: _Ticket, exc: BaseException | None) -> None:
        current = ticket.generation == self._generation
        if ticket.probe and current:
            self._probes_in_flight -= 1
        if exc is not None and not isinstance(exc, Exception):
            return  # cancellation and other BaseExceptions are neutral
        failed = exc is not None and isinstance(exc, self._failure_exceptions)
        if failed:
            self._failures += 1
        else:
            self._successes += 1
        if not current:
            return  # the state moved on since this call was admitted
        if self._state is CircuitState.CLOSED:
            if not failed:
                self._consecutive_failures = 0
                return
            self._consecutive_failures += 1
            if self._consecutive_failures >= self._failure_threshold:
                self._transition(CircuitState.OPEN)
        elif self._state is CircuitState.HALF_OPEN:
            if failed:
                self._consecutive_failures += 1
                self._transition(CircuitState.OPEN)
            else:
                self._transition(CircuitState.CLOSED)

    def _transition(self, new: CircuitState) -> None:
        old = self._state
        if old is new:
            return
        self._state = new
        self._generation += 1
        if new is CircuitState.OPEN:
            self._opened_at = self._clock.monotonic()
            if old is CircuitState.HALF_OPEN:
                logger.warning("%s re-opened: a probe call failed", self._label())
            else:
                logger.warning(
                    "%s opened after %d consecutive failure(s)",
                    self._label(),
                    self._consecutive_failures,
                )
        elif new is CircuitState.HALF_OPEN:
            self._probes_in_flight = 0
            logger.info("%s is half-open; admitting probe calls", self._label())
        else:
            self._consecutive_failures = 0
            logger.info("%s closed", self._label())
        if self._on_state_change is not None:
            try:
                self._on_state_change(old, new)
            except Exception:
                logger.exception("on_state_change callback of %s raised", self._label())

    def _label(self) -> str:
        return f"circuit {self._name!r}" if self._name else "circuit"
