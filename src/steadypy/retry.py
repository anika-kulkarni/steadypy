"""Retrying transient failures with exponential backoff and jitter.

Example::

    result = await retry(fetch_quote, attempts=5, base_delay=0.25, max_delay=5)

    @async_retry(attempts=5, retry_on=(ConnectionError, TimeoutError))
    async def fetch_quote() -> Quote: ...

Retries only ever happen for exceptions you opt into, and never for
cancellation: :class:`asyncio.CancelledError` (a :class:`BaseException`)
always propagates immediately, including while waiting between attempts.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import logging
import random
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass
from typing import Any, ParamSpec, Protocol, TypeVar, cast, overload

from steadypy._backoff import exponential_delay
from steadypy.clock import Clock, resolve_clock

__all__ = ["RetryAttempt", "async_retry", "retry"]

logger = logging.getLogger("steadypy.retry")

T = TypeVar("T")
P = ParamSpec("P")

ExceptionTypes = type[Exception] | tuple[type[Exception], ...]

_DEFAULT_MAX_DELAY = 10.0


@dataclass(frozen=True, slots=True)
class RetryAttempt:
    """What an ``on_retry`` callback is told about a failed attempt.

    Attributes:
        attempt: Number of the attempt that just failed, starting at 1.
        exception: The exception that attempt raised.
        delay: Seconds that will be slept before the next attempt.
        elapsed: Seconds since the first attempt started.
    """

    attempt: int
    exception: Exception
    delay: float
    elapsed: float


@dataclass(frozen=True, slots=True)
class _Policy:
    attempts: int
    base_delay: float
    max_delay: float
    multiplier: float
    jitter: float
    retry_on: tuple[type[Exception], ...]
    retry_if: Callable[[Exception], bool] | None
    deadline: float | None
    on_retry: Callable[[RetryAttempt], object] | None
    clock: Clock | None

    @classmethod
    def build(
        cls,
        *,
        attempts: int,
        base_delay: float,
        max_delay: float | None,
        multiplier: float,
        jitter: float,
        retry_on: ExceptionTypes,
        retry_if: Callable[[Exception], bool] | None,
        deadline: float | None,
        on_retry: Callable[[RetryAttempt], object] | None,
        clock: Clock | None,
    ) -> _Policy:
        if isinstance(attempts, bool) or not isinstance(attempts, int) or attempts < 1:
            raise ValueError("attempts must be an integer >= 1")
        if base_delay < 0:
            raise ValueError("base_delay must be >= 0")
        if max_delay is None:
            max_delay = max(_DEFAULT_MAX_DELAY, base_delay)
        elif max_delay < base_delay:
            raise ValueError("max_delay must be >= base_delay")
        if multiplier < 1:
            raise ValueError("multiplier must be >= 1")
        if not 0 <= jitter <= 1:
            raise ValueError("jitter must be between 0 and 1")
        if deadline is not None and deadline <= 0:
            raise ValueError("deadline must be > 0 or None")
        types = retry_on if isinstance(retry_on, tuple) else (retry_on,)
        for exc_type in types:
            if not (isinstance(exc_type, type) and issubclass(exc_type, Exception)):
                raise TypeError(
                    "retry_on must contain Exception subclasses; BaseException subclasses "
                    f"such as asyncio.CancelledError are never retried (got {exc_type!r})"
                )
        return cls(
            attempts=attempts,
            base_delay=base_delay,
            max_delay=max_delay,
            multiplier=multiplier,
            jitter=jitter,
            retry_on=types,
            retry_if=retry_if,
            deadline=deadline,
            on_retry=on_retry,
            clock=clock,
        )

    def delay_after(self, attempt: int) -> float:
        delay = exponential_delay(
            attempt - 1, base=self.base_delay, maximum=self.max_delay, multiplier=self.multiplier
        )
        if self.jitter:
            delay -= delay * self.jitter * random.random()
        return delay


async def retry(
    func: Callable[[], Awaitable[T]],
    *,
    attempts: int = 3,
    base_delay: float = 0.1,
    max_delay: float | None = None,
    multiplier: float = 2.0,
    jitter: float = 1.0,
    retry_on: ExceptionTypes = Exception,
    retry_if: Callable[[Exception], bool] | None = None,
    deadline: float | None = None,
    on_retry: Callable[[RetryAttempt], object] | None = None,
    clock: Clock | None = None,
) -> T:
    """Await ``func()``, retrying failures with exponential backoff.

    The wait before retry *n* (counting from 1) is
    ``base_delay * multiplier ** (n - 1)``, capped at ``max_delay``, and then
    randomized by ``jitter``. When retries run out, the exception from the
    last attempt is re-raised unchanged, with a note recording how many
    attempts were made.

    Args:
        func: A zero-argument callable returning a fresh awaitable for every
            attempt, such as an ``async def`` function. Use
            :func:`functools.partial` or a lambda to bind arguments. Passing a
            coroutine object (``fetch()`` instead of ``fetch``) is an error,
            since a coroutine can only be awaited once.
        attempts: Maximum number of attempts, including the first.
        base_delay: Delay before the first retry, in seconds.
        max_delay: Upper bound for any delay. Defaults to 10 seconds, or to
            ``base_delay`` if that is larger.
        multiplier: Growth factor between consecutive delays.
        jitter: Fraction of each delay that is randomized, between 0 and 1.
            The actual delay is drawn uniformly from
            ``[delay * (1 - jitter), delay]``. The default of 1 is "full jitter",
            which spreads out clients that failed at the same moment.
        retry_on: Exception class or tuple of classes that may be retried.
            Anything else propagates immediately. Must be :class:`Exception`
            subclasses.
        retry_if: Optional predicate for finer control, called with an
            exception that matched ``retry_on``; return ``False`` to re-raise it
            immediately (for example, an HTTP error with a 4xx status).
        deadline: Optional time budget in seconds, measured from the start of
            the first attempt. No backoff sleep or new attempt starts if it
            would end past the deadline. An attempt already in progress is not
            interrupted; wrap the call in :func:`asyncio.timeout` for a hard
            limit.
        on_retry: Optional callback, sync or async, invoked with a
            :class:`RetryAttempt` before each backoff sleep. Exceptions it
            raises propagate and stop the retries.
        clock: Time source for delays and the deadline; see
            :mod:`steadypy.testing`.

    Returns:
        Whatever the first successful attempt returns.

    Raises:
        TypeError: If ``func`` is not callable, is a coroutine object, or
            ``retry_on`` is not made of :class:`Exception` subclasses.
        ValueError: If a numeric argument is out of range.
    """
    policy = _Policy.build(
        attempts=attempts,
        base_delay=base_delay,
        max_delay=max_delay,
        multiplier=multiplier,
        jitter=jitter,
        retry_on=retry_on,
        retry_if=retry_if,
        deadline=deadline,
        on_retry=on_retry,
        clock=clock,
    )
    return await _retry(func, policy)


async def _retry(func: Callable[[], Awaitable[T]], policy: _Policy) -> T:
    candidate: object = func  # runtime guards for callers without type checking
    if inspect.iscoroutine(candidate):
        candidate.close()
        raise TypeError(
            "retry() needs a callable that returns a new awaitable for every attempt: "
            "pass `fetch` (or functools.partial(fetch, ...)), not `fetch()`"
        )
    if not callable(candidate):
        raise TypeError(f"func must be callable, not {type(func).__name__}")

    clock = resolve_clock(policy.clock)
    task = asyncio.current_task()
    started = clock.monotonic()
    attempt = 0
    while True:
        attempt += 1
        cancel_requests = task.cancelling() if task is not None else 0
        try:
            outcome: object = func()
            if inspect.isawaitable(outcome):
                return await cast(Awaitable[T], outcome)
        except policy.retry_on as exc:
            if task is not None and task.cancelling() > cancel_requests:
                # The task was asked to cancel during this attempt and the
                # attempt swallowed the CancelledError. Never retry through a
                # pending cancellation request.
                raise
            if policy.retry_if is not None and not policy.retry_if(exc):
                raise
            if attempt >= policy.attempts:
                exc.add_note(f"steadypy.retry: gave up after {_plural(attempt, 'attempt')}")
                raise
            delay = policy.delay_after(attempt)
            elapsed = clock.monotonic() - started
            if policy.deadline is not None and elapsed + delay > policy.deadline:
                exc.add_note(
                    f"steadypy.retry: gave up after {_plural(attempt, 'attempt')}; "
                    f"retrying would exceed the {policy.deadline:g}s deadline"
                )
                raise
            if policy.on_retry is not None:
                outcome = policy.on_retry(RetryAttempt(attempt, exc, delay, elapsed))
                if inspect.isawaitable(outcome):
                    await outcome
            logger.debug(
                "attempt %d of %s failed with %r; retrying in %.3gs",
                attempt,
                getattr(func, "__qualname__", func),
                exc,
                delay,
            )
        else:
            # Not retried: a function that is not async will never become one.
            raise TypeError(
                f"{getattr(func, '__qualname__', func)!s} returned "
                f"{type(outcome).__name__}, not an awaitable; retry() needs an async "
                "function or a callable that returns an awaitable"
            )
        # Sleep outside the except block so the failed attempt's exception is
        # not kept alive as the context of anything raised while waiting.
        await clock.sleep(delay)


def _plural(count: int, noun: str) -> str:
    return f"{count} {noun}" if count == 1 else f"{count} {noun}s"


class _RetryDecorator(Protocol):
    def __call__(
        self, func: Callable[P, Awaitable[T]], /
    ) -> Callable[P, Coroutine[Any, Any, T]]: ...


@overload
def async_retry(func: Callable[P, Awaitable[T]], /) -> Callable[P, Coroutine[Any, Any, T]]: ...


@overload
def async_retry(
    *,
    attempts: int = 3,
    base_delay: float = 0.1,
    max_delay: float | None = None,
    multiplier: float = 2.0,
    jitter: float = 1.0,
    retry_on: ExceptionTypes = Exception,
    retry_if: Callable[[Exception], bool] | None = None,
    deadline: float | None = None,
    on_retry: Callable[[RetryAttempt], object] | None = None,
    clock: Clock | None = None,
) -> _RetryDecorator: ...


def async_retry(
    func: Callable[..., Awaitable[Any]] | None = None,
    /,
    *,
    attempts: int = 3,
    base_delay: float = 0.1,
    max_delay: float | None = None,
    multiplier: float = 2.0,
    jitter: float = 1.0,
    retry_on: ExceptionTypes = Exception,
    retry_if: Callable[[Exception], bool] | None = None,
    deadline: float | None = None,
    on_retry: Callable[[RetryAttempt], object] | None = None,
    clock: Clock | None = None,
) -> Any:
    """Decorator form of :func:`retry` for ``async def`` functions.

    Usable bare (``@async_retry``) or with the same keyword arguments as
    :func:`retry` (``@async_retry(attempts=5)``). Every call of the decorated
    function is retried independently. Arguments are validated when the
    decorator is applied, not on the first call.
    """
    policy = _Policy.build(
        attempts=attempts,
        base_delay=base_delay,
        max_delay=max_delay,
        multiplier=multiplier,
        jitter=jitter,
        retry_on=retry_on,
        retry_if=retry_if,
        deadline=deadline,
        on_retry=on_retry,
        clock=clock,
    )

    def decorate(fn: Callable[P, Awaitable[T]]) -> Callable[P, Coroutine[Any, Any, T]]:
        if not callable(fn):
            raise TypeError(f"@async_retry can only decorate callables, not {type(fn).__name__}")

        @functools.wraps(fn)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            return await _retry(functools.partial(fn, *args, **kwargs), policy)

        return wrapper

    if func is not None:
        return decorate(func)
    return decorate
