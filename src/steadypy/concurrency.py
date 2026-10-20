"""Applying an async function to many items with bounded concurrency.

Example::

    pages = await bounded_map(fetch, urls, concurrency=10)

    async with bounded_stream(fetch, urls, concurrency=10, ordered=False) as results:
        async for page in results:
            ...

Items are pulled from the input lazily, one per free slot, so a huge or
infinite iterator never turns into millions of tasks: at most ``concurrency``
calls exist at any moment. Every call that is started is also finished
(completed, or cancelled and awaited) before these functions return.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable, Iterator
from types import TracebackType
from typing import Any, Generic, Literal, Self, TypeVar, overload

__all__ = ["BoundedStream", "bounded_map", "bounded_stream"]

T = TypeVar("T")
R = TypeVar("R")


_EXHAUSTED: Any = object()


class _Runner(Generic[T, R]):
    """Pulls items lazily and keeps at most ``concurrency`` calls in flight.

    An async input is read by a separate *pull* task, one item at a time,
    which is waited on together with the calls in flight. A slow input
    therefore never holds back results that are already available, and a
    cancelled consumer never interrupts the input in the middle of an item.
    """

    def __init__(
        self,
        func: Callable[[T], Awaitable[R]],
        items: Iterable[T] | AsyncIterable[T],
        concurrency: int,
    ) -> None:
        if isinstance(concurrency, bool) or not isinstance(concurrency, int) or concurrency < 1:
            raise ValueError("concurrency must be an integer >= 1")
        if not callable(func):
            raise TypeError(f"func must be callable, not {type(func).__name__}")
        self._func = func
        self._concurrency = concurrency
        self._sync_items: Iterator[T] | None = None
        self._async_items: AsyncIterator[T] | None = None
        if isinstance(items, AsyncIterable):
            self._async_items = aiter(items)
        elif isinstance(items, Iterable):
            self._sync_items = iter(items)
        else:
            raise TypeError(f"items must be iterable or async iterable, not {type(items).__name__}")
        self._exhausted = False
        self._next_index = 0
        self._next_to_yield = 0
        self._in_flight: dict[asyncio.Task[R], int] = {}
        self._pull: asyncio.Task[Any] | None = None

    async def next(self, *, ordered: bool) -> tuple[int, asyncio.Task[R]] | None:
        """Return the next finished task and its input index, or ``None`` at the end."""
        while True:
            self._collect_pull()
            self._fill()
            task = self._ready(ordered)
            if task is not None:
                index = self._in_flight.pop(task)
                if ordered:
                    self._next_to_yield += 1
                return index, task
            if not self._in_flight and self._pull is None:
                return None
            waiting: set[asyncio.Future[Any]] = set()
            if ordered and self._in_flight:
                waiting.add(self._head())
            elif not ordered:
                waiting.update(self._in_flight)
            if self._pull is not None:
                waiting.add(self._pull)
            await asyncio.wait(waiting, return_when=asyncio.FIRST_COMPLETED)

    async def aclose(self) -> None:
        """Stop pulling items; cancel and await every call still in flight."""
        self._exhausted = True
        tasks: list[asyncio.Task[Any]] = list(self._in_flight)
        self._in_flight.clear()
        if self._pull is not None:
            tasks.append(self._pull)
            self._pull = None
        await _cancel_and_wait(tasks)

    def _head(self) -> asyncio.Task[R]:
        return next(t for t, i in self._in_flight.items() if i == self._next_to_yield)

    def _ready(self, ordered: bool) -> asyncio.Task[R] | None:
        if not self._in_flight:
            return None
        if ordered:
            head = self._head()
            return head if head.done() else None
        finished = [t for t in self._in_flight if t.done()]
        # Several tasks may finish in the same loop iteration; prefer input
        # order among them so results are deterministic.
        return min(finished, key=self._in_flight.__getitem__) if finished else None

    def _fill(self) -> None:
        while not self._exhausted and len(self._in_flight) < self._concurrency:
            if self._async_items is not None:
                if self._pull is None:
                    self._pull = asyncio.create_task(self._pull_next(self._async_items))
                return  # the pull task delivers the next item when it is ready
            assert self._sync_items is not None
            try:
                item = next(self._sync_items)
            except StopIteration:
                self._exhausted = True
                return
            self._start(item)

    def _collect_pull(self) -> None:
        pull = self._pull
        if pull is None or not pull.done():
            return
        self._pull = None
        item = pull.result()  # an error from the input propagates from here
        if item is _EXHAUSTED:
            self._exhausted = True
        else:
            self._start(item)

    def _start(self, item: T) -> None:
        task = asyncio.create_task(self._call(item))
        self._in_flight[task] = self._next_index
        self._next_index += 1

    @staticmethod
    async def _pull_next(items: AsyncIterator[T]) -> Any:
        try:
            return await anext(items)
        except StopAsyncIteration:
            return _EXHAUSTED

    async def _call(self, item: T) -> R:
        return await self._func(item)


async def _cancel_and_wait(tasks: list[asyncio.Task[Any]]) -> None:
    """Cancel ``tasks`` and wait until every one of them has finished.

    If the caller is cancelled while waiting, the wait continues (no task may
    outlive the call) and the cancellation is re-raised afterwards.
    """
    for task in tasks:
        task.cancel()
    interrupted: asyncio.CancelledError | None = None
    while remaining := [t for t in tasks if not t.done()]:
        try:
            await asyncio.wait(remaining)
        except asyncio.CancelledError as exc:
            interrupted = exc
    for task in tasks:
        if not task.cancelled():
            task.exception()  # mark as retrieved; these results are discarded
    if interrupted is not None:
        raise interrupted


def _outcome(task: asyncio.Task[Any], return_exceptions: bool) -> Any:
    if return_exceptions and not task.cancelled():
        exc = task.exception()
        if isinstance(exc, Exception):
            return exc
    return task.result()


@overload
async def bounded_map(
    func: Callable[[T], Awaitable[R]],
    items: Iterable[T] | AsyncIterable[T],
    *,
    concurrency: int,
    return_exceptions: Literal[False] = False,
) -> list[R]: ...


@overload
async def bounded_map(
    func: Callable[[T], Awaitable[R]],
    items: Iterable[T] | AsyncIterable[T],
    *,
    concurrency: int,
    return_exceptions: Literal[True],
) -> list[R | Exception]: ...


async def bounded_map(
    func: Callable[[T], Awaitable[R]],
    items: Iterable[T] | AsyncIterable[T],
    *,
    concurrency: int,
    return_exceptions: bool = False,
) -> list[Any]:
    """Call ``func`` on every item with at most ``concurrency`` calls in flight.

    Results are returned in input order, while calls run and complete in any
    order: a slow item never holds back the others.

    Args:
        func: An async function taking one item.
        items: Any iterable or async iterable. It is consumed lazily, one item
            per free slot, and may be a generator of any length.
        concurrency: Maximum number of concurrent calls.
        return_exceptions: If ``False`` (the default), the first failure
            cancels every call still in flight and is raised once they have
            finished, and no further items are pulled. If ``True``, an
            :class:`Exception` raised for an item is placed in the result list
            instead. :class:`BaseException` subclasses such as cancellation
            always propagate.

    Returns:
        One result per item, in input order.

    Cancelling the caller cancels every call in flight and waits for them to
    finish before :class:`asyncio.CancelledError` propagates.
    """
    runner = _Runner(func, items, concurrency)
    results: dict[int, Any] = {}
    try:
        while (finished := await runner.next(ordered=False)) is not None:
            index, task = finished
            results[index] = _outcome(task, return_exceptions)
    finally:
        await runner.aclose()
    return [results[i] for i in range(len(results))]


class BoundedStream(Generic[R]):
    """The async iterator returned by :func:`bounded_stream`.

    It must be used as an async context manager so that calls still in flight
    when iteration stops early (``break``, an exception, cancellation) are
    reliably cancelled and awaited when the block exits.
    """

    def __init__(
        self, runner: _Runner[Any, Any], *, ordered: bool, return_exceptions: bool
    ) -> None:
        self._runner = runner
        self._ordered = ordered
        self._return_exceptions = return_exceptions
        self._entered = False
        self._closed = False

    async def __aenter__(self) -> Self:
        if self._entered:
            raise RuntimeError("a bounded_stream can only be entered once")
        self._entered = True
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._closed = True
        await self._runner.aclose()

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> R:
        if not self._entered or self._closed:
            raise RuntimeError(
                "iterate a bounded_stream inside its context manager: "
                "'async with bounded_stream(...) as results: async for r in results: ...'"
            )
        finished = await self._runner.next(ordered=self._ordered)
        if finished is None:
            raise StopAsyncIteration
        result: R = _outcome(finished[1], self._return_exceptions)
        return result


@overload
def bounded_stream(
    func: Callable[[T], Awaitable[R]],
    items: Iterable[T] | AsyncIterable[T],
    *,
    concurrency: int,
    ordered: bool = True,
    return_exceptions: Literal[False] = False,
) -> BoundedStream[R]: ...


@overload
def bounded_stream(
    func: Callable[[T], Awaitable[R]],
    items: Iterable[T] | AsyncIterable[T],
    *,
    concurrency: int,
    ordered: bool = True,
    return_exceptions: Literal[True],
) -> BoundedStream[R | Exception]: ...


def bounded_stream(
    func: Callable[[T], Awaitable[R]],
    items: Iterable[T] | AsyncIterable[T],
    *,
    concurrency: int,
    ordered: bool = True,
    return_exceptions: bool = False,
) -> BoundedStream[Any]:
    """Like :func:`bounded_map`, but yields results as they become available.

    Use it as ``async with bounded_stream(...) as results: async for r in results``.

    Args:
        func: An async function taking one item.
        items: Any iterable or async iterable, consumed lazily.
        concurrency: Maximum number of concurrent calls.
        ordered: If ``True`` (the default), results are yielded in input
            order. In this mode the ``concurrency`` limit also bounds results
            buffered behind a slow item, so one slow item pauses the start of
            new calls until it finishes. If ``False``, results are yielded in
            completion order and slots are refilled as soon as they free up.
            In both modes, if you need to know which input produced a result,
            return it from ``func``.
        return_exceptions: As for :func:`bounded_map`.
    """
    runner: _Runner[T, R] = _Runner(func, items, concurrency)
    return BoundedStream(runner, ordered=ordered, return_exceptions=return_exceptions)
