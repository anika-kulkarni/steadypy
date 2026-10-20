from __future__ import annotations

import asyncio
import itertools
from collections.abc import AsyncIterator, Iterator

import pytest

from steadypy import bounded_map, bounded_stream


class Tracker:
    """Records concurrency and lets the test decide when each item finishes."""

    def __init__(self) -> None:
        self.active = 0
        self.max_active = 0
        self.started: list[int] = []
        self.cancelled: list[int] = []
        self.gates: dict[int, asyncio.Event] = {}

    def gate(self, item: int) -> asyncio.Event:
        return self.gates.setdefault(item, asyncio.Event())

    async def __call__(self, item: int) -> int:
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        self.started.append(item)
        try:
            await self.gate(item).wait()
            return item * 10
        except asyncio.CancelledError:
            self.cancelled.append(item)
            raise
        finally:
            self.active -= 1


async def settle() -> None:
    for _ in range(10):
        await asyncio.sleep(0)


async def double(item: int) -> int:
    await asyncio.sleep(0)
    return item * 2


# -- bounded_map --------------------------------------------------------------


async def test_results_are_in_input_order_regardless_of_completion_order() -> None:
    tracker = Tracker()
    task = asyncio.create_task(bounded_map(tracker, range(5), concurrency=5))
    await settle()
    for item in reversed(range(5)):
        tracker.gate(item).set()
        await settle()
    assert await task == [0, 10, 20, 30, 40]


async def test_never_exceeds_the_concurrency_limit() -> None:
    tracker = Tracker()
    task = asyncio.create_task(bounded_map(tracker, range(20), concurrency=3))
    for item in range(20):
        await settle()
        assert tracker.active <= 3
        tracker.gate(item).set()
    await task
    assert tracker.max_active == 3


async def test_a_slow_item_does_not_hold_back_the_others() -> None:
    tracker = Tracker()
    task = asyncio.create_task(bounded_map(tracker, range(6), concurrency=2))
    await settle()
    for item in range(1, 6):  # item 0 stays slow
        tracker.gate(item).set()
        await settle()
    assert tracker.started == [0, 1, 2, 3, 4, 5]
    tracker.gate(0).set()
    assert await task == [0, 10, 20, 30, 40, 50]


async def test_input_is_consumed_lazily() -> None:
    pulled = 0

    def numbers() -> Iterator[int]:
        nonlocal pulled
        for n in itertools.count():
            pulled += 1
            yield n

    tracker = Tracker()
    task = asyncio.create_task(bounded_map(tracker, numbers(), concurrency=4))
    await settle()
    assert pulled == 4  # an infinite generator, but only one item per slot
    tracker.gate(0).set()
    await settle()
    assert pulled == 5
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_accepts_async_iterables() -> None:
    async def numbers() -> AsyncIterator[int]:
        for n in range(4):
            await asyncio.sleep(0)
            yield n

    assert await bounded_map(double, numbers(), concurrency=2) == [0, 2, 4, 6]


async def test_empty_input_returns_an_empty_list() -> None:
    assert await bounded_map(double, [], concurrency=3) == []


async def test_first_failure_cancels_in_flight_calls_and_stops_pulling() -> None:
    pulled = 0
    tracker = Tracker()

    def numbers() -> Iterator[int]:
        nonlocal pulled
        for n in range(100):
            pulled += 1
            yield n

    async def func(item: int) -> int:
        if item == 2:
            await asyncio.sleep(0)
            raise LookupError(f"item {item}")
        return await tracker(item)

    with pytest.raises(LookupError, match="item 2"):
        await bounded_map(func, numbers(), concurrency=4)
    assert sorted(tracker.cancelled) == [0, 1, 3]
    assert tracker.active == 0
    assert pulled == 4


async def test_return_exceptions_puts_failures_in_place() -> None:
    async def func(item: int) -> int:
        await asyncio.sleep(0)
        if item % 2:
            raise ValueError(item)
        return item

    results = await bounded_map(func, range(5), concurrency=2, return_exceptions=True)
    assert results[0::2] == [0, 2, 4]
    assert [type(r) for r in results[1::2]] == [ValueError, ValueError]


async def test_return_exceptions_never_swallows_base_exceptions() -> None:
    class Abort(BaseException):
        pass

    async def func(item: int) -> int:
        await asyncio.sleep(0)
        raise Abort(item)

    with pytest.raises(Abort):
        await bounded_map(func, range(3), concurrency=2, return_exceptions=True)


async def test_cancelling_the_caller_cancels_and_awaits_in_flight_calls() -> None:
    tracker = Tracker()
    task = asyncio.create_task(bounded_map(tracker, range(10), concurrency=3))
    await settle()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert sorted(tracker.cancelled) == [0, 1, 2]
    assert tracker.active == 0


async def test_errors_from_the_input_iterator_propagate_after_cleanup() -> None:
    tracker = Tracker()

    def numbers() -> Iterator[int]:
        yield 0
        yield 1
        raise RuntimeError("source failed")

    task = asyncio.create_task(bounded_map(tracker, numbers(), concurrency=2))
    await settle()
    tracker.gate(0).set()  # frees a slot, so the next pull hits the error
    with pytest.raises(RuntimeError, match="source failed"):
        await task
    assert tracker.cancelled == [1]
    assert tracker.active == 0


@pytest.mark.parametrize(
    ("kwargs", "error"),
    [
        ({"concurrency": 0}, ValueError),
        ({"concurrency": True}, ValueError),
        ({"concurrency": 2.5}, ValueError),
    ],
)
async def test_concurrency_is_validated(kwargs: dict[str, object], error: type) -> None:
    with pytest.raises(error):
        await bounded_map(double, [1], **kwargs)  # type: ignore[call-overload]


async def test_items_and_func_are_validated() -> None:
    with pytest.raises(TypeError, match="iterable"):
        await bounded_map(double, 42, concurrency=1)  # type: ignore[call-overload]
    with pytest.raises(TypeError, match="callable"):
        await bounded_map(42, [1], concurrency=1)  # type: ignore[call-overload]


# -- bounded_stream -----------------------------------------------------------


async def test_unordered_stream_yields_in_completion_order() -> None:
    tracker = Tracker()
    seen: list[int] = []

    async def consume() -> None:
        async with bounded_stream(tracker, range(3), concurrency=3, ordered=False) as results:
            async for result in results:
                seen.append(result)

    task = asyncio.create_task(consume())
    for item in (2, 0, 1):
        await settle()
        tracker.gate(item).set()
    await task
    assert seen == [20, 0, 10]


async def test_ordered_stream_bounds_work_buffered_behind_a_slow_item() -> None:
    tracker = Tracker()
    seen: list[int] = []

    async def consume() -> None:
        async with bounded_stream(tracker, range(10), concurrency=3) as results:
            async for result in results:
                seen.append(result)

    task = asyncio.create_task(consume())
    await settle()
    tracker.gate(1).set()
    tracker.gate(2).set()
    await settle()
    # Items 1 and 2 finished, but item 0 has not, so no new calls start.
    assert tracker.started == [0, 1, 2]
    assert seen == []
    tracker.gate(0).set()
    await settle()
    assert seen == [0, 10, 20]
    for gate in range(3, 10):
        tracker.gate(gate).set()
    await task
    assert seen == [n * 10 for n in range(10)]


async def test_breaking_out_of_a_stream_cancels_in_flight_calls() -> None:
    tracker = Tracker()
    tracker.gate(0).set()
    async with bounded_stream(tracker, itertools.count(), concurrency=4) as results:
        async for result in results:
            assert result == 0
            break
    # Slots are refilled when the next result is requested, so item 4 was never
    # started; the three calls still in flight were cancelled and awaited.
    assert tracker.started == [0, 1, 2, 3]
    assert sorted(tracker.cancelled) == [1, 2, 3]
    assert tracker.active == 0


async def test_stream_surfaces_failures_and_can_return_them() -> None:
    async def func(item: int) -> int:
        await asyncio.sleep(0)
        if item == 1:
            raise ValueError("bad item")
        return item

    async with bounded_stream(func, range(3), concurrency=1, return_exceptions=True) as results:
        collected = [r async for r in results]
    assert collected[0] == 0
    assert isinstance(collected[1], ValueError)
    assert collected[2] == 2

    with pytest.raises(ValueError, match="bad item"):
        async with bounded_stream(func, range(3), concurrency=1) as results:
            async for _ in results:
                pass


async def test_stream_must_be_used_as_a_context_manager() -> None:
    stream = bounded_stream(double, [1, 2], concurrency=1)
    with pytest.raises(RuntimeError, match="inside its context manager"):
        await anext(stream)
    async with stream:
        assert [r async for r in stream] == [2, 4]
    with pytest.raises(RuntimeError, match="inside its context manager"):
        await anext(stream)
    with pytest.raises(RuntimeError, match="only be entered once"):
        async with stream:
            pass


# -- cancellation during cleanup and composition -------------------------------


async def test_cleanup_finishes_even_if_the_caller_is_cancelled_again() -> None:
    release_cleanup = asyncio.Event()
    finished_cleanup: list[int] = []

    async def slow_to_cancel(item: int) -> int:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await release_cleanup.wait()  # e.g. flushing a buffer
            finished_cleanup.append(item)
            raise
        return item  # pragma: no cover

    task = asyncio.create_task(bounded_map(slow_to_cancel, range(2), concurrency=2))
    await settle()
    task.cancel()
    await settle()
    task.cancel()  # an impatient second cancellation
    await settle()
    assert not task.done()  # still waiting for the calls to finish cleaning up
    release_cleanup.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert sorted(finished_cleanup) == [0, 1]


async def test_stopping_a_supervisor_cancels_bounded_work_inside_it() -> None:
    from steadypy import TaskSupervisor

    tracker = Tracker()

    async def batch_job() -> list[int]:
        return await bounded_map(tracker, range(100), concurrency=5)

    async with TaskSupervisor() as supervisor:
        handle = supervisor.start(batch_job)
        await settle()
        assert tracker.active == 5
        supervisor.stop()
    assert handle.done()
    assert sorted(tracker.cancelled) == [0, 1, 2, 3, 4]
    assert tracker.active == 0
