from __future__ import annotations

import asyncio
import gc
import logging
import weakref
from itertools import pairwise

import pytest

from steadypy import SupervisedTask, TaskState, TaskSupervisor
from steadypy.testing import FakeClock


class Boom(Exception):
    pass


async def forever() -> None:
    await asyncio.Event().wait()


async def returns(value: int) -> int:
    await asyncio.sleep(0)
    return value


async def fails(message: str = "boom") -> None:
    await asyncio.sleep(0)
    raise Boom(message)


# -- basics -------------------------------------------------------------------


async def test_exit_waits_for_children_and_exposes_results() -> None:
    async with TaskSupervisor() as supervisor:
        a = supervisor.start(returns(1), name="a")
        b = supervisor.start(returns, 2, name="b")
    assert a.state is TaskState.COMPLETED
    assert b.state is TaskState.COMPLETED
    assert (a.result(), b.result()) == (1, 2)
    assert a.exception() is None
    assert supervisor.errors == ()


async def test_supervisor_with_no_tasks_exits_immediately() -> None:
    async with TaskSupervisor() as supervisor:
        pass
    assert dict(supervisor.tasks) == {}


async def test_task_names_are_visible_on_the_asyncio_task() -> None:
    seen: list[str] = []

    async def worker() -> None:
        task = asyncio.current_task()
        assert task is not None
        seen.append(task.get_name())

    async with TaskSupervisor() as supervisor:
        supervisor.start(worker, name="ingest")
    assert seen == ["ingest"]


async def test_default_names_are_unique() -> None:
    async def worker() -> None:
        await asyncio.sleep(0)

    async with TaskSupervisor() as supervisor:
        first = supervisor.start(worker)
        second = supervisor.start(worker)
        third = supervisor.start(worker())
    assert first.name.endswith("worker")
    assert second.name == f"{first.name}-2"
    assert third.name == f"{first.name}-3"
    assert set(supervisor.tasks) == {first.name, second.name, third.name}


async def test_duplicate_name_of_live_task_is_rejected_and_coroutine_closed() -> None:
    async with TaskSupervisor() as supervisor:
        supervisor.start(forever(), name="dup")
        coro = forever()
        with pytest.raises(ValueError, match="already running"):
            supervisor.start(coro, name="dup")
        assert coro.cr_frame is None  # closed, so no "never awaited" warning
        supervisor.stop()


async def test_name_can_be_reused_once_the_task_is_done() -> None:
    async with TaskSupervisor() as supervisor:
        first = supervisor.start(returns(1), name="job")
        await first.wait()
        second = supervisor.start(returns(2), name="job")
    assert supervisor.tasks["job"] is second
    assert second.result() == 2


async def test_tasks_mapping_is_a_read_only_snapshot() -> None:
    async with TaskSupervisor() as supervisor:
        handle = supervisor.start(returns(1), name="a")
        snapshot = supervisor.tasks
        with pytest.raises(TypeError):
            snapshot["b"] = handle  # type: ignore[index]
        supervisor.start(returns(2), name="b")
        assert set(snapshot) == {"a"}


async def test_handle_reports_running_then_completed() -> None:
    gate = asyncio.Event()

    async def worker() -> str:
        await gate.wait()
        return "ok"

    async with TaskSupervisor() as supervisor:
        handle = supervisor.start(worker)
        await asyncio.sleep(0)
        assert handle.state is TaskState.RUNNING
        assert not handle.done()
        with pytest.raises(asyncio.InvalidStateError):
            handle.result()
        with pytest.raises(asyncio.InvalidStateError):
            handle.exception()
        gate.set()
        await handle.wait()
        assert handle.state is TaskState.COMPLETED
        assert handle.result() == "ok"


# -- argument validation ------------------------------------------------------


async def test_start_outside_the_block_is_rejected() -> None:
    supervisor = TaskSupervisor()
    coro = forever()
    with pytest.raises(RuntimeError, match="outside"):
        supervisor.start(coro)
    assert coro.cr_frame is None
    async with supervisor:
        pass
    with pytest.raises(RuntimeError, match="already exited"):
        supervisor.start(forever)


async def test_supervisor_cannot_be_entered_twice() -> None:
    supervisor = TaskSupervisor()
    async with supervisor:
        pass
    with pytest.raises(RuntimeError, match="only be entered once"):
        async with supervisor:
            pass


async def test_restart_requires_a_callable_not_a_coroutine_object() -> None:
    async with TaskSupervisor() as supervisor:
        coro = forever()
        with pytest.raises(TypeError, match="cannot be restarted"):
            supervisor.start(coro, restart=True)
        assert coro.cr_frame is None
        with pytest.raises(TypeError, match="positional arguments"):
            supervisor.start(forever(), 1)  # type: ignore[call-overload]


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"restart": "sometimes"}, "restart must be"),
        ({"max_restarts": -1}, "max_restarts"),
        ({"restart_window": 0}, "restart_window"),
        ({"restart_delay": -1}, "restart_delay"),
        ({"restart_delay": 5, "max_restart_delay": 1}, "max_restart_delay"),
        ({"name": ""}, "non-empty"),
    ],
)
async def test_start_validates_arguments(kwargs: dict[str, object], message: str) -> None:
    async with TaskSupervisor() as supervisor:
        with pytest.raises(ValueError, match=message):
            supervisor.start(forever, **kwargs)  # type: ignore[call-overload]


async def test_non_awaitable_targets_are_rejected() -> None:
    async with TaskSupervisor() as supervisor:
        with pytest.raises(TypeError, match="target must be"):
            supervisor.start(42)  # type: ignore[call-overload]


async def test_callable_returning_a_non_awaitable_fails_the_task() -> None:
    def not_async() -> int:
        return 1

    with pytest.raises(ExceptionGroup) as info:
        async with TaskSupervisor() as supervisor:
            handle = supervisor.start(not_async)  # type: ignore[arg-type]
    assert handle.state is TaskState.FAILED
    assert info.group_contains(TypeError, match="not an awaitable")


# -- failure handling ---------------------------------------------------------


async def test_fail_fast_cancels_siblings_and_interrupts_the_body() -> None:
    sibling_cleaned_up = False
    body_interrupted = False

    async def sibling() -> None:
        nonlocal sibling_cleaned_up
        try:
            await forever()
        finally:
            sibling_cleaned_up = True

    with pytest.raises(ExceptionGroup) as info:
        async with TaskSupervisor() as supervisor:
            other = supervisor.start(sibling, name="sibling")
            bad = supervisor.start(fails, "kaput", name="bad")
            try:
                await forever()
            except asyncio.CancelledError:
                body_interrupted = True
                raise

    assert body_interrupted
    assert sibling_cleaned_up
    assert other.state is TaskState.CANCELLED
    assert bad.state is TaskState.FAILED
    assert info.group_contains(Boom, match="kaput")
    [error] = info.value.exceptions
    assert "raised by supervised task 'bad'" in error.__notes__
    # The cancellation used to interrupt the body is withdrawn, not leaked.
    current = asyncio.current_task()
    assert current is not None
    assert current.cancelling() == 0


async def test_fail_fast_after_the_body_finished() -> None:
    sibling_cancelled = asyncio.Event()

    async def sibling() -> None:
        try:
            await forever()
        except asyncio.CancelledError:
            sibling_cancelled.set()
            raise

    with pytest.raises(ExceptionGroup):
        async with TaskSupervisor() as supervisor:
            supervisor.start(sibling)
            supervisor.start(fails)
    assert sibling_cancelled.is_set()


async def test_isolated_mode_keeps_siblings_running() -> None:
    gate = asyncio.Event()

    async def sibling() -> str:
        await gate.wait()
        return "done"

    with pytest.raises(ExceptionGroup) as info:
        async with TaskSupervisor(fail_fast=False) as supervisor:
            bad = supervisor.start(fails, name="bad")
            good = supervisor.start(sibling, name="good")
            await bad.wait()
            assert not supervisor.stopping
            assert good.state is TaskState.RUNNING
            assert len(supervisor.errors) == 1
            gate.set()

    assert good.result() == "done"
    assert len(info.value.exceptions) == 1


async def test_isolated_mode_collects_every_failure() -> None:
    with pytest.raises(ExceptionGroup) as info:
        async with TaskSupervisor(fail_fast=False) as supervisor:
            supervisor.start(fails, "one")
            supervisor.start(fails, "two")
            supervisor.start(returns, 3)
    assert sorted(str(e) for e in info.value.exceptions) == ["one", "two"]


async def test_body_exception_cancels_children_and_propagates_unwrapped() -> None:
    cancelled = asyncio.Event()

    async def child() -> None:
        try:
            await forever()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    with pytest.raises(KeyError, match="body"):
        async with TaskSupervisor() as supervisor:
            handle = supervisor.start(child)
            await asyncio.sleep(0)
            raise KeyError("body")
    assert cancelled.is_set()
    assert handle.state is TaskState.CANCELLED


async def test_body_exception_is_grouped_with_child_failures() -> None:
    with pytest.raises(ExceptionGroup) as info:
        async with TaskSupervisor(fail_fast=False) as supervisor:
            handle = supervisor.start(fails, "child")
            await handle.wait()
            raise KeyError("body")
    assert [type(e) for e in info.value.exceptions] == [KeyError, Boom]


async def test_failed_handle_raises_from_result() -> None:
    with pytest.raises(ExceptionGroup):
        async with TaskSupervisor() as supervisor:
            handle = supervisor.start(fails, "x")
    assert isinstance(handle.exception(), Boom)
    with pytest.raises(Boom):
        handle.result()


async def test_system_exit_in_a_child_is_reraised_directly() -> None:
    async def exits() -> None:
        await asyncio.sleep(0)
        raise SystemExit(3)

    with pytest.raises(SystemExit) as info:
        async with TaskSupervisor() as supervisor:
            supervisor.start(exits, restart=True)
            await forever()
    assert info.value.code == 3


# -- stopping and cancellation ------------------------------------------------


async def test_stop_cancels_children_without_reporting_errors() -> None:
    async with TaskSupervisor() as supervisor:
        handles = [supervisor.start(forever) for _ in range(3)]
        await asyncio.sleep(0)
        supervisor.stop()
        supervisor.stop()  # idempotent
        assert supervisor.stopping
    assert all(h.state is TaskState.CANCELLED for h in handles)
    assert supervisor.errors == ()
    with pytest.raises(asyncio.CancelledError):
        handles[0].result()
    with pytest.raises(asyncio.CancelledError):
        handles[0].exception()


async def test_stop_rejects_new_tasks() -> None:
    async with TaskSupervisor() as supervisor:
        supervisor.stop()
        with pytest.raises(RuntimeError, match="stopping"):
            supervisor.start(forever)


async def test_stop_before_entering_is_an_error() -> None:
    with pytest.raises(RuntimeError, match="outside"):
        TaskSupervisor().stop()


async def test_cancelling_one_handle_leaves_the_others_running() -> None:
    gate = asyncio.Event()

    async def other() -> int:
        await gate.wait()
        return 7

    async with TaskSupervisor() as supervisor:
        victim = supervisor.start(forever, restart="always")
        survivor = supervisor.start(other)
        await asyncio.sleep(0)
        assert victim.cancel()
        await victim.wait()
        assert victim.state is TaskState.CANCELLED
        assert victim.restarts == 0
        assert not victim.cancel()  # already done
        assert survivor.state is TaskState.RUNNING
        gate.set()
    assert survivor.result() == 7
    assert supervisor.errors == ()


async def test_parent_cancellation_cancels_children_and_propagates() -> None:
    child_cleaned_up = asyncio.Event()
    entered = asyncio.Event()

    async def child() -> None:
        try:
            await forever()
        finally:
            await asyncio.sleep(0)  # cleanup that itself awaits
            child_cleaned_up.set()

    async def service() -> None:
        async with TaskSupervisor() as supervisor:
            supervisor.start(child)
            entered.set()
            await forever()

    task = asyncio.create_task(service())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert task.cancelled()
    assert child_cleaned_up.is_set()


# -- restarts -----------------------------------------------------------------


async def test_restart_on_failure_until_success(clock: FakeClock) -> None:
    runs = 0

    async def flaky() -> str:
        nonlocal runs
        runs += 1
        if runs < 3:
            raise Boom(f"run {runs}")
        return "connected"

    async with TaskSupervisor(clock=clock, restart_delay=1) as supervisor:
        handle = supervisor.start(flaky, restart=True)
        await clock.advance(10)

    assert handle.result() == "connected"
    assert (handle.restarts, handle.failures) == (2, 2)
    assert str(handle.last_exception) == "run 2"
    assert supervisor.errors == ()


async def test_restart_backoff_is_exponential_and_capped(clock: FakeClock) -> None:
    started_at: list[float] = []

    async def always_fails() -> None:
        started_at.append(clock.monotonic())
        raise Boom

    with pytest.raises(ExceptionGroup):
        async with TaskSupervisor(clock=clock, restart_delay=1, max_restart_delay=4) as sup:
            sup.start(always_fails, restart=True, max_restarts=5)
            await clock.advance(100)

    gaps = [b - a for a, b in pairwise(started_at)]
    assert gaps == [1, 2, 4, 4, 4]


async def test_restart_exhaustion_fails_the_task(clock: FakeClock) -> None:
    with pytest.raises(ExceptionGroup) as info:
        async with TaskSupervisor(clock=clock, restart_delay=0.1) as supervisor:
            handle = supervisor.start(fails, "persistent", restart=True, max_restarts=2)
            await clock.advance(10)
    assert handle.state is TaskState.FAILED
    assert (handle.restarts, handle.failures) == (2, 3)
    assert info.group_contains(Boom, match="persistent")


async def test_restart_always_also_restarts_after_a_normal_return(clock: FakeClock) -> None:
    runs = 0

    async def short_lived() -> int:
        nonlocal runs
        runs += 1
        return runs

    async with TaskSupervisor(clock=clock, restart_delay=0.5) as supervisor:
        handle = supervisor.start(short_lived, restart="always", max_restarts=3)
        await clock.advance(30)
    assert runs == 4
    assert handle.state is TaskState.COMPLETED
    assert handle.result() == 4


async def test_restart_on_failure_does_not_restart_after_success(clock: FakeClock) -> None:
    async with TaskSupervisor(clock=clock) as supervisor:
        handle = supervisor.start(returns, 5, restart=True)
    assert handle.restarts == 0
    assert handle.result() == 5


async def test_restart_window_forgets_old_restarts(clock: FakeClock) -> None:
    failures_at: list[float] = []
    release = asyncio.Event()

    async def worker() -> None:
        failures_at.append(clock.monotonic())
        if len(failures_at) == 2:
            await clock.sleep(50)  # a long healthy run before failing again
        if len(failures_at) == 4:
            await release.wait()
            return
        raise Boom

    async with TaskSupervisor(clock=clock, restart_delay=1) as supervisor:
        handle = supervisor.start(worker, restart=True, max_restarts=2, restart_window=10)
        await clock.advance(100)
        release.set()

    # Without the window, the third failure would have hit the limit of two
    # restarts. With it, the restart at t=0 had expired by t=51, so the backoff
    # also started again from 1s.
    assert failures_at == [0, 1, 52, 54]
    assert handle.state is TaskState.COMPLETED
    assert handle.restarts == 3


async def test_states_move_through_restarting(clock: FakeClock) -> None:
    with pytest.raises(ExceptionGroup):
        async with TaskSupervisor(clock=clock, restart_delay=5, fail_fast=False) as supervisor:
            handle = supervisor.start(fails, restart=True, max_restarts=1)
            await clock.advance(0)
            assert handle.state is TaskState.RESTARTING
            await clock.advance(4.9)
            assert handle.state is TaskState.RESTARTING
            await clock.advance(0.1)
            assert handle.state is TaskState.FAILED
            assert handle.restarts == 1


async def test_restarts_are_logged(clock: FakeClock, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING, logger="steadypy.supervisor")
    with pytest.raises(ExceptionGroup):
        async with TaskSupervisor(clock=clock, restart_delay=1) as supervisor:
            supervisor.start(fails, name="flaky", restart=True, max_restarts=1)
            await clock.advance(5)
    messages = [r.getMessage() for r in caplog.records]
    assert any("'flaky'" in m and "restarting in 1s" in m for m in messages)
    assert any("restart limit" in m for m in messages)


# -- cancellation edge cases --------------------------------------------------


async def test_parent_cancelled_while_the_exit_waits_for_children() -> None:
    cleaned_up = asyncio.Event()
    in_exit = asyncio.Event()

    async def child() -> None:
        try:
            await forever()
        finally:
            await asyncio.sleep(0)
            cleaned_up.set()

    async def service() -> None:
        async with TaskSupervisor() as supervisor:
            supervisor.start(child)
            in_exit.set()  # the body ends here; __aexit__ now waits for the child

    task = asyncio.create_task(service())
    await in_exit.wait()
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert cleaned_up.is_set()


async def test_swallowed_cancellation_is_never_restarted() -> None:
    inner: list[asyncio.Task[object]] = []
    runs = 0

    async def stubborn() -> None:
        nonlocal runs
        runs += 1
        task = asyncio.current_task()
        assert task is not None
        inner.append(task)
        try:
            await forever()
        except asyncio.CancelledError:
            return  # bad practice, but it happens

    async with TaskSupervisor() as supervisor:
        handle = supervisor.start(stubborn, restart="always")
        await asyncio.sleep(0)
        inner[0].cancel()  # cancel the asyncio task directly, not via the handle
        await handle.wait()
    assert runs == 1
    assert handle.state is TaskState.COMPLETED


async def stray_cancellation() -> None:
    """Await a future that something else cancelled; nobody cancelled us."""
    future = asyncio.get_running_loop().create_future()
    future.cancel()
    await future


async def test_stray_cancelled_error_without_restart_policy_ends_as_cancelled() -> None:
    async with TaskSupervisor() as supervisor:
        handle = supervisor.start(stray_cancellation)
        other = supervisor.start(returns, 1)
    assert handle.state is TaskState.CANCELLED
    assert other.result() == 1
    assert supervisor.errors == ()


async def test_stray_cancelled_error_restarts_a_task_with_a_restart_policy(
    clock: FakeClock, caplog: pytest.LogCaptureFixture
) -> None:
    runs = 0

    async def worker() -> str:
        nonlocal runs
        runs += 1
        if runs < 3:
            await stray_cancellation()
        return "recovered"

    caplog.set_level(logging.WARNING, logger="steadypy.supervisor")
    async with TaskSupervisor(clock=clock, restart_delay=1) as supervisor:
        handle = supervisor.start(worker, restart=True)
        await clock.advance(10)
    assert handle.result() == "recovered"
    assert handle.restarts == 2
    assert isinstance(handle.last_exception, asyncio.CancelledError)
    assert "CancelledError that nobody requested" in caplog.text


async def test_stray_cancellations_that_exhaust_restarts_end_as_cancelled(
    clock: FakeClock,
) -> None:
    async with TaskSupervisor(clock=clock, restart_delay=1) as supervisor:
        handle = supervisor.start(stray_cancellation, restart=True, max_restarts=2)
        await clock.advance(10)
    assert handle.state is TaskState.CANCELLED
    assert handle.restarts == 2
    assert supervisor.errors == ()


async def test_parent_cancelled_during_a_restart_backoff(clock: FakeClock) -> None:
    async def service() -> None:
        async with TaskSupervisor(clock=clock, restart_delay=3_600) as supervisor:
            supervisor.start(fails, restart=True)
            await forever()

    task = asyncio.create_task(service())
    await clock.advance(1)
    assert clock.pending_sleeps == 1  # the child is waiting out its backoff
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert clock.pending_sleeps == 0


async def test_cancelling_a_handle_during_its_backoff(clock: FakeClock) -> None:
    async with TaskSupervisor(clock=clock, restart_delay=10) as supervisor:
        handle = supervisor.start(fails, restart=True)
        await clock.advance(1)
        assert handle.state is TaskState.RESTARTING
        handle.cancel()
        await handle.wait()
        assert handle.state is TaskState.CANCELLED
    assert (handle.failures, handle.restarts) == (1, 0)
    assert supervisor.errors == ()


async def test_works_inside_asyncio_timeout() -> None:
    cancelled = asyncio.Event()

    async def child() -> None:
        try:
            await forever()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0), TaskSupervisor() as supervisor:
            supervisor.start(child)
            await forever()
    assert cancelled.is_set()


async def test_fail_fast_inside_an_unexpired_timeout_raises_the_failure() -> None:
    # The supervisor's own cancellation of the body must not be mistaken for
    # the timeout expiring, nor leave a stray cancellation request behind.
    with pytest.raises(ExceptionGroup):
        async with asyncio.timeout(3_600), TaskSupervisor() as supervisor:
            supervisor.start(fails)
            await forever()
    current = asyncio.current_task()
    assert current is not None
    assert current.cancelling() == 0
    await asyncio.sleep(0)  # and the task is still usable


# -- dynamic tasks, zero delays and introspection ------------------------------


async def test_children_can_start_siblings_while_the_exit_waits() -> None:
    results: list[str] = []

    async def spawner(supervisor: TaskSupervisor) -> None:
        await asyncio.sleep(0)
        supervisor.start(late_child, name="late")

    async def late_child() -> None:
        await asyncio.sleep(0)
        results.append("late child ran")

    async with TaskSupervisor() as supervisor:
        supervisor.start(spawner, supervisor)
    assert results == ["late child ran"]
    assert supervisor.tasks["late"].state is TaskState.COMPLETED


async def test_zero_restart_delay_restarts_immediately(clock: FakeClock) -> None:
    runs = 0

    async def flaky() -> None:
        nonlocal runs
        runs += 1
        if runs < 4:
            raise Boom

    async with TaskSupervisor(clock=clock, restart_delay=0) as supervisor:
        handle = supervisor.start(flaky, restart=True)
    assert runs == 4
    assert handle.restarts == 3
    assert clock.monotonic() == 0


async def test_introspection_strings() -> None:
    supervisor = TaskSupervisor(name="ingest")
    assert repr(supervisor) == "<TaskSupervisor 'ingest' new tasks=0 errors=0>"
    async with supervisor:
        handle = supervisor.start(forever, name="loop", restart="always")
        assert supervisor.name == "ingest"
        assert handle.restart_mode == "always"
        assert repr(supervisor) == "<TaskSupervisor 'ingest' running tasks=1 errors=0>"
        assert repr(handle) == "<SupervisedTask 'loop' state=running restarts=0 failures=0>"
        supervisor.stop()
        assert "stopping" in repr(supervisor)
    assert repr(supervisor) == "<TaskSupervisor 'ingest' closed tasks=0 errors=0>"


async def test_max_restart_delay_defaults_to_at_least_the_restart_delay(clock: FakeClock) -> None:
    starts: dict[str, list[float]] = {"a": [], "b": []}

    async def always_fails(label: str) -> None:
        starts[label].append(clock.monotonic())
        raise Boom

    with pytest.raises(ExceptionGroup):
        async with TaskSupervisor(clock=clock, restart_delay=60, fail_fast=False) as supervisor:
            supervisor.start(always_fails, "a", restart=True, max_restarts=2)
            supervisor.start(always_fails, "b", restart=True, max_restarts=1, restart_delay=100)
            await clock.advance(1_000)
    assert starts["a"] == [0, 60, 120]  # capped at 60, not doubled to 120
    assert starts["b"] == [0, 100]


# -- review fixes: lost cancellations, base errors, memory ------------------------


async def test_external_cancellation_survives_failures_raised_at_exit() -> None:
    entered = asyncio.Event()
    after_group: list[str] = []

    async def fails_during_cleanup() -> None:
        try:
            await forever()
        finally:
            raise Boom("cleanup failed")  # an error raised while being cancelled

    async def service() -> None:
        try:
            async with TaskSupervisor(fail_fast=False) as supervisor:
                supervisor.start(fails_during_cleanup)
                entered.set()
                await forever()
        except* Boom:
            after_group.append("handled the group")
        await asyncio.sleep(0)  # the pending cancellation must be delivered here
        after_group.append("unreachable")  # pragma: no cover

    task = asyncio.create_task(service())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert after_group == ["handled the group"]


async def test_base_exception_from_the_body_is_never_wrapped() -> None:
    with pytest.raises(SystemExit) as info:
        async with TaskSupervisor(fail_fast=False) as supervisor:
            handle = supervisor.start(fails)
            await handle.wait()
            raise SystemExit(2)
    assert info.value.code == 2


async def test_state_is_cancelled_as_soon_as_a_task_cancelled_before_starting_is_done() -> None:
    async with TaskSupervisor() as supervisor:
        handle = supervisor.start(forever)
        supervisor.stop()  # cancelled before its first step
        await asyncio.sleep(0)  # the task is done; its done callback has not run yet
        await handle.wait()
        assert handle.done()
        assert handle.state is TaskState.CANCELLED
        with pytest.raises(asyncio.CancelledError):
            handle.result()


def test_closing_an_abandoned_task_is_not_recorded_as_a_failure() -> None:
    loop = asyncio.new_event_loop()
    holder: list[object] = []

    async def start_and_abandon() -> None:
        supervisor = TaskSupervisor()
        await supervisor.__aenter__()
        handle = supervisor.start(forever)
        await asyncio.sleep(0)
        holder.append(handle)

    try:
        loop.run_until_complete(start_and_abandon())
        handle = holder[0]
        assert isinstance(handle, SupervisedTask)
        task = handle._task
        assert task is not None
        task.get_coro().close()  # what happens to a pending task when its loop is discarded
        assert handle.state is not TaskState.FAILED
        assert handle.failures == 0
    finally:
        loop.close()


async def test_finished_tasks_release_their_arguments() -> None:
    class Connection:
        pass

    connection = Connection()
    ref = weakref.ref(connection)

    async def handle_connection(conn: Connection) -> None:
        await asyncio.sleep(0)

    async with TaskSupervisor() as supervisor:
        handle = supervisor.start(handle_connection, connection)
        del connection
    gc.collect()
    assert handle.state is TaskState.COMPLETED
    assert ref() is None


async def test_retain_finished_false_keeps_only_live_tasks() -> None:
    with pytest.raises(ExceptionGroup) as info:
        async with TaskSupervisor(retain_finished=False, fail_fast=False) as supervisor:
            supervisor.start(forever, name="listener")
            handles = [supervisor.start(returns, n, name=f"conn-{n}") for n in range(50)]
            failing = supervisor.start(fails, "conn reset", name="conn-bad")
            for h in [*handles, failing]:
                await h.wait()
            await asyncio.sleep(0)
            assert set(supervisor.tasks) == {"listener"}
            assert len(supervisor.errors) == 1  # failures are still collected...
            again = supervisor.start(returns, 1, name="conn-0")  # ...and names are free again
            await again.wait()
            supervisor.stop()
    assert info.group_contains(Boom, match="conn reset")  # ...and raised at exit
    assert dict(supervisor.tasks) == {}
