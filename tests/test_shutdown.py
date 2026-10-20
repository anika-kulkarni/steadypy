from __future__ import annotations

import asyncio
import os
import signal
import sys
from collections.abc import Iterator
from typing import Any

import pytest

from steadypy import ShutdownSignal

unix_only = pytest.mark.skipif(sys.platform == "win32", reason="needs SIGUSR1/SIGUSR2")


@pytest.fixture
def shutdown() -> Iterator[ShutdownSignal]:
    instance = ShutdownSignal()
    yield instance
    instance.remove_signal_handlers()


async def test_trigger_releases_waiters_with_the_first_reason(shutdown: ShutdownSignal) -> None:
    waiters = [asyncio.create_task(shutdown.wait()) for _ in range(3)]
    await asyncio.sleep(0)
    assert not any(w.done() for w in waiters)
    assert not shutdown.is_set()

    assert shutdown.trigger("deploy")
    assert not shutdown.trigger("second request")
    assert await asyncio.gather(*waiters) == ["deploy"] * 3
    assert shutdown.is_set()
    assert shutdown.reason == "deploy"
    assert await shutdown.wait() == "deploy"  # already set: returns at once
    assert "triggered (deploy)" in repr(shutdown)


async def test_default_reason(shutdown: ShutdownSignal) -> None:
    assert shutdown.reason is None
    shutdown.trigger()
    assert shutdown.reason == "requested"


async def test_waiting_is_cancellable(shutdown: ShutdownSignal) -> None:
    waiter = asyncio.create_task(shutdown.wait())
    await asyncio.sleep(0)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    assert not shutdown.is_set()


@unix_only
async def test_signal_triggers_shutdown(shutdown: ShutdownSignal) -> None:
    installed = shutdown.install_signal_handlers([signal.SIGUSR1])
    assert installed == (signal.SIGUSR1,)
    os.kill(os.getpid(), signal.SIGUSR1)
    async with asyncio.timeout(5):  # only reached if delivery is broken
        assert await shutdown.wait() == "received SIGUSR1"


@unix_only
async def test_removing_handlers_restores_the_previous_ones(shutdown: ShutdownSignal) -> None:
    received: list[int] = []

    def previous(signum: int, frame: object) -> None:
        received.append(signum)

    original = signal.signal(signal.SIGUSR2, previous)
    try:
        shutdown.install_signal_handlers([signal.SIGUSR2])
        assert signal.getsignal(signal.SIGUSR2) is not previous
        shutdown.remove_signal_handlers()
        assert signal.getsignal(signal.SIGUSR2) is previous
        shutdown.remove_signal_handlers()  # idempotent
    finally:
        signal.signal(signal.SIGUSR2, original)


@unix_only
async def test_installing_the_same_signal_twice_is_a_no_op(shutdown: ShutdownSignal) -> None:
    assert shutdown.install_signal_handlers([signal.SIGUSR1]) == (signal.SIGUSR1,)
    assert shutdown.install_signal_handlers([signal.SIGUSR1]) == ()


async def test_defaults_to_sigint_and_sigterm_without_sending_them(
    shutdown: ShutdownSignal,
) -> None:
    before = (signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM))
    assert shutdown.install_signal_handlers() == (signal.SIGINT, signal.SIGTERM)
    shutdown.remove_signal_handlers()
    assert (signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM)) == before
    assert not shutdown.is_set()


async def test_falls_back_to_signal_module_without_loop_support(
    shutdown: ShutdownSignal, monkeypatch: pytest.MonkeyPatch
) -> None:
    loop = asyncio.get_running_loop()

    def unsupported(*args: Any) -> None:
        raise NotImplementedError

    installed: dict[int, Any] = {}

    def fake_signal(signum: int, handler: Any) -> Any:
        installed[signum] = handler
        return None

    monkeypatch.setattr(loop, "add_signal_handler", unsupported)
    monkeypatch.setattr(signal, "signal", fake_signal)
    monkeypatch.setattr(signal, "getsignal", lambda signum: signal.SIG_DFL)

    shutdown.install_signal_handlers([signal.SIGTERM])
    handler = installed[signal.SIGTERM]
    assert callable(handler)
    handler(signal.SIGTERM, None)  # what the interpreter would do on delivery
    async with asyncio.timeout(5):
        assert await shutdown.wait() == "received SIGTERM"

    shutdown.remove_signal_handlers()
    assert installed[signal.SIGTERM] == signal.SIG_DFL


async def test_installing_off_the_main_thread_is_refused(shutdown: ShutdownSignal) -> None:
    with pytest.raises(RuntimeError, match="main thread"):
        await asyncio.to_thread(shutdown.install_signal_handlers)
