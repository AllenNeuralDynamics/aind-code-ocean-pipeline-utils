"""Tests for aind_code_ocean_pipeline_utils.process."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import textwrap

import pytest

from aind_code_ocean_pipeline_utils import process
from aind_code_ocean_pipeline_utils.process import (
    GracefulExit,
    check_shutdown,
    install_shutdown_handlers,
    is_shutdown_requested,
    reset_shutdown_state,
    shutdown_handler,
)


@pytest.fixture(autouse=True)
def _clean_shutdown_state():
    """Restore default handlers and clear flags between tests."""
    reset_shutdown_state()
    yield
    reset_shutdown_state()


def test_graceful_exit_inherits_base_exception():
    assert issubclass(GracefulExit, BaseException)
    assert not issubclass(GracefulExit, Exception)


def test_graceful_exit_carries_signum_and_signame():
    exc = GracefulExit(signal.SIGINT)
    assert exc.signum == int(signal.SIGINT)
    assert exc.signame == "SIGINT"


def test_graceful_exit_unknown_signal_still_usable():
    exc = GracefulExit(9999)
    assert exc.signum == 9999
    assert "9999" in exc.signame


def test_install_is_idempotent():
    install_shutdown_handlers()
    handler_first = signal.getsignal(signal.SIGTERM)
    install_shutdown_handlers()
    handler_second = signal.getsignal(signal.SIGTERM)
    assert handler_first is handler_second


def test_signal_sets_flag_and_check_shutdown_raises():
    install_shutdown_handlers()
    assert not is_shutdown_requested()
    os.kill(os.getpid(), signal.SIGTERM)
    assert is_shutdown_requested()
    with pytest.raises(GracefulExit) as excinfo:
        check_shutdown()
    assert excinfo.value.signum == int(signal.SIGTERM)
    assert excinfo.value.signame == "SIGTERM"


def test_check_shutdown_is_noop_when_no_signal():
    install_shutdown_handlers()
    check_shutdown()  # must not raise


def test_graceful_exit_escapes_broad_except_exception():
    install_shutdown_handlers()
    os.kill(os.getpid(), signal.SIGTERM)
    with pytest.raises(GracefulExit):
        try:
            check_shutdown()
        except Exception:  # noqa: BLE001  # pragma: no cover - must not execute
            pytest.fail("GracefulExit should not be caught by `except Exception:`")


def test_shutdown_handler_context_exits_with_128_plus_signum():
    install_shutdown_handlers()
    os.kill(os.getpid(), signal.SIGTERM)
    with pytest.raises(SystemExit) as excinfo, shutdown_handler():
        check_shutdown()
    assert excinfo.value.code == 128 + int(signal.SIGTERM)


def test_shutdown_handler_invokes_callback_with_signum():
    install_shutdown_handlers()
    received: list[int] = []

    os.kill(os.getpid(), signal.SIGTERM)
    with pytest.raises(SystemExit), shutdown_handler(on_shutdown=received.append):
        check_shutdown()
    assert received == [int(signal.SIGTERM)]


def test_shutdown_handler_callback_exception_does_not_override_exit_code():
    install_shutdown_handlers()

    def _boom(_signum: int) -> None:
        raise RuntimeError("cleanup failed")

    os.kill(os.getpid(), signal.SIGTERM)
    with pytest.raises(SystemExit) as excinfo, shutdown_handler(on_shutdown=_boom):
        check_shutdown()
    assert excinfo.value.code == 128 + int(signal.SIGTERM)


def test_shutdown_handler_resets_state_on_clean_exit():
    with shutdown_handler():
        assert signal.getsignal(signal.SIGTERM) is process._signal_handler
    assert signal.getsignal(signal.SIGTERM) is not process._signal_handler
    assert not is_shutdown_requested()


def test_shutdown_handler_custom_exit_code_base():
    install_shutdown_handlers()
    os.kill(os.getpid(), signal.SIGINT)
    with pytest.raises(SystemExit) as excinfo, shutdown_handler(exit_code_base=200):
        check_shutdown()
    assert excinfo.value.code == 200 + int(signal.SIGINT)


def test_reset_restores_prior_handlers():
    sentinel_calls: list[int] = []

    def sentinel(signum: int, _frame: object) -> None:
        sentinel_calls.append(signum)

    prior = signal.signal(signal.SIGTERM, sentinel)
    try:
        install_shutdown_handlers()
        assert signal.getsignal(signal.SIGTERM) is process._signal_handler
        reset_shutdown_state()
        assert signal.getsignal(signal.SIGTERM) is sentinel
    finally:
        signal.signal(signal.SIGTERM, prior)


def test_double_signal_escalates_to_os_exit():
    """A second signal bypasses cleanup via os._exit(128 + signum).

    Run in a subprocess so the hard exit doesn't kill the test runner.
    """
    script = textwrap.dedent(
        """
        import os
        import signal
        import time

        from aind_code_ocean_pipeline_utils.process import install_shutdown_handlers

        install_shutdown_handlers()
        os.kill(os.getpid(), signal.SIGTERM)
        os.kill(os.getpid(), signal.SIGTERM)
        # If we reach here, escalation failed.
        time.sleep(1)
        raise SystemExit(0)
        """
    )
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, timeout=10, check=False)
    assert result.returncode == 128 + int(signal.SIGTERM), result.stderr.decode()
