"""Graceful shutdown for long-running pipeline processes.

SIGINT / SIGTERM are caught by a flag-only handler and surfaced at safe points
via :class:`GracefulExit`. A second signal escalates to an immediate
``os._exit``, so double-Ctrl+C is never blocked by a stuck cleanup path.

Typical use::

    from aind_code_ocean_pipeline_utils.process import shutdown_handler, check_shutdown

    with shutdown_handler():
        for shard in shards:
            check_shutdown()
            process(shard)

Notes
-----
The handler itself flips a :class:`threading.Event` and returns. Signal
handlers run at arbitrary instruction boundaries, and most of the stdlib
(including :mod:`logging` and :func:`sys.exit`) is not async-signal-safe;
doing real work inside the handler risks deadlocks and corrupted output.
:class:`GracefulExit` inherits from :class:`BaseException` (like
:class:`KeyboardInterrupt` / :class:`SystemExit`) so broad ``except
Exception:`` blocks in library code cannot swallow a shutdown request.
"""

from __future__ import annotations

import logging
import os
import signal
import sys
import threading
from collections.abc import Callable, Generator
from contextlib import contextmanager
from types import FrameType

__all__ = [
    "GracefulExit",
    "check_shutdown",
    "install_shutdown_handlers",
    "is_shutdown_requested",
    "reset_shutdown_state",
    "shutdown_handler",
]

_logger = logging.getLogger(__name__)

_HANDLED_SIGNALS: tuple[signal.Signals, ...] = (signal.SIGINT, signal.SIGTERM)

_shutdown_event = threading.Event()
_received_signum: int | None = None
_double_signal_event = threading.Event()
_previous_handlers: dict[signal.Signals, signal.Handlers | Callable[..., object] | int | None] = {}
_installed = False
_install_lock = threading.Lock()


class GracefulExit(BaseException):
    """Raised by :func:`check_shutdown` after a SIGINT or SIGTERM is received.

    Inherits from :class:`BaseException` (not :class:`Exception`) so that
    consumer code's broad ``except Exception:`` handlers do not accidentally
    swallow shutdown requests, mirroring :class:`KeyboardInterrupt` and
    :class:`SystemExit`.

    Attributes
    ----------
    signum : int
        The numeric signal that triggered shutdown.
    signame : str
        Human-readable signal name (e.g. ``"SIGINT"``).
    """

    signum: int
    signame: str

    def __init__(self, signum: int) -> None:
        self.signum = signum
        try:
            self.signame = signal.Signals(signum).name
        except ValueError:
            self.signame = f"signal {signum}"
        super().__init__(f"shutdown requested by {self.signame}")


def _signal_handler(signum: int, frame: FrameType | None) -> None:
    """Async-signal-safe handler: flip flags and return.

    A second signal of any handled kind escalates to ``os._exit`` so a stuck
    cleanup path (e.g. a blocked FUSE read) cannot indefinitely delay
    termination.
    """
    del frame
    global _received_signum
    if _shutdown_event.is_set():
        _double_signal_event.set()
        os._exit(128 + signum)
    _received_signum = signum
    _shutdown_event.set()


def install_shutdown_handlers() -> None:
    """Install signal handlers for SIGINT and SIGTERM.

    Idempotent: calling more than once has no additional effect. The prior
    handlers are recorded so :func:`reset_shutdown_state` can restore them.
    """
    global _installed
    with _install_lock:
        if _installed:
            return
        for sig in _HANDLED_SIGNALS:
            _previous_handlers[sig] = signal.getsignal(sig)
            signal.signal(sig, _signal_handler)
        _installed = True


def is_shutdown_requested() -> bool:
    """Return whether a shutdown signal has been received."""
    return _shutdown_event.is_set()


def check_shutdown() -> None:
    """Raise :class:`GracefulExit` if a shutdown signal has been received.

    Intended to be called at safe points in the main loop (shard boundaries,
    between I/O operations) rather than inside hot numeric kernels.
    """
    if _shutdown_event.is_set():
        assert _received_signum is not None
        raise GracefulExit(_received_signum)


def reset_shutdown_state() -> None:
    """Clear the shutdown flags and restore prior signal handlers.

    Primarily useful for tests and for long-lived processes that handle
    multiple shutdown scopes. Restores the handlers that were registered
    before :func:`install_shutdown_handlers` was called.
    """
    global _installed, _received_signum
    with _install_lock:
        if _installed:
            for sig, prev in _previous_handlers.items():
                try:
                    signal.signal(sig, prev)
                except (TypeError, ValueError, OSError):
                    signal.signal(sig, signal.SIG_DFL)
            _previous_handlers.clear()
            _installed = False
        _shutdown_event.clear()
        _double_signal_event.clear()
        _received_signum = None


@contextmanager
def shutdown_handler(
    *,
    exit_code_base: int = 128,
    on_shutdown: Callable[[int], None] | None = None,
) -> Generator[None, None, None]:
    """Install shutdown handlers and translate :class:`GracefulExit` into ``sys.exit``.

    Parameters
    ----------
    exit_code_base : int, default 128
        Base added to ``signum`` to produce the process exit code. ``128`` is
        the Unix "killed by signal N" convention, yielding 130 for SIGINT and
        143 for SIGTERM.
    on_shutdown : callable, optional
        If provided, called with the received signum before the process
        exits. Use for flushing caches, closing handles, etc. Exceptions
        raised inside the callback are logged but do not override the exit
        code.

    Yields
    ------
    None

    Notes
    -----
    On normal exit the shutdown state is reset and the prior signal handlers
    are restored, so the context manager is safe to use more than once per
    process. On a received signal, ``sys.exit(exit_code_base + signum)`` is
    called after ``on_shutdown`` runs.
    """
    install_shutdown_handlers()
    try:
        yield
    except GracefulExit as exc:
        if on_shutdown is not None:
            try:
                on_shutdown(exc.signum)
            except Exception:
                _logger.exception("on_shutdown callback raised; continuing to exit")
        sys.exit(exit_code_base + exc.signum)
    finally:
        reset_shutdown_state()
