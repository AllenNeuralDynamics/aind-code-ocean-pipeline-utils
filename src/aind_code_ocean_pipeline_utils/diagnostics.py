"""First-log-line diagnostics for pipeline workers.

Two helpers that drop into any capsule ``run()`` so that post-mortem
analysis of a killed or crashed container is tractable.

- :func:`log_data_tree` dumps a depth-bounded view of ``/data`` on
  startup so mount-path mismatches ("where did my data asset land?")
  are visible in the log without an interactive session.
- :func:`start_memory_reporter` logs RSS + cgroup limit periodically
  from a daemon thread. When the kernel OOM killer / AWS Batch sends
  SIGKILL, no ``except`` block runs, but the last line of the captured
  log shows peak approach-to-limit — enough to distinguish OOM from
  spot reclamation from application errors.

Both helpers are stdlib-only and degrade gracefully on non-Linux
systems where ``/proc/self/status`` or cgroup files are absent.
"""

from __future__ import annotations

import logging
import os
import threading
from pathlib import Path

__all__ = [
    "MemoryReporter",
    "log_data_tree",
    "start_memory_reporter",
]

_logger = logging.getLogger(__name__)

_PROC_STATUS = "/proc/self/status"
_CGROUP_LIMIT_PATHS: tuple[str, ...] = (
    "/sys/fs/cgroup/memory.max",  # cgroup v2
    "/sys/fs/cgroup/memory/memory.limit_in_bytes",  # cgroup v1
)
# Values at or above this are the kernel's "effectively unlimited"
# sentinels — skip logging them so "no limit" doesn't look like a
# meaningful ceiling.
_NO_LIMIT_SENTINEL = 1 << 50


def log_data_tree(
    root: Path | str = Path("/data"),
    *,
    max_depth: int = 3,
    logger: logging.Logger | None = None,
) -> None:
    """Log a depth-bounded listing of ``root`` on startup.

    Uses :func:`os.walk` with ``followlinks=True`` because Code Ocean
    stages data via symlink chains that :meth:`Path.glob` doesn't
    traverse by default. Each directory logs one line with its
    ``(dirs, files)`` counts. The depth cap keeps zarr chunk trees
    from flooding the log.

    Parameters
    ----------
    root : Path or str, default ``Path("/data")``
        Top of the tree. If missing, one line is logged and the
        function returns without an error.
    max_depth : int, default 3
        Number of levels below ``root`` to descend.
    logger : logging.Logger, optional
        Target logger. Defaults to this module's logger.
    """
    log = logger if logger is not None else _logger
    base = Path(root)
    if not base.exists():
        log.info("data tree: %s does not exist", base)
        return
    log.info("data tree under %s (max depth %d, symlinks followed):", base, max_depth)
    for cur_root, dirs, files in os.walk(base, followlinks=True):
        cur_path = Path(cur_root)
        depth = len(cur_path.relative_to(base).parts)
        if depth >= max_depth:
            dirs[:] = []
        rel = cur_path.relative_to(base) if cur_path != base else Path(".")
        log.info("  %s/  (%d dirs, %d files)", rel, len(dirs), len(files))


def _read_vmrss_kb(status_path: str = _PROC_STATUS) -> int | None:
    """Read VmRSS (kB) from ``status_path``; return None on any failure."""
    try:
        with open(status_path) as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except (OSError, ValueError):
        return None
    return None


def _read_cgroup_limit_bytes(
    candidate_paths: tuple[str, ...] = _CGROUP_LIMIT_PATHS,
) -> int | None:
    """Return the first readable cgroup memory limit in bytes, or None."""
    for path in candidate_paths:
        try:
            with open(path) as f:
                raw = f.read().strip()
        except OSError:
            continue
        if raw == "max":
            return None
        try:
            return int(raw)
        except ValueError:
            continue
    return None


class MemoryReporter:
    """Handle for a running background memory reporter.

    Returned by :func:`start_memory_reporter`. Callers can :meth:`stop`
    the reporter explicitly (useful for deterministic test teardown
    and for capsules that want to quiet the log before shutdown).
    The underlying thread is a daemon, so omitting :meth:`stop` is
    fine — the process will exit regardless.
    """

    def __init__(self, thread: threading.Thread, stop_event: threading.Event) -> None:
        self._thread = thread
        self._stop_event = stop_event

    def stop(self, *, timeout: float | None = 1.0) -> None:
        """Signal the reporter to exit and join its thread.

        Parameters
        ----------
        timeout : float or None, default 1.0
            Passed to :meth:`threading.Thread.join`. None waits
            indefinitely.
        """
        self._stop_event.set()
        self._thread.join(timeout=timeout)

    @property
    def is_alive(self) -> bool:
        """Whether the reporter thread is still running."""
        return self._thread.is_alive()


def start_memory_reporter(
    *,
    interval_s: float = 15.0,
    logger: logging.Logger | None = None,
    status_path: str = _PROC_STATUS,
    cgroup_limit_paths: tuple[str, ...] = _CGROUP_LIMIT_PATHS,
) -> MemoryReporter:
    """Start a daemon thread that logs RSS + cgroup limit periodically.

    On the first tick the cgroup limit is logged (if any). On every
    tick — including the first — VmRSS is logged as a GB value and,
    when a limit is known, as a percentage of that limit.

    The reporter silently skips ticks on platforms where
    ``/proc/self/status`` is unreadable (macOS, Windows).

    Parameters
    ----------
    interval_s : float, default 15.0
        Seconds between ticks. Also the worst-case delay between
        :meth:`MemoryReporter.stop` being called and the thread exiting.
    logger : logging.Logger, optional
        Target logger. Defaults to this module's logger.
    status_path : str
        Override for ``/proc/self/status``. Primarily a test seam.
    cgroup_limit_paths : tuple[str, ...]
        Override for the cgroup-limit candidate list. Primarily a
        test seam.

    Returns
    -------
    MemoryReporter
        Handle with a :meth:`MemoryReporter.stop` method.
    """
    log = logger if logger is not None else _logger
    stop_event = threading.Event()

    def _loop() -> None:
        limit = _read_cgroup_limit_bytes(cgroup_limit_paths)
        if limit is not None and limit < _NO_LIMIT_SENTINEL:
            log.info("memory: cgroup limit %.1f GB", limit / 1e9)
        # Loop: log a tick, then wait. Using Event.wait so stop() is
        # responsive even mid-interval.
        while True:
            rss_kb = _read_vmrss_kb(status_path)
            if rss_kb is not None:
                msg = f"memory: VmRSS {rss_kb / 1e6:.2f} GB"
                if limit is not None and limit < _NO_LIMIT_SENTINEL:
                    msg += f" ({100 * rss_kb * 1024 / limit:.0f}% of cgroup)"
                log.info(msg)
            if stop_event.wait(timeout=interval_s):
                return

    thread = threading.Thread(target=_loop, daemon=True, name="aind-mem-reporter")
    thread.start()
    return MemoryReporter(thread, stop_event)
