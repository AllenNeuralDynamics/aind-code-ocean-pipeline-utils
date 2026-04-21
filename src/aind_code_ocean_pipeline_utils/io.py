"""I/O helpers: retry on transient OS errors, atomic file writes.

The two primitives here address failure modes that recur in every pipeline
capsule: transient network or storage hiccups, and half-written files left
behind when a process is killed mid-write. Both exist to turn silent
correctness bugs into loud, retriable failures.

Examples
--------
Retry a flaky S3 download and write the result atomically::

    from aind_code_ocean_pipeline_utils.io import retry_on_oserror, atomic_json_write

    download = retry_on_oserror(_raw_download, retries=5)
    payload = download(url)
    atomic_json_write(out_path, payload)
"""

from __future__ import annotations

import errno
import functools
import json
import logging
import os
import tempfile
import time
from collections.abc import Callable, Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, TextIO, TypeVar

__all__ = [
    "TRANSIENT_ERRNOS",
    "atomic_json_write",
    "atomic_write_text",
    "retry_on_oserror",
]

_logger = logging.getLogger(__name__)

T = TypeVar("T")

#: errno values considered worth retrying.
#:
#: Permanent errors (ENOENT, EACCES, EINVAL, EISDIR, ...) are intentionally
#: excluded — retrying them hides configuration mistakes behind minutes of
#: exponential backoff. Callers with different failure models (e.g. a rate-
#: limited HTTP endpoint) can union in additional codes at the call site.
TRANSIENT_ERRNOS: frozenset[int] = frozenset(
    {
        errno.EIO,
        errno.EAGAIN,
        errno.EBUSY,
        errno.ENETDOWN,
        errno.ENETUNREACH,
        errno.ECONNRESET,
        errno.ETIMEDOUT,
        errno.EHOSTUNREACH,
    }
)


def retry_on_oserror(
    fn: Callable[..., T],
    *,
    retries: int = 5,
    initial_delay: float = 1.0,
    transient_errnos: frozenset[int] = TRANSIENT_ERRNOS,
) -> Callable[..., T]:
    """Return a wrapper that retries ``fn`` on transient :class:`OSError`.

    The wrapper catches :class:`OSError` whose ``errno`` is in
    ``transient_errnos``, waits ``(2 ** attempt) * initial_delay`` seconds,
    and retries up to ``retries`` times. Permanent errors surface
    immediately, as does the final attempt on exhaustion.

    Parameters
    ----------
    fn : callable
        The function to wrap. Can also be used as a bare decorator
        (``@retry_on_oserror``) since ``fn`` is positional.
    retries : int, default 5
        Number of retry attempts after the initial call. Total call count
        is ``retries + 1``.
    initial_delay : float, default 1.0
        Seconds to sleep before the first retry. Each subsequent retry
        doubles the delay (exponential backoff).
    transient_errnos : frozenset[int]
        errno values considered transient. Defaults to
        :data:`TRANSIENT_ERRNOS`.

    Returns
    -------
    callable
        A wrapper preserving ``fn``'s signature.
    """

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> T:
        start = time.monotonic()
        for attempt in range(retries + 1):
            try:
                return fn(*args, **kwargs)
            except OSError as exc:
                if exc.errno not in transient_errnos or attempt == retries:
                    raise
                delay = (2**attempt) * initial_delay
                _logger.warning(
                    "retry_on_oserror: %s errno=%d (%s) attempt=%d/%d elapsed=%.2fs sleeping=%.2fs",
                    getattr(fn, "__qualname__", repr(fn)),
                    exc.errno,
                    os.strerror(exc.errno or 0),
                    attempt + 1,
                    retries,
                    time.monotonic() - start,
                    delay,
                )
                time.sleep(delay)
        # Unreachable: either a successful return or a re-raised OSError
        # exits the loop.
        raise AssertionError("unreachable: retry loop exited without result")

    return wrapper


@contextmanager
def atomic_write_text(
    path: Path | str,
    *,
    fsync: bool = True,
    encoding: str = "utf-8",
) -> Generator[TextIO, None, None]:
    """Write ``path`` atomically via a sibling temp file + :func:`os.replace`.

    A file object is yielded for the caller to write to. On clean exit the
    buffer is flushed, optionally fsync'd, and the temp file is renamed
    over ``path``. On any exception the temp file is removed and the
    exception propagates; ``path`` is never left half-written.

    Parameters
    ----------
    path : Path or str
        Destination path. Its parent directory must already exist.
    fsync : bool, default True
        If True, fsync the temp file before rename. This is the only way
        to get durability guarantees on crash/power-loss; off-by-default
        would be a silent correctness regression on spot instances.
    encoding : str, default "utf-8"
        Text encoding for the yielded file handle.

    Yields
    ------
    TextIO
        A writable text file handle backed by the temp file.

    Notes
    -----
    Uses :func:`os.replace` (not :func:`os.rename`) so the rename is atomic
    on Windows as well as POSIX.
    """
    dest = Path(path)
    parent = dest.parent
    fd, tmp_name = tempfile.mkstemp(
        dir=parent if str(parent) else None,
        prefix=f".{dest.name}.",
        suffix=".tmp",
    )
    tmp_path = Path(tmp_name)
    handle = os.fdopen(fd, "w", encoding=encoding)
    committed = False
    try:
        try:
            yield handle
            handle.flush()
            if fsync:
                os.fsync(handle.fileno())
        finally:
            handle.close()
        os.replace(tmp_path, dest)
        committed = True
    finally:
        if not committed:
            try:
                tmp_path.unlink()
            except FileNotFoundError:
                pass


def atomic_json_write(
    path: Path | str,
    data: Any,
    *,
    fsync: bool = True,
    indent: int | None = 2,
    sort_keys: bool = True,
) -> None:
    """Serialize ``data`` as JSON and write to ``path`` atomically.

    Thin wrapper over :func:`atomic_write_text` for the common JSON case.

    Parameters
    ----------
    path : Path or str
        Destination path.
    data : Any
        JSON-serializable value.
    fsync : bool, default True
        See :func:`atomic_write_text`.
    indent : int or None, default 2
        Pass-through to :func:`json.dump`. Use ``None`` for compact output.
    sort_keys : bool, default True
        Pass-through to :func:`json.dump`. Sorted keys make diffs and
        fingerprints stable.
    """
    with atomic_write_text(path, fsync=fsync) as handle:
        json.dump(data, handle, indent=indent, sort_keys=sort_keys)
