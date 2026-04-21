"""Thread-pool submit wrapper that propagates :mod:`contextvars` correctly.

The Problem
-----------
:meth:`concurrent.futures.ThreadPoolExecutor.submit` runs the submitted
callable on a worker thread that does **not** inherit the caller's
:class:`contextvars.Context`. Any settings stored in context variables
(``scipy.fft.set_workers``, ``numpy.errstate``, domain-specific context
like request IDs or feature flags) silently disappear in the worker: the
code still runs, but the settings silently no-op, and bugs that only
appear at ``prefetch_chunks >= 2`` start showing up months later.

The Fix
-------
Snapshot the caller's context at submit time with
:func:`contextvars.copy_context` and run the callable inside that copy on
the worker. :func:`submit_with_context` does exactly that.

Per-submit copy, not shared
---------------------------
A :class:`~contextvars.Context` object can only be active on one thread at
a time — :meth:`Context.run` raises :class:`RuntimeError` if you try to
activate the same Context on two threads concurrently. So the copy must
be taken **per submit**, not once and reused across submits. A single
shared Context is the subtle bug that bit ``ecephys-mipmap-zarr`` at
``prefetch_chunks >= 2`` (commit ``f307c05``).

Example
-------
::

    from concurrent.futures import ThreadPoolExecutor
    from contextvars import ContextVar

    from aind_code_ocean_pipeline_utils.threading_utils import submit_with_context

    _mode = ContextVar("mode")

    _mode.set("fast")
    with ThreadPoolExecutor() as pool:
        future = submit_with_context(pool, worker)  # sees "fast"
        pool.submit(worker)  # does NOT see "fast"
"""

from __future__ import annotations

import contextvars
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable, TypeVar

__all__ = ["submit_with_context"]

T = TypeVar("T")


def submit_with_context(
    pool: ThreadPoolExecutor,
    fn: Callable[..., T],
    /,
    *args: Any,
    **kwargs: Any,
) -> "Future[T]":
    """Submit ``fn`` to ``pool`` preserving the caller's :class:`Context`.

    Equivalent to ``pool.submit(fn, *args, **kwargs)`` except the worker
    runs ``fn`` inside a fresh copy of the caller's
    :class:`~contextvars.Context`.

    Parameters
    ----------
    pool : concurrent.futures.ThreadPoolExecutor
        The executor to submit to. Positional-only so ``fn`` can use the
        name ``pool`` as a keyword argument without ambiguity.
    fn : callable
        The function to run on a worker thread.
    *args, **kwargs
        Forwarded to ``fn``.

    Returns
    -------
    concurrent.futures.Future
        The future returned by :meth:`ThreadPoolExecutor.submit`.

    Notes
    -----
    A new :class:`Context` is copied **per call**. Reusing a single copied
    context across submits would raise :class:`RuntimeError` as soon as
    two workers picked up the same submission concurrently.
    """
    ctx = contextvars.copy_context()
    return pool.submit(ctx.run, fn, *args, **kwargs)
