"""Logging helpers for Code Ocean pipelines.

This module has two groups of helpers:

:func:`attach_file_log`
    Stdlib-only. Attaches a :class:`logging.FileHandler` to the root
    logger so a capsule's log survives as a file alongside the stream
    output captured by Code Ocean. Always available.

:func:`install_rich_handler`, :func:`build_progress`, :func:`make_progress_callback`
    Rich-aware helpers that play nicely with
    :class:`rich.progress.Progress`. Require the optional ``[rich]``
    extra. Rich is imported lazily on first call; if the extra isn't
    installed the call raises :class:`ImportError` with install
    instructions.

Rich progress: the problem
--------------------------
:class:`rich.progress.Progress` (and :class:`rich.live.Live` more generally)
repaints a live area 2–10 times per second. If Python's standard
:mod:`logging` emits a record from inside a ``with Progress():`` block
without going through rich, the next tick paints over the tail of the log
output. The most common casualty is :meth:`logging.Logger.exception`:
multi-line tracebacks get clipped and the ``ErrorType: message`` line that
tells you *what* went wrong silently disappears.

The fix: route :mod:`logging` through :class:`rich.logging.RichHandler`,
sharing the **same** :class:`rich.console.Console` instance that
:class:`Progress` / :class:`Live` uses. Rich then serializes log output
with the live area (pauses, prints, resumes)::

    from aind_code_ocean_pipeline_utils.log import install_rich_handler
    from rich.progress import Progress

    console = install_rich_handler()
    with Progress(console=console) as progress:   # <- same console!
        ...

Passing a separate ``Console`` to ``Progress`` (or letting it create its own)
reintroduces the bug. This module's signature — returning the ``Console`` —
is designed to make sharing the obvious path.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from rich.console import Console
    from rich.progress import Progress, TaskID

__all__ = [
    "attach_file_log",
    "build_progress",
    "install_rich_handler",
    "make_progress_callback",
]


_RICH_EXTRA_MSG = (
    "aind_code_ocean_pipeline_utils.log's rich-aware helpers require "
    "the [rich] extra. Install with: "
    "pip install aind-code-ocean-pipeline-utils[rich]"
)


def _require_rich() -> Any:
    """Import rich lazily, raising a pointed error if the extra is missing."""
    try:
        import rich.console
        import rich.logging
        import rich.progress
    except ImportError as exc:
        raise ImportError(_RICH_EXTRA_MSG) from exc
    return rich


# ── Stdlib-only: file-logging primitive ──


def attach_file_log(
    path: Path,
    *,
    level: int = logging.INFO,
    mode: str = "w",
    logger: logging.Logger | None = None,
) -> logging.FileHandler:
    """Attach a :class:`logging.FileHandler` alongside existing handlers.

    Keeps any previously configured StreamHandler in place so output still
    goes to stdout/stderr (captured by Code Ocean) while the log also lands
    on disk — useful for preserving a trace in ``/results`` after the
    capsule run ends.

    Parameters
    ----------
    path : pathlib.Path
        Destination file. Parent directories are created if needed.
    level : int, default ``logging.INFO``
        Level set on the installed handler.
    mode : str, default ``"w"``
        File-open mode. Defaults to overwrite so each role restarts its
        own log fresh. Pass ``"a"`` if you deliberately want to append
        across invocations.
    logger : logging.Logger, optional
        Logger to attach to. Defaults to the root logger.

    Returns
    -------
    logging.FileHandler
        The handler, already attached. Callers can keep the reference if
        they want to remove it later.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(path, mode=mode)
    handler.setLevel(level)
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s %(levelname)s %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        ),
    )
    target = logger if logger is not None else logging.getLogger()
    target.addHandler(handler)
    return handler


# ── Rich-aware helpers (require the [rich] extra) ──

_HANDLER_MARKER = "_aind_pipeline_utils_rich_handler"


def install_rich_handler(
    logger: logging.Logger | None = None,
    *,
    level: int = logging.INFO,
    console: Console | None = None,
    show_path: bool = False,
    rich_tracebacks: bool = True,
) -> Console:
    """Attach a :class:`RichHandler` to ``logger`` and return the :class:`Console`.

    Parameters
    ----------
    logger : logging.Logger, optional
        Logger to configure. Defaults to the root logger.
    level : int, default ``logging.INFO``
        Level to set on ``logger`` *and* on the installed handler.
    console : rich.console.Console, optional
        Console the handler should render to. If omitted, a new one is
        created. **Pass this same Console to any Progress / Live instance
        in the process** — see the module docstring for why.
    show_path : bool, default False
        Pass-through to :class:`RichHandler`. Module paths in log output
        are noise for CLI use; enable for debug builds.
    rich_tracebacks : bool, default True
        Pass-through to :class:`RichHandler`. Enables syntax-highlighted
        tracebacks with local-variable context.

    Returns
    -------
    rich.console.Console
        The :class:`Console` used by the handler — either ``console`` if
        supplied, or the freshly-created one. Callers should share it with
        any :class:`Progress` / :class:`Live` instance they construct.

    Notes
    -----
    Idempotent per logger: calling repeatedly on the same logger replaces
    the previously installed handler rather than stacking duplicates. The
    logger's other (non-rich) handlers are left untouched.
    """
    rich = _require_rich()
    target = logger if logger is not None else logging.getLogger()
    shared_console = console if console is not None else rich.console.Console()

    _remove_existing_handlers(target)

    handler = rich.logging.RichHandler(
        console=shared_console,
        show_path=show_path,
        rich_tracebacks=rich_tracebacks,
        level=level,
    )
    setattr(handler, _HANDLER_MARKER, True)
    target.addHandler(handler)
    target.setLevel(level)

    return shared_console


def _remove_existing_handlers(logger: logging.Logger) -> None:
    """Remove handlers previously installed by this module from ``logger``."""
    to_remove = [h for h in logger.handlers if getattr(h, _HANDLER_MARKER, False)]
    for handler in to_remove:
        logger.removeHandler(handler)


def _find_installed_console() -> Console:
    """Return the :class:`Console` from the root logger's installed :class:`RichHandler`.

    Raises :class:`RuntimeError` if :func:`install_rich_handler` has
    not been called. The error message points the caller at the fix.
    """
    for handler in logging.getLogger().handlers:
        if getattr(handler, _HANDLER_MARKER, False):
            # handler.console is a rich.console.Console; the getattr is
            # needed because the RichHandler class isn't in the static
            # type namespace after the rich lazy-import refactor.
            console: Console = handler.console  # type: ignore[attr-defined]
            return console
    msg = (
        "no rich handler found on the root logger; call install_rich_handler() "
        "before build_progress(), or pass console= explicitly"
    )
    raise RuntimeError(msg)


@contextmanager
def build_progress(
    total_items: int,
    *,
    console: Console | None = None,
    refresh_per_second: float = 0.5,
) -> Iterator[tuple[Progress, TaskID, TaskID]]:
    """Two-row progress display: overall counter + per-item bar.

    Yields ``(progress, overall_task, item_task)``. The overall task
    tracks completed items out of ``total_items`` — the caller
    advances it with ``progress.advance(overall_task)`` after each
    item finishes. The item task is initially hidden; the caller
    resets + retargets it per item::

        with build_progress(len(items)) as (progress, overall, item):
            for it in items:
                progress.reset(item, total=it.size, description=it.name, visible=True)
                do_work(it, on_progress=make_progress_callback(progress, item))
                progress.advance(overall)

    The :class:`Console` used for rendering must be the same one the
    :class:`RichHandler` is writing to; otherwise log output gets
    painted over. By default the :class:`Console` is discovered from
    the installed handler, so callers who use
    :func:`install_rich_handler` get correct behavior automatically.

    Parameters
    ----------
    total_items : int
        Expected number of items the caller will process.
    console : rich.console.Console, optional
        Explicit console override. If omitted, the module looks up
        the :class:`Console` attached to the installed
        :class:`RichHandler`.
    refresh_per_second : float, default 0.5
        Live-area refresh rate. Kept low by default because most
        pipeline capsules have long per-item work and high-frequency
        repaints burn CPU for no user benefit.

    Yields
    ------
    tuple[rich.progress.Progress, rich.progress.TaskID, rich.progress.TaskID]
        ``(progress, overall_task, item_task)``.

    Raises
    ------
    RuntimeError
        If ``console`` is ``None`` and no rich handler has been
        installed on the root logger.
    """
    rich = _require_rich()
    resolved = console if console is not None else _find_installed_console()
    columns = (
        rich.progress.SpinnerColumn(),
        rich.progress.TextColumn("[progress.description]{task.description}"),
        rich.progress.BarColumn(),
        rich.progress.TaskProgressColumn(),
        rich.progress.TimeRemainingColumn(),
    )
    with rich.progress.Progress(
        *columns, console=resolved, refresh_per_second=refresh_per_second,
    ) as progress:
        overall_task = progress.add_task("overall", total=total_items)
        item_task = progress.add_task("item", total=1, visible=False)
        yield progress, overall_task, item_task


def make_progress_callback(
    progress: Progress,
    task_id: TaskID,
) -> Callable[[int, int], None]:
    """Return a ``(processed, total) -> None`` callback that updates ``task_id``.

    Matches the ``ProgressCallback`` shape several AIND processing
    libraries expect (``build_mipmap(progress=...)`` and similar) so
    consumers can pass the returned callable directly.

    Parameters
    ----------
    progress : rich.progress.Progress
        The Progress instance the task belongs to.
    task_id : rich.progress.TaskID
        Which task the callback should update.

    Returns
    -------
    callable
        Accepts ``(processed, total)`` and calls
        :meth:`Progress.update` with ``completed=processed,
        total=total``.
    """

    def _callback(processed: int, total: int) -> None:
        progress.update(task_id, completed=processed, total=total)

    return _callback
