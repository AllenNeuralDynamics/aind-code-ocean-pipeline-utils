"""Rich-aware logging setup that plays nicely with :class:`rich.progress.Progress`.

The Problem
-----------
:class:`rich.progress.Progress` (and :class:`rich.live.Live` more generally)
repaints a live area 2–10 times per second. If Python's standard
:mod:`logging` emits a record from inside a ``with Progress():`` block
without going through rich, the next tick paints over the tail of the log
output. The most common casualty is :meth:`logging.Logger.exception`:
multi-line tracebacks get clipped and the ``ErrorType: message`` line that
tells you *what* went wrong silently disappears.

The Fix
-------
Route :mod:`logging` through :class:`rich.logging.RichHandler`, sharing the
**same** :class:`rich.console.Console` instance that :class:`Progress` /
:class:`Live` uses. Rich then serializes log output with the live area
(pauses, prints, resumes)::

    from aind_code_ocean_pipeline_utils.log import install_rich_handler
    from rich.progress import Progress

    console = install_rich_handler()
    with Progress(console=console) as progress:   # <- same console!
        ...

Passing a separate ``Console`` to ``Progress`` (or letting it create its own)
reintroduces the bug. This module's signature — returning the ``Console`` —
is designed to make sharing the obvious path.

Availability
------------
Requires the optional ``[rich]`` extra::

    pip install aind-code-ocean-pipeline-utils[rich]
"""

from __future__ import annotations

import logging

try:
    from rich.console import Console
    from rich.logging import RichHandler
except ImportError as exc:  # pragma: no cover - tested via subprocess
    raise ImportError(
        "aind_code_ocean_pipeline_utils.log requires the [rich] extra. "
        "Install with: pip install aind-code-ocean-pipeline-utils[rich]"
    ) from exc

__all__ = ["install_rich_handler"]

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
    target = logger if logger is not None else logging.getLogger()
    shared_console = console if console is not None else Console()

    _remove_existing_handlers(target)

    handler = RichHandler(
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
