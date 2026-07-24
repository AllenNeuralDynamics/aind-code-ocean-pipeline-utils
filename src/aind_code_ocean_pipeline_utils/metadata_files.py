"""Locate and forward AIND metadata sidecar files (bounded, stdlib-only).

A derived data asset must carry the ancillary metadata of its inputs —
``subject``, ``procedures``, ``instrument``, ``acquisition`` — forwarded
**verbatim** (a derived asset inherits, unchanged, the subject/procedures/etc. of
what it was made from). This is deliberately *not* a provenance concern (it does
not touch the breadcrumb DAG) and deliberately *not* schema-coupled (a byte-copy
never parses the file, so it is immune to aind-data-schema version churn).

The one metadata file a derived asset does **not** forward is
``data_description.json`` — that describes a *different* asset and must be
authored fresh (see
:func:`aind_code_ocean_pipeline_utils.metadata.make_derived_data_description`).
``processing.json`` is likewise not forwarded — provenance is emitted as
breadcrumb shards (see :mod:`.records`).

Both finders here walk with ``os.walk(followlinks=True)`` (Code Ocean stages
inputs via ``/tmp`` symlink chains that ``Path.rglob`` skips) and are **depth
bounded** — the whole point, versus the old unbounded ``rglob`` that stalled for
minutes on a SmartSPIM zarr / sorted-ephys asset's millions of files.
"""

from __future__ import annotations

import logging
import os
import shutil
from collections.abc import Iterable
from pathlib import Path

__all__ = [
    "DEFAULT_INHERITED_METADATA",
    "find_metadata_file",
    "forward_metadata",
]

_logger = logging.getLogger(__name__)

#: Ancillary metadata a derived asset inherits verbatim. Excludes
#: ``data_description.json`` (authored, not inherited) and ``processing.json``
#: (emitted as breadcrumbs).
DEFAULT_INHERITED_METADATA: tuple[str, ...] = (
    "subject.json",
    "procedures.json",
    "instrument.json",
    "acquisition.json",
)


def find_metadata_file(root: str | Path, filename: str, *, max_depth: int = 4) -> Path | None:
    """Return the first ``filename`` found under ``root``, or ``None``.

    Bounded, symlink-following walk. Use it to locate a mounted input's
    ``data_description.json`` (to derive from) or any inherited sidecar.

    Parameters
    ----------
    root : str or pathlib.Path
        Directory to search (e.g. ``/data``).
    filename : str
        Exact basename to match.
    max_depth : int, default 4
        Maximum directory depth (relative to ``root``) to descend. Keeps the walk
        off the millions of files deep inside a raw data asset.

    Returns
    -------
    pathlib.Path or None
        The first match in walk order, or ``None`` if absent.
    """
    base = Path(root)
    for dirpath, dirnames, filenames in os.walk(base, followlinks=True):
        try:
            depth = len(Path(dirpath).relative_to(base).parts)
        except ValueError:
            depth = 0
        if depth >= max_depth:
            dirnames[:] = []
        if filename in filenames:
            return Path(dirpath) / filename
    return None


def forward_metadata(
    input_dir: str | Path,
    output_dir: str | Path,
    *,
    names: Iterable[str] = DEFAULT_INHERITED_METADATA,
    max_depth: int = 4,
) -> list[Path]:
    """Copy each named metadata file from ``input_dir`` to ``output_dir`` (verbatim).

    Locates each name via :func:`find_metadata_file` (first match) and byte-copies
    it to ``output_dir/<name>``. Best-effort: a file that is absent is skipped, and
    one that cannot be copied is logged and skipped — never raises, so an emit path
    can forward unconditionally.

    Used both to forward an input's inherited sidecars into a node's ``/results``
    and, at the assembler, to *lift* those files (and the authored
    ``data_description.json``) from a producer's output to the final asset root.

    Parameters
    ----------
    input_dir : str or pathlib.Path
        Source tree (typically ``/data``).
    output_dir : str or pathlib.Path
        Destination directory (created if needed).
    names : Iterable[str], default :data:`DEFAULT_INHERITED_METADATA`
        Basenames to forward. Pass a set including ``data_description.json`` when
        lifting an authored asset-root file.
    max_depth : int, default 4
        Depth bound for :func:`find_metadata_file`.

    Returns
    -------
    list[pathlib.Path]
        The destination paths actually written, in ``names`` order.
    """
    src = Path(input_dir)
    dst = Path(output_dir)
    written: list[Path] = []
    for name in names:
        found = find_metadata_file(src, name, max_depth=max_depth)
        if found is None:
            continue
        try:
            dst.mkdir(parents=True, exist_ok=True)
            target = dst / name
            shutil.copyfile(found, target)
            written.append(target)
        except OSError as exc:
            _logger.warning("could not forward %s: %s", name, exc)
    return written
