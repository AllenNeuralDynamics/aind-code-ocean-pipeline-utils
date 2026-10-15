"""Locate and forward AIND metadata sidecar files (bounded, stdlib-only).

A derived data asset must carry the ancillary metadata of its inputs —
``subject``, ``procedures``, ``instrument``, ``acquisition`` (``rig`` and
``session`` before schema v2) — forwarded
**verbatim** (a derived asset inherits, unchanged, the subject/procedures/etc. of
what it was made from). This is deliberately *not* a provenance concern (it does
not touch the breadcrumb DAG) and deliberately *not* schema-coupled (a byte-copy
never parses the file, so it is immune to aind-data-schema version churn).

:func:`inherit_metadata` is the entry point for a node's inputs: it keeps every
source's files under an archive subdirectory and fills the root from one of them, so
a later rule about multi-subject or multi-acquisition assets can still be applied.
:func:`forward_metadata` copies first matches, for lifting files between directories.

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

import json
import logging
import os
import re
import shutil
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

__all__ = [
    "DEFAULT_ARCHIVE_DIR",
    "DEFAULT_INHERITED_METADATA",
    "InheritedMetadata",
    "find_metadata_file",
    "forward_metadata",
    "inherit_metadata",
    "read_data_description_fields",
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
    # Pre-v2 names of instrument and acquisition; a v1 input carries only these.
    "rig.json",
    "session.json",
)


def _stable_str(value: object) -> str | None:
    """Return ``value`` if it is a non-empty string, else ``None``."""
    return value if isinstance(value, str) and value else None


def read_data_description_fields(source: str | Path | Mapping[str, Any]) -> dict[str, Any]:
    """Best-effort raw read of version-stable primitives from a ``data_description``.

    Reads a ``data_description.json`` (path) or an already-parsed mapping as a **raw
    dict** -- it is **never** schema-validated. Only fields whose meaning is stable
    across aind-data-schema versions are pulled; everything else (removed/renamed
    fields, changed ``Organization``/``Person`` shapes) is ignored. This is immune
    to schema churn, so it works on an input whose schema version is unknown or old
    -- use it to author a fresh *derived* record from the extracted primitives plus
    caller-supplied (pipeline-specific) constants, without ever parsing the input
    into a versioned model.

    Parameters
    ----------
    source : str or pathlib.Path or Mapping[str, Any]
        A path to a ``data_description.json``, or an already-parsed mapping. An
        unreadable / non-object source yields all-empty fields (never raises).

    Returns
    -------
    dict[str, Any]
        ``{"name", "subject_id", "project_name", "investigator_names",
        "modality_abbreviations"}``. Each is ``None`` when absent/unreadable, except
        the two lists, which hold the string ``name`` of each investigator and the
        ``abbreviation`` of each modality (``modalities``, or ``modality`` before v1).
    """
    raw: Any
    if isinstance(source, Mapping):
        raw = source
    else:
        try:
            raw = json.loads(Path(source).read_text())
        except (OSError, json.JSONDecodeError) as exc:
            _logger.debug("could not read data_description %s: %s", source, exc)
            raw = {}
    if not isinstance(raw, Mapping):
        raw = {}

    investigators: list[str] = []
    for entry in raw.get("investigators") or []:
        name = _stable_str(entry.get("name")) if isinstance(entry, Mapping) else _stable_str(entry)
        if name:
            investigators.append(name)

    modalities: list[str] = []
    for entry in raw.get("modalities") or raw.get("modality") or []:
        abbreviation = _stable_str(entry.get("abbreviation")) if isinstance(entry, Mapping) else None
        if abbreviation:
            modalities.append(abbreviation)

    return {
        "name": _stable_str(raw.get("name")),
        "subject_id": _stable_str(raw.get("subject_id")),
        "project_name": _stable_str(raw.get("project_name")),
        "investigator_names": investigators,
        "modality_abbreviations": modalities,
    }


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
        dirnames.sort()  # first match must not depend on filesystem listing order
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


_DATA_DESCRIPTION = "data_description.json"

#: Where each source's metadata is kept verbatim, one directory per source. Under
#: ``provenance/`` it travels on the edges that already carry the shards, and the shard
#: reader ignores it because it reads only files directly inside ``provenance/``.
DEFAULT_ARCHIVE_DIR = "provenance/metadata"

# One subject means these describe the whole asset; one acquisition, these.
_SUBJECT_FILES = frozenset({"subject.json", "procedures.json"})
_ACQUISITION_FILES = frozenset({"instrument.json", "acquisition.json", "rig.json", "session.json"})

# aind-data-schema-models' ``DataRegex.DERIVED``, kept here so this module stays stdlib-only.
_DERIVED_NAME = re.compile(
    r"^(?P<input>.+?_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})_.+?_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}"
)

RootPolicy = Literal["first", "drop"]


@dataclass(frozen=True)
class InheritedMetadata:
    """What :func:`inherit_metadata` wrote.

    Attributes
    ----------
    sources : list[str]
        Archive keys, sorted; one directory each under the archive.
    root : list[pathlib.Path]
        Files written to the output root.
    root_source : str or None
        The archive key the root files were copied from.
    """

    sources: list[str]
    root: list[Path]
    root_source: str | None


def inherit_metadata(
    sources: Iterable[str | Path],
    output_dir: str | Path,
    *,
    names: Iterable[str] = DEFAULT_INHERITED_METADATA,
    root: RootPolicy = "first",
    archive_dir: str = DEFAULT_ARCHIVE_DIR,
    max_depth: int = 4,
) -> InheritedMetadata:
    """Keep every source's metadata, and choose one set for the output root.

    Each source's files in ``names``, plus its ``data_description.json``, are copied
    verbatim to ``output_dir/<archive_dir>/<source directory name>/``. A source that
    already holds an archive (an upstream node's output) contributes its archived
    entries instead of its own root copies, so the archive accumulates across a
    pipeline without duplicates. Sources are explicit because ``/data`` also holds
    reference assets (atlases, templates) that are not ancestors.

    The root takes every file from a single archived source, so its files always
    agree: the one with the most of ``names``, ties broken by key. Under
    ``root="drop"`` the subject files are omitted unless every source names the
    same subject, and the acquisition files unless every source traces back to the
    same raw asset, as aind-data-schema's inheritance rules prescribe. With one
    source the policies agree.

    Best-effort like :func:`forward_metadata`: unreadable or uncopyable files are
    logged and skipped, and nothing raises.

    Parameters
    ----------
    sources : Iterable[str or pathlib.Path]
        Ancestor directories, typically the asset mounts under ``/data``.
    output_dir : str or pathlib.Path
        The node's output, typically ``/results``.
    names : Iterable[str], default :data:`DEFAULT_INHERITED_METADATA`
        Files to inherit. ``data_description.json`` is always archived, never
        copied to the root.
    root : {"first", "drop"}, default "first"
        How to fill the root when sources disagree; see above.
    archive_dir : str, default :data:`DEFAULT_ARCHIVE_DIR`
        Archive path, relative to each source and to ``output_dir``.
    max_depth : int, default 4
        Depth bound when locating files inside a source without an archive.

    Returns
    -------
    InheritedMetadata
        The archive keys and the root files written.
    """
    wanted = [n for n in dict.fromkeys(names) if n != _DATA_DESCRIPTION]
    archive = Path(output_dir) / archive_dir
    entries: dict[str, dict[str, Path]] = {}
    for source in map(Path, sources):
        for key, found in _source_entries(source, [*wanted, _DATA_DESCRIPTION], archive_dir, max_depth):
            if key in entries or not any(name in found for name in wanted):
                continue
            copied = {name: dst for name, src in found.items() if (dst := _copy(src, archive / key / name))}
            if copied:
                entries[key] = copied

    keys = sorted(entries)
    if not keys:
        return InheritedMetadata(sources=[], root=[], root_source=None)
    counts = {k: sum(name in entries[k] for name in wanted) for k in keys}
    chosen = next(k for k in keys if counts[k] == max(counts.values()))
    omit: set[str] = set()
    if root == "drop" and len(keys) > 1:
        if not _single(_subject_id(entries[k]) for k in keys):
            omit |= _SUBJECT_FILES
        if not _single(_root_asset(entries[k]) for k in keys):
            omit |= _ACQUISITION_FILES
        if omit & set(wanted):
            _logger.info(
                "sources span subjects or acquisitions; leaving %s out of the root", sorted(omit & set(wanted))
            )
    written = [
        dst
        for name in wanted
        if name in entries[chosen] and name not in omit
        if (dst := _copy(entries[chosen][name], Path(output_dir) / name))
    ]
    return InheritedMetadata(sources=keys, root=written, root_source=chosen)


def _source_entries(
    source: Path, names: list[str], archive_dir: str, max_depth: int
) -> list[tuple[str, dict[str, Path]]]:
    archived = source / archive_dir
    if archived.is_dir():
        return [
            (entry.name, {n: entry / n for n in names if (entry / n).is_file()})
            for entry in sorted(archived.iterdir())
            if entry.is_dir()
        ]
    found = {n: p for n in names if (p := find_metadata_file(source, n, max_depth=max_depth)) is not None}
    return [(source.name, found)]


def _copy(src: Path, dst: Path) -> Path | None:
    try:
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, dst)
    except OSError as exc:
        _logger.warning("could not copy %s: %s", src, exc)
        return None
    return dst


def _read_json(path: Path | None) -> Mapping[str, Any]:
    if path is None:
        return {}
    try:
        loaded = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    return loaded if isinstance(loaded, Mapping) else {}


def _subject_id(files: Mapping[str, Path]) -> str | None:
    subject = _stable_str(_read_json(files.get("subject.json")).get("subject_id"))
    return subject or read_data_description_fields(_read_json(files.get(_DATA_DESCRIPTION)))["subject_id"]


def _root_asset(files: Mapping[str, Path]) -> str | None:
    name = read_data_description_fields(_read_json(files.get(_DATA_DESCRIPTION)))["name"]
    if name is None:
        return None
    match = _DERIVED_NAME.match(name)
    return match.group("input") if match else name


def _single(values: Iterable[str | None]) -> bool:
    """Return whether every value is known and all are equal."""
    seen = list(values)
    return None not in seen and len(set(seen)) == 1
