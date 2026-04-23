"""Launcher / worker / aggregator skeleton for CO pipeline capsules.

Most embarrassingly-parallel AIND processing capsules follow the same
three-role shape:

- **Launcher**: discover items, write one ``config.json`` per item into a
  subdirectory under ``/results/``. The CO pipeline's Flatten fan-out then
  stages each directory as a distinct worker input.
- **Worker**: find *its* config among whatever got mounted under
  ``/data`` (CO's staging can nest unpredictably), run one item, write a
  ``manifest_<name>.json`` alongside its output.
- **Aggregator**: Collect runs after the workers; walk ``/data`` for
  every ``manifest_*.json`` and merge them into one final manifest.

This module captures the pattern so consumers don't reinvent it (badly)
per-capsule. The API is intentionally file-oriented — Code Ocean's IPC
is "files on disk," so every handoff here is a JSON file.

Non-obvious invariants
----------------------

**Schema-tagged configs, not path-shape detection.** Workers detect
their config by a marker key in the JSON body, not by a path glob like
``stream_*/config.json``. CO's Flatten + Target Map Path combinations
produce unpredictable ``/data/<wrapper>/<optional-extra-wrapper>/stream_<n>/config.json``
layouts. Schema matching doesn't care what the path looks like.

**``os.walk(followlinks=True)``, not ``Path.rglob``.** CO stages inputs
via ``/tmp`` symlink chains that ``Path.glob("**/...")`` silently skips
(even in Python 3.13 where ``recurse_symlinks=True`` exists, it is
opt-in).

**Per-item directory namespacing on the launcher side.** Configs go in
``stream_<safe>/config.json``, not ``<safe>.json``, so a worker writing
its output to a dir of its own name can't collide with Collect-time
paths.
"""

from __future__ import annotations

import json
import logging
import os
import re
from collections.abc import Callable, Iterable
from enum import StrEnum
from pathlib import Path
from typing import Any

from .io import atomic_json_write

__all__ = [
    "Role",
    "StreamConfigError",
    "default_sanitize",
    "find_launcher_manifest",
    "find_stream_config",
    "find_worker_manifests",
    "merge_manifests",
    "write_stream_configs",
]

_logger = logging.getLogger(__name__)

_SANITIZE_RE = re.compile(r"[^A-Za-z0-9._-]+")


class Role(StrEnum):
    """Pipeline roles. Consumers pick one at CLI time."""

    MONOLITH = "monolith"
    LAUNCHER = "launcher"
    WORKER = "worker"
    AGGREGATOR = "aggregator"


class StreamConfigError(RuntimeError):
    """Raised when :func:`find_stream_config` cannot find exactly one match.

    The :attr:`paths` attribute holds the offending matches:

    - empty list: no config was found (data not mounted, or pipeline mis-routed)
    - 2+ entries: ambiguous match (pipeline wiring is wrong)

    Both cases are terminal from the worker's point of view — there is
    no recovery path since the worker has no input to process.
    """

    paths: list[Path]

    def __init__(self, message: str, paths: list[Path]) -> None:
        super().__init__(message)
        self.paths = paths


def default_sanitize(name: str) -> str:
    """Return a filesystem-safe basename for ``name``.

    Matches the reference capsule implementations: keep alphanumerics,
    dot, dash, underscore; collapse every other run to ``_``; strip
    leading/trailing underscores.
    """
    return _SANITIZE_RE.sub("_", name).strip("_")


def write_stream_configs(
    items: Iterable[dict[str, Any]],
    *,
    results_dir: Path | str,
    schema_marker: str,
    schema_version: int = 1,
    name_key: str = "name",
    dir_prefix: str = "stream_",
    sanitize: Callable[[str], str] = default_sanitize,
) -> list[Path]:
    """Launcher side: write one config JSON per item, schema-tagged.

    Each output is ``<results_dir>/<dir_prefix><sanitize(item[name_key])>/config.json``
    containing the full item mapping plus ``{schema_marker: schema_version}``
    so workers can recognize it among nested staged inputs.

    Parameters
    ----------
    items : Iterable[dict]
        The work items. Each must contain ``name_key``.
    results_dir : Path or str
        Base directory, typically ``Path("/results")``. Created if missing.
    schema_marker : str
        Key injected into every config. Pick something
        capsule-specific and stable (e.g. ``"_mipmap_stream_config"``)
        so the worker's :func:`find_stream_config` can recognize its
        own configs.
    schema_version : int, default 1
        Value written under ``schema_marker``. Bump if the config shape
        changes in an incompatible way.
    name_key : str, default "name"
        Which key in each item supplies the directory-name source.
    dir_prefix : str, default "stream_"
        Prefix on each per-item subdirectory.
    sanitize : callable, default :func:`default_sanitize`
        Transform applied to the name before use as a directory.

    Returns
    -------
    list[Path]
        Absolute paths of the written ``config.json`` files, in input order.

    Raises
    ------
    KeyError
        If an item is missing ``name_key``.
    """
    base = Path(results_dir)
    base.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []
    for item in items:
        if name_key not in item:
            msg = f"item is missing required key {name_key!r}: {dict(item)!r}"
            raise KeyError(msg)
        safe = sanitize(str(item[name_key]))
        stream_dir = base / f"{dir_prefix}{safe}"
        stream_dir.mkdir(parents=True, exist_ok=True)
        cfg_path = stream_dir / "config.json"

        payload = {schema_marker: schema_version, **item}
        atomic_json_write(cfg_path, payload)
        written.append(cfg_path)
        _logger.info("wrote stream config: %s", cfg_path)
    return written


def find_stream_config(
    data_dir: Path | str,
    *,
    schema_marker: str,
    filename: str = "config.json",
) -> tuple[Path, dict[str, Any]]:
    """Locate exactly one schema-tagged config under ``data_dir`` for workers.

    Walks ``data_dir`` with ``os.walk(followlinks=True)`` looking for
    files named ``filename`` whose JSON content is a dict carrying a
    truthy value under ``schema_marker``. Files that fail to parse are
    skipped silently.

    Parameters
    ----------
    data_dir : Path or str
        Typically ``Path("/data")``.
    schema_marker : str
        The key that must appear in (and have a truthy value in) the
        config body. Must match what :func:`write_stream_configs` used.
    filename : str, default "config.json"
        Basename to look for. Other files are ignored.

    Returns
    -------
    tuple[Path, dict[str, Any]]
        The path and parsed contents of the single matching config.

    Raises
    ------
    StreamConfigError
        If the walk finds zero or more than one matching config.
    """
    base = Path(data_dir)
    found: list[tuple[Path, dict[str, Any]]] = []
    for root, _dirs, files in os.walk(base, followlinks=True):
        if filename not in files:
            continue
        path = Path(root) / filename
        try:
            parsed = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(parsed, dict) and parsed.get(schema_marker):
            found.append((path, parsed))

    if len(found) == 1:
        return found[0]
    paths = [p for p, _ in found]
    if not found:
        msg = (
            f"no {filename!r} carrying marker {schema_marker!r} found "
            f"under {base} (is the pipeline staging the expected input?)"
        )
    else:
        pretty = ", ".join(str(p) for p in paths)
        msg = (
            f"ambiguous match: found {len(found)} {filename!r} carrying marker {schema_marker!r} under {base}: {pretty}"
        )
    raise StreamConfigError(msg, paths)


def find_worker_manifests(
    data_dir: Path | str,
    *,
    filename_prefix: str = "manifest_",
    filename_suffix: str = ".json",
    strict: bool = False,
) -> list[tuple[Path, dict[str, Any]]]:
    """Collect every worker manifest under ``data_dir`` for aggregator.

    Matches files whose basename starts with ``filename_prefix`` and
    ends with ``filename_suffix``. The default pattern (``manifest_*.json``)
    deliberately does not match ``launcher_manifest.json`` — use
    :func:`find_launcher_manifest` for that.

    Parameters
    ----------
    data_dir : Path or str
        Typically ``Path("/data")``.
    filename_prefix, filename_suffix : str
        Basename delimiters. Defaults cover the reference convention.
    strict : bool, default False
        If True, parse failures raise :class:`json.JSONDecodeError`.
        Default behavior is warn-and-continue, matching the reference
        aggregator which should not fail a run for one corrupt manifest.

    Returns
    -------
    list[tuple[Path, dict[str, Any]]]
        Worker manifests, in walk order.
    """
    base = Path(data_dir)
    manifests: list[tuple[Path, dict[str, Any]]] = []
    for root, _dirs, files in os.walk(base, followlinks=True):
        for name in files:
            if not (name.startswith(filename_prefix) and name.endswith(filename_suffix)):
                continue
            # Don't swallow launcher_manifest.json even if the caller
            # relaxes the prefix — that's a separate concept.
            if name == "launcher_manifest.json" and filename_prefix == "manifest_":
                continue
            path = Path(root) / name
            try:
                parsed = json.loads(path.read_text())
            except (OSError, json.JSONDecodeError):
                if strict:
                    raise
                _logger.warning("could not parse worker manifest %s; skipping", path)
                continue
            if not isinstance(parsed, dict):
                if strict:
                    msg = f"worker manifest {path} is not a JSON object"
                    raise ValueError(msg)
                _logger.warning("worker manifest %s is not a JSON object; skipping", path)
                continue
            manifests.append((path, parsed))
    return manifests


def find_launcher_manifest(
    data_dir: Path | str,
    *,
    filename: str = "launcher_manifest.json",
) -> dict[str, Any] | None:
    """Locate the single launcher manifest, if any, for aggregator.

    Parameters
    ----------
    data_dir : Path or str
        Typically ``Path("/data")``.
    filename : str, default "launcher_manifest.json"
        Exact basename to match.

    Returns
    -------
    dict[str, Any] or None
        Parsed contents of the first match found, or None if no match.
        A warning is logged if more than one is present and the first
        walk-order result is returned — duplicate launcher manifests
        shouldn't happen in a normal pipeline run but shouldn't fail
        the aggregator either.
    """
    base = Path(data_dir)
    matches: list[Path] = []

    for root, _dirs, files in os.walk(base, followlinks=True):
        if filename in files:
            matches.append(Path(root) / filename)

    if not matches:
        return None

    if len(matches) > 1:
        pretty = ", ".join(str(p) for p in matches)
        _logger.warning("multiple %s found (using first): %s", filename, pretty)

    try:
        parsed = json.loads(matches[0].read_text())
    except (OSError, json.JSONDecodeError) as exc:
        _logger.warning("could not parse %s: %s", matches[0], exc)
        return None

    if not isinstance(parsed, dict):
        _logger.warning("expected %s to contain a JSON object, got %s", matches[0], type(parsed).__name__)
        return None

    return parsed


def merge_manifests(
    manifests: Iterable[dict[str, Any]],
    *,
    result_key: str = "result",
    error_key: str = "error",
    stream_key: str = "stream",
) -> dict[str, list[Any]]:
    """Partition worker manifests into ``{"built": [...], "skipped": [...]}``.

    A manifest is counted as **built** if it has ``result_key``; the
    value under that key is added to ``"built"``. A manifest with
    ``error_key`` (and no ``result_key``) is added to ``"skipped"`` as
    ``{"stream": <stream_key value>, "reason": <error_key value>}``.
    Manifests with neither are ignored (they carry no outcome info).

    Parameters
    ----------
    manifests : Iterable[dict]
        Worker manifests as parsed. Pass the second element of each
        tuple from :func:`find_worker_manifests`.
    result_key : str, default "result"
        Key whose presence marks a successful build.
    error_key : str, default "error"
        Key whose presence marks a skipped/failed item.
    stream_key : str, default "stream"
        Key identifying which item the manifest refers to. Used for
        the ``"stream"`` field in skipped entries.

    Returns
    -------
    dict
        ``{"built": [...], "skipped": [...]}``. Callers typically wrap
        this with role/commit/version metadata before writing the
        final aggregated manifest.
    """
    built: list[Any] = []
    skipped: list[dict[str, Any]] = []
    for manifest in manifests:
        if result_key in manifest:
            built.append(manifest[result_key])
            continue
        if error_key in manifest:
            skipped.append(
                {
                    "stream": manifest.get(stream_key, "unknown"),
                    "reason": manifest[error_key],
                }
            )
    return {"built": built, "skipped": skipped}
