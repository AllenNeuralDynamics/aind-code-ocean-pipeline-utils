"""Schema-free provenance breadcrumbs for Code Ocean pipeline nodes.

A Code Ocean pipeline is a Nextflow DAG, but no single capsule sees the whole
graph, and the true edges are only knowable with *local* information along the
data flow: a node's inputs **are** its DAG parents. This module records that,
per node, as a small JSON **shard** — without ever parsing an upstream record
into a versioned schema.

Each shard is an **envelope we own and read** wrapped around **payloads we author
but never parse on read**:

- Owned, read on the hot path: ``v``, ``node``, ``parents`` (an adjacency list —
  a dir of these *is* a DAG), and optional launcher-owned ``pipeline.name`` /
  ``experimenters`` / ``data_process_schema_version``.
- Opaque, authored on write and copied forward byte-for-byte on read:
  ``data_process`` (a best-effort aind-data-schema ``DataProcess``) and
  ``pipeline.code``.

The read path (:func:`read_records`, :func:`frontier`, propagation in
:func:`record_step`) is **stdlib-only** and touches only the owned keys. A
malformed, wrong-version, or future-schema payload therefore cannot break routing
or sink a run. aind-data-schema is imported **lazily**, only to author *this*
node's own ``DataProcess`` on write (the ``[metadata]`` extra); if it is absent or
authoring fails, the envelope is still written with ``data_process`` omitted, so
the DAG topology always survives.

Why this and not a ``processing.json`` frontier-append: the removed approach
merged *parsed current-schema* ``Processing`` objects, which only works if every
capsule agrees on one aind-data-schema version — a constraint that does not hold as
capsules drift and the schema itself relocates. Here the frontier algorithm runs
over trivial envelopes; no schema is in the hot path. Assembling a compliant
``Processing`` from a directory of shards is deferred to an optional, best-effort
step (see :mod:`.metadata`) run only when a stable target is actually required.
"""

from __future__ import annotations

import json
import logging
import os
import re
from collections.abc import Generator, Iterable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .io import atomic_json_write
from .provenance import capsule_commit, package_version
from .role_dispatch import default_sanitize

__all__ = [
    "RECORD_VERSION",
    "RecordContext",
    "frontier",
    "make_record",
    "read_records",
    "record_step",
    "write_record",
]

_logger = logging.getLogger(__name__)

#: Envelope format version. Additive-only: unknown keys are ignored on read and
#: new keys never bump this. Bump only on a breaking change to the owned keys.
RECORD_VERSION = 1

#: Subdirectory (under a node's output dir) holding one ``<node>.json`` per step.
_PROVENANCE_DIR = "provenance"

#: aind-data-schema constrains ``Code.commit_hash`` to this pattern; a value that
#: does not match is dropped rather than failing the whole ``DataProcess`` build.
_COMMIT_HASH_RE = re.compile(r"^[0-9a-fA-F]{7,60}$")


def _utcnow() -> datetime:
    """Return a timezone-aware UTC ``datetime`` (schema-free, cheap import)."""
    return datetime.now(UTC)


def make_record(
    node: str,
    *,
    parents: Sequence[str] | None = None,
    data_process: Mapping[str, Any] | None = None,
    data_process_schema_version: str | None = None,
    pipeline: Mapping[str, Any] | None = None,
    experimenters: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Build a provenance envelope (pure; no I/O, no aind-data-schema).

    Optional keys are included only when supplied, so the on-disk shard stays
    minimal. ``data_process`` and ``pipeline`` are stored verbatim as opaque
    payloads — this function never inspects them.

    Parameters
    ----------
    node : str
        Unique node id. Becomes the shard filename and, when a ``DataProcess`` is
        authored, its ``name`` (the key space of ``Processing.dependency_graph``).
    parents : Sequence[str], optional
        Direct-parent node ids. Serialized as a list (``[]`` for a source node).
    data_process : Mapping[str, Any], optional
        Serialized aind-data-schema ``DataProcess`` for this step. Opaque.
    data_process_schema_version : str, optional
        Version of aind-data-schema that authored ``data_process`` (so a later
        assembler knows what it holds). Recorded only when ``data_process`` is set.
    pipeline : Mapping[str, Any], optional
        Run-level pipeline block ``{"name": ..., "code": {...}}``; launcher-owned.
    experimenters : Sequence[str], optional
        Run-level default responsible people; launcher-owned. Per-node
        experimenters live inside ``data_process``, not here.

    Returns
    -------
    dict[str, Any]
        The envelope.
    """
    record: dict[str, Any] = {
        "v": RECORD_VERSION,
        "node": node,
        "parents": list(parents or []),
    }
    if data_process is not None:
        record["data_process"] = dict(data_process)
        if data_process_schema_version is not None:
            record["data_process_schema_version"] = data_process_schema_version
    if pipeline is not None:
        record["pipeline"] = dict(pipeline)
    if experimenters is not None:
        record["experimenters"] = list(experimenters)
    return record


def write_record(
    record: Mapping[str, Any],
    output_dir: str | Path,
    *,
    provenance_dir: str = _PROVENANCE_DIR,
) -> Path:
    """Write ``record`` to ``<output_dir>/<provenance_dir>/<node>.json`` atomically.

    Parameters
    ----------
    record : Mapping[str, Any]
        An envelope (see :func:`make_record`); must carry a non-empty ``node``.
    output_dir : str or pathlib.Path
        Node output directory (the ``provenance/`` subdir is created).
    provenance_dir : str, default ``"provenance"``
        Subdirectory name.

    Returns
    -------
    pathlib.Path
        Path to the written shard.

    Raises
    ------
    ValueError
        If ``record`` has no non-empty string ``node``. (Callers on the emit path
        run inside :func:`record_step`, which swallows this.)

    Notes
    -----
    The filename is ``default_sanitize(node)`` — distinct node ids that sanitize
    to the same basename would collide. Node ids are expected to be
    filesystem-friendly and unit-namespaced, so this is not a concern in practice.
    """
    node = record.get("node")
    if not isinstance(node, str) or not node:
        msg = "record must carry a non-empty string 'node' to be written"
        raise ValueError(msg)
    prov = Path(output_dir) / provenance_dir
    prov.mkdir(parents=True, exist_ok=True)
    path = prov / f"{default_sanitize(node)}.json"
    atomic_json_write(path, dict(record))
    return path


def read_records(
    root: str | Path,
    *,
    max_depth: int = 3,
    provenance_dir: str = _PROVENANCE_DIR,
) -> list[dict[str, Any]]:
    """Load provenance shards under ``root`` (bounded, symlink-aware, tolerant).

    Walks ``root`` with ``os.walk(followlinks=True)`` — Code Ocean stages inputs
    via ``/tmp`` symlink chains that ``Path.rglob`` silently skips — pruning any
    branch deeper than ``max_depth`` so the walk never descends into the millions
    of sidecar files on a real data asset. Only ``*.json`` files inside a directory
    literally named ``provenance_dir`` are read. Unreadable / non-object / node-less
    files are skipped; a second shard for the same ``node`` is dropped (a warning is
    logged if its content differs — a real id collision, not the harmless launcher
    duplicate).

    Parameters
    ----------
    root : str or pathlib.Path
        Directory to scan (e.g. ``/data`` for edge inputs, or a node's own output
        dir). ``<root>/<asset>/provenance/`` sits at depth 2, so the default
        ``max_depth`` reaches it; the ``/data``→``capsule/data`` wrapper is a
        symlink at the root and adds no depth level.
    max_depth : int, default 3
        Maximum directory depth (relative to ``root``) to descend.
    provenance_dir : str, default ``"provenance"``
        Directory name whose ``*.json`` children are shards.

    Returns
    -------
    list[dict[str, Any]]
        Deduplicated shards (possibly empty), first-seen wins.
    """
    base = Path(root)
    by_node: dict[str, dict[str, Any]] = {}
    for dirpath, dirnames, filenames in os.walk(base, followlinks=True):
        try:
            depth = len(Path(dirpath).relative_to(base).parts)
        except ValueError:
            depth = 0
        if depth >= max_depth:
            dirnames[:] = []
        if os.path.basename(dirpath) != provenance_dir:
            continue
        for filename in sorted(filenames):
            if not filename.endswith(".json"):
                continue
            path = Path(dirpath) / filename
            try:
                parsed = json.loads(path.read_text())
            except (OSError, json.JSONDecodeError) as exc:
                _logger.debug("skipping unreadable provenance shard %s: %s", path, exc)
                continue
            if not isinstance(parsed, dict):
                continue
            node = parsed.get("node")
            if not isinstance(node, str) or not node:
                continue
            existing = by_node.get(node)
            if existing is None:
                by_node[node] = parsed
                continue
            chosen, conflict = _reconcile(existing, parsed)
            by_node[node] = chosen
            if conflict:
                _logger.warning(
                    "conflicting provenance for node %r (keeping first); duplicate at %s",
                    node,
                    path,
                )
    return list(by_node.values())


def _reconcile(existing: dict[str, Any], incoming: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    """Choose between two shards for the same node; report whether they truly conflict.

    A **superset supersedes**: if one record's keys contain the other's and they
    agree on every shared key, the fuller one wins silently. This lets a minimal
    fan-out stub (``{v, node, parents}``, side-written by
    :func:`aind_code_ocean_pipeline_utils.role_dispatch.write_stream_configs`) be
    superseded by the full shard (with the authored ``data_process`` payload) that
    rides the node's direct edge — no spurious conflict where both reach the
    terminal. Records that differ on a shared key are a genuine collision: keep the
    first and flag it.

    Returns
    -------
    tuple[dict[str, Any], bool]
        The chosen record, and ``True`` if the two genuinely conflict.
    """
    if existing == incoming:
        return existing, False
    ek, ik = existing.keys(), incoming.keys()
    if ik >= ek and all(incoming[k] == existing[k] for k in ek):
        return incoming, False  # incoming is a richer superset
    if ek >= ik and all(existing[k] == incoming[k] for k in ik):
        return existing, False  # existing is a richer superset
    return existing, True  # genuine conflict


def frontier(records: Iterable[Mapping[str, Any]]) -> list[str]:
    """Return the sink node ids: those not named in any record's ``parents``.

    This is the de-schema'd core of the old (now removed) frontier-append: the set
    of nodes nothing (yet) depends on. A new step wired to this frontier attaches to
    the head of every incoming branch (fan-in) at once.

    Parameters
    ----------
    records : Iterable[Mapping[str, Any]]
        Envelopes (see :func:`read_records`).

    Returns
    -------
    list[str]
        Sink node ids, in first-seen order.
    """
    nodes: list[str] = []
    seen: set[str] = set()
    referenced: set[str] = set()
    for record in records:
        node = record.get("node")
        if isinstance(node, str) and node and node not in seen:
            seen.add(node)
            nodes.append(node)
        for parent in record.get("parents") or []:
            if isinstance(parent, str):
                referenced.add(parent)
    return [node for node in nodes if node not in referenced]


@dataclass
class RecordContext:
    """Mutable handle yielded by :func:`record_step` for runtime details.

    Values set before the ``with`` block exits are folded into the authored
    ``DataProcess``. ``parameters`` set here are merged over any passed to
    :func:`record_step` (context wins on key clashes).
    """

    parameters: dict[str, Any] = field(default_factory=dict)
    notes: str | None = None
    output_path: str | Path | None = None
    experimenters: list[str] | None = None


def _author_payload(
    *,
    node: str,
    process_type: str,
    stage: str,
    start: datetime,
    end: datetime,
    code_url: str | None,
    code_dir: str,
    version: str | None,
    commit_hash: str | None,
    experimenters: Sequence[str] | None,
    parameters: Mapping[str, Any] | None,
    output_path: str | Path | None,
    notes: str | None,
) -> tuple[dict[str, Any] | None, str | None]:
    """Build this node's ``DataProcess`` payload, best-effort; never raises.

    Lazily imports aind-data-schema (the ``[metadata]`` extra) and the reference
    ``make_data_process`` / derivation helpers. On a missing extra or any failure,
    returns ``(None, None)`` so the caller still writes a topology-only envelope.

    Returns
    -------
    tuple[dict[str, Any] or None, str or None]
        ``(serialized DataProcess, aind-data-schema version)`` or ``(None, None)``.
    """
    try:
        from aind_data_schema.core.processing import ProcessName, ProcessStage

        from .metadata import make_data_process
        from .provenance import derive_code_url

        try:
            ptype = ProcessName(process_type)
            label: str | None = None
        except ValueError:
            ptype = ProcessName.OTHER
            label = process_type

        commit = commit_hash if commit_hash is not None else capsule_commit(code_dir=code_dir)
        if commit is not None and not _COMMIT_HASH_RE.match(commit):
            commit = None

        proc = make_data_process(
            process_type=ptype,
            code_url=derive_code_url(explicit=code_url, code_dir=code_dir) or "",
            experimenters=list(experimenters or []),
            start=start,
            end=end,
            stage=ProcessStage(stage),
            name=node,
            version=version,
            commit_hash=commit,
            parameters=dict(parameters) if parameters else None,
            output_path=str(output_path) if output_path is not None else None,
            notes=notes or label,
        )
        payload: dict[str, Any] = proc.model_dump(mode="json")
        return payload, package_version("aind-data-schema")
    except Exception as exc:
        _logger.warning("data_process authoring failed for node %r: %s", node, exc)
        return None, None


def _finalize_record(
    *,
    node: str,
    process_type: str,
    stage: str,
    incoming_dir: str | Path,
    output_dir: str | Path,
    parents: Sequence[str] | None,
    experimenters: Sequence[str] | None,
    code_url: str | None,
    code_dir: str,
    version: str | None,
    commit_hash: str | None,
    parameters: Mapping[str, Any] | None,
    notes: str | None,
    pipeline: Mapping[str, Any] | None,
    ctx: RecordContext,
    start: datetime,
) -> Path:
    """Build the shard: infer parents, propagate upstream shards, write this node's shard."""
    end = _utcnow()
    merged_params: dict[str, Any] = {**(dict(parameters) if parameters else {}), **ctx.parameters}
    resolved_notes = ctx.notes if ctx.notes is not None else notes
    resolved_output = ctx.output_path if ctx.output_path is not None else output_dir
    resolved_exps = ctx.experimenters if ctx.experimenters is not None else experimenters

    data_process, schema_version = _author_payload(
        node=node,
        process_type=process_type,
        stage=stage,
        start=start,
        end=end,
        code_url=code_url,
        code_dir=code_dir,
        version=version,
        commit_hash=commit_hash,
        experimenters=resolved_exps,
        parameters=merged_params or None,
        output_path=resolved_output,
        notes=resolved_notes,
    )

    # Union of edge inputs and this node's own output dir. The latter makes a
    # monolith (several steps in one process, sharing output_dir) chain correctly:
    # step N sees step N-1's just-written shard. First-seen wins on overlap.
    incoming = _dedup_records([*read_records(incoming_dir), *read_records(output_dir)])
    resolved_parents = list(parents) if parents is not None else frontier(incoming)
    # A node is never its own parent. A launcher's fan-out stubs (written into this
    # node's own output_dir by write_stream_configs) and a monolith re-run both put
    # a same-node shard in the scanned set, so frontier would otherwise return self;
    # an explicit parents= list could also name it by mistake. Drop it either way --
    # the propagation loop below already excludes the same-node shard.
    resolved_parents = [p for p in resolved_parents if p != node]

    record = make_record(
        node,
        parents=resolved_parents,
        data_process=data_process,
        data_process_schema_version=schema_version,
        pipeline=pipeline,
    )

    # Propagate every ancestor shard forward, then write our own (last, so a
    # monolith re-run overwrites the prior copy harmlessly).
    for upstream in incoming:
        if upstream.get("node") == node:
            continue
        write_record(upstream, output_dir)
    return write_record(record, output_dir)


def _dedup_records(records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Deduplicate by ``node`` (superset supersedes), dropping node-less records."""
    out: dict[str, dict[str, Any]] = {}
    for record in records:
        node = record.get("node")
        if not (isinstance(node, str) and node):
            continue
        incoming = dict(record)
        existing = out.get(node)
        out[node] = incoming if existing is None else _reconcile(existing, incoming)[0]
    return list(out.values())


@contextmanager
def record_step(
    node: str,
    *,
    process_type: str,
    stage: str = "Processing",
    incoming_dir: str | Path = "/data",
    output_dir: str | Path = "/results",
    parents: Sequence[str] | None = None,
    experimenters: Sequence[str] | None = None,
    code_url: str | None = None,
    code_dir: str = "/code",
    version: str | None = None,
    commit_hash: str | None = None,
    parameters: Mapping[str, Any] | None = None,
    notes: str | None = None,
    pipeline: Mapping[str, Any] | None = None,
) -> Generator[RecordContext, None, None]:
    """Context manager that writes a provenance shard when the block succeeds.

    Yields a :class:`RecordContext` for ``parameters`` / ``notes`` /
    ``output_path`` / ``experimenters`` discovered at runtime. On a clean exit the
    step is timed, its ``DataProcess`` authored (best-effort), parents inferred, and
    the shard written to ``<output_dir>/provenance/`` alongside forwarded copies of
    every incoming ancestor shard. If the block raises, the exception propagates and
    **nothing is written** — a failed step leaves no record. The emit itself is
    best-effort and never raises.

    Parents are inferred (``parents=None``, the default) as the frontier of the
    shards arriving on edges plus any already in ``output_dir`` (topology-free — the
    capsule never hardcodes its DAG position). Pass ``parents`` explicitly to
    override — for an ambiguous frontier, an unwired/test context, or a fan-out
    worker not fed via ``write_stream_configs(producer_record=...)``.

    Parameters
    ----------
    node : str
        Unique node id (also the ``DataProcess.name``).
    process_type : str
        aind-data-schema ``ProcessName`` value; an unknown label becomes ``OTHER``
        with the label preserved as ``notes``. Ignored if the ``[metadata]`` extra
        is absent (the payload is simply omitted).
    stage : str, default ``"Processing"``
        aind-data-schema ``ProcessStage`` value.
    incoming_dir : str or pathlib.Path, default ``"/data"``
        Where upstream shards arrive (scanned for ``provenance/`` subdirs).
    output_dir : str or pathlib.Path, default ``"/results"``
        Where this node writes; pass a per-item subdir for fan-out.
    parents : Sequence[str], optional
        Explicit parent node ids; overrides frontier inference.
    experimenters : Sequence[str], optional
        People responsible; folded into the authored ``DataProcess``.
    code_url : str, optional
        Repository URL override; auto-derived from git/env otherwise.
    code_dir : str, default ``"/code"``
        Git checkout used to derive ``code_url`` / ``commit_hash``.
    version : str, optional
        Code version stamp.
    commit_hash : str, optional
        Git commit override; auto-derived otherwise. A value failing the schema's
        hash pattern is dropped.
    parameters : Mapping[str, Any], optional
        Static run parameters; merged under any set on the context.
    notes : str, optional
        Free-text notes; a context value or an ``OTHER`` label takes precedence.
    pipeline : Mapping[str, Any], optional
        Run-level pipeline block to record in the envelope (launcher).

    Yields
    ------
    RecordContext
    """
    ctx = RecordContext()
    start = _utcnow()
    yield ctx
    try:
        _finalize_record(
            node=node,
            process_type=process_type,
            stage=stage,
            incoming_dir=incoming_dir,
            output_dir=output_dir,
            parents=parents,
            experimenters=experimenters,
            code_url=code_url,
            code_dir=code_dir,
            version=version,
            commit_hash=commit_hash,
            parameters=parameters,
            notes=notes,
            pipeline=pipeline,
            ctx=ctx,
            start=start,
        )
    except Exception as exc:
        _logger.warning("provenance emit failed for node %r: %s", node, exc)
