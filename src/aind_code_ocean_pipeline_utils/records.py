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

Upstream capsules that do not use this library but write an aind-data-schema
``processing.json`` still join the DAG: :func:`read_processing_records` turns each
such file into envelopes by reading only its raw ``name`` / ``dependency_graph``
keys, carrying each ``DataProcess`` forward as an opaque payload.

Why this and not a ``processing.json`` frontier-append: merging *parsed
current-schema* ``Processing`` objects only works if every capsule agrees on one
aind-data-schema version — a constraint that does not hold as capsules drift and
the schema itself relocates. Here the frontier algorithm runs over trivial
envelopes; no schema is in the hot path. Schema parsing happens once, at the node
that builds the final ``processing.json``
(:func:`aind_code_ocean_pipeline_utils.metadata.assemble_processing`).
"""

from __future__ import annotations

import hashlib
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
    "read_processing_records",
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
    label: str | None = None,
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
    label : str, optional
        Display name for the assembled ``DataProcess.name`` when it differs from
        ``node``. Set on records read from a ``processing.json``, whose node ids carry
        a content digest so identically named steps from different files stay distinct.

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
    if label is not None:
        record["label"] = label
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
        # Sorted so first-seen order, and with it parent order and dedup, is reproducible.
        dirnames[:] = [] if depth >= max_depth else sorted(dirnames)
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
                _logger.warning("conflicting provenance for node %r; duplicate at %s", node, path)
    return list(by_node.values())


def _reconcile(existing: dict[str, Any], incoming: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    """Choose between two shards for the same node; report whether they truly conflict.

    A **superset supersedes**: if one record's keys contain the other's and they
    agree on every shared key, the fuller one wins silently. This lets a minimal
    fan-out stub (``{v, node, parents}``, side-written by
    :func:`aind_code_ocean_pipeline_utils.role_dispatch.write_stream_configs`) be
    superseded by the full shard (with the authored ``data_process`` payload) that
    rides the node's direct edge — no spurious conflict where both reach the
    terminal. Otherwise a record carrying a ``data_process`` payload beats one
    without, since the payload is what assembly cannot recover; failing that, the
    first is kept. Either way, records that differ on a shared key are flagged.

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
    if "data_process" in incoming and "data_process" not in existing:
        return incoming, True
    return existing, True


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


#: Standard aind-data-schema filename that :func:`read_processing_records` converts.
_PROCESSING_FILENAME = "processing.json"


def _nonempty_str(value: object) -> str | None:
    """Return ``value`` if it is a non-empty string, else ``None``."""
    return value if isinstance(value, str) and value else None


def _processes_in(raw: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    """Return the raw ``DataProcess`` dicts of a ``processing.json`` of any schema version."""
    found: list[Any] = []
    if isinstance(raw.get("data_processes"), list):
        found = list(raw["data_processes"])
    else:  # aind-data-schema 1.x: processes live under the pipeline and each analysis
        pipeline = raw.get("processing_pipeline")
        if isinstance(pipeline, Mapping) and isinstance(pipeline.get("data_processes"), list):
            found.extend(pipeline["data_processes"])
        for analysis in raw.get("analyses") or []:
            if isinstance(analysis, Mapping) and isinstance(analysis.get("data_processes"), list):
                found.extend(analysis["data_processes"])
    return [process for process in found if isinstance(process, Mapping)]


def _records_from_processing(raw: Mapping[str, Any], digest: str) -> list[dict[str, Any]]:
    """Convert one parsed ``processing.json`` into envelopes keyed ``<name>@<digest>``."""
    processes = _processes_in(raw)
    names: list[str] = []
    uses: dict[str, int] = {}
    for process in processes:
        base = _nonempty_str(process.get("name")) or _nonempty_str(process.get("process_type")) or "process"
        uses[base] = uses.get(base, 0) + 1
        # A graph-less file may repeat a name; a graph-bearing one cannot (the schema forbids it).
        names.append(base if uses[base] == 1 else f"{base} ({uses[base]})")

    graph = raw.get("dependency_graph") if isinstance(raw.get("dependency_graph"), Mapping) else None
    known = set(names)
    pipelines = {
        name: dict(pipeline)
        for pipeline in raw.get("pipelines") or []
        if isinstance(pipeline, Mapping) and (name := _nonempty_str(pipeline.get("name")))
    }
    schema_version = _nonempty_str(raw.get("schema_version"))

    records: list[dict[str, Any]] = []
    for index, (name, process) in enumerate(zip(names, processes, strict=True)):
        if graph is not None:
            parent_names = [p for p in graph.get(name) or [] if isinstance(p, str) and p in known]
        else:
            parent_names = names[index - 1 : index]
        pipeline_name = _nonempty_str(process.get("pipeline_name"))
        pipeline = {"name": pipeline_name, "code": pipelines[pipeline_name]} if pipeline_name in pipelines else None
        records.append(
            make_record(
                f"{name}@{digest}",
                parents=[f"{p}@{digest}" for p in parent_names],
                data_process=process,
                data_process_schema_version=schema_version,
                pipeline=pipeline,
                label=name,
            )
        )
    return records


def read_processing_records(
    root: str | Path,
    *,
    max_depth: int = 2,
    provenance_dir: str = _PROVENANCE_DIR,
) -> list[dict[str, Any]]:
    """Convert the ``processing.json`` files under ``root`` into envelopes (schema-free).

    This is how a capsule that writes an aind-data-schema ``processing.json`` but
    never calls :func:`record_step` joins the DAG. Each file is read as a raw dict:
    every ``DataProcess`` in it becomes one envelope carrying that process verbatim as
    its payload, with ``parents`` taken from the file's ``dependency_graph``, or from
    list order when it has none (aind-data-schema 1.x). Nothing is validated, so a
    file of any schema version converts; a payload the assembler cannot validate
    becomes a placeholder there.

    Node ids are ``<name>@<digest>``, the digest taken over the file's content, so one
    file reached along two edges yields the same ids while two files that each name a
    step ``"Spike sorting"`` stay distinct. ``label`` keeps the bare name.

    A ``processing.json`` beside a ``provenance_dir`` directory is skipped, because the
    shards there already describe its steps.

    Parameters
    ----------
    root : str or pathlib.Path
        Directory to scan (e.g. ``/data``).
    max_depth : int, default 2
        Maximum directory depth (relative to ``root``) to descend;
        ``<root>/<asset>/processing.json`` sits at depth 1.
    provenance_dir : str, default ``"provenance"``
        Shard directory whose presence supersedes a sibling ``processing.json``.

    Returns
    -------
    list[dict[str, Any]]
        Envelopes, possibly empty. Unreadable or unrecognized files are skipped.
    """
    base = Path(root)
    records: list[dict[str, Any]] = []
    for dirpath, dirnames, filenames in os.walk(base, followlinks=True):
        has_shards = provenance_dir in dirnames
        try:
            depth = len(Path(dirpath).relative_to(base).parts)
        except ValueError:
            depth = 0
        dirnames[:] = [] if depth >= max_depth else sorted(dirnames)
        if _PROCESSING_FILENAME not in filenames or has_shards:
            continue
        path = Path(dirpath) / _PROCESSING_FILENAME
        try:
            raw = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            _logger.debug("skipping unreadable %s: %s", path, exc)
            continue
        if not isinstance(raw, dict):
            continue
        digest = hashlib.sha256(json.dumps(raw, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:8]
        records.extend(_records_from_processing(raw, digest))
    return records


@dataclass
class RecordContext:
    """Mutable handle yielded by :func:`record_step`.

    Set ``parameters`` / ``notes`` / ``output_path`` / ``experimenters`` before the
    ``with`` block exits to fold them into the authored ``DataProcess``;
    ``parameters`` merge over any passed to :func:`record_step` (context wins on key
    clashes). ``node`` and ``parents`` are resolved when the block starts, and
    :meth:`fanout_shards` hands them to a fan-out.
    """

    parameters: dict[str, Any] = field(default_factory=dict)
    notes: str | None = None
    output_path: str | Path | None = None
    experimenters: list[str] | None = None
    node: str = field(default="", init=False)
    parents: tuple[str, ...] = field(default=(), init=False)
    _upstream: list[dict[str, Any]] = field(default_factory=list, init=False, repr=False)
    _envelope_extras: dict[str, Any] = field(default_factory=dict, init=False, repr=False)

    def fanout_shards(self) -> list[dict[str, Any]]:
        """Return the shards a fan-out unit needs to wire its worker to this node.

        Pass the result to ``write_stream_configs(provenance=...)``. It holds every
        upstream shard plus a payload-less stub of this node with the same
        ``parents``, so a worker infers this node as its parent and the stub is
        superseded wherever this node's full shard also arrives.

        Returns
        -------
        list[dict[str, Any]]
            Upstream shards followed by this node's stub.
        """
        stub = make_record(self.node, parents=self.parents, **self._envelope_extras)
        return [*self._upstream, stub]


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
    pipeline_name: str | None,
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
            pipeline_name=pipeline_name,
        )
        payload: dict[str, Any] = proc.model_dump(mode="json")
        return payload, package_version("aind-data-schema")
    except Exception as exc:  # noqa: BLE001 -- authoring is best-effort
        _logger.warning("data_process authoring failed for node %r: %s", node, exc)
        return None, None


def _gather_upstream(
    node: str,
    *,
    incoming_dir: str | Path,
    output_dir: str | Path,
    read_processing: bool,
) -> list[dict[str, Any]]:
    """Collect the shards this node descends from, excluding any shard of ``node`` itself.

    Reads the edge inputs and this node's own ``output_dir``. The latter makes a
    monolith (several steps in one process, sharing ``output_dir``) chain correctly:
    step N sees step N-1's just-written shard. A same-node shard (a launcher's own
    fan-out stub, or a monolith re-run) is dropped so it can neither become a parent
    nor be propagated over the fresh one.
    """
    found = [*read_records(incoming_dir), *read_records(output_dir)]
    if read_processing:
        found.extend(read_processing_records(incoming_dir))
    return [record for record in _dedup_records(found) if record.get("node") != node]


def _finalize_record(
    *,
    node: str,
    process_type: str,
    stage: str,
    output_dir: str | Path,
    experimenters: Sequence[str] | None,
    code_url: str | None,
    code_dir: str,
    version: str | None,
    commit_hash: str | None,
    parameters: Mapping[str, Any] | None,
    notes: str | None,
    ctx: RecordContext,
    start: datetime,
) -> Path:
    """Write this node's shard and forward copies of the upstream shards."""
    end = _utcnow()
    merged_params: dict[str, Any] = {**(dict(parameters) if parameters else {}), **ctx.parameters}
    resolved_notes = ctx.notes if ctx.notes is not None else notes
    resolved_output = ctx.output_path if ctx.output_path is not None else output_dir
    resolved_exps = ctx.experimenters if ctx.experimenters is not None else experimenters
    pipeline = ctx._envelope_extras.get("pipeline")

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
        pipeline_name=_nonempty_str(pipeline.get("name")) if pipeline else None,
    )
    record = make_record(
        node,
        parents=ctx.parents,
        data_process=data_process,
        data_process_schema_version=schema_version,
        **ctx._envelope_extras,
    )
    for upstream in ctx._upstream:
        write_record(upstream, output_dir)
    return write_record(record, output_dir)


def _dedup_records(records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Deduplicate by ``node`` (see :func:`_reconcile`), dropping node-less records."""
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
    run_experimenters: Sequence[str] | None = None,
    code_url: str | None = None,
    code_dir: str = "/code",
    version: str | None = None,
    commit_hash: str | None = None,
    parameters: Mapping[str, Any] | None = None,
    notes: str | None = None,
    pipeline: Mapping[str, Any] | None = None,
    read_processing: bool = True,
) -> Generator[RecordContext, None, None]:
    """Context manager that writes a provenance shard when the block succeeds.

    When the block starts, the upstream shards are read and ``parents`` resolved;
    both are exposed on the yielded :class:`RecordContext`. On a clean exit the step
    is timed, its ``DataProcess`` authored (best-effort), and its shard written to
    ``<output_dir>/provenance/`` alongside forwarded copies of every upstream shard.
    If the block raises, the exception propagates and **nothing is written** — a
    failed step leaves no record. Reading and writing provenance is best-effort and
    never raises.

    Parents are inferred (``parents=None``, the default) as the frontier of the
    upstream shards: those arriving on edges, those already in ``output_dir``, and,
    with ``read_processing``, the steps of any upstream ``processing.json`` (see
    :func:`read_processing_records`). The capsule never hardcodes its DAG position.
    Pass ``parents`` explicitly to override — for an ambiguous frontier or an
    unwired/test context. A node is never its own parent.

    Parameters
    ----------
    node : str
        Unique node id (also the ``DataProcess.name``). Fan-out workers must
        include their unit in it.
    process_type : str
        aind-data-schema ``ProcessName`` value; an unknown label becomes ``OTHER``
        with the label kept as ``notes`` unless notes are given. Ignored if the
        ``[metadata]`` extra is absent (the payload is simply omitted).
    stage : str, default ``"Processing"``
        aind-data-schema ``ProcessStage`` value.
    incoming_dir : str or pathlib.Path, default ``"/data"``
        Where upstream shards arrive (scanned for ``provenance/`` subdirs).
    output_dir : str or pathlib.Path, default ``"/results"``
        Where this node writes; pass a per-item subdir for fan-out.
    parents : Sequence[str], optional
        Explicit parent node ids; overrides frontier inference.
    experimenters : Sequence[str], optional
        People responsible for this step; folded into its ``DataProcess``.
    run_experimenters : Sequence[str], optional
        People responsible for the whole run, recorded on the envelope. Assembly
        fills them into every step that names none. Set it once, at the launcher.
    code_url : str, optional
        Repository URL override; auto-derived from git/env otherwise.
    code_dir : str, default ``"/code"``
        Git checkout used to derive ``code_url`` / ``commit_hash``.
    version : str, optional
        Code version stamp (e.g. ``package_version("my-package")``). Not derived.
    commit_hash : str, optional
        Git commit override; auto-derived otherwise. A value failing the schema's
        hash pattern is dropped.
    parameters : Mapping[str, Any], optional
        Static run parameters; merged under any set on the context.
    notes : str, optional
        Free-text notes; a value set on the context takes precedence.
    pipeline : Mapping[str, Any], optional
        Run-level pipeline block ``{"name": ..., "code": {...}}`` (launcher). Stamps
        this step's ``pipeline_name``; assembly adds ``code`` to ``Processing.pipelines``.
    read_processing : bool, default True
        Also read upstream ``processing.json`` files, so capsules that do not use
        this library still appear as parents.

    Yields
    ------
    RecordContext
    """
    ctx = RecordContext()
    ctx.node = node
    extras: dict[str, Any] = {}
    if pipeline is not None:
        extras["pipeline"] = dict(pipeline)
    if run_experimenters is not None:
        extras["experimenters"] = list(run_experimenters)
    ctx._envelope_extras = extras
    try:
        ctx._upstream = _gather_upstream(
            node, incoming_dir=incoming_dir, output_dir=output_dir, read_processing=read_processing
        )
    except Exception as exc:  # noqa: BLE001 -- provenance must never sink the run
        _logger.warning("could not read upstream provenance for node %r: %s", node, exc)
    resolved = list(parents) if parents is not None else frontier(ctx._upstream)
    ctx.parents = tuple(p for p in resolved if p != node)
    start = _utcnow()
    yield ctx
    try:
        _finalize_record(
            node=node,
            process_type=process_type,
            stage=stage,
            output_dir=output_dir,
            experimenters=experimenters,
            code_url=code_url,
            code_dir=code_dir,
            version=version,
            commit_hash=commit_hash,
            parameters=parameters,
            notes=notes,
            ctx=ctx,
            start=start,
        )
    except Exception as exc:  # noqa: BLE001 -- provenance must never sink the run
        _logger.warning("provenance emit failed for node %r: %s", node, exc)
