"""Build a Code Ocean node's aind-data-schema ``processing.json`` incrementally.

Optional module — requires the ``[metadata]`` extra (``aind-data-schema``).
Import it explicitly (it is *not* re-exported from the package root, since the
core package is stdlib-only).

Why this exists
---------------
A Code Ocean pipeline is a Nextflow DAG, but no single capsule sees the whole
graph, and the standard metadata aggregator chains standalone ``DataProcess``
records in filesystem-discovery order — which has nothing to do with the real
topology. The only place the true edges are knowable with *local* information
is along the data-flow: a node's inputs **are** its DAG parents.

So each node builds its ``processing.json`` from the ``processing.json`` files
handed to it by its upstream nodes (read from ``/data``), appends its own
``DataProcess`` wired to the *frontier* of the merged upstream graph, and writes
the result to ``/results``. Fan-in (a node with several upstream
``processing.json`` inputs) is a graph union plus an edge from the new node to
each incoming branch's frontier. The terminal node then holds the complete,
correct DAG. This is fully compatible with the existing aggregator, which
preserves ``dependency_graph`` from any ``processing.json`` it receives.

Pairs with :mod:`aind_code_ocean_pipeline_utils.provenance` for stamping each
``DataProcess`` with a commit hash / package version. Verified against
aind-data-schema 2.7.1.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from aind_data_schema.core.processing import (
    Code,
    DataProcess,
    Processing,
    ProcessName,
    ProcessStage,
)

__all__ = [
    "append_process",
    "emit_processing",
    "make_data_process",
    "read_processings",
    "utcnow",
    "write_processing",
]

_logger = logging.getLogger(__name__)


def utcnow() -> datetime:
    """Return a timezone-aware UTC ``datetime``.

    ``DataProcess`` timestamps must be timezone-aware; the naive
    :func:`datetime.datetime.now` fails validation.

    Returns
    -------
    datetime.datetime
        The current time in UTC, tz-aware.
    """
    return datetime.now(UTC)


def make_data_process(
    *,
    process_type: ProcessName,
    code_url: str,
    experimenters: Sequence[str],
    start: datetime,
    end: datetime | None = None,
    stage: ProcessStage = ProcessStage.PROCESSING,
    name: str | None = None,
    version: str | None = None,
    commit_hash: str | None = None,
    parameters: Mapping[str, Any] | None = None,
    output_path: str | Path | None = None,
    notes: str | None = None,
) -> DataProcess:
    """Build a single :class:`DataProcess` for this capsule's step.

    Parameters
    ----------
    process_type : ProcessName
        The aind-data-schema process category (e.g.
        ``ProcessName.IMAGE_ATLAS_ALIGNMENT``).
    code_url : str
        Repository URL of the capsule that ran this step.
    experimenters : Sequence[str]
        Names of those responsible; serialized as a list.
    start : datetime.datetime
        Timezone-aware start time (capture before the work runs).
    end : datetime.datetime, optional
        Timezone-aware end time; defaults to :func:`utcnow` if omitted.
    stage : ProcessStage, default ``ProcessStage.PROCESSING``
        Processing vs Analysis stage.
    name : str, optional
        Unique node name. Required if this process will participate in a
        dependency graph (i.e. for :func:`append_process`).
    version : str, optional
        Code version (e.g. package version of the logic that ran).
    commit_hash : str, optional
        Git commit of the capsule code.
    parameters : Mapping[str, Any], optional
        Run parameters, stored on the ``Code`` record.
    output_path : str or pathlib.Path, optional
        Where this step wrote its outputs.
    notes : str, optional
        Free-text notes (required by the schema when
        ``process_type`` is ``ProcessName.OTHER``).

    Returns
    -------
    DataProcess
    """
    return DataProcess(
        process_type=process_type,
        name=name,
        stage=stage,
        experimenters=list(experimenters),
        start_date_time=start,
        end_date_time=end if end is not None else utcnow(),
        output_path=str(output_path) if output_path is not None else None,
        code=Code(
            url=code_url,
            version=version,
            commit_hash=commit_hash,
            parameters=dict(parameters) if parameters else None,
        ),
        notes=notes,
    )


def read_processings(input_dir: str | Path, *, pattern: str = "*processing.json") -> list[Processing]:
    """Load upstream :class:`Processing` records handed to this node.

    Recursively globs ``input_dir`` (Code Ocean mounts upstream outputs under
    ``/data``), deduplicating by resolved path. Unreadable / invalid files are
    skipped with a warning rather than raising, so a metadata-emit path never
    crashes the capsule.

    Parameters
    ----------
    input_dir : str or pathlib.Path
        Directory to search (typically ``/data``).
    pattern : str, default ``"*processing.json"``
        Glob applied recursively.

    Returns
    -------
    list[Processing]
        Parsed upstream Processing objects (possibly empty).
    """
    root = Path(input_dir)
    seen: set[Path] = set()
    out: list[Processing] = []
    for file_path in sorted(root.rglob(pattern)):
        try:
            key = file_path.resolve()
        except OSError:
            key = file_path
        if key in seen:
            continue
        seen.add(key)
        try:
            data = json.loads(file_path.read_text())
            out.append(Processing.model_validate(data))
        except Exception as exc:
            _logger.warning("skipping unreadable processing.json %s: %s", file_path, exc)
    return out


def _dedup_codes(codes: Sequence[Code]) -> list[Code]:
    """Deduplicate ``Code`` entries by ``(url, name, version)``, preserving order."""
    seen: set[tuple[str | None, str | None, str | None]] = set()
    out: list[Code] = []
    for code in codes:
        key = (code.url, code.name, code.version)
        if key in seen:
            continue
        seen.add(key)
        out.append(code)
    return out


def append_process(
    incoming: Sequence[Processing],
    new: DataProcess,
    *,
    pipelines: Sequence[Code] | None = None,
) -> Processing:
    """Merge upstream graphs and append ``new`` at the frontier.

    Builds a single :class:`Processing` whose ``dependency_graph`` is the union
    of every incoming graph plus one node for ``new`` that depends on the
    *frontier* of the merged graph — the set of upstream processes that nothing
    else depends on yet (the sinks). A fan-in (several ``incoming`` graphs)
    therefore connects ``new`` to the head of each branch. Duplicate process
    names across branches (a diamond) are collapsed: the first record wins and
    its dependency lists are unioned.

    Parameters
    ----------
    incoming : Sequence[Processing]
        Upstream Processing records (from :func:`read_processings`). Empty for
        a source node.
    new : DataProcess
        This node's process. Must have a unique, non-``None`` ``name``.
    pipelines : Sequence[Code], optional
        Pipeline repositories to record; unioned with any carried by
        ``incoming`` and deduplicated.

    Returns
    -------
    Processing
        The merged record with ``new`` appended.

    Raises
    ------
    ValueError
        If ``new.name`` is ``None``, if any incoming ``DataProcess`` lacks a
        name, or if ``new.name`` already exists upstream.
    """
    data_processes: list[DataProcess] = []
    graph: dict[str, list[str]] = {}

    def _register(process: DataProcess, deps: Sequence[str]) -> None:
        name = process.name
        if name is None:
            raise ValueError("encountered a DataProcess with no name; cannot graph it")
        if name in graph:
            for dep in deps:
                if dep not in graph[name]:
                    graph[name].append(dep)
            return
        data_processes.append(process)
        graph[name] = list(deps)

    for processing in incoming:
        existing = processing.dependency_graph or {}
        for process in processing.data_processes:
            pname = process.name
            if pname is None:
                raise ValueError("incoming DataProcess has no name; cannot graph it")
            _register(process, list(existing.get(pname, [])))

    new_name = new.name
    if new_name is None:
        raise ValueError("new DataProcess must have a name to be added to the graph")
    if new_name in graph:
        raise ValueError(f"DataProcess name {new_name!r} already present upstream")

    referenced = {dep for deps in graph.values() for dep in deps}
    frontier = [name for name in graph if name not in referenced]
    _register(new, frontier)

    merged_pipelines: list[Code] = []
    for processing in incoming:
        merged_pipelines.extend(processing.pipelines or [])
    if pipelines:
        merged_pipelines.extend(pipelines)
    deduped = _dedup_codes(merged_pipelines)

    return Processing(
        data_processes=data_processes,
        dependency_graph=graph,
        pipelines=deduped or None,
    )


def write_processing(processing: Processing, output_dir: str | Path) -> Path:
    """Write ``processing.json`` into ``output_dir`` via aind-data-schema.

    Parameters
    ----------
    processing : Processing
        The record to serialize.
    output_dir : str or pathlib.Path
        Destination directory (created if needed); the file is always named
        ``processing.json`` per the aind-data-schema convention.

    Returns
    -------
    pathlib.Path
        Path to the written ``processing.json``.
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    processing.write_standard_file(output_directory=out)
    return out / "processing.json"


def emit_processing(
    new: DataProcess,
    *,
    input_dir: str | Path,
    output_dir: str | Path,
    pipelines: Sequence[Code] | None = None,
) -> Path:
    """Read upstream graphs, append ``new``, and write ``processing.json``.

    Convenience one-call wrapper around :func:`read_processings`,
    :func:`append_process`, and :func:`write_processing` for the common capsule
    path.

    Parameters
    ----------
    new : DataProcess
        This node's process (must have a unique name).
    input_dir : str or pathlib.Path
        Where to read upstream ``processing.json`` files (typically ``/data``).
    output_dir : str or pathlib.Path
        Where to write the merged ``processing.json`` (typically a
        subject-namespaced subdir of ``/results``).
    pipelines : Sequence[Code], optional
        Pipeline repositories to record.

    Returns
    -------
    pathlib.Path
        Path to the written ``processing.json``.
    """
    incoming = read_processings(input_dir)
    processing = append_process(incoming, new, pipelines=pipelines)
    return write_processing(processing, output_dir)
