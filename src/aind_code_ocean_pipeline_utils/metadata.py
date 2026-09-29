"""Author aind-data-schema ``DataProcess`` / ``Processing`` records (deferred use).

Optional module — requires the ``[metadata]`` extra (``aind-data-schema``).
Import it explicitly (it is *not* re-exported from the package root, since the
core package is stdlib-only).

Scope
-----
Per-node provenance in a live pipeline is emitted as **schema-free breadcrumbs**
by :mod:`.records` — no aind-data-schema on that hot path. This module is the
schema side:

- :func:`assemble_processing` / :func:`write_assembled_processing` turn the
  breadcrumbs and upstream ``processing.json`` files reaching a node into one
  validated ``Processing``. This is the only place incoming payloads are parsed.
- :func:`make_data_process` builds one validated ``DataProcess`` (called by
  :func:`aind_code_ocean_pipeline_utils.records.record_step` to author a shard's
  opaque payload).
- :func:`write_processing` serializes a ``Processing``.
- :func:`make_derived_data_description` / :func:`write_data_description` author the
  one metadata file the derived asset owns (deriving it from the input asset's data
  description rather than forwarding it, which would misdescribe a different asset).
"""

from __future__ import annotations

import logging
import warnings
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .records import _dedup_records, read_processing_records, read_records

# aind-data-schema (the optional ``[metadata]`` extra) is imported LAZILY inside the
# functions that construct its models -- so importing this module (e.g. the wheel
# smoke test's walk-import of every submodule) never fails on the minimal install;
# only *calling* a function without the extra does. Type-only names stay valid via
# TYPE_CHECKING + ``from __future__ import annotations``.
if TYPE_CHECKING:
    from aind_data_schema.components.identifiers import Code
    from aind_data_schema.core.data_description import DataDescription
    from aind_data_schema.core.processing import (
        DataProcess,
        Processing,
        ProcessName,
        ProcessStage,
    )

__all__ = [
    "assemble_processing",
    "make_data_process",
    "make_derived_data_description",
    "utcnow",
    "write_assembled_processing",
    "write_data_description",
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
    stage: ProcessStage | None = None,
    name: str | None = None,
    version: str | None = None,
    commit_hash: str | None = None,
    parameters: Mapping[str, Any] | None = None,
    output_path: str | Path | None = None,
    notes: str | None = None,
    pipeline_name: str | None = None,
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
    stage : ProcessStage, optional
        Processing vs Analysis stage; defaults to ``ProcessStage.PROCESSING``.
    name : str, optional
        Unique node name. Required if this process will participate in a
        dependency graph.
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
    pipeline_name : str, optional
        Name of the pipeline this step ran in; must match an entry of
        ``Processing.pipelines`` once assembled.

    Returns
    -------
    DataProcess
    """
    from aind_data_schema.core.processing import Code, DataProcess, ProcessStage

    return DataProcess(
        process_type=process_type,
        name=name,
        stage=stage if stage is not None else ProcessStage.PROCESSING,
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
        pipeline_name=pipeline_name,
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


def _topological(parents: Mapping[str, Sequence[str]]) -> list[str]:
    """Order nodes parents-first, stable in first-seen order; a cycle keeps the rest as seen."""
    remaining = dict(parents)
    placed: set[str] = set()
    ordered: list[str] = []
    while remaining:
        ready = [node for node, ps in remaining.items() if all(p in placed for p in ps)] or list(remaining)
        for node in ready:
            placed.add(node)
            ordered.append(node)
            del remaining[node]
    return ordered


def _parse_time(value: object) -> datetime | None:
    """Parse a tz-aware ISO timestamp, or return ``None``."""
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def _placeholder(
    name: str,
    raw: Mapping[str, Any] | None,
    reason: str,
    fallback_start: datetime,
) -> DataProcess:
    """Build a minimal valid ``DataProcess`` standing in for a step with no usable payload.

    Keeps what the raw payload states plainly (type, start time, code URL) and says
    in ``notes`` that the step is a placeholder, so the graph stays connected without
    passing off invented values as recorded ones.
    """
    from aind_data_schema.components.identifiers import Code
    from aind_data_schema.core.processing import DataProcess, ProcessName, ProcessStage

    raw = raw or {}
    process_type = ProcessName.OTHER
    for key in ("process_type", "name"):  # aind-data-schema 1.x keeps the type in ``name``
        try:
            process_type = ProcessName(raw.get(key))
            break
        except ValueError:
            continue
    note = f"Placeholder: {reason}."
    start = _parse_time(raw.get("start_date_time"))
    if start is None:
        start = fallback_start
        note += " Start time not recorded."
    code = raw.get("code")
    url = code.get("url") if isinstance(code, Mapping) else raw.get("code_url")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)  # Code without commit_hash/version warns
        return DataProcess(
            process_type=process_type,
            name=name,
            stage=ProcessStage.PROCESSING,
            code=Code(url=url if isinstance(url, str) else ""),
            experimenters=[],
            start_date_time=start,
            notes=note,
        )


@dataclass
class _RunDefaults:
    """Run-level values that this run's own steps inherit at assembly."""

    experimenters: list[str]
    pipeline_codes: dict[str, Mapping[str, Any]]
    pipeline_name: str | None

    @classmethod
    def from_records(cls, records: Iterable[Mapping[str, Any]]) -> _RunDefaults:
        """Collect defaults; records carrying a ``label`` came from a ``processing.json`` and are not this run's."""
        experimenters: list[str] = []
        codes: dict[str, Mapping[str, Any]] = {}
        own_pipelines: set[str] = set()
        for record in records:
            own = "label" not in record
            block = record.get("pipeline")
            if isinstance(block, Mapping) and isinstance(block.get("name"), str):
                codes.setdefault(block["name"], block.get("code") or {})
                if own:
                    own_pipelines.add(block["name"])
            if own:
                for person in record.get("experimenters") or []:
                    if isinstance(person, str) and person not in experimenters:
                        experimenters.append(person)
        pipeline_name = next(iter(own_pipelines)) if len(own_pipelines) == 1 else None
        return cls(experimenters, codes, pipeline_name)


def _build_process(
    record: Mapping[str, Any] | None,
    name: str,
    defaults: _RunDefaults,
) -> DataProcess | tuple[Mapping[str, Any] | None, str]:
    """Validate a record's payload as a ``DataProcess``, or return ``(raw, reason)`` for a placeholder."""
    from aind_data_schema.core.processing import DataProcess
    from pydantic import ValidationError

    if record is None:
        return None, "no provenance was recorded for this step"
    payload = record.get("data_process")
    if not isinstance(payload, Mapping):
        return None, "this step recorded its place in the graph but no DataProcess"
    data = {**payload, "name": name}
    if "label" not in record:
        if not data.get("experimenters") and defaults.experimenters:
            data["experimenters"] = defaults.experimenters
        if not data.get("pipeline_name") and defaults.pipeline_name is not None:
            data["pipeline_name"] = defaults.pipeline_name
    try:
        return DataProcess.model_validate(data)
    except ValidationError as exc:
        version = record.get("data_process_schema_version") or "unknown"
        return payload, f"its DataProcess (schema {version}) failed validation: {str(exc).splitlines()[0]}"


def _resolve_pipelines(
    processes: dict[str, DataProcess],
    pipeline_codes: Mapping[str, Mapping[str, Any]],
) -> list[Code]:
    """Build ``Processing.pipelines`` for the names steps reference, unlinking any that cannot be built."""
    from aind_data_schema.components.identifiers import Code
    from pydantic import ValidationError

    pipelines: list[Code] = []
    for pipeline_name in sorted({p.pipeline_name for p in processes.values() if p.pipeline_name}):
        try:
            pipelines.append(Code.model_validate({**pipeline_codes.get(pipeline_name, {}), "name": pipeline_name}))
        except ValidationError as exc:
            _logger.warning("dropping pipeline %r from processing: %s", pipeline_name, exc)
            for node, process in processes.items():
                if process.pipeline_name == pipeline_name:
                    processes[node] = process.model_copy(update={"pipeline_name": None})
    return pipelines


def assemble_processing(
    input_dir: str | Path = "/data",
    output_dir: str | Path | None = None,
    *,
    read_processing: bool = True,
) -> Processing:
    """Build one validated ``Processing`` from the provenance reaching this node.

    Reads the breadcrumb shards under ``input_dir`` and, when given, ``output_dir``
    (where this node's own :func:`~aind_code_ocean_pipeline_utils.records.record_step`
    wrote), plus, with ``read_processing``, every upstream ``processing.json`` under
    ``input_dir``. Each step becomes one ``DataProcess`` and each ``parents`` list its
    ``dependency_graph`` entry, ordered parents-first.

    A step that cannot be assembled from what it recorded becomes a **placeholder**
    ``DataProcess`` (type ``Other`` unless the raw payload names a valid type, with
    ``notes`` saying why) instead of failing the whole record. That covers a node
    named as a parent that left no shard, a shard without a ``DataProcess`` payload
    (a fan-out stub, or a node without the ``[metadata]`` extra), and a payload that
    fails validation under the installed aind-data-schema.

    Steps recorded by ``record_step`` with no experimenters inherit the run-level
    ``experimenters`` a launcher set with ``run_experimenters=``; when exactly one
    run-level ``pipeline`` is recorded, they also inherit its ``pipeline_name``.
    Steps read from a ``processing.json`` are left as their authors recorded them.

    Parameters
    ----------
    input_dir : str or pathlib.Path, default ``"/data"``
        Where upstream provenance arrives.
    output_dir : str or pathlib.Path, optional
        This node's output directory, read for shards only, never for a
        ``processing.json`` (which may be this function's own earlier output).
    read_processing : bool, default True
        Also read upstream ``processing.json`` files.

    Returns
    -------
    Processing
        The assembled record; ``dependency_graph`` keys are the step names.

    Raises
    ------
    ImportError
        If the ``[metadata]`` extra is not installed.
    pydantic.ValidationError
        If the assembled record still fails validation.
    """
    from aind_data_schema.core.processing import Processing

    found = list(read_records(input_dir))
    if output_dir is not None:
        found.extend(read_records(output_dir))
    if read_processing:
        found.extend(read_processing_records(input_dir))
    records = {record["node"]: record for record in _dedup_records(found)}

    parents: dict[str, list[str]] = {}
    for node, record in records.items():
        parents[node] = [p for p in record.get("parents") or [] if isinstance(p, str) and p != node]
    for node in list(parents):
        for parent in parents[node]:
            parents.setdefault(parent, [])  # a parent that left no shard is a hole
    order = _topological(parents)

    labels = {node: records.get(node, {}).get("label") or node for node in order}
    label_uses = Counter(labels.values())
    names = {node: labels[node] if label_uses[labels[node]] == 1 else node for node in order}

    defaults = _RunDefaults.from_records(records.values())
    processes: dict[str, DataProcess] = {}
    holes: dict[str, tuple[Mapping[str, Any] | None, str]] = {}
    for node in order:
        built = _build_process(records.get(node), names[node], defaults)
        if isinstance(built, tuple):
            holes[node] = built
        else:
            processes[node] = built

    fallback_start = min((p.start_date_time for p in processes.values()), default=utcnow())
    for node, (raw, reason) in holes.items():
        processes[node] = _placeholder(names[node], raw, reason, fallback_start)
    if holes:
        _logger.warning(
            "assembled processing with %d placeholder step(s): %s",
            len(holes),
            ", ".join(names[node] for node in holes),
        )

    pipelines = _resolve_pipelines(processes, defaults.pipeline_codes)
    return Processing(
        data_processes=[processes[node] for node in order],
        dependency_graph={names[node]: [names[p] for p in parents[node]] for node in order},
        pipelines=pipelines or None,
    )


def write_assembled_processing(
    input_dir: str | Path = "/data",
    output_dir: str | Path = "/results",
    *,
    read_processing: bool = True,
) -> Path | None:
    """Assemble and write ``processing.json`` into ``output_dir``; best-effort, never raises.

    The one call a pipeline's final node needs; see :func:`assemble_processing`.
    Shards already in ``output_dir`` (from this node's own ``record_step``) are
    included, so call it after that block exits.

    Parameters
    ----------
    input_dir : str or pathlib.Path, default ``"/data"``
        Where upstream provenance arrives.
    output_dir : str or pathlib.Path, default ``"/results"``
        Where ``processing.json`` is written.
    read_processing : bool, default True
        Also read upstream ``processing.json`` files.

    Returns
    -------
    pathlib.Path or None
        The written file, or ``None`` if assembly failed (logged as a warning).
    """
    try:
        processing = assemble_processing(input_dir, output_dir, read_processing=read_processing)
        return write_processing(processing, output_dir)
    except Exception as exc:  # noqa: BLE001 -- metadata must never sink the run
        _logger.warning("could not assemble processing.json: %s", exc)
        return None


def make_derived_data_description(
    parent: DataDescription | Mapping[str, Any],
    process_name: str,
    *,
    source_data: Sequence[str] | None = None,
    **overrides: Any,
) -> DataDescription:
    """Derive a ``DataLevel.DERIVED`` :class:`DataDescription` from a parent asset.

    Thin wrapper over ``DataDescription.from_data_description`` (which picks
    ``from_raw`` / ``from_derived`` by the parent's ``data_level``). The parent's
    institution, funding, investigators, project name, modalities, and subject id
    are inherited; ``data_level`` becomes ``DERIVED`` and a derived name is
    generated. This is the one metadata file to **author** for the produced asset
    rather than forward — forwarding the parent's would misdescribe a different
    asset.

    Parameters
    ----------
    parent : DataDescription or Mapping[str, Any]
        The input asset's data description, as an object or a parsed dict (validated
        here). Validating an older-schema parent may raise; callers on an emit path
        should treat this best-effort.
    process_name : str
        Name of the process that produced the derived asset (folded into the name).
    source_data : Sequence[str], optional
        Source asset name(s). Defaults to the parent's own name.
    **overrides
        Any ``DataDescription`` field to override on the derived record.

    Returns
    -------
    DataDescription
        A ``DERIVED`` data description.
    """
    from aind_data_schema.core.data_description import DataDescription

    parent_dd = parent if isinstance(parent, DataDescription) else DataDescription.model_validate(dict(parent))
    return DataDescription.from_data_description(
        parent_dd,
        process_name,
        source_data=list(source_data) if source_data else None,
        **overrides,
    )


def write_data_description(data_description: DataDescription, output_dir: str | Path) -> Path:
    """Write ``data_description.json`` into ``output_dir`` via aind-data-schema.

    Parameters
    ----------
    data_description : DataDescription
        The record to serialize.
    output_dir : str or pathlib.Path
        Destination directory (created if needed); the file is always named
        ``data_description.json`` per the aind-data-schema convention.

    Returns
    -------
    pathlib.Path
        Path to the written ``data_description.json``.
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    data_description.write_standard_file(output_directory=out)
    return out / "data_description.json"
