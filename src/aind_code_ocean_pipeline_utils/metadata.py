"""Author aind-data-schema ``DataProcess`` / ``Processing`` records (deferred use).

Optional module — requires the ``[metadata]`` extra (``aind-data-schema``).
Import it explicitly (it is *not* re-exported from the package root, since the
core package is stdlib-only).

Scope
-----
Per-node provenance in a live pipeline is emitted as **schema-free breadcrumbs**
by :mod:`.records` — no aind-data-schema on that hot path. What remains here is
the *authoring* side, used only when a compliant record is actually needed:

- :func:`make_data_process` builds one validated ``DataProcess`` (called by
  :func:`aind_code_ocean_pipeline_utils.records.record_step` to author a shard's
  opaque payload).
- :func:`write_processing` serializes a fully-assembled ``Processing``.
- :func:`make_derived_data_description` / :func:`write_data_description` author the
  one metadata file the derived asset owns (deriving it from the input asset's data
  description rather than forwarding it, which would misdescribe a different asset).

These are the seeds of the deferred, best-effort *assembler* that turns a
directory of breadcrumb shards into a compliant ``Processing`` (envelopes →
``dependency_graph``, payloads → ``data_processes``), run only under duress
against whatever schema version is current then. The old frontier-append over
*parsed* upstream ``Processing`` objects (``read_processings`` / ``append_process``
/ ``emit_processing``) — which required every capsule to agree on one
aind-data-schema version — has been removed in favor of the breadcrumbs.

Pairs with :mod:`aind_code_ocean_pipeline_utils.provenance` for stamping each
``DataProcess`` with a commit hash / package version.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from aind_data_schema.core.data_description import DataDescription
from aind_data_schema.core.processing import (
    Code,
    DataProcess,
    Processing,
    ProcessName,
    ProcessStage,
)

__all__ = [
    "make_data_process",
    "make_derived_data_description",
    "utcnow",
    "write_data_description",
    "write_processing",
]


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
