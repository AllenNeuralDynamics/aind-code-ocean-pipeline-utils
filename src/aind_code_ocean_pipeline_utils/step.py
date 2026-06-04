"""Frictionless ``processing.json`` emission for a Code Ocean capsule step.

Optional module — requires the ``[metadata]`` extra (``aind-data-schema``).
Import it explicitly; it is *not* re-exported from the package root.

Every capsule in every pipeline is expected to emit a ``processing.json`` so
the terminal node carries the full DAG (see :mod:`.metadata` for *why* the
frontier-append model is correct, and why it lets you drop the buggy
aggregator). This module collapses that ceremony to a single decorator::

    from aind_code_ocean_pipeline_utils.step import capsule_step

    @capsule_step("Skull stripping", name="mri-skull-stripping")
    def run() -> None:
        ...   # the actual work, unchanged

On return the wrapper times the run, builds one :class:`DataProcess`, merges
upstream ``processing.json`` files from ``/data``, appends this node at the
frontier, writes ``/results/processing.json``, and (by default) forwards the
ancillary metadata files riding the chain. For steps that compute their
parameters/notes at runtime, the :func:`processing_step` context manager twin
exposes a mutable :class:`StepContext`::

    with processing_step("Image atlas alignment", name="mri-registration") as step:
        step.parameters = {"mask_dilate": 4}
        step.notes = "build5 template"
        ...

Design choices (so authors never fight the schema):

* **process_type accepts a plain string** — a known label coerces to the
  matching :class:`ProcessName`; an unknown one becomes ``ProcessName.OTHER``
  with the label preserved as ``notes``. No need to memorize the closed enum.
* **name is required and explicit** — it is the DAG node key; inferring it
  risks corrupting the graph.
* **code_url, commit_hash, version, experimenters auto-derive** from the git
  checkout, Code Ocean env vars, and the upstream metadata, with explicit
  overrides. Anything underivable degrades to ``None``/``[]`` rather than
  failing.
* **All metadata work is best-effort** — a failure here logs and is swallowed
  so it never sinks the capsule. The wrapped function's *own* exceptions
  propagate (a failed step emits nothing).
"""

from __future__ import annotations

import functools
import json
import logging
import os
import re
import shutil
import subprocess
from collections.abc import Callable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeVar

from .provenance import capsule_commit

# aind-data-schema (the heavy, optional ``[metadata]`` dependency) is imported
# LAZILY — only inside the emit path, which runs *after* the wrapped work. This
# keeps decoration/startup free of the ~80 ms schema import, defers that cost to
# the end of a successful run, skips it entirely when the work raises early, and
# lets the decorator degrade to a logged no-op if the extra isn't installed.
# Type-only references stay valid via TYPE_CHECKING + ``from __future__``.
if TYPE_CHECKING:
    from aind_data_schema.core.processing import Code, Processing, ProcessName, ProcessStage

__all__ = [
    "DEFAULT_FORWARDED_METADATA",
    "StepContext",
    "capsule_step",
    "coerce_process_type",
    "derive_code_url",
    "derive_experimenters",
    "forward_metadata",
    "processing_step",
]

_logger = logging.getLogger(__name__)

_F = TypeVar("_F", bound=Callable[..., Any])

# aind-data-schema constrains Code.commit_hash to this pattern; an env- or
# git-supplied value that does not match is silently dropped rather than
# failing the whole DataProcess build.
_COMMIT_HASH_RE = re.compile(r"^[0-9a-fA-F]{7,60}$")

# Code Ocean does not expose a single canonical "capsule repo URL" env var;
# these are the names seen across CO / CI environments, checked in order.
_DEFAULT_CODE_URL_ENV_VARS: tuple[str, ...] = ("CO_CAPSULE_URL", "GIT_URL", "REPO_URL")

# Env vars carrying a comma/semicolon-separated list of responsible people.
_DEFAULT_EXPERIMENTER_ENV_VARS: tuple[str, ...] = (
    "AIND_EXPERIMENTERS",
    "PROCESSOR_FULL_NAME",
)

# Ancillary aind-data-schema files copied input -> output by default. NOT
# processing.json (that is built fresh) and NOT a fixed-path assumption — each
# is located by recursive search so double-nested pipeline mounts still work.
DEFAULT_FORWARDED_METADATA: tuple[str, ...] = (
    "subject.json",
    "data_description.json",
    "procedures.json",
    "instrument.json",
    "acquisition.json",
    "quality_control.json",
)


def coerce_process_type(value: str | ProcessName) -> tuple[ProcessName, str | None]:
    """Resolve a process type from a string or enum member.

    Parameters
    ----------
    value : str or ProcessName
        Either a :class:`ProcessName` member, or a human label. A label equal
        to a member's value (e.g. ``"Skull stripping"``) resolves to that
        member; any other string becomes :attr:`ProcessName.OTHER`.

    Returns
    -------
    tuple[ProcessName, str or None]
        The resolved process type and, when the input was an unrecognized
        string, that string as a ``notes`` fallback (``None`` otherwise). The
        schema requires ``notes`` whenever the type is ``OTHER``.
    """
    from aind_data_schema.core.processing import ProcessName

    if isinstance(value, ProcessName):
        return value, None
    try:
        return ProcessName(value), None
    except ValueError:
        return ProcessName.OTHER, value


def _normalize_remote_url(url: str) -> str:
    """Normalize a git remote URL to an https form, dropping a trailing ``.git``.

    ``git@github.com:org/repo.git`` -> ``https://github.com/org/repo`` and
    ``https://github.com/org/repo.git`` -> ``https://github.com/org/repo``.
    Anything unrecognized is returned unchanged.
    """
    url = url.strip()
    scp = re.match(r"^git@([^:]+):(.+)$", url)
    if scp:
        url = f"https://{scp.group(1)}/{scp.group(2)}"
    if url.endswith(".git"):
        url = url[: -len(".git")]
    return url


def derive_code_url(
    *,
    explicit: str | None = None,
    code_dir: str = "/code",
    env_vars: Sequence[str] = _DEFAULT_CODE_URL_ENV_VARS,
) -> str | None:
    """Best-effort repository URL for the capsule that ran this step.

    Resolution order: ``explicit`` override, then ``git -C <code_dir> remote
    get-url origin`` (normalized to https), then the first non-empty
    ``env_vars`` value. Returns ``None`` if nothing resolves. Never raises.

    Parameters
    ----------
    explicit : str, optional
        Caller-supplied URL; wins outright when given.
    code_dir : str, default ``"/code"``
        Git checkout to inspect (Code Ocean mounts source at ``/code``).
    env_vars : Sequence[str]
        Env var names to consult, in priority order.

    Returns
    -------
    str or None
    """
    if explicit:
        return explicit
    try:
        result = subprocess.run(
            ["git", "-C", code_dir, "remote", "get-url", "origin"],
            capture_output=True,
            text=True,
            check=True,
            timeout=5.0,
        )
        remote = result.stdout.strip()
        if remote:
            return _normalize_remote_url(remote)
    except (OSError, subprocess.SubprocessError) as exc:
        _logger.debug("derive_code_url: git fallback failed: %s", exc)
    for name in env_vars:
        val = os.environ.get(name, "").strip()
        if val:
            return _normalize_remote_url(val)
    return None


def _experimenters_from_processings(processings: Sequence[Processing]) -> list[str]:
    """Collect distinct experimenter names from upstream DataProcesses."""
    out: list[str] = []
    for processing in processings:
        for proc in processing.data_processes:
            for person in proc.experimenters or []:
                if person and person not in out:
                    out.append(person)
    return out


def _investigators_from_data_description(input_dir: Path) -> list[str]:
    """Pull investigator names from the first ``data_description.json`` found."""
    out: list[str] = []
    for path in sorted(input_dir.rglob("data_description.json")):
        try:
            data = json.loads(path.read_text())
        except (OSError, ValueError) as exc:
            _logger.debug("skipping unreadable %s: %s", path, exc)
            continue
        for inv in data.get("investigators") or []:
            name = inv.get("name") if isinstance(inv, Mapping) else inv
            if isinstance(name, str) and name and name not in out:
                out.append(name)
        if out:
            break
    return out


def _experimenters_from_env(env_vars: Sequence[str]) -> list[str]:
    """Split the first populated env var on commas/semicolons into names."""
    for name in env_vars:
        raw = os.environ.get(name, "").strip()
        if raw:
            return [part.strip() for part in re.split(r"[,;]", raw) if part.strip()]
    return []


def derive_experimenters(
    *,
    explicit: Sequence[str] | None = None,
    input_dir: str | Path = "/data",
    incoming: Sequence[Processing] | None = None,
    env_vars: Sequence[str] = _DEFAULT_EXPERIMENTER_ENV_VARS,
) -> list[str]:
    """Best-effort list of people responsible for this automated step.

    The schema requires an ``experimenters`` list but an automated capsule has
    no real author, so this derives a sensible value rather than asking the
    caller to invent one. Resolution order (first non-empty wins):

    1. ``explicit`` override.
    2. Experimenters carried by upstream ``processing.json`` files.
    3. ``investigators`` in an upstream ``data_description.json``.
    4. The first populated ``env_vars`` entry (split on ``,``/``;``).
    5. ``[]`` (schema-valid).

    Never raises.

    Parameters
    ----------
    explicit : Sequence[str], optional
        Caller-supplied names; used verbatim when given.
    input_dir : str or pathlib.Path, default ``"/data"``
        Where to look for upstream metadata.
    incoming : Sequence[Processing], optional
        Pre-read upstream Processing records; read from ``input_dir`` if
        omitted.
    env_vars : Sequence[str]
        Env var names to consult, in priority order.

    Returns
    -------
    list[str]
    """
    if explicit:
        return list(explicit)
    from .metadata import read_processings

    root = Path(input_dir)
    upstream = incoming if incoming is not None else read_processings(root)
    from_procs = _experimenters_from_processings(upstream)
    if from_procs:
        return from_procs
    from_dd = _investigators_from_data_description(root)
    if from_dd:
        return from_dd
    return _experimenters_from_env(env_vars)


def forward_metadata(
    input_dir: str | Path,
    output_dir: str | Path,
    *,
    names: Sequence[str] = DEFAULT_FORWARDED_METADATA,
) -> list[Path]:
    """Copy ancillary aind-data-schema metadata files from input to output.

    Locates each name by recursive search under ``input_dir`` (so doubly
    nested Code Ocean pipeline mounts are handled) and copies the first match
    to ``output_dir`` at the top level. ``processing.json`` is deliberately
    excluded — it is rebuilt, not forwarded. Best-effort: a file that cannot be
    copied is logged and skipped.

    Parameters
    ----------
    input_dir : str or pathlib.Path
        Source tree (typically ``/data``).
    output_dir : str or pathlib.Path
        Destination directory (typically ``/results``); created if needed.
    names : Sequence[str]
        Filenames to forward.

    Returns
    -------
    list[pathlib.Path]
        The destination paths actually written.
    """
    src_root = Path(input_dir)
    dst_root = Path(output_dir)
    written: list[Path] = []
    for filename in names:
        matches = sorted(src_root.rglob(filename))
        if not matches:
            continue
        dst = dst_root / filename
        try:
            dst_root.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(matches[0], dst)
            written.append(dst)
        except OSError as exc:
            _logger.warning("could not forward %s: %s", filename, exc)
    return written


@dataclass
class StepContext:
    """Mutable handle yielded by :func:`processing_step` for runtime details.

    Attributes set on the context before the ``with`` block exits are folded
    into the emitted :class:`DataProcess`. ``parameters`` set here are merged
    over any passed to :func:`processing_step` (context wins on key clashes).
    """

    parameters: dict[str, Any] = field(default_factory=dict)
    notes: str | None = None
    output_path: str | Path | None = None
    experimenters: list[str] | None = None


def _valid_commit(commit: str | None) -> str | None:
    """Return ``commit`` only if it matches the schema's hash pattern."""
    if commit and _COMMIT_HASH_RE.match(commit):
        return commit
    if commit:
        _logger.debug("dropping commit_hash %r: fails schema pattern", commit)
    return None


def _emit_step(
    *,
    ctx: StepContext,
    process_type: str | ProcessName,
    name: str,
    start: datetime,
    input_dir: str | Path,
    output_dir: str | Path,
    code_url: str | None,
    code_dir: str,
    experimenters: Sequence[str] | None,
    stage: ProcessStage | str,
    version: str | None,
    commit_hash: str | None,
    parameters: Mapping[str, Any] | None,
    notes: str | None,
    pipelines: Sequence[Code] | None,
    forward: bool,
) -> None:
    """Build and write this step's processing.json. Best-effort; never raises.

    The aind-data-schema import lives here (not at module top) so a capsule pays
    the schema cost only on a successful run, and a missing ``[metadata]`` extra
    degrades to a logged warning rather than an import error at startup.
    """
    try:
        from aind_data_schema.core.processing import ProcessStage

        from .metadata import emit_processing, make_data_process

        ptype, label = coerce_process_type(process_type)
        resolved_notes = ctx.notes or notes or label
        resolved_stage = ProcessStage(stage) if isinstance(stage, str) else stage
        resolved_exps = ctx.experimenters if ctx.experimenters is not None else experimenters
        if resolved_exps is None:
            resolved_exps = derive_experimenters(input_dir=input_dir)
        merged_params: dict[str, Any] = {**(parameters or {}), **ctx.parameters}
        out_path = ctx.output_path if ctx.output_path is not None else output_dir
        proc = make_data_process(
            process_type=ptype,
            code_url=derive_code_url(explicit=code_url, code_dir=code_dir) or "",
            experimenters=resolved_exps,
            start=start,
            stage=resolved_stage,
            name=name,
            version=version,
            commit_hash=_valid_commit(commit_hash if commit_hash is not None else capsule_commit(code_dir=code_dir)),
            parameters=merged_params or None,
            output_path=out_path,
            notes=resolved_notes,
        )
        emit_processing(proc, input_dir=input_dir, output_dir=output_dir, pipelines=pipelines)
        if forward:
            forward_metadata(input_dir, output_dir)
    except Exception as exc:
        _logger.warning("processing.json emit failed for step %r: %s", name, exc)


@contextmanager
def processing_step(
    process_type: str | ProcessName,
    *,
    name: str,
    input_dir: str | Path = "/data",
    output_dir: str | Path = "/results",
    code_url: str | None = None,
    code_dir: str = "/code",
    experimenters: Sequence[str] | None = None,
    stage: ProcessStage | str = "Processing",
    version: str | None = None,
    commit_hash: str | None = None,
    parameters: Mapping[str, Any] | None = None,
    notes: str | None = None,
    pipelines: Sequence[Code] | None = None,
    forward_metadata: bool = True,
) -> Any:
    """Context manager that emits ``processing.json`` when the block succeeds.

    Yields a :class:`StepContext` for setting ``parameters``/``notes``/
    ``output_path``/``experimenters`` discovered at runtime. On normal exit the
    step is timed and emitted (see module docstring). If the block raises, the
    exception propagates and **nothing is emitted** — a failed step leaves no
    record. The emit itself is best-effort and never raises.

    Parameters
    ----------
    process_type : str or ProcessName
        Step category; a string is coerced (unknown -> ``OTHER`` + notes). See
        :func:`coerce_process_type`.
    name : str
        Unique DAG node name (required).
    input_dir : str or pathlib.Path, default ``"/data"``
        Where upstream ``processing.json`` / metadata are read.
    output_dir : str or pathlib.Path, default ``"/results"``
        Where this node writes; pass a subject-namespaced subdir for fan-out.
    code_url : str, optional
        Repository URL override; auto-derived from git/env otherwise.
    code_dir : str, default ``"/code"``
        Git checkout used to derive ``code_url`` and ``commit_hash``.
    experimenters : Sequence[str], optional
        Responsible people; auto-derived from upstream metadata otherwise.
    stage : ProcessStage or str, default ``"Processing"``
        Processing vs Analysis. A string is coerced to ``ProcessStage`` at emit
        time (kept as a string default so importing this module needs no schema).
    version : str, optional
        Code version stamp.
    commit_hash : str, optional
        Git commit override; auto-derived otherwise. A value failing the
        schema's hash pattern is dropped.
    parameters : Mapping[str, Any], optional
        Static run parameters; merged under any set on the context.
    notes : str, optional
        Free-text notes; a context value or an ``OTHER`` label takes
        precedence.
    pipelines : Sequence[Code], optional
        Pipeline repositories to record.
    forward_metadata : bool, default True
        Copy ancillary metadata files input -> output on success.

    Yields
    ------
    StepContext
    """
    ctx = StepContext()
    start = datetime.now(UTC)  # tz-aware; schema-free so it stays a cheap import
    yield ctx
    _emit_step(
        ctx=ctx,
        process_type=process_type,
        name=name,
        start=start,
        input_dir=input_dir,
        output_dir=output_dir,
        code_url=code_url,
        code_dir=code_dir,
        experimenters=experimenters,
        stage=stage,
        version=version,
        commit_hash=commit_hash,
        parameters=parameters,
        notes=notes,
        pipelines=pipelines,
        forward=forward_metadata,
    )


def capsule_step(
    process_type: str | ProcessName,
    *,
    name: str,
    input_dir: str | Path = "/data",
    output_dir: str | Path = "/results",
    code_url: str | None = None,
    code_dir: str = "/code",
    experimenters: Sequence[str] | None = None,
    stage: ProcessStage | str = "Processing",
    version: str | None = None,
    commit_hash: str | None = None,
    parameters: Mapping[str, Any] | None = None,
    notes: str | None = None,
    pipelines: Sequence[Code] | None = None,
    forward_metadata: bool = True,
) -> Callable[[_F], _F]:
    """Wrap a capsule entry point so it emits ``processing.json`` on success.

    The wrapped callable runs unchanged; on a clean return its run is timed and
    a ``processing.json`` is written (frontier-appended onto upstream graphs),
    with ancillary metadata forwarded by default. If the callable raises, the
    exception propagates and nothing is emitted. Metadata work is best-effort.

    This is the static-parameter form; use :func:`processing_step` when the
    parameters or notes are computed at runtime. All keyword arguments share
    the meaning documented on :func:`processing_step`.

    Returns
    -------
    Callable
        A decorator preserving the wrapped function's signature and return
        value.
    """

    def decorator(func: _F) -> _F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            with processing_step(
                process_type,
                name=name,
                input_dir=input_dir,
                output_dir=output_dir,
                code_url=code_url,
                code_dir=code_dir,
                experimenters=experimenters,
                stage=stage,
                version=version,
                commit_hash=commit_hash,
                parameters=parameters,
                notes=notes,
                pipelines=pipelines,
                forward_metadata=forward_metadata,
            ):
                return func(*args, **kwargs)

        return wrapper  # type: ignore[return-value]

    return decorator
