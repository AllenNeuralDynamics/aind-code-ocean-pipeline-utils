# AIND Code Ocean pipeline utils

![CI](https://github.com/AllenNeuralDynamics/aind-code-ocean-pipeline-utils/actions/workflows/ci-call.yml/badge.svg)
[![PyPI - Version](https://img.shields.io/pypi/v/aind-code-ocean-pipeline-utils)](https://pypi.org/project/aind-code-ocean-pipeline-utils/)
[![semantic-release: angular](https://img.shields.io/badge/semantic--release-angular-e10079?logo=semantic-release)](https://github.com/semantic-release/semantic-release)
[![License](https://img.shields.io/badge/license-MIT-brightgreen)](LICENSE)
[![ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![Copier](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/copier-org/copier/master/img/badge/badge-grayscale-inverted-border.json)](https://github.com/copier-org/copier)

Utilities for AIND Code Ocean capsules. They assemble a pipeline's
`processing.json`, lay a capsule out as launcher, workers, and aggregator, and
guard long runs against termination, flaky I/O, stale caches, logs lost under
progress bars, and context lost across thread pools.

## Installation

The core is stdlib-only; two extras add dependencies.

```bash
pip install aind-code-ocean-pipeline-utils
pip install "aind-code-ocean-pipeline-utils[rich]"      # log: rich handler, progress bars
pip install "aind-code-ocean-pipeline-utils[metadata]"  # metadata: aind-data-schema
```

## A capsule, end to end

```python
from pathlib import Path

from aind_code_ocean_pipeline_utils import (
    check_shutdown,
    forward_metadata,
    log_data_tree,
    package_version,
    record_step,
    shutdown_handler,
)
from aind_code_ocean_pipeline_utils.log import install_rich_handler
from aind_code_ocean_pipeline_utils.metadata import write_assembled_processing

install_rich_handler()
log_data_tree(Path("/data"))  # what Code Ocean mounted
with shutdown_handler():  # SIGTERM exits at the next check_shutdown()
    with record_step("sort", process_type="Spike sorting", version=package_version("my-package")):
        for shard in shards:
            check_shutdown()
            process(shard)
forward_metadata("/data", "/results")  # subject.json, procedures.json, ...
write_assembled_processing("/data", "/results")  # in the pipeline's final capsule only
```

## What's in the package

| To | Use |
| --- | --- |
| [write a pipeline's `processing.json`](#the-final-capsule-writes-processingjson) | `metadata.write_assembled_processing` |
| [record this capsule's step](#opted-in-capsules-record-their-own-step) | `record_step` |
| [carry provenance across a fan-out](#a-launcher-hands-its-record-to-fan-out-workers) | `step.fanout_shards()` |
| [copy or author the other metadata files](#the-other-metadata-files-are-forwarded-or-authored) | `forward_metadata`, `metadata.make_derived_data_description` |
| [stamp a commit or package version](#commit-and-version-stamps) | `capsule_commit`, `package_version` |
| [fan work out and merge the results](#launcher-workers-aggregator) | `write_stream_configs`, `find_stream_config`, `merge_manifests` |
| [read a boolean App Panel parameter](#app-panel-parameters) | `parse_truthy` |
| [stop cleanly on SIGTERM](#graceful-shutdown) | `shutdown_handler`, `check_shutdown` |
| [retry flaky I/O, write files atomically](#retries-and-atomic-writes) | `retry_on_oserror`, `atomic_json_write` |
| [tell whether cached output is stale](#input-fingerprints) | `input_fingerprint` |
| [keep `contextvars` in thread-pool workers](#context-across-thread-pools) | `submit_with_context` |
| [log under progress bars, keep a log file](#logging-under-progress-bars) | `log.install_rich_handler`, `log.build_progress`, `attach_file_log` |
| [see the mounts and memory use](#mounts-and-memory) | `log_data_tree`, `start_memory_reporter` |

Everything imports from `aind_code_ocean_pipeline_utils` except the `log.` and
`metadata.` names, which live in submodules so the package root needs neither extra.

## Recording provenance and processing.json

Every AIND asset should carry a `processing.json`: the steps that produced it, and
a `dependency_graph` saying which fed which. No capsule sees the whole pipeline,
but each sees its inputs, which are its parents. So each capsule that opts in
records its own step, the records travel with the data, and the final capsule
assembles them. A capsule that never opts in still appears if it writes its own
`processing.json`.

### The final capsule writes processing.json

```python
from aind_code_ocean_pipeline_utils.metadata import write_assembled_processing

write_assembled_processing("/data", "/results")  # -> /results/processing.json
```

It reads the records and upstream `processing.json` files under `/data`, plus
this capsule's own records in `/results`, so call it after this capsule's
`record_step` block. A step it cannot recover in full, such as a parent that left
no record or a step recorded under a schema version the installed aind-data-schema
rejects, becomes a placeholder whose `notes` say why. The call logs a failure and
returns `None` rather than raising; `assemble_processing` returns the `Processing`
without writing it.

### Opted-in capsules record their own step

```python
with record_step(
    "mri-registration",  # unique node id in the pipeline
    process_type="Image atlas alignment",  # a ProcessName value; any other becomes "Other"
    version=package_version("my-package"),
) as step:
    step.parameters = {"mask_dilate": 4}  # values known only at runtime
    ...
```

On a clean exit, `record_step` writes `/results/provenance/mri-registration.json`
beside copies of every upstream record; if the block raises, it writes nothing.
Parents are inferred from what arrives in `/data`, so a capsule never states its
position in the DAG. `code_url` and `commit_hash` come from the `/code` checkout,
but `version` must be passed. Without the `[metadata]` extra, the record keeps
the step's place in the graph and loses its details.

Two wiring rules hold. Every pipeline edge must carry `/results/provenance/`, and
a fan-out worker's node id must include its unit, as in `f"sort-{probe}"`.

### A launcher hands its record to fan-out workers

A Flatten fan-out gives each worker only its own `stream_<name>/` directory, so
the launcher writes its records into each one:

```python
with record_step("discover", process_type="Other", run_experimenters=["Jane Doe"]) as step:
    write_stream_configs(
        items,
        results_dir=Path("/results"),
        schema_marker=MARKER,
        provenance=step.fanout_shards(),
    )
```

Each worker then infers `discover` as its parent. At assembly, `run_experimenters`
fills every step of the run that names none, and a
`pipeline={"name": ..., "code": {"url": ...}}` block fills `Processing.pipelines`.

### The other metadata files are forwarded or authored

`forward_metadata("/data", "/results")` copies `subject.json`, `procedures.json`,
`instrument.json`, and `acquisition.json` verbatim. The derived asset's
`data_description.json` describes a new asset, so it is authored instead, with
`metadata.make_derived_data_description` or from the fields
`read_data_description_fields` extracts from any schema version.

### Commit and version stamps

`capsule_commit()` reads `CO_COMMIT`, `GIT_COMMIT`, or `COMMIT_ID`, then falls
back to `git -C /code rev-parse HEAD`. `package_version(name)` wraps
`importlib.metadata.version`. Both return `None` rather than raise, so a manifest
can be stamped unconditionally.

## Structuring a pipeline capsule

### Launcher, workers, aggregator

Most parallel AIND capsules take three roles. The launcher writes one
`config.json` per item under `/results/stream_<name>/`, Code Ocean's Flatten hands
each directory to its own worker, and the aggregator merges the workers'
manifests. `Role` names these three, plus `MONOLITH` for a capsule that runs all
of them in one process.

```python
MARKER = "_mycapsule_stream_config"

# Launcher
write_stream_configs(items, results_dir=Path("/results"), schema_marker=MARKER)

# Worker: exactly one staged config anywhere under /data
cfg_path, cfg = find_stream_config(Path("/data"), schema_marker=MARKER)

# Aggregator
workers = find_worker_manifests(Path("/data"))
launcher = find_launcher_manifest(Path("/data"))
merged = merge_manifests(m for _, m in workers)  # {"built": [...], "skipped": [...]}
```

A worker finds its config by the marker key in the JSON, not by path, because
Flatten and Target Map Path nest inputs unpredictably. `find_stream_config` raises
`StreamConfigError`, listing the candidate `paths`, on zero or several matches.

### App Panel parameters

With `named_parameters: true`, the App Panel passes every parameter as a string,
so a boolean flag does not survive. `parse_truthy(args.flag)` reads one back:
`true`, `yes`, `y`, `t` (any case) and non-zero numbers are `True`; everything
else, including `"0"`, `"false"`, and `""`, is `False`.

## Surviving long runs

### Graceful shutdown

```python
with shutdown_handler():
    for shard in shards:
        check_shutdown()  # raises GracefulExit after SIGINT/SIGTERM
        process(shard)
```

A signal only sets a flag, so work stops at the next `check_shutdown()` rather
than mid-write, and the process exits with `128 + signum` (143 for SIGTERM).
`GracefulExit` inherits from `BaseException`, so `except Exception:` cannot
swallow it. A second signal exits at once through `os._exit`.

### Retries and atomic writes

```python
download = retry_on_oserror(_raw_download, retries=5)
atomic_json_write(out_path, download(url))
```

`retry_on_oserror` retries only `TRANSIENT_ERRNOS` (EIO, EAGAIN, EBUSY, network
errnos); pass `transient_errnos=` to widen it. A permanent error such as `ENOENT`
raises at once instead of hiding a config mistake behind minutes of backoff.
`atomic_json_write` and `atomic_write_text` write a temp file, fsync it, and
`os.replace` it over the destination, so no reader sees a half-written file.

### Input fingerprints

`input_fingerprint({"window": 0.01, "channels": [0, 1, 2]})` returns
`"sha256:…"`, equal for equal inputs in any key order. Store it beside cached
output and compare on resume. A non-JSON value raises `TypeError` naming its key
path, so coerce `Path` or `ndarray` at the call site.

### Context across thread pools

`ThreadPoolExecutor.submit` runs its function in an empty context, so a
`ContextVar` setting such as `scipy.fft.set_workers` silently lapses in the
worker. `submit_with_context(pool, fn, *args)` copies the caller's context for
each submit; one shared copy would raise `RuntimeError` once two threads ran it.

## Seeing what happened

### Logging under progress bars

A rich `Progress` repaints several times a second, painting over any log line
that bypassed rich, often the tail of a traceback. `install_rich_handler`
(`[rich]` extra) routes logging through rich and returns its `Console`; pass that
console to every `Progress`.

```python
from aind_code_ocean_pipeline_utils.log import build_progress, install_rich_handler, make_progress_callback

install_rich_handler()
with build_progress(len(items)) as (progress, overall, item):  # uses the installed console
    for it in items:
        progress.reset(item, total=it.size, description=it.name, visible=True)
        do_work(it, on_progress=make_progress_callback(progress, item))
        progress.advance(overall)
```

`attach_file_log(Path("/results/run.log"))` adds a file handler beside the
console one, so the log survives the run in `/results`. It needs no extra.

### Mounts and memory

```python
log_data_tree(Path("/data"))
reporter = start_memory_reporter()  # logs RSS against the cgroup limit every 15 s
...
reporter.stop()
```

`log_data_tree` follows Code Ocean's symlinked mounts to a bounded depth. An OOM
kill runs no `except` block and flushes no output, so the reporter's last line is
what tells an OOM apart from a spot reclamation afterwards.

## Development

```bash
# Set up environment
uv sync

# Run the full check suite
./scripts/run_linters_and_checks.sh -c

# Individual tools
uv run pytest
uv run ruff format
uv run ruff check
uv run mypy
```

See [`CLAUDE.md`](CLAUDE.md) for the design invariants each module is
required to preserve.

## License

MIT; see [LICENSE](LICENSE).
