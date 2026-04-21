# AIND Code Ocean pipeline utils

![CI](https://github.com/AllenNeuralDynamics/aind-code-ocean-pipeline-utils/actions/workflows/ci-call.yml/badge.svg)
[![PyPI - Version](https://img.shields.io/pypi/v/aind-code-ocean-pipeline-utils)](https://pypi.org/project/aind-code-ocean-pipeline-utils/)
[![semantic-release: angular](https://img.shields.io/badge/semantic--release-angular-e10079?logo=semantic-release)](https://github.com/semantic-release/semantic-release)
[![License](https://img.shields.io/badge/license-MIT-brightgreen)](LICENSE)
[![ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![Copier](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/copier-org/copier/master/img/badge/badge-grayscale-inverted-border.json)](https://github.com/copier-org/copier)

Small, focused utilities for long-running AIND Code Ocean capsules. Each
module addresses a single failure mode that recurs in pipeline code:
sudden termination, flaky I/O, stale cache reuse, logging eaten by rich
progress bars, and context variables lost across thread-pool submits.

Core modules (`process`, `io`, `cache`, `threading_utils`) have **no
runtime dependencies** beyond the standard library. The optional
[`log`](#log--rich-aware-logging-optional-rich-extra) module requires
the `[rich]` extra.

## Installation

```bash
pip install aind-code-ocean-pipeline-utils
# with rich-aware logging
pip install aind-code-ocean-pipeline-utils[rich]
```

## Modules at a glance

| Module            | Purpose                                                          | Deps        |
|-------------------|------------------------------------------------------------------|-------------|
| `process`         | Graceful SIGINT/SIGTERM handling with safe-point shutdown        | stdlib      |
| `io`              | Retry on transient OS errors; atomic file writes                 | stdlib      |
| `cache`           | Deterministic fingerprints for cache keys and resume validation  | stdlib      |
| `threading_utils` | ThreadPoolExecutor submit that propagates `contextvars`          | stdlib      |
| `role_dispatch`   | Launcher / worker / aggregator skeleton for CO pipeline capsules | stdlib      |
| `diagnostics`     | `/data` tree + RSS/cgroup reporting for post-mortem debugging    | stdlib      |
| `provenance`      | `capsule_commit()` + `package_version()` for manifest stamping   | stdlib      |
| `cli`             | `parse_truthy()` for CO app-panel string parameters              | stdlib      |
| `log`             | Rich logging + `build_progress` / `make_progress_callback`       | `[rich]`    |

Core primitives are re-exported at the package level:

```python
from aind_code_ocean_pipeline_utils import (
    GracefulExit, check_shutdown, shutdown_handler,
    retry_on_oserror, atomic_json_write, atomic_write_text,
    input_fingerprint, canonical_params,
    submit_with_context,
    Role, StreamConfigError,
    write_stream_configs, find_stream_config,
    find_worker_manifests, find_launcher_manifest, merge_manifests,
    log_data_tree, start_memory_reporter,
    capsule_commit, package_version,
    parse_truthy,
)
```

`log` must be imported from its submodule (keeps the top-level import
stdlib-only):

```python
from aind_code_ocean_pipeline_utils.log import install_rich_handler
```

## `process` — graceful shutdown

Main loops poll `check_shutdown()` at safe points (shard boundaries,
between I/O operations) rather than aborting mid-kernel. The context
manager translates a shutdown signal into `sys.exit(128 + signum)`,
matching the Unix killed-by-signal convention (130 for SIGINT, 143 for
SIGTERM).

```python
from aind_code_ocean_pipeline_utils import check_shutdown, shutdown_handler

with shutdown_handler():
    for shard in shards:
        check_shutdown()   # raises GracefulExit on SIGINT/SIGTERM
        process(shard)
```

`GracefulExit` inherits from `BaseException` — consumer code's broad
`except Exception:` blocks cannot accidentally swallow it. A second
signal of the same kind escalates via `os._exit` so a stuck cleanup
path cannot block termination indefinitely.

## `io` — retry and atomic writes

```python
from aind_code_ocean_pipeline_utils import (
    retry_on_oserror, atomic_json_write, atomic_write_text, TRANSIENT_ERRNOS,
)

download = retry_on_oserror(_raw_download, retries=5)
payload = download(url)

atomic_json_write(out_path, payload)

with atomic_write_text(log_path) as f:
    f.write("...")
```

`retry_on_oserror` uses a deliberately narrow `TRANSIENT_ERRNOS` set
(EIO, EAGAIN, EBUSY, network errnos). Permanent errors like `ENOENT`
or `EACCES` surface immediately rather than hiding config mistakes
behind minutes of exponential backoff. Callers with different failure
models can union in additional codes at the call site.

`atomic_write_text` writes to a sibling temp file, fsyncs, and uses
`os.replace` for cross-platform atomic rename. The destination is
never left half-written.

## `cache` — input fingerprints

```python
from aind_code_ocean_pipeline_utils import input_fingerprint

fp = input_fingerprint({"window": 0.01, "channels": [0, 1, 2]})
# -> "sha256:3f1c..."
```

Equal inputs — regardless of key insertion order — produce equal
fingerprints. The `sha256:` prefix leaves room to change the algorithm
later without breaking consumers that string-compare fingerprints.

Non-JSON values raise `TypeError` with a message identifying the
offending key path. Callers coerce at the call site (`Path → str`,
`ndarray → list` with a size ceiling) to keep fingerprints
reproducible across Python versions.

## `threading_utils` — contextvar propagation

`ThreadPoolExecutor.submit(fn, ...)` runs `fn` on a worker with an
*empty* context: any `ContextVar`-backed setting (`scipy.fft.set_workers`,
`numpy.errstate`, custom request-ID / feature-flag vars) silently
no-ops in the worker. `submit_with_context` copies the caller's
context per submit so worker settings match the caller.

```python
from concurrent.futures import ThreadPoolExecutor
from aind_code_ocean_pipeline_utils import submit_with_context

with ThreadPoolExecutor() as pool:
    future = submit_with_context(pool, worker, arg1, arg2)
```

The copy is per submit, not once and reused — `Context.run` raises
`RuntimeError` if the same `Context` is active on two threads
concurrently.

## `role_dispatch` — launcher / worker / aggregator skeleton

Most embarrassingly-parallel AIND processing capsules follow the same
three-role shape: the launcher discovers items and writes one
`config.json` per item under `/results/stream_<safe>/`; CO's Flatten
fan-out stages each directory as a distinct worker input; the
aggregator Collects and merges per-worker manifests.

```python
from aind_code_ocean_pipeline_utils import (
    Role, StreamConfigError,
    write_stream_configs, find_stream_config,
    find_worker_manifests, find_launcher_manifest, merge_manifests,
)

MARKER = "_mycapsule_stream_config"

# Launcher
write_stream_configs(
    items, results_dir=Path("/results"), schema_marker=MARKER,
)

# Worker — finds exactly one staged config anywhere under /data
cfg_path, cfg = find_stream_config(Path("/data"), schema_marker=MARKER)

# Aggregator
workers = find_worker_manifests(Path("/data"))
launcher = find_launcher_manifest(Path("/data"))
merged = merge_manifests(m for _, m in workers)  # {"built": [...], "skipped": [...]}
```

Workers detect their config by a marker key in the JSON body, never by
path shape — CO's Flatten + Target Map Path combinations produce
unpredictable nesting. `find_stream_config` raises `StreamConfigError`
(with `paths` attribute) on zero or ambiguous matches; both are
terminal for the worker.

## `diagnostics` — first-log-line mount and memory reporting

```python
from aind_code_ocean_pipeline_utils import log_data_tree, start_memory_reporter

log_data_tree(Path("/data"))           # mount shape visible in log on startup
reporter = start_memory_reporter()     # daemon thread, logs RSS + cgroup limit
# ... worker runs ...
reporter.stop()
```

`log_data_tree` uses `os.walk(followlinks=True)` so CO's staged symlink
chains get traversed; depth is bounded so zarr chunk trees don't flood
the log. `start_memory_reporter` logs peak approach-to-limit, which is
the only signal that survives an OOM SIGKILL (no `except` block runs;
stdout isn't flushed) — enough to distinguish OOM from spot reclamation
from application errors in postmortems.

## `provenance` — manifest stamping

```python
from aind_code_ocean_pipeline_utils import capsule_commit, package_version

manifest = {
    "capsule_commit": capsule_commit(),      # env var, then `git rev-parse HEAD`
    "package_version": package_version("my-package"),
    # ... pipeline output ...
}
```

`capsule_commit` checks `CO_COMMIT` / `GIT_COMMIT` / `COMMIT_ID` env
vars in order, then falls back to `git -C /code rev-parse HEAD`.
Returns the full 40-character hash or `None` — never raises, so
manifest-emit paths can stamp unconditionally. `package_version` is a
thin wrapper over `importlib.metadata.version` that returns `None` on
`PackageNotFoundError`.

## `cli` — app-panel parameter parsing

```python
from aind_code_ocean_pipeline_utils import parse_truthy

disable_fast_filter = parse_truthy(args.disable_fast_filter)
```

Code Ocean's app panel passes parameters as strings when
`named_parameters: true`, so bool flags (argparse `store_true`, tyro
`--flag`/`--no-flag`) don't round-trip. `parse_truthy` accepts
`{"true","yes","y","t"}` (case-insensitive) and any numeric string
whose value is non-zero (`"1"`, `"42"`, `"3.14"`). Everything else
— including `"0"`, `"0.0"`, `"false"`, and the empty string — is `False`.

## `log` — rich-aware logging (optional `[rich]` extra)

`rich.progress.Progress` repaints its live area 2–10 times per second.
If `logging` emits a record from inside a `with Progress():` block
*without* going through rich, the next tick paints over the tail of
the log output — the line that tells you *what* went wrong silently
disappears. Sharing a single `Console` between the `RichHandler` and
`Progress` serializes the two.

```python
from aind_code_ocean_pipeline_utils.log import install_rich_handler
from rich.progress import Progress

console = install_rich_handler()
with Progress(console=console) as progress:   # same console!
    ...
```

Pass the returned `Console` to any `Progress` / `Live` instance in the
process. A separate `Console` reintroduces the bug.

### Two-row progress helper

`build_progress` sets up the common pattern of an overall-item counter
plus a per-item progress bar, reusing the `Console` installed above so
log output and progress ticks don't fight:

```python
from aind_code_ocean_pipeline_utils.log import (
    build_progress, install_rich_handler, make_progress_callback,
)

install_rich_handler()   # must come first

with build_progress(len(items)) as (progress, overall, item):
    for it in items:
        progress.reset(item, total=it.size, description=it.name, visible=True)
        cb = make_progress_callback(progress, item)
        do_work(it, on_progress=cb)
        progress.advance(overall)
```

`build_progress` raises `RuntimeError` if `install_rich_handler` hasn't
been called (unless you pass `console=` explicitly) — keeps the
shared-`Console` invariant honest.

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

MIT — see [LICENSE](LICENSE).
