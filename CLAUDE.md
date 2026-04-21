# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
uv sync

# Run all checks (formatting, linting, type checking, tests)
./scripts/run_linters_and_checks.sh -c

# Run tests
uv run pytest

# Run a single test file
uv run pytest tests/test_example.py

# Run a single test by name
uv run pytest -k "test_name"

# Formatting
uv run ruff format

# Linting
uv run ruff check
uv run ruff check --fix

# Type checking
uv run mypy

# Spell checking
uv run codespell --check-filenames

```

Always use `uv run` to execute commands, `uv add` to add dependencies, and `uv sync` to set up the environment. Never use bare `pip` or `python`.

## Architecture

This is a Python package using a `src/` layout. Source code lives in `src/aind_code_ocean_pipeline_utils/`, tests in `tests/`.

- Build system: hatchling
- Formatting/linting: ruff (line length 120, numpy docstring convention)
- Testing: pytest with coverage reporting
- Type checking: mypy (strict mode)
- Versioning: commitizen (semantic versioning via conventional commits)

### Package layout

Planned module structure (see `TODO.md` for full API sketches and rationale):

```
src/aind_code_ocean_pipeline_utils/
├── process.py          # graceful shutdown — SIGINT/SIGTERM → GracefulExit(BaseException)
├── io.py               # retry_on_oserror, atomic_json_write / atomic_write_text
├── cache.py            # input_fingerprint — SHA256 of canonical JSON
├── log.py              # rich+stdlib-logging compatibility (optional extra [rich])
└── threading_utils.py  # copy_context()-per-submit ThreadPoolExecutor wrapper
```

Module names deliberately avoid shadowing stdlib (`logging`, `concurrent`) so sibling modules can `import logging` / `from concurrent.futures import ...` without ambiguity.

**Core (`process`, `io`, `cache`) stays stdlib-only.** Optional modules gate heavy deps via named extras (e.g. `pip install aind-code-ocean-pipeline-utils[rich]`). Any new dependency outside those three modules must come with its own extra.

### Release plan

- **v0.1.0** — `process` + `io` + `cache` (stdlib-only)
- **v0.2.0** — add `log` with `[rich]` extra
- **v0.3.0** — add `threading_utils` (stdlib-only, no extra)

### Design invariants to preserve

These are load-bearing — every module has a subtle bug it exists to prevent. Don't relax them without understanding the original failure mode documented in `TODO.md`:

- `process.GracefulExit` inherits from `BaseException`, not `Exception`, so consumer `except Exception:` blocks don't swallow shutdown signals. Signal handlers flip a `threading.Event` and return — never call `sys.exit`, `logger.exception`, or other non-async-signal-safe code from inside the handler. Exit code = `128 + signum`. Second signal escalates via `os._exit`.
- `io.retry_on_oserror` uses a narrow `TRANSIENT_ERRNOS` frozenset (EIO, EAGAIN, EBUSY, network errnos). Permanent errors (ENOENT, EACCES, EINVAL) must raise immediately — retrying them hides config typos behind minutes of backoff. Use `os.replace` (not `os.rename`) for cross-platform atomicity. `fsync` defaults to True.
- `cache.input_fingerprint` requires JSON-serializable input and raises `TypeError` on anything else. Callers coerce `Path → str`, `ndarray → list`, etc. at the call site. Return value is prefixed `sha256:` so the algorithm can change without breaking string comparisons.
- `threading_utils.submit_with_context` calls `copy_context()` **per submit**, not once shared across submits. A shared Context raises `RuntimeError` when active on two threads concurrently (bit `ecephys-mipmap-zarr` at `prefetch_chunks >= 2`).
- `log.install_rich_handler` returns the `Console` object so the caller can pass the same one to `rich.progress.Progress(console=...)`. Not sharing it is what causes `logger.exception` inside `with Progress():` to lose its traceback tail.

### Target consumers

- `ecephys-mipmap-zarr` (AllenNeuralDynamics) — currently vendors all five patterns in `_engine.py` / `_fast_phase_shift.py`. Reference consumer for shaking out the v0.1 API.
- `pl-oversplitting-analysis-capsule` (ccg monorepo) — has none of the patterns; its `--merge-only` resume does a raw file-exists check with no fingerprint validation. Migration candidate after first bug report.
