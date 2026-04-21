# TODO

## Initial module layout

Patterns identified from surveying two AIND Code Ocean consumers (`ecephys-mipmap-zarr`, `pl-oversplitting-analysis-capsule`). v0.1 ships `process` + `io` + `cache`; `log` and `threading_utils` land in later releases (see Release plan below).

```
src/aind_code_ocean_pipeline_utils/
├── process.py          # graceful shutdown (core)
├── io.py               # retry_on_oserror, atomic_json_write (core)
├── cache.py            # input_fingerprint (core)
├── log.py              # rich+logger compatibility (optional extra [rich])
└── threading_utils.py  # copy_context()-per-submit wrapper (stdlib-only)
```

Module names avoid shadowing stdlib (`logging`, `concurrent`) so sibling modules can `import logging` / `from concurrent.futures import ...` without ambiguity. Core stays stdlib-only; optional modules gate their heavy deps via named extras (`pip install aind-code-ocean-pipeline-utils[rich]`).

## Target consumers (known as of initial scoping, 2026-04-20)

- [`ecephys-mipmap-zarr`](https://github.com/AllenNeuralDynamics/ecephys-mipmap-zarr): currently vendors all five core patterns in `_engine.py` / `_fast_phase_shift.py`. Would migrate imports once utils is published.
- `pl-oversplitting-analysis-capsule` (ccg monorepo): has **none** of the patterns. `--merge-only` resume uses raw file-exists check with no fingerprint validation — would silently reuse stale outputs if upstream inputs changed. Would migrate on first bug report.

Absence in `pl-oversplitting` is the motivating data point: these are the "pipeline capsules keep reinventing the same three things, badly" patterns.

---

## Module: `process` — graceful shutdown

### API sketch

```python
# signals are flipped into a threading.Event; main loop polls at safe points
install_shutdown_handlers() -> None
    # registers SIGINT and SIGTERM; flag-only handler (async-signal-safe)
    # idempotent

is_shutdown_requested() -> bool
    # main loop calls this at safe points

check_shutdown() -> None
    # raises GracefulExit if a signal has arrived; no-op otherwise

class GracefulExit(BaseException):
    signum: int
    # BaseException (not Exception) so library code's `except Exception:` doesn't swallow it

@contextmanager
def shutdown_handler(exit_code_base: int = 128) -> Generator[None, None, None]:
    # one-stop context manager: installs handlers on enter, catches GracefulExit on exit,
    # flushes via an optional user callback, calls sys.exit(exit_code_base + signum)
    # -> 130 for SIGINT, 143 for SIGTERM
```

### Non-obvious design points to preserve

1. **Handler flips a `threading.Event` and returns.** Never call `sys.exit` / `logger.exception` / anything nontrivial from inside the signal handler — Python signal handlers run at arbitrary instruction boundaries and most stdlib code is not async-signal-safe.
2. **`GracefulExit` extends `BaseException`**, not `Exception`. Consumer code's broad `except Exception:` handlers shouldn't swallow it. Mirrors how `KeyboardInterrupt` / `SystemExit` are handled by the stdlib.
3. **Main loop checks the flag at shard boundaries**, not inside hot compute. This lets an in-flight numba kernel or scipy filter finish before teardown — abort-on-signal-mid-FFT would corrupt partial writes.
4. **Exit code = 128 + signum.** Matches "killed by that signal" Unix convention so wrapper scripts / CI see the expected status.
5. **Second signal escalates.** Double Ctrl+C should hard-exit (handler sets a second flag; if already set, `os._exit(128 + signum)` bypasses cleanup). Protects against a stuck FUSE read blocking the graceful path indefinitely.

### LOC estimate

~40 core + ~60 tests.

---

## Module: `io` — retry + atomic-write

### API sketch

```python
retry_on_oserror(
    fn: Callable[..., T],
    *,
    retries: int = 5,
    initial_delay: float = 1.0,
    transient_errnos: frozenset[int] = TRANSIENT_ERRNOS,
) -> Callable[..., T]
    # wrap fn; retry on OSError whose errno is in transient_errnos,
    # exponential backoff 2**attempt * initial_delay.
    # Permanent errors (ENOENT, EACCES, EINVAL) raise immediately.

TRANSIENT_ERRNOS: frozenset[int] = frozenset({
    errno.EIO, errno.EAGAIN, errno.EBUSY,
    errno.ENETDOWN, errno.ENETUNREACH,
    errno.ECONNRESET, errno.ETIMEDOUT, errno.EHOSTUNREACH,
})

atomic_json_write(path: Path, data: Any, *, fsync: bool = True) -> None
    # write to path.with_suffix('.tmp'), fsync, rename to path.
    # os.replace is atomic on POSIX and Windows.
    # Corrupt half-written files therefore cannot exist at `path`.

@contextmanager
def atomic_write_text(path: Path, *, fsync: bool = True) -> Generator[TextIO, ...]:
    # generalization: yield a file handle writing to tmp; commit on clean exit.
```

### Non-obvious design points

1. **Narrow `transient_errnos`.** Default set excludes permanent errors (`ENOENT`, `EACCES`, `EINVAL`, `EISDIR`, etc.). Naive "retry on OSError" hides config typos behind 31s of exponential backoff before surfacing.
2. **Expose `TRANSIENT_ERRNOS` as a public frozenset.** Callers with different failure models (e.g. a rate-limited HTTP endpoint) can union in additional codes or swap the set entirely.
3. **`fsync` default True.** On spot instances, fsync is the only thing that gives durability guarantees before rename. Off-by-default would be a silent correctness regression.
4. **`os.replace`, not `os.rename`.** On Windows, `rename` fails if destination exists; `replace` is atomic cross-platform. Rediscovered every time.
5. **Log-on-retry.** Emit a structured `WARNING` log line on each retry attempt with `t0`, `t1`, errno, and delay so operators can see transient vs persistent failures in retrospect.

### LOC estimate

~70 core + ~90 tests.

---

## Module: `cache` — input fingerprint

### API sketch

```python
input_fingerprint(params: Mapping[str, Any]) -> str
    # SHA256 of canonical JSON(params). Sorted keys, UTF-8,
    # no whitespace. Returns "sha256:<64 hex chars>".

canonical_params(params: Mapping[str, Any]) -> str
    # lower-level: return the canonical JSON string (useful for logging / debug).
```

### Non-obvious design points

1. **Strict JSON-serializable inputs.** Any non-JSON value (numpy arrays, Path, custom classes) raises `TypeError` with a helpful message pointing at the offending key. Consumers are responsible for coercing (`Path → str`, `ndarray → list` with a size ceiling) before passing — this keeps the fingerprint well-defined and reproducible across Python versions.
2. **`sorted_keys=True` is not enough.** Nested dicts need recursive sorting; use `json.dumps(..., sort_keys=True, default=None)` and crash on non-JSON-encodable values.
3. **No salting / secret suffix.** Fingerprints are for cache keys and resume validation, not security. Plain SHA256 is sufficient.
4. **Return the `sha256:` prefix.** Makes logs obvious and leaves room to change the algorithm later without breaking consumers that string-compare fingerprints.

### LOC estimate

~15 core + ~30 tests.

---

## Module: `log` (optional, `[rich]` extra)

### API sketch

```python
install_rich_handler(
    logger: logging.Logger | None = None,
    level: int = logging.INFO,
    console: rich.console.Console | None = None,
) -> rich.console.Console
    # Configure a RichHandler on the root logger (or the one passed).
    # If a Console is provided, use it; else create one and return it.
    # The same Console should then be passed to any Progress / Live construct
    # in the same process — this is the trap that makes `logger.exception`
    # inside `with Progress():` lose its traceback tail.
```

### Non-obvious design points

1. **Return the `Console` object** even when the caller passed None — so subsequent `rich.progress.Progress(console=...)` calls can share it.
2. **`show_path=False`** by default. Module paths in CLI logs are noise; full tracebacks still show source lines via `rich_tracebacks=True`.
3. **Document the symptom.** Module docstring should spell out: "if `logger.exception(...)` inside `with Progress():` loses its final line, you didn't pass `console=` to Progress."

### LOC estimate

~10 core + ~20 tests.

---

## Module: `threading_utils` (stdlib-only, no extra)

### API sketch

```python
submit_with_context(
    pool: concurrent.futures.ThreadPoolExecutor,
    fn: Callable[..., T],
    /,
    *args,
    **kwargs,
) -> Future[T]
    # Equivalent to pool.submit(fn, *args, **kwargs) but copy_context() per submit
    # so each worker runs fn inside a fresh copy of the caller's contextvars.
```

### Non-obvious design points

1. **Copy per submit, not once.** `Context.run()` raises `RuntimeError` if the same Context is active on two threads concurrently. Bit us in `ecephys-mipmap-zarr` at `prefetch_chunks >= 2` (commit `f307c05`). A single shared Context object is a subtle bug; per-submit is correct.
2. **Default-use recommendation.** Document that `ThreadPoolExecutor.submit(fn, ...)` is subtly wrong whenever `fn` depends on `scipy.fft.set_workers`, `numpy.errstate`, or any other contextvar. Without this wrapper, the thread starts with an empty context and the caller's settings silently no-op in the worker.

### LOC estimate

~10 core + ~30 tests.

---

## Release plan

- **v0.1.0** — `process` + `io` + `cache` (core only, stdlib-only deps).
- **v0.2.0** — add `log` with `[rich]` extra.
- **v0.3.0** — add `threading_utils` (stdlib-only, no extra needed).

Migrate `ecephys-mipmap-zarr` as the reference consumer after v0.1; use its experience to shake out API issues before wider adoption.

## Open questions

- Is there an existing AIND utilities package I should fold into vs. standing up alongside? (Confirmed this one is the intended canonical home.)
- Should `process.GracefulExit` carry the signal name (`"SIGINT"`) or the numeric signum? Numeric is more portable; name is friendlier in logs. Probably expose both (`exc.signum` + `exc.signame`).
- Double-signal escalation: second Ctrl+C → `os._exit(130)` vs a grace period before hard-killing. Leaning `os._exit` on double-signal; add a config knob if anyone complains.
