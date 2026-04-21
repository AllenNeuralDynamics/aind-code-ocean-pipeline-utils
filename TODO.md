# TODO

## Status

Initial module set is implemented. API sketches and per-module rationale have moved into module docstrings; design invariants live in [`CLAUDE.md`](CLAUDE.md).

```
src/aind_code_ocean_pipeline_utils/
├── process.py          # graceful shutdown (stdlib)
├── io.py               # retry_on_oserror, atomic writes (stdlib)
├── cache.py            # input_fingerprint (stdlib)
├── threading_utils.py  # copy_context()-per-submit wrapper (stdlib)
└── log.py              # rich+logger compatibility ([rich] extra)
```

No tag or version bump yet — holding release until a real consumer exists (see release-cadence guidance in Claude memory).

## Next up

- **Reference-consumer migration: `ecephys-mipmap-builder-capsule`** — swap its vendored copies of these patterns for imports from this package. First actual usage, expected to shake out API friction before wider adoption. Path: `/home/galen.lynch/Documents/Code/ecephys-mipmap-builder-capsule`.
- **`pl-oversplitting-analysis-capsule`** (ccg monorepo): currently has none of these patterns; `--merge-only` resume relies on a bare file-exists check with no fingerprint validation, so stale outputs can be silently reused when upstream inputs change. Migration candidate on the first bug report that motivates it.

## Resolved design questions

- `GracefulExit` exposes both `signum` (numeric, portable) and `signame` (string, log-friendly).
- Double-signal escalation: second signal hard-exits via `os._exit(128 + signum)`. No grace-period knob added; revisit if a consumer reports a legitimate need.
- This package is the canonical home for these patterns — no pre-existing AIND utilities package to fold into.
