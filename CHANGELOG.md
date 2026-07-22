# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


### Feat

- re-export cli + provenance from top level; extend README
- **log**: add build_progress and make_progress_callback
- **provenance**: add capsule_commit and package_version
- **cli**: add parse_truthy for CO app-panel parameters
- re-export role_dispatch + diagnostics from top level; extend README
- **diagnostics**: add data-tree and memory reporters
- **role_dispatch**: add launcher/worker/aggregator skeleton
- re-export core API from top-level package
- **threading_utils**: add contextvars-preserving submit wrapper
- **log**: add rich-aware logging under [rich] extra
- **cache**: add input_fingerprint for cache keys
- **io**: add retry_on_oserror and atomic writes
- **process**: add graceful shutdown module

## v0.4.2 (2026-07-22)

### Fix

- publish to pypi

## v0.4.1 (2026-06-04)

### Perf

- **step**: lazy-import aind-data-schema so the metadata tax is near-zero

## v0.4.0 (2026-06-03)

### Feat

- **step**: add capsule_step decorator for frictionless processing.json

## v0.3.0 (2026-06-03)

### Feat

- add optional metadata module for processing.json DAG assembly

## v0.2.0 (2026-04-23)

### Feat

- add file handler

## v0.1.0 (2026-04-21)

### Feat

- re-export cli + provenance from top level; extend README
- **log**: add build_progress and make_progress_callback
- **provenance**: add capsule_commit and package_version
- **cli**: add parse_truthy for CO app-panel parameters
- re-export role_dispatch + diagnostics from top level; extend README
- **diagnostics**: add data-tree and memory reporters
- **role_dispatch**: add launcher/worker/aggregator skeleton
- re-export core API from top-level package
- **threading_utils**: add contextvars-preserving submit wrapper
- **log**: add rich-aware logging under [rich] extra
- **cache**: add input_fingerprint for cache keys
- **io**: add retry_on_oserror and atomic writes
- **process**: add graceful shutdown module
