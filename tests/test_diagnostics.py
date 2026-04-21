"""Tests for aind_code_ocean_pipeline_utils.diagnostics."""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from aind_code_ocean_pipeline_utils.diagnostics import (
    MemoryReporter,
    log_data_tree,
    start_memory_reporter,
)

_LOGGER_NAME = "aind_code_ocean_pipeline_utils.diagnostics"


# ------------------------------------------------------------ log_data_tree --


def test_log_data_tree_missing_root(tmp_path: Path, caplog: pytest.LogCaptureFixture):
    missing = tmp_path / "nope"
    with caplog.at_level(logging.INFO, logger=_LOGGER_NAME):
        log_data_tree(missing)
    records = [r.message for r in caplog.records]
    assert any("does not exist" in m for m in records)


def test_log_data_tree_emits_per_directory_line(tmp_path: Path, caplog: pytest.LogCaptureFixture):
    (tmp_path / "a" / "b").mkdir(parents=True)
    (tmp_path / "a" / "file.txt").write_text("x")
    with caplog.at_level(logging.INFO, logger=_LOGGER_NAME):
        log_data_tree(tmp_path)
    messages = [r.message for r in caplog.records]
    # Header line + at least one per-directory line
    assert any("data tree under" in m for m in messages)
    assert any("./" in m for m in messages)
    assert any("a/" in m for m in messages)


def test_log_data_tree_respects_max_depth(tmp_path: Path, caplog: pytest.LogCaptureFixture):
    (tmp_path / "l1" / "l2" / "l3" / "l4").mkdir(parents=True)
    with caplog.at_level(logging.INFO, logger=_LOGGER_NAME):
        log_data_tree(tmp_path, max_depth=2)
    messages = " ".join(r.message for r in caplog.records)
    # Depth 0 (".") and depth 1 ("l1") should be present; depth 3+ should be cut.
    assert "l1/l2" in messages
    assert "l1/l2/l3" not in messages


def test_log_data_tree_follows_symlinks(tmp_path: Path, caplog: pytest.LogCaptureFixture):
    real = tmp_path / "real"
    (real / "inside").mkdir(parents=True)
    link = tmp_path / "data"
    link.symlink_to(real, target_is_directory=True)
    with caplog.at_level(logging.INFO, logger=_LOGGER_NAME):
        log_data_tree(link)
    messages = [r.message for r in caplog.records]
    # The symlinked child must appear.
    assert any("inside" in m for m in messages)


def test_log_data_tree_uses_explicit_logger(tmp_path: Path):
    (tmp_path / "child").mkdir()
    custom = logging.getLogger("test_log_data_tree_custom")
    records: list[logging.LogRecord] = []

    class _Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    handler = _Capture(level=logging.INFO)
    custom.addHandler(handler)
    custom.setLevel(logging.INFO)
    try:
        log_data_tree(tmp_path, logger=custom)
    finally:
        custom.removeHandler(handler)
    assert records, "custom logger did not receive the tree output"


# ---------------------------------------------------- start_memory_reporter --


def _status_file(tmp_path: Path, rss_kb: int) -> Path:
    path = tmp_path / "status"
    path.write_text(f"Name:\tpy\nVmRSS:\t{rss_kb} kB\nVmSize:\t999999 kB\n")
    return path


def _limit_file(tmp_path: Path, raw: str) -> Path:
    path = tmp_path / "memory.max"
    path.write_text(raw)
    return path


def test_memory_reporter_logs_tick_before_stop(tmp_path: Path, caplog: pytest.LogCaptureFixture):
    status = _status_file(tmp_path, rss_kb=1_048_576)  # ~1 GiB
    limit = _limit_file(tmp_path, str(4 * 1024**3))  # 4 GiB

    with caplog.at_level(logging.INFO, logger=_LOGGER_NAME):
        reporter = start_memory_reporter(
            interval_s=60.0,  # long; rely on the pre-wait first tick
            status_path=str(status),
            cgroup_limit_paths=(str(limit),),
        )
        # Race-tolerant: join via stop(); the first tick always fires
        # before the wait because we log-then-wait inside the loop.
        reporter.stop(timeout=5.0)

    messages = [r.message for r in caplog.records]
    assert any("cgroup limit 4.3 GB" in m for m in messages)
    assert any("VmRSS" in m and "% of cgroup" in m for m in messages)


def test_memory_reporter_without_cgroup_limit(tmp_path: Path, caplog: pytest.LogCaptureFixture):
    status = _status_file(tmp_path, rss_kb=524_288)

    with caplog.at_level(logging.INFO, logger=_LOGGER_NAME):
        reporter = start_memory_reporter(
            interval_s=60.0,
            status_path=str(status),
            cgroup_limit_paths=(str(tmp_path / "nope"),),
        )
        reporter.stop(timeout=5.0)

    messages = [r.message for r in caplog.records]
    assert any("VmRSS" in m for m in messages)
    assert not any("cgroup limit" in m for m in messages)
    # Percentage is omitted when no limit known.
    assert not any("% of cgroup" in m for m in messages)


def test_memory_reporter_ignores_max_sentinel(tmp_path: Path, caplog: pytest.LogCaptureFixture):
    status = _status_file(tmp_path, rss_kb=1024)
    limit = _limit_file(tmp_path, "max")

    with caplog.at_level(logging.INFO, logger=_LOGGER_NAME):
        reporter = start_memory_reporter(
            interval_s=60.0,
            status_path=str(status),
            cgroup_limit_paths=(str(limit),),
        )
        reporter.stop(timeout=5.0)

    messages = [r.message for r in caplog.records]
    assert not any("cgroup limit" in m for m in messages)


def test_memory_reporter_skips_when_status_unreadable(tmp_path: Path, caplog: pytest.LogCaptureFixture):
    with caplog.at_level(logging.INFO, logger=_LOGGER_NAME):
        reporter = start_memory_reporter(
            interval_s=60.0,
            status_path=str(tmp_path / "nope"),
            cgroup_limit_paths=(str(tmp_path / "also_nope"),),
        )
        reporter.stop(timeout=5.0)
    # Nothing about VmRSS — it silently degraded.
    messages = [r.message for r in caplog.records]
    assert not any("VmRSS" in m for m in messages)


def test_memory_reporter_stop_joins_thread(tmp_path: Path):
    status = _status_file(tmp_path, rss_kb=1024)
    reporter = start_memory_reporter(
        interval_s=60.0,
        status_path=str(status),
        cgroup_limit_paths=(),
    )
    assert isinstance(reporter, MemoryReporter)
    reporter.stop(timeout=5.0)
    assert not reporter.is_alive


def test_memory_reporter_stop_is_responsive_during_wait(
    tmp_path: Path,
):
    """stop() should return quickly even with a large interval_s."""
    import time

    status = _status_file(tmp_path, rss_kb=1024)
    reporter = start_memory_reporter(
        interval_s=3600.0,  # one hour
        status_path=str(status),
        cgroup_limit_paths=(),
    )
    t0 = time.monotonic()
    reporter.stop(timeout=5.0)
    elapsed = time.monotonic() - t0
    assert elapsed < 1.0, f"stop() took {elapsed:.2f}s — Event.wait not being interrupted"
    assert not reporter.is_alive
