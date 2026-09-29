"""Tests for aind_code_ocean_pipeline_utils.io."""

from __future__ import annotations

import errno
import json
import logging
import os
from pathlib import Path
from unittest.mock import patch

import pytest

from aind_code_ocean_pipeline_utils.io import (
    TRANSIENT_ERRNOS,
    atomic_json_write,
    atomic_write_text,
    retry_on_oserror,
)

# ---------------------------------------------------------------- retry --


def test_transient_errnos_includes_expected():
    assert errno.EIO in TRANSIENT_ERRNOS
    assert errno.EAGAIN in TRANSIENT_ERRNOS
    assert errno.ECONNRESET in TRANSIENT_ERRNOS


def test_transient_errnos_excludes_permanent():
    # Retrying these would hide config typos.
    assert errno.ENOENT not in TRANSIENT_ERRNOS
    assert errno.EACCES not in TRANSIENT_ERRNOS
    assert errno.EINVAL not in TRANSIENT_ERRNOS
    assert errno.EISDIR not in TRANSIENT_ERRNOS


def test_retry_returns_value_on_first_success():
    def fn(x: int) -> int:
        return x + 1

    assert retry_on_oserror(fn)(41) == 42


def test_retry_recovers_after_transient_failures():
    calls = {"n": 0}

    def flaky() -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            raise OSError(errno.EIO, "transient")
        return "ok"

    with patch("time.sleep") as sleep_mock:
        result = retry_on_oserror(flaky, retries=5, initial_delay=0.1)()
    assert result == "ok"
    assert calls["n"] == 3
    # Two retries before success → two sleeps at 0.1, 0.2
    assert [c.args[0] for c in sleep_mock.call_args_list] == [0.1, 0.2]


def test_retry_raises_immediately_on_permanent_errno():
    calls = {"n": 0}

    def fn() -> None:
        calls["n"] += 1
        raise OSError(errno.ENOENT, "missing")

    with patch("time.sleep") as sleep_mock, pytest.raises(OSError) as excinfo:
        retry_on_oserror(fn, retries=5)()
    assert excinfo.value.errno == errno.ENOENT
    assert calls["n"] == 1
    sleep_mock.assert_not_called()


def test_retry_exhaustion_raises_last_error():
    def fn() -> None:
        raise OSError(errno.EIO, "always broken")

    with patch("time.sleep"), pytest.raises(OSError) as excinfo:
        retry_on_oserror(fn, retries=2, initial_delay=0.01)()
    assert excinfo.value.errno == errno.EIO


def test_retry_call_count_is_retries_plus_one():
    calls = {"n": 0}

    def fn() -> None:
        calls["n"] += 1
        raise OSError(errno.EIO, "fail")

    with patch("time.sleep"), pytest.raises(OSError):
        retry_on_oserror(fn, retries=3, initial_delay=0.01)()
    assert calls["n"] == 4  # initial + 3 retries


def test_retry_logs_warning_on_each_retry(caplog: pytest.LogCaptureFixture):
    def fn() -> None:
        raise OSError(errno.EIO, "broken")

    with (
        patch("time.sleep"),
        caplog.at_level(logging.WARNING, logger="aind_code_ocean_pipeline_utils.io"),
        pytest.raises(OSError),
    ):
        retry_on_oserror(fn, retries=2, initial_delay=0.01)()
    retry_records = [r for r in caplog.records if "retry_on_oserror" in r.message]
    assert len(retry_records) == 2
    for rec in retry_records:
        assert rec.levelno == logging.WARNING
        assert "errno" in rec.message


def test_retry_honors_custom_transient_errnos():
    calls = {"n": 0}

    def fn() -> None:
        calls["n"] += 1
        raise OSError(errno.EACCES, "permission")

    custom = TRANSIENT_ERRNOS | {errno.EACCES}
    with patch("time.sleep"), pytest.raises(OSError):
        retry_on_oserror(fn, retries=1, initial_delay=0.01, transient_errnos=custom)()
    assert calls["n"] == 2


def test_retry_preserves_function_metadata():
    def some_op(x: int) -> int:
        """Docstring."""
        return x

    wrapped = retry_on_oserror(some_op)
    assert wrapped.__name__ == "some_op"
    assert wrapped.__doc__ == "Docstring."


# ------------------------------------------------------ atomic_write_text --


def test_atomic_write_text_commits_on_success(tmp_path: Path):
    dest = tmp_path / "out.txt"
    with atomic_write_text(dest) as f:
        f.write("hello\n")
    assert dest.read_text() == "hello\n"


def test_atomic_write_text_no_partial_on_exception(tmp_path: Path):
    dest = tmp_path / "out.txt"
    with pytest.raises(RuntimeError), atomic_write_text(dest) as f:
        f.write("partial")
        raise RuntimeError("boom")
    assert not dest.exists()
    # And no .tmp sibling left behind
    assert list(tmp_path.iterdir()) == []


def test_atomic_write_text_overwrites_existing(tmp_path: Path):
    dest = tmp_path / "out.txt"
    dest.write_text("old")
    with atomic_write_text(dest) as f:
        f.write("new")
    assert dest.read_text() == "new"


def test_atomic_write_text_calls_fsync_by_default(tmp_path: Path):
    dest = tmp_path / "out.txt"
    with patch("os.fsync") as fsync_mock, atomic_write_text(dest) as f:
        f.write("x")
    assert fsync_mock.called


def test_atomic_write_text_skips_fsync_when_disabled(tmp_path: Path):
    dest = tmp_path / "out.txt"
    with patch("os.fsync") as fsync_mock, atomic_write_text(dest, fsync=False) as f:
        f.write("x")
    fsync_mock.assert_not_called()


def test_atomic_write_text_uses_os_replace(tmp_path: Path):
    dest = tmp_path / "out.txt"
    with patch("os.replace", wraps=os.replace) as replace_mock, atomic_write_text(dest) as f:
        f.write("x")
    assert replace_mock.called


def test_atomic_write_text_accepts_string_path(tmp_path: Path):
    dest = tmp_path / "out.txt"
    with atomic_write_text(str(dest)) as f:
        f.write("x")
    assert dest.read_text() == "x"


# ------------------------------------------------------ atomic_json_write --


def test_atomic_json_write_roundtrip(tmp_path: Path):
    dest = tmp_path / "data.json"
    atomic_json_write(dest, {"b": 2, "a": 1})
    loaded = json.loads(dest.read_text())
    assert loaded == {"a": 1, "b": 2}


def test_atomic_json_write_sort_keys_default(tmp_path: Path):
    dest = tmp_path / "data.json"
    atomic_json_write(dest, {"b": 2, "a": 1})
    raw = dest.read_text()
    assert raw.index('"a"') < raw.index('"b"')


def test_atomic_json_write_no_partial_on_serialization_error(tmp_path: Path):
    dest = tmp_path / "data.json"
    with pytest.raises(TypeError):
        atomic_json_write(dest, {"bad": object()})
    assert not dest.exists()
    assert list(tmp_path.iterdir()) == []


def test_atomic_json_write_compact_indent(tmp_path: Path):
    dest = tmp_path / "data.json"
    atomic_json_write(dest, {"a": 1}, indent=None)
    assert dest.read_text() == '{"a": 1}'
