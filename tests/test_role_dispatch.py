"""Tests for aind_code_ocean_pipeline_utils.role_dispatch."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from aind_code_ocean_pipeline_utils.role_dispatch import (
    Role,
    StreamConfigError,
    default_sanitize,
    find_launcher_manifest,
    find_stream_config,
    find_worker_manifests,
    merge_manifests,
    write_stream_configs,
)

SCHEMA_MARKER = "_test_stream_config"


# -------------------------------------------------------------- Role / sanitize --


def test_role_is_string_enum():
    assert isinstance(Role.WORKER, str)
    assert Role.WORKER == "worker"
    assert Role.LAUNCHER.value == "launcher"


def test_default_sanitize_keeps_allowed_chars():
    assert default_sanitize("abc-1.2_3") == "abc-1.2_3"


def test_default_sanitize_collapses_runs():
    assert default_sanitize("a b  c") == "a_b_c"
    assert default_sanitize("foo/bar\\baz") == "foo_bar_baz"


def test_default_sanitize_strips_leading_trailing_underscores():
    assert default_sanitize("!!!hello!!!") == "hello"


# ---------------------------------------------------------- write_stream_configs --


def test_write_stream_configs_creates_per_item_dirs(tmp_path: Path):
    items = [
        {"name": "probeA.zarr", "source": "s3://bucket/A"},
        {"name": "probe B", "source": "s3://bucket/B"},
    ]
    paths = write_stream_configs(
        items,
        results_dir=tmp_path,
        schema_marker=SCHEMA_MARKER,
    )
    assert len(paths) == 2
    assert paths[0].parent.name == "stream_probeA.zarr"
    assert paths[1].parent.name == "stream_probe_B"
    assert all(p.name == "config.json" for p in paths)


def test_write_stream_configs_stamps_schema_marker_and_version(tmp_path: Path):
    paths = write_stream_configs(
        [{"name": "x"}],
        results_dir=tmp_path,
        schema_marker=SCHEMA_MARKER,
        schema_version=3,
    )
    parsed = json.loads(paths[0].read_text())
    assert parsed[SCHEMA_MARKER] == 3
    assert parsed["name"] == "x"


def test_write_stream_configs_preserves_item_fields(tmp_path: Path):
    item = {"name": "x", "source": "s3://b/x", "custom": [1, 2, 3]}
    paths = write_stream_configs(
        [item],
        results_dir=tmp_path,
        schema_marker=SCHEMA_MARKER,
    )
    parsed = json.loads(paths[0].read_text())
    for key, value in item.items():
        assert parsed[key] == value


def test_write_stream_configs_missing_name_key_raises(tmp_path: Path):
    with pytest.raises(KeyError, match="name"):
        write_stream_configs(
            [{"source": "s3://bucket/X"}],
            results_dir=tmp_path,
            schema_marker=SCHEMA_MARKER,
        )


def test_write_stream_configs_respects_custom_sanitize(tmp_path: Path):
    paths = write_stream_configs(
        [{"name": "Probe.1"}],
        results_dir=tmp_path,
        schema_marker=SCHEMA_MARKER,
        sanitize=str.lower,
    )
    assert paths[0].parent.name == "stream_probe.1"


def test_write_stream_configs_is_atomic_on_serialization_error(tmp_path: Path):
    items = [{"name": "ok"}, {"name": "bad", "payload": object()}]
    with pytest.raises(TypeError):
        write_stream_configs(
            items,
            results_dir=tmp_path,
            schema_marker=SCHEMA_MARKER,
        )
    # The first write committed; the second left no partial file.
    ok_dir = tmp_path / "stream_ok"
    bad_dir = tmp_path / "stream_bad"
    assert (ok_dir / "config.json").exists()
    # bad_dir was created, but its config.json must not exist; no .tmp sibling.
    assert not (bad_dir / "config.json").exists()
    siblings = list(bad_dir.iterdir()) if bad_dir.exists() else []
    assert siblings == []


def test_write_stream_configs_side_writes_producer_record(tmp_path: Path):
    # Fan-out breadcrumb: the launcher shard lands in EACH per-item provenance/,
    # so a fanned worker can infer it as a parent by frontier.
    items = [{"name": "unitA"}, {"name": "unit B"}]
    producer = {"v": 1, "node": "discover", "parents": []}
    write_stream_configs(
        items,
        results_dir=tmp_path,
        schema_marker=SCHEMA_MARKER,
        producer_record=producer,
    )
    for safe in ("unitA", "unit_B"):
        shard = tmp_path / f"stream_{safe}" / "provenance" / "discover.json"
        assert shard.exists()
        assert json.loads(shard.read_text()) == producer


def test_write_stream_configs_does_not_pollute_config_json(tmp_path: Path):
    # config.json (the work-item contract) must stay untouched by the breadcrumb.
    items = [{"name": "unitA", "source": "s3://x"}]
    producer = {"v": 1, "node": "discover", "parents": []}
    write_stream_configs(
        items,
        results_dir=tmp_path,
        schema_marker=SCHEMA_MARKER,
        producer_record=producer,
    )
    cfg = json.loads((tmp_path / "stream_unitA" / "config.json").read_text())
    assert "node" not in cfg
    assert "parents" not in cfg
    assert cfg["source"] == "s3://x"


def test_write_stream_configs_skips_nodeless_producer_record(tmp_path: Path, caplog):
    import logging

    with caplog.at_level(logging.WARNING):
        write_stream_configs(
            [{"name": "unitA"}],
            results_dir=tmp_path,
            schema_marker=SCHEMA_MARKER,
            producer_record={"v": 1, "parents": []},  # no 'node'
        )
    assert not (tmp_path / "stream_unitA" / "provenance").exists()
    assert any("no 'node'" in m for m in caplog.messages)


def test_write_stream_configs_writes_every_provenance_shard(tmp_path: Path, caplog):
    import logging

    shards = [
        {"v": 1, "node": "upstream", "parents": []},
        {"v": 1, "node": "discover", "parents": ["upstream"]},
        {"v": 1, "parents": []},  # no 'node'
    ]
    with caplog.at_level(logging.WARNING):
        write_stream_configs(
            [{"name": "unitA"}],
            results_dir=tmp_path,
            schema_marker=SCHEMA_MARKER,
            provenance=shards,
        )
    prov = tmp_path / "stream_unitA" / "provenance"
    assert sorted(p.name for p in prov.iterdir()) == ["discover.json", "upstream.json"]
    assert json.loads((prov / "discover.json").read_text()) == shards[1]
    assert any("no 'node'" in m for m in caplog.messages)


def test_write_stream_configs_no_producer_record_writes_no_provenance(tmp_path: Path):
    write_stream_configs([{"name": "unitA"}], results_dir=tmp_path, schema_marker=SCHEMA_MARKER)
    assert not (tmp_path / "stream_unitA" / "provenance").exists()


# -------------------------------------------------------------- find_stream_config --


def _write_config(dir_path: Path, payload: dict) -> Path:
    dir_path.mkdir(parents=True, exist_ok=True)
    cfg = dir_path / "config.json"
    cfg.write_text(json.dumps(payload))
    return cfg


def test_find_stream_config_picks_single_by_marker(tmp_path: Path):
    real = _write_config(tmp_path / "a/b/stream_x", {SCHEMA_MARKER: 1, "name": "x"})
    _write_config(tmp_path / "a/other/stream_y", {"name": "y"})  # no marker, ignored
    path, parsed = find_stream_config(tmp_path, schema_marker=SCHEMA_MARKER)
    assert path == real
    assert parsed["name"] == "x"


def test_find_stream_config_ignores_non_dict_and_unmarked(tmp_path: Path):
    (tmp_path / "bad1").mkdir()
    (tmp_path / "bad1" / "config.json").write_text("[1,2,3]")
    (tmp_path / "bad2").mkdir()
    (tmp_path / "bad2" / "config.json").write_text("not json")
    real = _write_config(tmp_path / "good", {SCHEMA_MARKER: 1, "name": "x"})
    path, _ = find_stream_config(tmp_path, schema_marker=SCHEMA_MARKER)
    assert path == real


def test_find_stream_config_follows_symlinks(tmp_path: Path):
    staging = tmp_path / "real" / "stream_x"
    cfg = _write_config(staging, {SCHEMA_MARKER: 1, "name": "x"})
    link = tmp_path / "data"
    link.symlink_to(tmp_path / "real", target_is_directory=True)
    found, _ = find_stream_config(link, schema_marker=SCHEMA_MARKER)
    assert found.resolve() == cfg.resolve()


def test_find_stream_config_zero_matches_raises(tmp_path: Path):
    with pytest.raises(StreamConfigError) as excinfo:
        find_stream_config(tmp_path, schema_marker=SCHEMA_MARKER)
    assert excinfo.value.paths == []
    assert "no 'config.json'" in str(excinfo.value)


def test_find_stream_config_multiple_matches_raises_with_paths(tmp_path: Path):
    p1 = _write_config(tmp_path / "a", {SCHEMA_MARKER: 1, "name": "x"})
    p2 = _write_config(tmp_path / "b", {SCHEMA_MARKER: 1, "name": "y"})
    with pytest.raises(StreamConfigError) as excinfo:
        find_stream_config(tmp_path, schema_marker=SCHEMA_MARKER)
    assert set(excinfo.value.paths) == {p1, p2}
    assert "ambiguous" in str(excinfo.value).lower()


def test_find_stream_config_respects_custom_filename(tmp_path: Path):
    cfg_dir = tmp_path / "nested"
    cfg_dir.mkdir()
    real = cfg_dir / "run.json"
    real.write_text(json.dumps({SCHEMA_MARKER: 1, "name": "x"}))
    # A decoy config.json shouldn't interfere.
    (cfg_dir / "config.json").write_text("{}")
    path, _ = find_stream_config(tmp_path, schema_marker=SCHEMA_MARKER, filename="run.json")
    assert path == real


# --------------------------------------------------------- find_worker_manifests --


def _write(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload))


def test_find_worker_manifests_walk_order(tmp_path: Path):
    _write(tmp_path / "w1" / "manifest_a.json", {"stream": "a", "result": 1})
    _write(tmp_path / "w2" / "manifest_b.json", {"stream": "b", "result": 2})
    _write(tmp_path / "w3" / "other.json", {"ignored": True})  # wrong prefix
    results = find_worker_manifests(tmp_path)
    names = {p.name for p, _ in results}
    assert names == {"manifest_a.json", "manifest_b.json"}


def test_find_worker_manifests_ignores_launcher_manifest(tmp_path: Path):
    _write(tmp_path / "manifest_a.json", {"stream": "a", "result": 1})
    _write(tmp_path / "launcher_manifest.json", {"role": "launcher"})
    results = find_worker_manifests(tmp_path)
    assert {p.name for p, _ in results} == {"manifest_a.json"}


def test_find_worker_manifests_warn_and_continue_on_corrupt(tmp_path: Path, caplog: pytest.LogCaptureFixture):
    _write(tmp_path / "manifest_good.json", {"stream": "g", "result": 1})
    (tmp_path / "manifest_bad.json").write_text("not json")
    with caplog.at_level(logging.WARNING, logger="aind_code_ocean_pipeline_utils.role_dispatch"):
        results = find_worker_manifests(tmp_path)
    assert len(results) == 1
    assert results[0][0].name == "manifest_good.json"
    assert any("manifest_bad.json" in r.message for r in caplog.records)


def test_find_worker_manifests_strict_raises_on_corrupt(tmp_path: Path):
    (tmp_path / "manifest_bad.json").write_text("not json")
    with pytest.raises(json.JSONDecodeError):
        find_worker_manifests(tmp_path, strict=True)


def test_find_worker_manifests_strict_raises_on_non_object(tmp_path: Path):
    _write(tmp_path / "manifest_x.json", [1, 2, 3])
    with pytest.raises(ValueError, match="not a JSON object"):
        find_worker_manifests(tmp_path, strict=True)


def test_find_worker_manifests_follows_symlinks(tmp_path: Path):
    real = tmp_path / "real"
    _write(real / "manifest_x.json", {"stream": "x", "result": 1})
    link = tmp_path / "data"
    link.symlink_to(real, target_is_directory=True)
    results = find_worker_manifests(link)
    assert len(results) == 1


# --------------------------------------------------------- find_launcher_manifest --


def test_find_launcher_manifest_returns_none_when_missing(tmp_path: Path):
    assert find_launcher_manifest(tmp_path) is None


def test_find_launcher_manifest_single(tmp_path: Path):
    _write(tmp_path / "nested" / "launcher_manifest.json", {"role": "launcher"})
    result = find_launcher_manifest(tmp_path)
    assert result == {"role": "launcher"}


def test_find_launcher_manifest_multiple_warns(tmp_path: Path, caplog: pytest.LogCaptureFixture):
    _write(tmp_path / "a" / "launcher_manifest.json", {"role": "launcher", "n": 1})
    _write(tmp_path / "b" / "launcher_manifest.json", {"role": "launcher", "n": 2})
    with caplog.at_level(logging.WARNING, logger="aind_code_ocean_pipeline_utils.role_dispatch"):
        result = find_launcher_manifest(tmp_path)
    assert result is not None
    assert any("multiple" in r.message for r in caplog.records)


def test_find_launcher_manifest_returns_none_on_corrupt(tmp_path: Path, caplog: pytest.LogCaptureFixture):
    (tmp_path / "launcher_manifest.json").write_text("not json")
    with caplog.at_level(logging.WARNING, logger="aind_code_ocean_pipeline_utils.role_dispatch"):
        assert find_launcher_manifest(tmp_path) is None


# ----------------------------------------------------------------- merge_manifests --


def test_merge_manifests_partitions_built_and_skipped():
    manifests = [
        {"stream": "a", "result": {"path": "/r/a"}},
        {"stream": "b", "error": "discovery failed"},
        {"stream": "c", "result": {"path": "/r/c"}},
    ]
    merged = merge_manifests(manifests)
    assert merged == {
        "built": [{"path": "/r/a"}, {"path": "/r/c"}],
        "skipped": [{"stream": "b", "reason": "discovery failed"}],
    }


def test_merge_manifests_ignores_empty_manifests():
    merged = merge_manifests([{"stream": "x"}, {}])
    assert merged == {"built": [], "skipped": []}


def test_merge_manifests_skipped_fills_unknown_stream():
    merged = merge_manifests([{"error": "something"}])
    assert merged["skipped"] == [{"stream": "unknown", "reason": "something"}]


def test_merge_manifests_result_takes_precedence_over_error():
    merged = merge_manifests([{"stream": "x", "result": 1, "error": "also"}])
    assert merged == {"built": [1], "skipped": []}


def test_merge_manifests_custom_keys():
    manifests = [
        {"item": "a", "ok": {"path": "/r/a"}},
        {"item": "b", "fail": "bad"},
    ]
    merged = merge_manifests(
        manifests,
        result_key="ok",
        error_key="fail",
        stream_key="item",
    )
    assert merged == {
        "built": [{"path": "/r/a"}],
        "skipped": [{"stream": "b", "reason": "bad"}],
    }
