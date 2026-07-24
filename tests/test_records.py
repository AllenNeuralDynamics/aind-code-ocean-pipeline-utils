"""Tests for aind_code_ocean_pipeline_utils.records (schema-free provenance)."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from aind_code_ocean_pipeline_utils.records import (
    RECORD_VERSION,
    frontier,
    make_record,
    read_records,
    record_step,
    write_record,
)

# aind-data-schema drives only the opaque payload; the routing layer is stdlib.
_HAS_ADS = True
try:  # pragma: no cover - env probe
    import aind_data_schema.core.processing  # noqa: F401
except ImportError:  # pragma: no cover
    _HAS_ADS = False


# --------------------------------------------------------------- make_record --


def test_make_record_minimal_omits_optional_keys():
    rec = make_record("a")
    assert rec == {"v": RECORD_VERSION, "node": "a", "parents": []}
    # Optional payloads are absent, not null.
    assert "data_process" not in rec
    assert "pipeline" not in rec
    assert "experimenters" not in rec


def test_make_record_includes_supplied_payloads():
    rec = make_record(
        "b",
        parents=["a"],
        data_process={"name": "b"},
        data_process_schema_version="2.2.2",
        pipeline={"name": "pipe", "code": {"url": "x"}},
        experimenters=["Jane"],
    )
    assert rec["parents"] == ["a"]
    assert rec["data_process"] == {"name": "b"}
    assert rec["data_process_schema_version"] == "2.2.2"
    assert rec["pipeline"] == {"name": "pipe", "code": {"url": "x"}}
    assert rec["experimenters"] == ["Jane"]


def test_make_record_schema_version_dropped_without_payload():
    # A version tag is meaningless without a payload to tag.
    rec = make_record("c", data_process_schema_version="2.2.2")
    assert "data_process_schema_version" not in rec


# ------------------------------------------------------- write / read round --


def test_write_record_places_shard_under_provenance(tmp_path: Path):
    path = write_record(make_record("node-1", parents=["p"]), tmp_path)
    assert path == tmp_path / "provenance" / "node-1.json"
    assert json.loads(path.read_text())["node"] == "node-1"


def test_write_record_requires_node(tmp_path: Path):
    with pytest.raises(ValueError, match="non-empty string 'node'"):
        write_record({"v": 1, "parents": []}, tmp_path)


def test_read_records_roundtrip_and_dedup(tmp_path: Path):
    write_record(make_record("a"), tmp_path)
    write_record(make_record("b", parents=["a"]), tmp_path)
    records = read_records(tmp_path)
    assert {r["node"] for r in records} == {"a", "b"}


def test_read_records_empty_when_missing(tmp_path: Path):
    assert read_records(tmp_path / "does-not-exist") == []


def test_read_records_only_reads_provenance_dirs(tmp_path: Path):
    # A shard-shaped file NOT under a provenance/ dir is ignored.
    (tmp_path / "notprov").mkdir()
    (tmp_path / "notprov" / "x.json").write_text(json.dumps({"v": 1, "node": "ghost", "parents": []}))
    write_record(make_record("real"), tmp_path)
    assert {r["node"] for r in read_records(tmp_path)} == {"real"}


def test_read_records_ignores_non_json_and_non_object(tmp_path: Path):
    prov = tmp_path / "provenance"
    prov.mkdir()
    (prov / "note.txt").write_text("not json")
    (prov / "list.json").write_text(json.dumps([1, 2, 3]))
    (prov / "nonode.json").write_text(json.dumps({"v": 1, "parents": []}))
    write_record(make_record("keep"), tmp_path)
    assert {r["node"] for r in read_records(tmp_path)} == {"keep"}


def test_read_records_bounded_depth(tmp_path: Path):
    # provenance at depth 2 is found; at depth 4 it is pruned away.
    shallow = tmp_path / "asset"
    write_record(make_record("shallow"), shallow)  # <root>/asset/provenance -> depth 2
    deep = tmp_path / "l1" / "l2" / "l3"
    write_record(make_record("deep"), deep)  # <root>/l1/l2/l3/provenance -> depth 4
    found = {r["node"] for r in read_records(tmp_path, max_depth=3)}
    assert found == {"shallow"}
    # Raising the bound reaches the deep one.
    assert "deep" in {r["node"] for r in read_records(tmp_path, max_depth=5)}


def test_read_records_follows_symlinked_dirs(tmp_path: Path):
    # Mimic CO staging: the real provenance lives elsewhere, mounted via a symlink.
    real = tmp_path / "real_asset"
    write_record(make_record("via-symlink"), real)
    staged = tmp_path / "staged"
    staged.mkdir()
    (staged / "asset").symlink_to(real, target_is_directory=True)
    found = {r["node"] for r in read_records(staged)}
    assert found == {"via-symlink"}


def test_read_records_warns_on_conflicting_node(tmp_path: Path, caplog: pytest.LogCaptureFixture):
    prov = tmp_path / "provenance"
    prov.mkdir()
    (prov / "one.json").write_text(json.dumps({"v": 1, "node": "dup", "parents": ["x"]}))
    (prov / "two.json").write_text(json.dumps({"v": 1, "node": "dup", "parents": ["y"]}))
    with caplog.at_level(logging.WARNING):
        records = read_records(tmp_path)
    assert len(records) == 1  # one kept
    assert any("conflicting provenance for node 'dup'" in m for m in caplog.messages)


def test_read_records_superset_supersedes_stub(tmp_path: Path, caplog: pytest.LogCaptureFixture):
    # The fan-out case: a minimal stub {v,node,parents} and the full shard (with a
    # data_process payload) for the same node meet at the terminal. The full one
    # must win, silently -- the stub is a subset that agrees on shared keys.
    stub_dir = tmp_path / "from_worker"
    write_record({"v": 1, "node": "discover", "parents": []}, stub_dir)
    full_dir = tmp_path / "from_direct_edge"
    write_record(
        {"v": 1, "node": "discover", "parents": [], "data_process": {"name": "discover"}},
        full_dir,
    )
    with caplog.at_level(logging.WARNING):
        records = read_records(tmp_path)
    assert len(records) == 1
    assert records[0].get("data_process") == {"name": "discover"}  # full won
    assert not any("conflicting" in m for m in caplog.messages)


def test_read_records_superset_supersedes_regardless_of_scan_order(tmp_path: Path):
    # Full first, then stub -> still keeps the full (order-independent).
    write_record({"v": 1, "node": "d", "parents": [], "data_process": {"x": 1}}, tmp_path / "a_full")
    write_record({"v": 1, "node": "d", "parents": []}, tmp_path / "z_stub")
    records = read_records(tmp_path)
    assert records[0].get("data_process") == {"x": 1}


def test_read_records_silent_on_identical_duplicate(tmp_path: Path, caplog: pytest.LogCaptureFixture):
    # The launcher-duplicate case: same node id, identical content -> no warning.
    prov = tmp_path / "provenance"
    prov.mkdir()
    body = {"v": 1, "node": "dup", "parents": ["x"]}
    (prov / "one.json").write_text(json.dumps(body))
    (prov / "two.json").write_text(json.dumps(body))
    with caplog.at_level(logging.WARNING):
        records = read_records(tmp_path)
    assert len(records) == 1
    assert not any("conflicting" in m for m in caplog.messages)


# ------------------------------------------------------------------ frontier --


def test_frontier_linear_chain():
    records = [
        make_record("a"),
        make_record("b", parents=["a"]),
        make_record("c", parents=["b"]),
    ]
    assert frontier(records) == ["c"]


def test_frontier_fan_in_has_multiple_sinks():
    records = [make_record("a"), make_record("b")]  # two independent sources
    assert set(frontier(records)) == {"a", "b"}


def test_frontier_diamond():
    records = [
        make_record("a"),
        make_record("b", parents=["a"]),
        make_record("c", parents=["a"]),
        make_record("d", parents=["b", "c"]),
    ]
    assert frontier(records) == ["d"]


def test_frontier_preserves_first_seen_order():
    records = [make_record("z"), make_record("y"), make_record("x")]
    assert frontier(records) == ["z", "y", "x"]


# --------------------------------------------------------------- record_step --


def test_record_step_source_node_has_no_parents(tmp_path: Path):
    empty_in = tmp_path / "in"
    empty_in.mkdir()
    out = tmp_path / "out"
    with record_step("A", process_type="Other", incoming_dir=empty_in, output_dir=out):
        pass
    shard = json.loads((out / "provenance" / "A.json").read_text())
    assert shard["node"] == "A"
    assert shard["parents"] == []


def test_record_step_linear_edge_infers_parent_and_propagates(tmp_path: Path):
    # Node A writes to out_a; node B consumes out_a on its edge.
    out_a = tmp_path / "a"
    with record_step("A", process_type="Other", incoming_dir=tmp_path / "empty", output_dir=out_a):
        pass
    out_b = tmp_path / "b"
    with record_step("B", process_type="Other", incoming_dir=out_a, output_dir=out_b):
        pass
    b_shard = json.loads((out_b / "provenance" / "B.json").read_text())
    assert b_shard["parents"] == ["A"]
    # A's shard is propagated forward so the DAG reaches the terminal intact.
    assert (out_b / "provenance" / "A.json").exists()


def test_record_step_monolith_chains_via_output_union(tmp_path: Path):
    # Two steps in one process sharing output_dir; incoming has no shards.
    out = tmp_path / "out"
    empty_in = tmp_path / "empty"
    with record_step("A", process_type="Other", incoming_dir=empty_in, output_dir=out):
        pass
    with record_step("B", process_type="Other", incoming_dir=empty_in, output_dir=out):
        pass
    b_shard = json.loads((out / "provenance" / "B.json").read_text())
    assert b_shard["parents"] == ["A"]


def test_record_step_explicit_parents_override_frontier(tmp_path: Path):
    out = tmp_path / "out"
    with record_step(
        "B",
        process_type="Other",
        incoming_dir=tmp_path / "empty",
        output_dir=out,
        parents=["hand-declared"],
    ):
        pass
    b_shard = json.loads((out / "provenance" / "B.json").read_text())
    assert b_shard["parents"] == ["hand-declared"]


def test_record_step_body_failure_emits_nothing(tmp_path: Path):
    out = tmp_path / "out"
    with pytest.raises(RuntimeError, match="boom"):
        with record_step("A", process_type="Other", incoming_dir=tmp_path / "empty", output_dir=out):
            raise RuntimeError("boom")
    assert not (out / "provenance").exists()


def test_record_step_survives_payload_authoring_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    # Force the (lazily imported) DataProcess authoring to blow up; _author_payload
    # must swallow it and return no payload, so the envelope + routing are still
    # written (topology-only) and the run is never sunk. Exercises the real path.
    def _raise(*_a: object, **_k: object) -> None:
        raise RuntimeError("schema exploded")

    monkeypatch.setattr("aind_code_ocean_pipeline_utils.metadata.make_data_process", _raise)
    out = tmp_path / "out"
    with record_step("A", process_type="Other", incoming_dir=tmp_path / "empty", output_dir=out):
        pass
    shard = json.loads((out / "provenance" / "A.json").read_text())
    assert shard["node"] == "A"
    assert shard["parents"] == []
    assert "data_process" not in shard  # authoring failed -> envelope only


@pytest.mark.skipif(not _HAS_ADS, reason="requires the [metadata] extra (aind-data-schema)")
def test_record_step_authors_data_process_when_schema_present(tmp_path: Path):
    out = tmp_path / "out"
    with record_step(
        "align-1",
        process_type="Other",
        stage="Processing",
        incoming_dir=tmp_path / "empty",
        output_dir=out,
        experimenters=["Jane Doe"],
        code_url="https://example.com/repo",
        notes="ran the thing",
    ) as ctx:
        ctx.parameters = {"k": 1}
    shard = json.loads((out / "provenance" / "align-1.json").read_text())
    assert "data_process" in shard
    dp = shard["data_process"]
    # node is stamped as the DataProcess name (the dependency_graph key space).
    assert dp["name"] == "align-1"
    assert dp["experimenters"] == ["Jane Doe"]
    assert shard["data_process_schema_version"] is not None


@pytest.mark.skipif(not _HAS_ADS, reason="requires the [metadata] extra (aind-data-schema)")
def test_record_step_empty_experimenters_is_valid(tmp_path: Path):
    # experimenters=[] must author a valid DataProcess (schema has no min_length).
    out = tmp_path / "out"
    with record_step("A", process_type="Other", incoming_dir=tmp_path / "empty", output_dir=out):
        pass
    dp = json.loads((out / "provenance" / "A.json").read_text())["data_process"]
    assert dp["experimenters"] == []
