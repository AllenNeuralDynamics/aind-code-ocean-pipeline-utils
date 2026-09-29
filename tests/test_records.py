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
    read_processing_records,
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


def test_record_step_never_lists_itself_as_parent_from_own_output(tmp_path: Path):
    # A launcher side-writes its own fan-out stub into its output_dir during the
    # body (as write_stream_configs does). Parent inference then scans output_dir
    # and would re-ingest that same-node stub -- a node must never become its own
    # parent. It is a source node, so parents stay empty.
    out = tmp_path / "out"
    empty_in = tmp_path / "empty"
    with record_step("ibl-discover", process_type="Other", incoming_dir=empty_in, output_dir=out):
        write_record(make_record("ibl-discover"), out / "stream_0")
    shard = json.loads((out / "provenance" / "ibl-discover.json").read_text())
    assert shard["parents"] == []


def test_record_step_strips_self_from_explicit_parents(tmp_path: Path):
    # Even a hand-declared parents= naming the node itself is dropped.
    out = tmp_path / "out"
    with record_step(
        "B",
        process_type="Other",
        incoming_dir=tmp_path / "empty",
        output_dir=out,
        parents=["A", "B"],
    ):
        pass
    shard = json.loads((out / "provenance" / "B.json").read_text())
    assert shard["parents"] == ["A"]


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


def test_read_records_payload_beats_stub_with_different_parents(tmp_path: Path, caplog: pytest.LogCaptureFixture):
    # A stale stub disagreeing on parents must not displace the shard carrying the payload.
    write_record({"v": 1, "node": "L", "parents": []}, tmp_path / "a_stub")
    write_record({"v": 1, "node": "L", "parents": ["X"], "data_process": {"x": 1}}, tmp_path / "z_full")
    with caplog.at_level(logging.WARNING):
        records = read_records(tmp_path)
    assert records[0]["parents"] == ["X"]
    assert any("conflicting provenance for node 'L'" in m for m in caplog.messages)


# ---------------------------------------------------- read_processing_records --


def _v2_processing(*names: str, graph: dict[str, list[str]] | None = None) -> dict:
    processing: dict = {
        "schema_version": "2.3.0",
        "data_processes": [{"name": n, "process_type": "Other", "notes": n} for n in names],
    }
    if graph is not None:
        processing["dependency_graph"] = graph
    return processing


def _write_processing_json(directory: Path, body: dict) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "processing.json").write_text(json.dumps(body))


def test_read_processing_records_uses_dependency_graph(tmp_path: Path):
    _write_processing_json(
        tmp_path / "asset",
        _v2_processing("sort", "curate", "qc", graph={"sort": [], "curate": ["sort"], "qc": ["sort"]}),
    )
    records = {r["label"]: r for r in read_processing_records(tmp_path)}
    digest = records["sort"]["node"].split("@")[1]
    assert records["sort"]["node"] == f"sort@{digest}"
    assert records["curate"]["parents"] == [f"sort@{digest}"]
    assert records["qc"]["parents"] == [f"sort@{digest}"]
    assert records["sort"]["data_process"]["name"] == "sort"  # payload carried verbatim
    assert records["sort"]["data_process_schema_version"] == "2.3.0"
    assert frontier(records.values()) == [f"curate@{digest}", f"qc@{digest}"]


def test_read_processing_records_chains_a_graphless_v1_file(tmp_path: Path):
    v1 = {
        "schema_version": "1.1.3",
        "processing_pipeline": {
            "data_processes": [{"name": "Ephys preprocessing"}, {"name": "Spike sorting"}],
        },
        "analyses": [{"data_processes": [{"name": "Other"}]}],
    }
    _write_processing_json(tmp_path / "asset", v1)
    records = read_processing_records(tmp_path)
    assert [r["label"] for r in records] == ["Ephys preprocessing", "Spike sorting", "Other"]
    assert [r["parents"] for r in records] == [[], [records[0]["node"]], [records[1]["node"]]]


def test_read_processing_records_disambiguates_repeated_names_in_one_file(tmp_path: Path):
    _write_processing_json(tmp_path / "asset", _v2_processing("Other", "Other"))
    assert [r["label"] for r in read_processing_records(tmp_path)] == ["Other", "Other (2)"]


def test_read_processing_records_same_file_twice_yields_same_ids(tmp_path: Path):
    body = _v2_processing("sort")
    _write_processing_json(tmp_path / "edge_one", body)
    _write_processing_json(tmp_path / "edge_two", body)
    nodes = [r["node"] for r in read_processing_records(tmp_path)]
    assert len(nodes) == 2
    assert nodes[0] == nodes[1]


def test_read_processing_records_distinct_files_do_not_collide(tmp_path: Path):
    first, second = _v2_processing("sort"), _v2_processing("sort")
    second["data_processes"][0]["notes"] = "another session"
    _write_processing_json(tmp_path / "session_a", first)
    _write_processing_json(tmp_path / "session_b", second)
    records = read_processing_records(tmp_path)
    assert {r["label"] for r in records} == {"sort"}
    assert len({r["node"] for r in records}) == 2


def test_read_processing_records_skips_file_beside_shards(tmp_path: Path):
    _write_processing_json(tmp_path / "asset", _v2_processing("sort"))
    write_record(make_record("sort"), tmp_path / "asset")
    assert read_processing_records(tmp_path) == []


def test_read_processing_records_skips_unreadable_and_bounds_depth(tmp_path: Path):
    (tmp_path / "bad").mkdir()
    (tmp_path / "bad" / "processing.json").write_text("{not json")
    _write_processing_json(tmp_path / "list", {"data_processes": "nope"})
    _write_processing_json(tmp_path / "a" / "b" / "c", _v2_processing("too-deep"))
    assert read_processing_records(tmp_path) == []


def test_read_processing_records_carries_pipeline_block(tmp_path: Path):
    body = _v2_processing("sort")
    body["data_processes"][0]["pipeline_name"] = "ephys"
    body["pipelines"] = [{"name": "ephys", "url": "https://example.com/pipeline"}]
    _write_processing_json(tmp_path / "asset", body)
    (record,) = read_processing_records(tmp_path)
    assert record["pipeline"] == {"name": "ephys", "code": body["pipelines"][0]}


# ------------------------------------------- record_step: upstream and fan-out --


def test_record_step_attaches_to_upstream_processing_json(tmp_path: Path):
    # A capsule that only writes processing.json still becomes this node's parent.
    incoming = tmp_path / "data"
    _write_processing_json(
        incoming / "sorted", _v2_processing("sort", "curate", graph={"sort": [], "curate": ["sort"]})
    )
    out = tmp_path / "out"
    with record_step("B", process_type="Other", incoming_dir=incoming, output_dir=out) as step:
        pass
    (curate,) = step.parents
    assert curate.startswith("curate@")
    shard = json.loads((out / "provenance" / "B.json").read_text())
    assert shard["parents"] == [curate]
    # The converted steps travel on as shards, so later nodes need not see the file.
    assert {r.get("label") for r in read_records(out)} == {"sort", "curate", None}


def test_record_step_read_processing_false_ignores_processing_json(tmp_path: Path):
    incoming = tmp_path / "data"
    _write_processing_json(incoming / "sorted", _v2_processing("sort"))
    with record_step(
        "B", process_type="Other", incoming_dir=incoming, output_dir=tmp_path / "out", read_processing=False
    ) as step:
        pass
    assert step.parents == ()


def test_record_step_resolves_parents_before_the_body(tmp_path: Path):
    out_a = tmp_path / "a"
    with record_step("A", process_type="Other", incoming_dir=tmp_path / "empty", output_dir=out_a):
        pass
    with record_step("B", process_type="Other", incoming_dir=out_a, output_dir=tmp_path / "b") as step:
        assert step.node == "B"
        assert step.parents == ("A",)


def test_fanout_shards_carry_parents_and_upstream(tmp_path: Path):
    # X -> L (launcher, fans out) -> W (worker). The worker must see L as its parent
    # and receive X, and L's stub must agree with L's full shard.
    out_x = tmp_path / "x"
    with record_step("X", process_type="Other", incoming_dir=tmp_path / "empty", output_dir=out_x):
        pass
    out_l = tmp_path / "l"
    with record_step(
        "L", process_type="Other", incoming_dir=out_x, output_dir=out_l, run_experimenters=["Ada"]
    ) as step:
        shards = step.fanout_shards()
        for shard in shards:
            write_record(shard, out_l / "stream_s1")
    stub = shards[-1]
    assert stub == {"v": 1, "node": "L", "parents": ["X"], "experimenters": ["Ada"]}
    assert [s["node"] for s in shards] == ["X", "L"]

    full = json.loads((out_l / "provenance" / "L.json").read_text())
    assert full["parents"] == ["X"]  # the stub in out_l/stream_s1 did not erase the parent
    assert full["experimenters"] == ["Ada"]

    out_w = tmp_path / "w"
    with record_step("W", process_type="Other", incoming_dir=out_l / "stream_s1", output_dir=out_w) as worker:
        pass
    assert worker.parents == ("L",)
    assert {r["node"] for r in read_records(out_w)} == {"X", "L", "W"}
