"""Tests for the optional ``metadata`` module (requires ``[metadata]`` extra)."""

import json

import pytest

pytest.importorskip("aind_data_schema")

from aind_data_schema.core.processing import (
    DataProcess,
    Processing,
    ProcessName,
)

from aind_code_ocean_pipeline_utils.metadata import (
    append_process,
    emit_processing,
    make_data_process,
    read_processings,
    utcnow,
    write_processing,
)

URL = "https://github.com/AllenNeuralDynamics/example-capsule"


def dp(name: str, process_type: ProcessName = ProcessName.IMAGE_ATLAS_ALIGNMENT):
    return make_data_process(
        process_type=process_type,
        code_url=URL,
        experimenters=["tester"],
        start=utcnow(),
        name=name,
    )


def test_make_data_process_defaults_end_and_fields():
    proc = dp("alpha")
    assert proc.name == "alpha"
    assert proc.end_date_time is not None  # defaulted
    assert proc.code.url == URL


def test_source_node_has_empty_deps():
    out = append_process([], dp("root"))
    assert out.dependency_graph == {"root": []}
    assert [p.name for p in out.data_processes] == ["root"]


def test_linear_chain_wires_to_frontier():
    first = append_process([], dp("A"))
    second = append_process([first], dp("B"))
    assert second.dependency_graph == {"A": [], "B": ["A"]}


def test_fan_in_diamond_dedups_and_wires_both_heads():
    # Two upstream branches that share node A: A->B and A->C
    branch_ab = append_process([append_process([], dp("A"))], dp("B"))
    # rebuild C off the same A (independent branch carrying A and C)
    a_only = append_process([], dp("A"))
    branch_ac = append_process([a_only], dp("C"))

    merged = append_process([branch_ab, branch_ac], dp("D"))
    g = merged.dependency_graph
    assert g is not None
    # A appears once despite being in both branches
    assert [p.name for p in merged.data_processes] == ["A", "B", "C", "D"]
    assert g["A"] == []
    assert g["B"] == ["A"]
    assert g["C"] == ["A"]
    assert set(g["D"]) == {"B", "C"}  # new node attaches to both branch heads


def test_duplicate_new_name_raises():
    upstream = append_process([], dp("A"))
    with pytest.raises(ValueError, match="already present upstream"):
        append_process([upstream], dp("A"))


def test_missing_name_raises():
    # The schema auto-fills name from process_type, so force a genuinely
    # nameless DataProcess to exercise the guard.
    nameless = dp("placeholder").model_copy(update={"name": None})
    with pytest.raises(ValueError, match="must have a name"):
        append_process([], nameless)


def test_emit_processing_round_trip(tmp_path):
    # node 1 (source): no upstream processing.json in its input dir
    in1 = tmp_path / "in1"
    in1.mkdir()
    out1 = tmp_path / "out1"
    path1 = emit_processing(dp("preprocess"), input_dir=in1, output_dir=out1)
    assert path1 == out1 / "processing.json"

    # node 2 consumes node 1's output as its input
    out2 = tmp_path / "out2"
    emit_processing(dp("register"), input_dir=out1, output_dir=out2)

    loaded = Processing.model_validate(json.loads((out2 / "processing.json").read_text()))
    assert loaded.dependency_graph == {"preprocess": [], "register": ["preprocess"]}
    assert isinstance(loaded.data_processes[0], DataProcess)


def test_read_processings_skips_unreadable(tmp_path):
    good = tmp_path / "good"
    good.mkdir()
    write_processing(append_process([], dp("ok")), good)
    # a bogus file matching the glob must be skipped, not raise
    (good / "broken_processing.json").write_text("{ not valid json")
    procs = read_processings(good)
    assert len(procs) == 1
    assert procs[0].data_processes[0].name == "ok"
