"""Tests for the ``step`` module (requires the ``[metadata]`` extra)."""

import json

import pytest

pytest.importorskip("aind_data_schema")

from aind_data_schema.core.processing import Processing, ProcessName

from aind_code_ocean_pipeline_utils.metadata import emit_processing, make_data_process, utcnow
from aind_code_ocean_pipeline_utils.step import (
    DEFAULT_FORWARDED_METADATA,
    capsule_step,
    coerce_process_type,
    derive_code_url,
    derive_experimenters,
    forward_metadata,
    processing_step,
)

URL = "https://github.com/AllenNeuralDynamics/example-capsule"


def _load(path):
    return Processing.model_validate(json.loads(path.read_text()))


# --- coerce_process_type ----------------------------------------------------


def test_coerce_known_string():
    pt, notes = coerce_process_type("Skull stripping")
    assert pt is ProcessName.SKULL_STRIPPING
    assert notes is None


def test_coerce_enum_passthrough():
    pt, notes = coerce_process_type(ProcessName.IMAGE_ATLAS_ALIGNMENT)
    assert pt is ProcessName.IMAGE_ATLAS_ALIGNMENT
    assert notes is None


def test_coerce_unknown_string_becomes_other_with_notes():
    pt, notes = coerce_process_type("frobnicate the voxels")
    assert pt is ProcessName.OTHER
    assert notes == "frobnicate the voxels"


# --- derive_code_url --------------------------------------------------------


def test_code_url_explicit_wins():
    assert derive_code_url(explicit=URL, code_dir="/nonexistent") == URL


def test_code_url_normalizes_ssh_and_env(monkeypatch):
    monkeypatch.setenv("CO_CAPSULE_URL", "git@github.com:org/repo.git")
    # git lookup in a bogus dir fails, falls through to env
    assert derive_code_url(code_dir="/nonexistent") == "https://github.com/org/repo"


def test_code_url_none_when_nothing(monkeypatch):
    for var in ("CO_CAPSULE_URL", "GIT_URL", "REPO_URL"):
        monkeypatch.delenv(var, raising=False)
    assert derive_code_url(code_dir="/nonexistent", env_vars=()) is None


# --- derive_experimenters ---------------------------------------------------


def test_experimenters_explicit_wins(tmp_path):
    assert derive_experimenters(explicit=["A", "B"], input_dir=tmp_path) == ["A", "B"]


def test_experimenters_from_upstream_processing(tmp_path):
    out = tmp_path / "up"
    proc = make_data_process(
        process_type=ProcessName.SKULL_STRIPPING,
        code_url=URL,
        experimenters=["Yoni Browning"],
        start=utcnow(),
        name="skull",
    )
    emit_processing(proc, input_dir=tmp_path / "empty", output_dir=out)
    assert derive_experimenters(input_dir=out) == ["Yoni Browning"]


def test_experimenters_from_data_description(tmp_path):
    (tmp_path / "data_description.json").write_text(
        json.dumps({"investigators": [{"name": "Jane Doe"}, {"name": "John Roe"}]})
    )
    assert derive_experimenters(input_dir=tmp_path) == ["Jane Doe", "John Roe"]


def test_experimenters_from_env(tmp_path, monkeypatch):
    monkeypatch.setenv("AIND_EXPERIMENTERS", "Alice ; Bob,Carol")
    assert derive_experimenters(input_dir=tmp_path) == ["Alice", "Bob", "Carol"]


def test_experimenters_empty_last_resort(tmp_path, monkeypatch):
    monkeypatch.delenv("AIND_EXPERIMENTERS", raising=False)
    monkeypatch.delenv("PROCESSOR_FULL_NAME", raising=False)
    assert derive_experimenters(input_dir=tmp_path) == []


# --- forward_metadata -------------------------------------------------------


def test_forward_metadata_copies_known_and_skips_processing(tmp_path):
    src = tmp_path / "data" / "nested"
    src.mkdir(parents=True)
    (src / "subject.json").write_text("{}")
    (src / "data_description.json").write_text("{}")
    (src / "processing.json").write_text("{}")  # must NOT be forwarded
    out = tmp_path / "results"
    written = forward_metadata(tmp_path / "data", out)
    names = {p.name for p in written}
    assert "subject.json" in names
    assert "data_description.json" in names
    assert "processing.json" not in names
    assert "processing.json" not in DEFAULT_FORWARDED_METADATA


# --- processing_step / capsule_step -----------------------------------------


def test_capsule_step_emits_and_returns(tmp_path, monkeypatch):
    monkeypatch.delenv("AIND_EXPERIMENTERS", raising=False)
    out = tmp_path / "results"

    @capsule_step(
        "Image atlas alignment", name="register", input_dir=tmp_path / "data", output_dir=out, code_dir="/nonexistent"
    )
    def run():
        return 42

    assert run() == 42
    loaded = _load(out / "processing.json")
    assert loaded.dependency_graph == {"register": []}
    assert loaded.data_processes[0].process_type == ProcessName.IMAGE_ATLAS_ALIGNMENT


def test_processing_step_records_dynamic_params(tmp_path):
    out = tmp_path / "results"
    with processing_step(
        "weird custom step",
        name="custom",
        input_dir=tmp_path / "data",
        output_dir=out,
        code_dir="/nonexistent",
        parameters={"static": 1},
    ) as step:
        step.parameters = {"static": 1, "dynamic": 2}
        step.notes = "ran with dilate=4"

    loaded = _load(out / "processing.json")
    proc = loaded.data_processes[0]
    assert proc.process_type == ProcessName.OTHER
    assert proc.notes == "ran with dilate=4"  # context notes win over OTHER label
    assert proc.code.parameters.model_dump() == {"static": 1, "dynamic": 2}


def test_other_label_used_as_notes_when_unset(tmp_path):
    out = tmp_path / "results"
    with processing_step(
        "my bespoke step", name="bespoke", input_dir=tmp_path / "data", output_dir=out, code_dir="/nonexistent"
    ):
        pass
    proc = _load(out / "processing.json").data_processes[0]
    assert proc.notes == "my bespoke step"


def test_failed_step_emits_nothing(tmp_path):
    out = tmp_path / "results"

    @capsule_step(
        "Image atlas alignment", name="boom", input_dir=tmp_path / "data", output_dir=out, code_dir="/nonexistent"
    )
    def run():
        raise RuntimeError("kaboom")

    with pytest.raises(RuntimeError, match="kaboom"):
        run()
    assert not (out / "processing.json").exists()


def test_bad_commit_hash_dropped_not_fatal(tmp_path):
    out = tmp_path / "results"
    with processing_step(
        "Image atlas alignment",
        name="reg",
        input_dir=tmp_path / "data",
        output_dir=out,
        code_dir="/nonexistent",
        commit_hash="not-a-hash",
    ):
        pass
    proc = _load(out / "processing.json").data_processes[0]
    assert proc.code.commit_hash is None  # dropped, step still emitted


def test_chain_through_two_steps_builds_graph(tmp_path):
    data1 = tmp_path / "d1"
    out1 = tmp_path / "o1"

    @capsule_step(
        "Skull stripping", name="skull", input_dir=data1, output_dir=out1, code_dir="/nonexistent", experimenters=["E"]
    )
    def step1():
        pass

    step1()

    out2 = tmp_path / "o2"

    @capsule_step(
        "Image atlas alignment",
        name="register",
        input_dir=out1,
        output_dir=out2,
        code_dir="/nonexistent",
        experimenters=["E"],
    )
    def step2():
        pass

    step2()
    loaded = _load(out2 / "processing.json")
    assert loaded.dependency_graph == {"skull": [], "register": ["skull"]}
