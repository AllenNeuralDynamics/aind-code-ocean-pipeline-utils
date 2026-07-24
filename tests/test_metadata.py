"""Tests for the optional ``metadata`` module (requires ``[metadata]`` extra)."""

import json
from datetime import UTC, datetime

import pytest

pytest.importorskip("aind_data_schema")

from aind_data_schema.components.identifiers import Person
from aind_data_schema.core.data_description import DataDescription, Funding
from aind_data_schema.core.processing import (
    DataProcess,
    Processing,
    ProcessName,
)
from aind_data_schema_models.data_name_patterns import DataLevel
from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.organizations import Organization

from aind_code_ocean_pipeline_utils.metadata import (
    make_data_process,
    make_derived_data_description,
    utcnow,
    write_data_description,
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


def test_make_data_process_records_experimenters_and_parameters():
    proc = make_data_process(
        process_type=ProcessName.OTHER,
        code_url=URL,
        experimenters=["a", "b"],
        start=utcnow(),
        name="p",
        parameters={"k": 1},
        notes="other requires notes",
    )
    assert proc.experimenters == ["a", "b"]
    assert proc.code.parameters is not None


def test_make_data_process_empty_experimenters_is_valid():
    # experimenters is required but has no min_length -> [] validates.
    proc = make_data_process(
        process_type=ProcessName.OTHER,
        code_url=URL,
        experimenters=[],
        start=utcnow(),
        name="p",
        notes="n",
    )
    assert proc.experimenters == []


def raw_dd() -> DataDescription:
    # A minimal valid RAW data description (mirrors the aind-data-schema example).
    return DataDescription(
        modalities=[Modality.ECEPHYS],
        subject_id="123456",
        creation_time=datetime(2022, 2, 21, 16, 30, 1, tzinfo=UTC),
        institution=Organization.AIND,
        investigators=[Person(name="Jane Doe")],
        funding_source=[Funding(funder=Organization.AI)],
        project_name="Example project",
        data_level=DataLevel.RAW,
    )


def test_make_derived_data_description_inherits_and_marks_derived():
    parent = raw_dd()
    derived = make_derived_data_description(parent, "ibl-preprocess")
    assert derived.data_level == DataLevel.DERIVED
    assert derived.institution == parent.institution  # inherited
    assert [p.name for p in derived.investigators] == ["Jane Doe"]
    assert derived.subject_id == "123456"
    assert derived.source_data == [parent.name]  # defaults to parent name


def test_make_derived_data_description_accepts_dict_parent():
    parent_dict = raw_dd().model_dump(mode="json")
    derived = make_derived_data_description(parent_dict, "ibl-preprocess", source_data=["asset-A"])
    assert derived.data_level == DataLevel.DERIVED
    assert derived.source_data == ["asset-A"]


def test_write_data_description_round_trip(tmp_path):
    derived = make_derived_data_description(raw_dd(), "ibl-preprocess")
    path = write_data_description(derived, tmp_path / "out")
    assert path == tmp_path / "out" / "data_description.json"
    loaded = DataDescription.model_validate(json.loads(path.read_text()))
    assert loaded.data_level == DataLevel.DERIVED


def test_write_processing_round_trip(tmp_path):
    processing = Processing(
        data_processes=[dp("preprocess"), dp("register")],
        dependency_graph={"preprocess": [], "register": ["preprocess"]},
    )
    path = write_processing(processing, tmp_path / "out")
    assert path == tmp_path / "out" / "processing.json"
    loaded = Processing.model_validate(json.loads(path.read_text()))
    assert loaded.dependency_graph == {"preprocess": [], "register": ["preprocess"]}
    assert isinstance(loaded.data_processes[0], DataProcess)
