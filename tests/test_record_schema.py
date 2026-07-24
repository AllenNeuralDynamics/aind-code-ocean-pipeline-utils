"""Tests for the versioned provenance-record JSON Schema artifact."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

# jsonschema is a dev-only test dependency (not a runtime extra); skip cleanly when
# absent, e.g. in the CI job that runs against the installed wheel without dev deps.
pytest.importorskip("jsonschema")

from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError

from aind_code_ocean_pipeline_utils.record_schema import (
    RECORD_VERSION,
    SCHEMA_ARTIFACT_RELPATH,
    SCHEMA_VERSION,
    build_record_schema,
    record_schema,
)
from aind_code_ocean_pipeline_utils.records import make_record

_ARTIFACT = Path(__file__).resolve().parents[1] / "src" / "aind_code_ocean_pipeline_utils" / SCHEMA_ARTIFACT_RELPATH


def _validator() -> Draft202012Validator:
    schema = build_record_schema()
    Draft202012Validator.check_schema(schema)  # the schema is itself valid draft 2020-12
    return Draft202012Validator(schema)


# ------------------------------------------------------------- schema itself --


def test_schema_is_valid_draft_2020_12():
    Draft202012Validator.check_schema(build_record_schema())


def test_schema_version_is_semver_and_decoupled_from_package():
    # Three parts, and NOT read from the package version — its own axis.
    assert len(SCHEMA_VERSION.split(".")) == 3
    assert SCHEMA_VERSION.startswith(f"{RECORD_VERSION}.")  # major matches the wire 'v'


def test_artifact_matches_builder():
    # Drift guard: the checked-in file must equal the source-of-truth builder.
    # Re-run `uv run python scripts/export_record_schema.py` if this fails.
    assert json.loads(_ARTIFACT.read_text()) == build_record_schema()


def test_loader_returns_same_as_builder():
    assert record_schema() == build_record_schema()


def test_artifact_lives_under_versioned_directory():
    assert f"v{RECORD_VERSION}" in SCHEMA_ARTIFACT_RELPATH
    assert _ARTIFACT.exists()


# ---------------------------------------------------- make_record conformance --


def test_minimal_make_record_validates():
    _validator().validate(make_record("a"))


def test_full_make_record_validates():
    record = make_record(
        "b",
        parents=["a"],
        data_process={"name": "b", "anything": [1, 2]},
        data_process_schema_version="2.2.2",
        pipeline={"name": "pipe", "code": {"url": "x"}},
        experimenters=["Jane"],
    )
    _validator().validate(record)


def test_opaque_data_process_accepts_arbitrary_object():
    # The payload is opaque: any object shape validates (we never parse it).
    record = make_record("c", data_process={"whatever": {"nested": True}, "x": 1})
    _validator().validate(record)


def test_additive_unknown_key_is_allowed():
    # additionalProperties: true -> forward-compatible for vendored copies.
    record = make_record("d")
    record["some_future_key"] = {"added": "later"}
    _validator().validate(record)


# ------------------------------------------------------------- rejection cases --


def test_missing_node_is_rejected():
    with pytest.raises(ValidationError):
        _validator().validate({"v": RECORD_VERSION, "parents": []})


def test_empty_node_is_rejected():
    with pytest.raises(ValidationError):
        _validator().validate({"v": RECORD_VERSION, "node": "", "parents": []})


def test_wrong_wire_version_is_rejected():
    with pytest.raises(ValidationError):
        _validator().validate({"v": 999, "node": "a", "parents": []})


def test_non_string_parent_is_rejected():
    with pytest.raises(ValidationError):
        _validator().validate({"v": RECORD_VERSION, "node": "a", "parents": [1]})


def test_pipeline_requires_name():
    with pytest.raises(ValidationError):
        _validator().validate({"v": RECORD_VERSION, "node": "a", "parents": [], "pipeline": {"code": {}}})
