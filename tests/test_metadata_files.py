"""Tests for aind_code_ocean_pipeline_utils.metadata_files."""

from __future__ import annotations

import json
from pathlib import Path

from aind_code_ocean_pipeline_utils.metadata_files import (
    DEFAULT_INHERITED_METADATA,
    find_metadata_file,
    forward_metadata,
    read_data_description_fields,
)


def _write(path: Path, body: object = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(body if body is not None else {"ok": True}))


# ------------------------------------------------------------ find_metadata_file --


def test_find_metadata_file_locates_nested(tmp_path: Path):
    _write(tmp_path / "asset" / "subject.json")
    found = find_metadata_file(tmp_path, "subject.json")
    assert found == tmp_path / "asset" / "subject.json"


def test_find_metadata_file_missing_returns_none(tmp_path: Path):
    assert find_metadata_file(tmp_path, "subject.json") is None


def test_find_metadata_file_is_depth_bounded(tmp_path: Path):
    deep = tmp_path / "a" / "b" / "c" / "d" / "e"
    _write(deep / "subject.json")  # well beyond max_depth=2
    assert find_metadata_file(tmp_path, "subject.json", max_depth=2) is None
    assert find_metadata_file(tmp_path, "subject.json", max_depth=8) is not None


def test_find_metadata_file_follows_symlinked_dirs(tmp_path: Path):
    real = tmp_path / "real"
    _write(real / "instrument.json")
    staged = tmp_path / "staged"
    staged.mkdir()
    (staged / "mnt").symlink_to(real, target_is_directory=True)
    assert find_metadata_file(staged, "instrument.json") is not None


# --------------------------------------------------------------- forward_metadata --


def test_forward_metadata_copies_present_skips_absent(tmp_path: Path):
    src = tmp_path / "in"
    _write(src / "asset" / "subject.json", {"subject_id": "42"})
    _write(src / "asset" / "procedures.json")
    # instrument.json and acquisition.json intentionally absent.
    out = tmp_path / "out"
    written = forward_metadata(src, out)
    names = {p.name for p in written}
    assert names == {"subject.json", "procedures.json"}
    assert json.loads((out / "subject.json").read_text())["subject_id"] == "42"


def test_forward_metadata_verbatim_copy(tmp_path: Path):
    src = tmp_path / "in"
    payload = {"anything": [1, {"nested": True}]}
    _write(src / "subject.json", payload)
    out = tmp_path / "out"
    forward_metadata(src, out, names=["subject.json"])
    assert json.loads((out / "subject.json").read_text()) == payload


def test_forward_metadata_custom_names_can_lift_data_description(tmp_path: Path):
    # Pack's lift step: include data_description.json in the names.
    src = tmp_path / "discover_out"
    _write(src / "data_description.json", {"data_level": "derived"})
    out = tmp_path / "asset_root"
    written = forward_metadata(src, out, names=["data_description.json", *DEFAULT_INHERITED_METADATA])
    assert (out / "data_description.json").exists()
    assert [p.name for p in written] == ["data_description.json"]  # only the present one


def test_forward_metadata_empty_when_nothing_found(tmp_path: Path):
    assert forward_metadata(tmp_path / "empty", tmp_path / "out") == []


# ------------------------------------------------ read_data_description_fields --


def test_read_fields_extracts_stable_primitives_ignoring_old_schema(tmp_path: Path):
    # An OLD-schema data_description: extra/renamed fields (modality, platform,
    # input_data_name) and old Organization/Person shapes must be ignored; only the
    # version-stable primitives come back.
    old = {
        "name": "ecephys_791094_2025-10-08_16-48-57",
        "subject_id": "791094",
        "project_name": "Brain Computer Interface",
        "modality": [{"abbreviation": "ecephys"}],
        "input_data_name": "ecephys_791094_2025-10-08_16-48-57",
        "platform": {"abbreviation": "ecephys"},
        "institution": {"abbreviation": "ROR", "registry_name": "Research Organization Registry"},
        "investigators": [{"name": "Jane Doe", "abbreviation": None}, {"name": "John Roe"}],
    }
    p = tmp_path / "data_description.json"
    p.write_text(json.dumps(old))
    fields = read_data_description_fields(p)
    assert fields == {
        "name": "ecephys_791094_2025-10-08_16-48-57",
        "subject_id": "791094",
        "project_name": "Brain Computer Interface",
        "investigator_names": ["Jane Doe", "John Roe"],
    }


def test_read_fields_accepts_a_mapping_directly():
    fields = read_data_description_fields({"subject_id": "42", "investigators": []})
    assert fields["subject_id"] == "42"
    assert fields["name"] is None
    assert fields["investigator_names"] == []


def test_read_fields_investigators_may_be_bare_strings():
    fields = read_data_description_fields({"investigators": ["Ada", {"name": "Bo"}, {}, None]})
    assert fields["investigator_names"] == ["Ada", "Bo"]


def test_read_fields_missing_or_unreadable_source_is_empty(tmp_path: Path):
    missing = read_data_description_fields(tmp_path / "nope.json")
    assert missing == {"name": None, "subject_id": None, "project_name": None, "investigator_names": []}
    (tmp_path / "bad.json").write_text("{ not json")
    assert read_data_description_fields(tmp_path / "bad.json")["subject_id"] is None
    # A non-object JSON body (e.g. a list) yields empty fields, never raises.
    (tmp_path / "list.json").write_text("[1, 2, 3]")
    assert read_data_description_fields(tmp_path / "list.json")["name"] is None
