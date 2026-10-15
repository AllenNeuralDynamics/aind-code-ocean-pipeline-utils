"""Tests for aind_code_ocean_pipeline_utils.metadata_files."""

from __future__ import annotations

import json
from pathlib import Path

from aind_code_ocean_pipeline_utils.metadata_files import (
    DEFAULT_ARCHIVE_DIR,
    DEFAULT_INHERITED_METADATA,
    InheritedMetadata,
    find_metadata_file,
    forward_metadata,
    inherit_metadata,
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
    # instrument/acquisition and their pre-v2 names intentionally absent.
    out = tmp_path / "out"
    written = forward_metadata(src, out)
    names = {p.name for p in written}
    assert names == {"subject.json", "procedures.json"}
    assert json.loads((out / "subject.json").read_text())["subject_id"] == "42"


def test_forward_metadata_forwards_pre_v2_names(tmp_path: Path):
    src = tmp_path / "in"
    for name in ("subject.json", "procedures.json", "rig.json", "session.json"):
        _write(src / "asset" / name)
    written = forward_metadata(src, tmp_path / "out")
    assert {p.name for p in written} == {"subject.json", "procedures.json", "rig.json", "session.json"}


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
        "modality_abbreviations": ["ecephys"],
    }


def test_read_fields_accepts_a_mapping_directly():
    fields = read_data_description_fields({"subject_id": "42", "investigators": []})
    assert fields["subject_id"] == "42"
    assert fields["name"] is None
    assert fields["investigator_names"] == []


def test_read_fields_investigators_may_be_bare_strings():
    fields = read_data_description_fields({"investigators": ["Ada", {"name": "Bo"}, {}, None]})
    assert fields["investigator_names"] == ["Ada", "Bo"]


def test_read_fields_modalities_skip_entries_without_an_abbreviation():
    fields = read_data_description_fields({"modalities": [{"abbreviation": "SPIM"}, {"name": "x"}, "ecephys"]})
    assert fields["modality_abbreviations"] == ["SPIM"]


def test_read_fields_missing_or_unreadable_source_is_empty(tmp_path: Path):
    missing = read_data_description_fields(tmp_path / "nope.json")
    assert missing == {
        "name": None,
        "subject_id": None,
        "project_name": None,
        "investigator_names": [],
        "modality_abbreviations": [],
    }
    (tmp_path / "bad.json").write_text("{ not json")
    assert read_data_description_fields(tmp_path / "bad.json")["subject_id"] is None
    # A non-object JSON body (e.g. a list) yields empty fields, never raises.
    (tmp_path / "list.json").write_text("[1, 2, 3]")
    assert read_data_description_fields(tmp_path / "list.json")["name"] is None


# --------------------------------------------------------------- inherit_metadata --


def _source(root: Path, mount: str, subject: str, raw: str, files: tuple[str, ...] = DEFAULT_INHERITED_METADATA[:4]):
    for name in files:
        _write(
            root / mount / name, {"subject_id": subject, "from": mount} if name == "subject.json" else {"from": mount}
        )
    _write(root / mount / "data_description.json", {"name": raw, "subject_id": subject})
    return root / mount


RAW_A = "ecephys_100_2025-01-01_00-00-00"
RAW_B = "ecephys_200_2025-02-02_00-00-00"


def _from(path: Path) -> str:
    return json.loads(path.read_text())["from"]


def test_inherit_archives_every_source_and_roots_one(tmp_path: Path):
    sources = [_source(tmp_path / "data", m, s, r) for m, s, r in (("b", "200", RAW_B), ("a", "100", RAW_A))]
    out = tmp_path / "results"
    result = inherit_metadata(sources, out)
    assert result.sources == ["a", "b"]
    assert result.root_source == "a"
    for key in ("a", "b"):
        assert {p.name for p in (out / DEFAULT_ARCHIVE_DIR / key).iterdir()} == {
            *DEFAULT_INHERITED_METADATA[:4],
            "data_description.json",
        }
    assert {_from(p) for p in result.root} == {"a"}  # every root file from one source
    assert not (out / "data_description.json").exists()


def test_inherit_roots_the_most_complete_source(tmp_path: Path):
    sparse = _source(tmp_path, "a", "100", RAW_A, files=("subject.json",))
    full = _source(tmp_path, "b", "100", RAW_A)
    result = inherit_metadata([sparse, full], tmp_path / "out")
    assert result.root_source == "b"


def test_drop_omits_subject_and_acquisition_files_across_subjects(tmp_path: Path):
    sources = [_source(tmp_path, "a", "100", RAW_A), _source(tmp_path, "b", "200", RAW_B)]
    result = inherit_metadata(sources, tmp_path / "out", root="drop")
    assert result.root == []
    assert len(list((tmp_path / "out" / DEFAULT_ARCHIVE_DIR).iterdir())) == 2


def test_drop_keeps_subject_files_for_one_subject_over_two_acquisitions(tmp_path: Path):
    other_session = "ecephys_100_2025-03-03_00-00-00"
    sources = [_source(tmp_path, "a", "100", RAW_A), _source(tmp_path, "b", "100", other_session)]
    result = inherit_metadata(sources, tmp_path / "out", root="drop")
    assert {p.name for p in result.root} == {"subject.json", "procedures.json"}


def test_drop_keeps_everything_for_derived_assets_of_one_acquisition(tmp_path: Path):
    sources = [
        _source(tmp_path, "probe1", "100", f"{RAW_A}_sorted_2025-01-02_00-00-00"),
        _source(tmp_path, "probe2", "100", f"{RAW_A}_sorted_2025-01-03_00-00-00"),
    ]
    result = inherit_metadata(sources, tmp_path / "out", root="drop")
    assert {p.name for p in result.root} == set(DEFAULT_INHERITED_METADATA[:4])


def test_inherit_carries_an_upstream_archive_without_its_root_copies(tmp_path: Path):
    sources = [_source(tmp_path / "data", "a", "100", RAW_A), _source(tmp_path / "data", "b", "200", RAW_B)]
    upstream = tmp_path / "node1"
    inherit_metadata(sources, upstream)
    _write(upstream / "data_description.json", {"name": f"{RAW_A}_sorted_2025-01-02_00-00-00"})
    result = inherit_metadata([upstream], tmp_path / "node2")
    assert result.sources == ["a", "b"]
    assert result.root_source == "a"


def test_inherit_skips_reference_assets_without_inherited_files(tmp_path: Path):
    _write(tmp_path / "ccf" / "data_description.json", {"name": "ccf"})
    ancestor = _source(tmp_path, "a", "100", RAW_A)
    result = inherit_metadata([tmp_path / "ccf", ancestor], tmp_path / "out")
    assert result.sources == ["a"]


def test_inherit_with_no_sources_writes_nothing(tmp_path: Path):
    result = inherit_metadata([tmp_path / "missing"], tmp_path / "out")
    assert result == InheritedMetadata(sources=[], root=[], root_source=None)
    assert not (tmp_path / "out").exists()


def test_find_metadata_file_walks_in_sorted_order(tmp_path: Path):
    for d in ("zz", "aa", "mm"):
        _write(tmp_path / d / "subject.json")
    assert find_metadata_file(tmp_path, "subject.json") == tmp_path / "aa" / "subject.json"


def test_archive_is_not_read_as_provenance_shards(tmp_path: Path):
    from aind_code_ocean_pipeline_utils.records import read_records

    ancestor = tmp_path / "data" / "a"
    _write(ancestor / "subject.json", {"node": "looks-like-a-shard", "subject_id": "100"})
    _write(ancestor / "data_description.json", {"name": RAW_A})
    _write(tmp_path / "results" / "provenance" / "step.json", {"node": "step"})
    inherit_metadata([ancestor], tmp_path / "results")
    assert (tmp_path / "results" / "provenance" / "metadata" / "a" / "subject.json").exists()
    assert [r["node"] for r in read_records(tmp_path / "results")] == ["step"]
