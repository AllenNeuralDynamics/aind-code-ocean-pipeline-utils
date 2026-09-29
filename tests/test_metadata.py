"""Tests for the optional ``metadata`` module (requires ``[metadata]`` extra)."""

import json
import warnings
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
    assemble_processing,
    make_data_process,
    make_derived_data_description,
    utcnow,
    write_assembled_processing,
    write_data_description,
    write_processing,
)
from aind_code_ocean_pipeline_utils.records import make_record, record_step, write_record

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


# ------------------------------------------------------------- assembly --


@pytest.fixture(autouse=True)
def _quiet_code_warning():
    # aind-data-schema warns on every Code without commit_hash/version; irrelevant here.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        yield


def step(node: str, incoming, output, **kwargs):
    kwargs.setdefault("process_type", "Spike sorting")
    kwargs.setdefault("code_url", URL)
    with record_step(node, incoming_dir=incoming, output_dir=output, **kwargs) as ctx:
        pass
    return ctx


def by_name(processing: Processing) -> dict[str, DataProcess]:
    return {p.name: p for p in processing.data_processes}


def test_assemble_linear_chain_orders_parents_first(tmp_path):
    step("A", tmp_path / "none", tmp_path / "a")
    step("B", tmp_path / "a", tmp_path / "b")
    step("C", tmp_path / "b", tmp_path / "c")
    processing = assemble_processing(tmp_path / "c")
    assert [p.name for p in processing.data_processes] == ["A", "B", "C"]
    assert processing.dependency_graph == {"A": [], "B": ["A"], "C": ["B"]}


def test_assemble_fan_in_and_output_dir(tmp_path):
    # Two branches reach a terminal that also records its own step in output_dir.
    step("left", tmp_path / "none", tmp_path / "data" / "left")
    step("right", tmp_path / "none", tmp_path / "data" / "right")
    step("merge", tmp_path / "data", tmp_path / "results")
    processing = assemble_processing(tmp_path / "data", tmp_path / "results")
    assert processing.dependency_graph == {"left": [], "right": [], "merge": ["left", "right"]}


def test_assemble_fills_holes_with_placeholders(tmp_path):
    out = tmp_path / "out"
    write_record(make_record("worker", parents=["launcher", "unrecorded"]), out)
    step("sink", out, tmp_path / "final")
    processing = assemble_processing(tmp_path / "final")
    processes = by_name(processing)
    assert processing.dependency_graph == {
        "launcher": [],
        "unrecorded": [],
        "worker": ["launcher", "unrecorded"],
        "sink": ["worker"],
    }
    assert processes["unrecorded"].process_type == ProcessName.OTHER
    assert "no provenance was recorded" in processes["unrecorded"].notes
    assert "no DataProcess" in processes["worker"].notes
    # A placeholder's time is not invented from the clock when real steps exist.
    assert processes["unrecorded"].start_date_time == processes["sink"].start_date_time


def test_assemble_fanout_without_launcher_edge(tmp_path):
    # Only the workers reach the terminal: the launcher arrives as a stub and becomes
    # a placeholder, but its upstream X arrives in full.
    step("X", tmp_path / "none", tmp_path / "x")
    launcher_out = tmp_path / "launcher"
    with record_step("L", process_type="Other", incoming_dir=tmp_path / "x", output_dir=launcher_out) as launcher:
        for shard in launcher.fanout_shards():
            write_record(shard, launcher_out / "stream_1")
    step("W", launcher_out / "stream_1", tmp_path / "data" / "w")
    processing = assemble_processing(tmp_path / "data")
    assert processing.dependency_graph == {"X": [], "L": ["X"], "W": ["L"]}
    processes = by_name(processing)
    assert processes["X"].process_type == ProcessName.SPIKE_SORTING
    assert "Placeholder" in processes["L"].notes


def written_processing(directory, *processes: DataProcess, graph=None, **kwargs):
    processing = Processing(data_processes=list(processes), dependency_graph=graph, **kwargs)
    write_processing(processing, directory)
    return processing


def test_assemble_joins_upstream_processing_json_to_recorded_steps(tmp_path):
    # A stock capsule wrote processing.json; our capsule used record_step downstream.
    data = tmp_path / "data"
    written_processing(data / "sorted", dp("sort"), dp("curate"), graph={"sort": [], "curate": ["sort"]})
    step("pack", data, tmp_path / "results")
    processing = assemble_processing(data, tmp_path / "results")
    assert processing.dependency_graph == {"sort": [], "curate": ["sort"], "pack": ["curate"]}
    assert by_name(processing)["sort"].experimenters == ["tester"]


def test_assemble_terminal_only_reads_processing_json(tmp_path):
    # No node opted in: the terminal alone reassembles what upstream capsules wrote.
    data = tmp_path / "data"
    written_processing(data / "sorted", dp("sort"), graph={"sort": []})
    processing = assemble_processing(data)
    assert processing.dependency_graph == {"sort": []}


def test_assemble_keeps_digest_only_for_colliding_names(tmp_path):
    data = tmp_path / "data"
    written_processing(data / "session_a", dp("sort"), graph={"sort": []}, notes="a")
    written_processing(data / "session_b", dp("sort"), graph={"sort": []}, notes="b")
    written_processing(data / "session_b_extra", dp("unique"), graph={"unique": []})
    names = set(by_name(assemble_processing(data)))
    assert "unique" in names
    colliding = names - {"unique"}
    assert len(colliding) == 2
    assert all(name.startswith("sort@") for name in colliding)


def test_assemble_placeholders_an_unparseable_v1_payload(tmp_path):
    v1 = {
        "schema_version": "1.1.3",
        "processing_pipeline": {
            "data_processes": [
                {"name": "Spike sorting", "start_date_time": "2024-01-02T03:04:05+00:00", "software_version": "1"}
            ]
        },
    }
    (tmp_path / "asset").mkdir()
    (tmp_path / "asset" / "processing.json").write_text(json.dumps(v1))
    (process,) = assemble_processing(tmp_path).data_processes
    assert process.name == "Spike sorting"
    assert process.process_type == ProcessName.SPIKE_SORTING
    assert process.start_date_time == datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)
    assert "failed validation" in process.notes


def test_assemble_back_fills_run_experimenters_on_own_steps_only(tmp_path):
    data = tmp_path / "data"
    written_processing(data / "sorted", dp("sort"), graph={"sort": []})
    upstream = make_data_process(
        process_type=ProcessName.OTHER, code_url=URL, experimenters=[], start=utcnow(), name="foreign", notes="n"
    )
    written_processing(data / "other", upstream, graph={"foreign": []})
    step("launch", data, tmp_path / "l", run_experimenters=["Ada"])
    step("work", tmp_path / "l", tmp_path / "w", experimenters=["Grace"])
    step("idle", tmp_path / "w", tmp_path / "i")
    processes = by_name(assemble_processing(tmp_path / "i"))
    assert processes["launch"].experimenters == ["Ada"]
    assert processes["work"].experimenters == ["Grace"]  # per-step value wins
    assert processes["idle"].experimenters == ["Ada"]
    assert processes["foreign"].experimenters == []  # another run's step is left alone


def test_assemble_records_run_pipeline(tmp_path):
    pipeline = {"name": "my-pipeline", "code": {"url": "https://example.com/pipeline", "version": "1.0"}}
    step("launch", tmp_path / "none", tmp_path / "l", pipeline=pipeline)
    step("work", tmp_path / "l", tmp_path / "w")
    processing = assemble_processing(tmp_path / "w")
    assert [p.name for p in processing.pipelines] == ["my-pipeline"]
    assert processing.pipelines[0].url == "https://example.com/pipeline"
    assert {p.pipeline_name for p in processing.data_processes} == {"my-pipeline"}


def test_assemble_unlinks_a_pipeline_without_usable_code(tmp_path):
    step("launch", tmp_path / "none", tmp_path / "l", pipeline={"name": "no-code"})
    processing = assemble_processing(tmp_path / "l")
    assert processing.pipelines is None
    assert processing.data_processes[0].pipeline_name is None


def test_write_assembled_processing_round_trips_and_is_rerunnable(tmp_path):
    data, results = tmp_path / "data", tmp_path / "results"
    written_processing(data / "sorted", dp("sort"), graph={"sort": []})
    step("pack", data, results)
    path = write_assembled_processing(data, results)
    assert path == results / "processing.json"
    first = json.loads(path.read_text())
    # The written file sits beside results/provenance, so a second call must not re-read it.
    write_assembled_processing(data, results)
    second = json.loads(path.read_text())
    assert first["dependency_graph"] == second["dependency_graph"] == {"sort": [], "pack": ["sort"]}


def test_write_assembled_processing_never_raises(tmp_path, monkeypatch):
    def _boom(*_a, **_k):
        raise RuntimeError("schema exploded")

    monkeypatch.setattr("aind_code_ocean_pipeline_utils.metadata.assemble_processing", _boom)
    assert write_assembled_processing(tmp_path, tmp_path / "out") is None
