"""Tests for aind_code_ocean_pipeline_utils.profiling."""

from __future__ import annotations

import json
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

from aind_code_ocean_pipeline_utils import profiling
from aind_code_ocean_pipeline_utils.profiling import StageProfile, machine_info, peak_rss_gb

# ------------------------------------------------------------- peak_rss_gb --


def test_peak_rss_reads_vmhwm(tmp_path: Path):
    status = tmp_path / "status"
    status.write_text("VmPeak:\t 9000000 kB\nVmHWM:\t 2000000 kB\nVmRSS:\t 1000000 kB\n")
    assert peak_rss_gb(str(status)) == round(2_000_000 * 1024 / 1e9, 3)


def test_peak_rss_missing_file(tmp_path: Path):
    assert peak_rss_gb(str(tmp_path / "nope")) is None


@pytest.mark.skipif(not Path("/proc/self/status").exists(), reason="needs /proc")
def test_peak_rss_of_subprocess_is_its_own():
    # A child's ru_maxrss would start at the parent's peak; VmHWM must not.
    hog = b"\x01" * (200 * 2**20)  # filled, so the pages are resident
    code = "from aind_code_ocean_pipeline_utils.profiling import peak_rss_gb; print(peak_rss_gb())"
    child = float(subprocess.run([sys.executable, "-c", code], check=True, capture_output=True, text=True).stdout)
    parent = peak_rss_gb()
    assert parent is not None
    assert child < parent
    del hog


# ------------------------------------------------------------ machine_info --


def test_cpu_model(tmp_path: Path):
    cpuinfo = tmp_path / "cpuinfo"
    cpuinfo.write_text("processor\t: 0\nmodel name\t: AMD EPYC 9R14\nprocessor\t: 1\nmodel name\t: AMD EPYC 9R14\n")
    assert profiling._cpu_model(str(cpuinfo)) == "AMD EPYC 9R14"
    assert profiling._cpu_model(str(tmp_path / "nope")) is None


@pytest.mark.parametrize(("text", "expected"), [("800000 100000\n", 8.0), ("max 100000\n", None), ("junk", None)])
def test_cgroup_cpus(tmp_path: Path, text: str, expected: float | None):
    cpu_max = tmp_path / "cpu.max"
    cpu_max.write_text(text)
    assert profiling._cgroup_cpus(str(cpu_max)) == expected


def test_machine_info_reads_code_ocean_env(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("CO_CPUS", "8")
    monkeypatch.setenv("CO_MEMORY", str(16 * 10**9))
    info = machine_info(query_instance_type=False)
    assert info["co_cpus"] == 8
    assert info["co_memory_gb"] == 16.0
    assert info["instance_type"] is None
    json.dumps(info)


def test_machine_info_without_code_ocean_env(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("CO_CPUS", raising=False)
    monkeypatch.setenv("CO_MEMORY", "not-a-number")
    info = machine_info(query_instance_type=False)
    assert info["co_cpus"] is None
    assert info["co_memory_gb"] is None


class _FakeImds(BaseHTTPRequestHandler):
    def do_PUT(self) -> None:
        self._reply(b"token-123")

    def do_GET(self) -> None:
        ok = self.path.endswith("/instance-type") and self.headers["X-aws-ec2-metadata-token"] == "token-123"
        self._reply(b"c7a.2xlarge" if ok else b"", 200 if ok else 401)

    def _reply(self, body: bytes, status: int = 200) -> None:
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        pass


def test_instance_type_from_imdsv2():
    server = HTTPServer(("127.0.0.1", 0), _FakeImds)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        url = f"http://127.0.0.1:{server.server_port}/latest"
        assert profiling._instance_type(url, timeout=2.0) == "c7a.2xlarge"
    finally:
        server.shutdown()


def test_instance_type_unreachable():
    server = HTTPServer(("127.0.0.1", 0), _FakeImds)
    port = server.server_port
    server.server_close()
    assert profiling._instance_type(f"http://127.0.0.1:{port}/latest", timeout=0.5) is None


# ------------------------------------------------------------ StageProfile --


def _busy(seconds: float) -> None:
    import time

    end = time.process_time() + seconds
    while time.process_time() < end:
        pass


def test_stage_records_timings_and_extras():
    profile = StageProfile(machine={"co_cpus": 8})
    with profile.stage("decode") as extra:
        _busy(0.05)
        extra["n_chunks"] = 3
    rec = profile.stages["decode"]
    assert rec["n_chunks"] == 3
    assert rec["cpu_s"] >= 0.04
    assert rec["wall_s"] >= rec["cpu_s"] * 0.5
    assert rec["cores_busy"] > 0
    assert "error" not in rec


def test_stage_counts_waited_subprocess_cpu():
    profile = StageProfile(machine={})
    code = "import time\nend = time.process_time() + 0.2\nwhile time.process_time() < end: pass"
    with profile.stage("child"):
        subprocess.run([sys.executable, "-c", code], check=True)
    assert profile.stages["child"]["child_cpu_s"] >= 0.15


def test_stage_records_failure_and_reraises():
    profile = StageProfile(machine={})
    with pytest.raises(KeyError), profile.stage("bad"):
        raise KeyError("x")
    assert profile.stages["bad"]["error"] == "KeyError"


def test_duplicate_stage_rejected():
    profile = StageProfile(machine={})
    with profile.stage("a"):
        pass
    with pytest.raises(ValueError, match="already recorded"), profile.stage("a"):
        pass


def test_write_keeps_stage_order(tmp_path: Path):
    profile = StageProfile(machine={"co_cpus": 16})
    for name in ("fetch", "decode", "build"):
        with profile.stage(name):
            pass
    out = tmp_path / "profile.json"
    profile.write(out, extra={"window_s": 120})
    report = json.loads(out.read_text())
    assert list(report["stages"]) == ["fetch", "decode", "build"]
    assert report["machine"] == {"co_cpus": 16}
    assert report["window_s"] == 120


def test_default_machine_is_gathered(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(profiling, "_instance_type", lambda: "c7a.4xlarge")
    assert StageProfile().machine["instance_type"] == "c7a.4xlarge"
