"""Stage timings and machine facts for benchmarking a capsule run.

:class:`StageProfile` times named stages of a run (wall, CPU, cores busy, peak
memory) and writes them with :func:`machine_info` into one JSON report, so every
result says which machine produced it. The Code Ocean UI lets the machine change
between runs without leaving a trace in the run itself.

Stdlib-only. Readers of ``/proc`` and cgroup files return ``None`` where those
files are missing (macOS, Windows), so the helpers also run on a laptop.
"""

from __future__ import annotations

import contextlib
import os
import platform
import resource
import time
import urllib.request
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

from .diagnostics import _CGROUP_LIMIT_PATHS, _NO_LIMIT_SENTINEL, _PROC_STATUS, _read_cgroup_limit_bytes
from .io import atomic_json_write

__all__ = [
    "StageProfile",
    "machine_info",
    "peak_rss_gb",
]

_CPUINFO = "/proc/cpuinfo"
_CGROUP_CPU_MAX = "/sys/fs/cgroup/cpu.max"
_IMDS_URL = "http://169.254.169.254/latest"
# IMDS answers in milliseconds when reachable; a container that can't reach it
# (hop limit 1) should cost machine_info() at most this per request.
_IMDS_TIMEOUT_S = 0.5


def _read_status_kb(field: str, status_path: str = _PROC_STATUS) -> int | None:
    """Read one ``kB`` field from ``status_path``; return None on any failure."""
    try:
        with open(status_path) as f:
            for line in f:
                if line.startswith(f"{field}:"):
                    return int(line.split()[1])
    except (OSError, ValueError):
        return None
    return None


def peak_rss_gb(status_path: str = _PROC_STATUS) -> float | None:
    """Peak resident memory of this process so far, in GB, or None off Linux.

    Reads ``VmHWM`` rather than ``getrusage().ru_maxrss``: Linux carries
    ``ru_maxrss`` across ``fork`` and ``execve``, so a subprocess reports its
    parent's peak until it exceeds it.

    Parameters
    ----------
    status_path : str
        Override for ``/proc/self/status``. Primarily a test seam.

    Returns
    -------
    float or None
        Peak resident set size in GB (1e9 bytes).
    """
    hwm_kb = _read_status_kb("VmHWM", status_path)
    return None if hwm_kb is None else round(hwm_kb * 1024 / 1e9, 3)


def _cpu_model(cpuinfo_path: str = _CPUINFO) -> str | None:
    """First ``model name`` in ``cpuinfo_path``, or None."""
    try:
        with open(cpuinfo_path) as f:
            for line in f:
                if line.startswith("model name"):
                    return line.split(":", 1)[1].strip()
    except OSError:
        return None
    return None


def _cgroup_cpus(cpu_max_path: str = _CGROUP_CPU_MAX) -> float | None:
    """CPUs allowed by the cgroup v2 quota (``quota / period``), or None if unlimited."""
    try:
        with open(cpu_max_path) as f:
            quota, period = f.read().split()
        return None if quota == "max" else round(int(quota) / int(period), 2)
    except (OSError, ValueError):
        return None


def _instance_type(base_url: str = _IMDS_URL, timeout: float = _IMDS_TIMEOUT_S) -> str | None:
    """EC2 instance type from IMDSv2, or None when the metadata service is unreachable."""
    # IMDS is link-local; an http_proxy from the environment would only send it astray.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        token_request = urllib.request.Request(
            f"{base_url}/api/token", method="PUT", headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"}
        )
        with opener.open(token_request, timeout=timeout) as response:
            token = response.read().decode()
        type_request = urllib.request.Request(
            f"{base_url}/meta-data/instance-type", headers={"X-aws-ec2-metadata-token": token}
        )
        with opener.open(type_request, timeout=timeout) as response:
            return str(response.read().decode().strip()) or None
    except (OSError, ValueError):
        return None


def _env_int(name: str) -> int | None:
    try:
        return int(os.environ[name])
    except (KeyError, ValueError):
        return None


def machine_info(*, query_instance_type: bool = True) -> dict[str, Any]:
    """Describe the machine and container this process runs on.

    Parameters
    ----------
    query_instance_type : bool, default True
        Ask the EC2 metadata service for the instance type. Off EC2, or where a
        container can't reach the service, this costs up to about a second and
        yields None.

    Returns
    -------
    dict
        ``cpu_count`` (host), ``sched_cpus`` (this process's affinity),
        ``cgroup_cpus`` and ``cgroup_memory_gb`` (container limits, None when
        unlimited), ``co_cpus`` and ``co_memory_gb`` (Code Ocean's ``CO_CPUS`` /
        ``CO_MEMORY``), ``cpu_model``, ``instance_type``, ``python``.
    """
    sched_cpus = len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None
    memory_limit = _read_cgroup_limit_bytes(_CGROUP_LIMIT_PATHS)
    if memory_limit is not None and memory_limit >= _NO_LIMIT_SENTINEL:
        memory_limit = None
    co_memory = _env_int("CO_MEMORY")
    return {
        "cpu_count": os.cpu_count(),
        "sched_cpus": sched_cpus,
        "cgroup_cpus": _cgroup_cpus(),
        "cgroup_memory_gb": None if memory_limit is None else round(memory_limit / 1e9, 2),
        "co_cpus": _env_int("CO_CPUS"),
        "co_memory_gb": None if co_memory is None else round(co_memory / 1e9, 2),
        "cpu_model": _cpu_model(),
        "instance_type": _instance_type() if query_instance_type else None,
        "python": platform.python_version(),
    }


def _children_cpu_s() -> float:
    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    return usage.ru_utime + usage.ru_stime


class StageProfile:
    """Time named stages of a run and write them, with the machine, as one report.

    Each :meth:`stage` records wall time, CPU time of this process (all threads)
    and of the subprocesses it waited for during the stage, ``cores_busy`` (their
    CPU over wall), and ``peak_rss_gb``. Peak memory is the process high-water mark
    so far, so it only grows from one stage to the next; run a stage in its own
    process to measure it alone.

    Parameters
    ----------
    machine : Mapping, optional
        Machine description for the report. Defaults to :func:`machine_info`,
        gathered once when the profile is created.

    Examples
    --------
    >>> profile = StageProfile(machine={})
    >>> with profile.stage("decode") as extra:
    ...     extra["n_chunks"] = 120
    >>> sorted(profile.stages["decode"])
    ['began', 'child_cpu_s', 'cores_busy', 'cpu_s', 'n_chunks', 'peak_rss_gb', 'wall_s']
    """

    def __init__(self, machine: Mapping[str, Any] | None = None) -> None:
        self.machine: dict[str, Any] = dict(machine) if machine is not None else machine_info()
        self.stages: dict[str, dict[str, Any]] = {}

    @contextlib.contextmanager
    def stage(self, name: str) -> Iterator[dict[str, Any]]:
        """Time the ``with`` block as stage ``name``.

        Yields a dict the block can fill with its own figures (rates, sizes); they
        are stored beside the timings. A block that raises is still recorded, with
        ``error`` set to the exception's type name, and the exception propagates.

        Parameters
        ----------
        name : str
            Stage name, unique within this profile.

        Yields
        ------
        dict
            Extra fields to store with the stage.

        Raises
        ------
        ValueError
            If ``name`` was already recorded.
        """
        if name in self.stages:
            msg = f"stage {name!r} already recorded"
            raise ValueError(msg)
        extra: dict[str, Any] = {}
        error: str | None = None
        began = time.time()
        wall0, cpu0, child0 = time.monotonic(), time.process_time(), _children_cpu_s()
        try:
            yield extra
        except BaseException as exc:
            error = type(exc).__name__
            raise
        finally:
            wall = time.monotonic() - wall0
            cpu = time.process_time() - cpu0
            child_cpu = _children_cpu_s() - child0
            record: dict[str, Any] = {
                "began": round(began, 3),
                "wall_s": round(wall, 3),
                "cpu_s": round(cpu, 3),
                "child_cpu_s": round(child_cpu, 3),
                "cores_busy": round((cpu + child_cpu) / wall, 2) if wall > 0 else None,
                "peak_rss_gb": peak_rss_gb(),
            }
            if error is not None:
                record["error"] = error
            self.stages[name] = record | extra

    def report(self, extra: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Return the report: ``machine``, ``stages`` in run order, and any ``extra`` keys.

        Parameters
        ----------
        extra : Mapping, optional
            Top-level fields to add, such as the run's configuration.

        Returns
        -------
        dict
            JSON-serializable report.
        """
        return {"machine": self.machine, "stages": self.stages, **(extra or {})}

    def write(self, path: Path | str, extra: Mapping[str, Any] | None = None) -> None:
        """Write :meth:`report` to ``path`` atomically, keeping stages in run order.

        Parameters
        ----------
        path : Path or str
            Destination, typically under ``/results``.
        extra : Mapping, optional
            Passed to :meth:`report`.
        """
        atomic_json_write(path, self.report(extra), sort_keys=False)
