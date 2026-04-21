"""Tests for aind_code_ocean_pipeline_utils.threading_utils."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar

import pytest

from aind_code_ocean_pipeline_utils.threading_utils import submit_with_context

_mode: ContextVar[str] = ContextVar("mode")


def test_returns_fn_result():
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = submit_with_context(pool, lambda x, y: x + y, 2, 3)
        assert future.result() == 5


def test_propagates_kwargs():
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = submit_with_context(pool, lambda *, x, y: x * y, x=3, y=7)
        assert future.result() == 21


def test_context_var_visible_in_worker():
    _mode.set("from_caller")
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = submit_with_context(pool, _mode.get)
        assert future.result() == "from_caller"


def test_bare_submit_does_not_see_caller_context():
    """Regression fence: stdlib submit behaves differently than ours."""
    _mode.set("from_caller")
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(lambda: _mode.get("default_value"))
        # The worker thread starts with a fresh, empty context.
        assert future.result() == "default_value"


def test_post_submit_mutation_does_not_leak_into_worker():
    """Fresh copy captures value at submit time, not at execution time."""
    _mode.set("first")
    started = threading.Event()
    release = threading.Event()

    def worker() -> str:
        started.set()
        release.wait(timeout=2)
        return _mode.get()

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = submit_with_context(pool, worker)
        started.wait(timeout=2)
        _mode.set("second")  # must NOT affect the in-flight worker
        release.set()
        assert future.result(timeout=5) == "first"


def test_concurrent_submits_with_different_values_do_not_collide():
    """Per-submit copy is the whole point: a shared Context would deadlock."""
    n = 8
    results: list[str] = [""] * n

    def make_worker(i: int):
        def worker() -> str:
            return f"{_mode.get()}:{i}"

        return worker

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = []
        for i in range(n):
            _mode.set(f"caller_{i}")
            futures.append((i, submit_with_context(pool, make_worker(i))))

        for i, future in futures:
            results[i] = future.result(timeout=5)

    for i, result in enumerate(results):
        assert result == f"caller_{i}:{i}"


def test_exception_propagates_through_future():
    def boom() -> None:
        raise ValueError("kaboom")

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = submit_with_context(pool, boom)
        with pytest.raises(ValueError, match="kaboom"):
            future.result()


def test_context_isolation_between_workers():
    """Each worker sees an independent copy; worker mutations don't leak out."""
    _mode.set("original")

    def mutator() -> str:
        _mode.set("worker_mutated")
        return _mode.get()

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = submit_with_context(pool, mutator)
        assert future.result() == "worker_mutated"

    # Caller's Context is untouched.
    assert _mode.get() == "original"
