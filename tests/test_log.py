"""Tests for aind_code_ocean_pipeline_utils.log."""

from __future__ import annotations

import logging

import pytest

rich = pytest.importorskip("rich")
from rich.console import Console  # noqa: E402
from rich.logging import RichHandler  # noqa: E402
from rich.progress import Progress  # noqa: E402

from aind_code_ocean_pipeline_utils.log import (  # noqa: E402
    build_progress,
    install_rich_handler,
    make_progress_callback,
)


@pytest.fixture
def isolated_logger() -> logging.Logger:
    logger = logging.getLogger("aind_test_log_module")
    logger.handlers.clear()
    logger.setLevel(logging.WARNING)
    return logger


def test_returns_console_when_none_supplied(isolated_logger: logging.Logger):
    returned = install_rich_handler(isolated_logger)
    assert isinstance(returned, Console)


def test_returns_exact_console_when_supplied(isolated_logger: logging.Logger):
    console = Console()
    returned = install_rich_handler(isolated_logger, console=console)
    assert returned is console


def test_installs_rich_handler_on_logger(isolated_logger: logging.Logger):
    install_rich_handler(isolated_logger)
    assert any(isinstance(h, RichHandler) for h in isolated_logger.handlers)


def test_sets_level_on_logger_and_handler(isolated_logger: logging.Logger):
    install_rich_handler(isolated_logger, level=logging.DEBUG)
    assert isolated_logger.level == logging.DEBUG
    rich_handler = next(h for h in isolated_logger.handlers if isinstance(h, RichHandler))
    assert rich_handler.level == logging.DEBUG


def test_handler_defaults_show_path_false_and_rich_tracebacks_true(isolated_logger: logging.Logger):
    install_rich_handler(isolated_logger)
    handler = next(h for h in isolated_logger.handlers if isinstance(h, RichHandler))
    assert handler._log_render.show_path is False
    assert handler.rich_tracebacks is True


def test_idempotent_replaces_prior_handler(isolated_logger: logging.Logger):
    install_rich_handler(isolated_logger)
    install_rich_handler(isolated_logger)
    rich_handlers = [h for h in isolated_logger.handlers if isinstance(h, RichHandler)]
    assert len(rich_handlers) == 1


def test_preserves_unrelated_handlers(isolated_logger: logging.Logger):
    sentinel = logging.NullHandler()
    isolated_logger.addHandler(sentinel)
    install_rich_handler(isolated_logger)
    assert sentinel in isolated_logger.handlers


def test_default_logger_is_root(monkeypatch: pytest.MonkeyPatch):
    root = logging.getLogger()
    prior_handlers = list(root.handlers)
    prior_level = root.level
    try:
        install_rich_handler()
        assert any(isinstance(h, RichHandler) for h in root.handlers)
    finally:
        root.handlers = prior_handlers
        root.setLevel(prior_level)


def test_same_console_usable_with_progress(isolated_logger: logging.Logger):
    """The returned Console must be acceptable as Progress(console=...).

    Guards the module's central contract: sharing the Console is what
    prevents Progress ticks from clobbering log output.
    """
    console = install_rich_handler(isolated_logger)
    with Progress(console=console, transient=True) as progress:
        task = progress.add_task("x", total=1)
        progress.advance(task)
    # No assertion; the point is it must not raise.


# -------------------------------------------------------------- build_progress --


@pytest.fixture
def _cleanup_root_logger():
    root = logging.getLogger()
    prior_handlers = list(root.handlers)
    prior_level = root.level
    yield
    root.handlers = prior_handlers
    root.setLevel(prior_level)


def test_build_progress_yields_three_objects(_cleanup_root_logger):
    install_rich_handler()
    with build_progress(5) as (progress, overall, item):
        assert isinstance(progress, Progress)
        assert overall != item
        # Overall task has total=5; item task starts hidden with total=1.
        overall_task = progress.tasks[0]
        item_task = progress.tasks[1]
        assert overall_task.total == 5
        assert item_task.visible is False


def test_build_progress_requires_installed_handler_without_console():
    # Clear root logger of any previously-installed handlers so the
    # discovery path has nothing to find.
    root = logging.getLogger()
    prior = list(root.handlers)
    root.handlers = [h for h in root.handlers if not getattr(h, "_aind_pipeline_utils_rich_handler", False)]
    try:
        with pytest.raises(RuntimeError, match="install_rich_handler"):
            with build_progress(5):
                pass
    finally:
        root.handlers = prior


def test_build_progress_accepts_explicit_console_without_installed_handler():
    root = logging.getLogger()
    prior = list(root.handlers)
    root.handlers = [h for h in root.handlers if not getattr(h, "_aind_pipeline_utils_rich_handler", False)]
    try:
        console = Console()
        with build_progress(3, console=console) as (progress, _overall, _item):
            assert progress.console is console
    finally:
        root.handlers = prior


def test_build_progress_uses_installed_handler_console(_cleanup_root_logger):
    shared = install_rich_handler()
    with build_progress(2) as (progress, _overall, _item):
        assert progress.console is shared


def test_build_progress_overall_advances_independently(_cleanup_root_logger):
    install_rich_handler()
    with build_progress(3) as (progress, overall, _item):
        progress.advance(overall)
        progress.advance(overall)
        assert progress.tasks[0].completed == 2


# ----------------------------------------------------- make_progress_callback --


def test_make_progress_callback_updates_task(_cleanup_root_logger):
    install_rich_handler()
    with build_progress(1) as (progress, _overall, item):
        progress.reset(item, total=100, description="x", visible=True)
        cb = make_progress_callback(progress, item)
        cb(25, 100)
        assert progress.tasks[1].completed == 25
        cb(100, 100)
        assert progress.tasks[1].completed == 100
        assert progress.tasks[1].finished


def test_make_progress_callback_updates_total_too(_cleanup_root_logger):
    install_rich_handler()
    with build_progress(1) as (progress, _overall, item):
        cb = make_progress_callback(progress, item)
        cb(5, 20)
        assert progress.tasks[1].total == 20
        cb(30, 50)  # total changed mid-stream (e.g. discovery revealed more work)
        assert progress.tasks[1].total == 50
        assert progress.tasks[1].completed == 30
