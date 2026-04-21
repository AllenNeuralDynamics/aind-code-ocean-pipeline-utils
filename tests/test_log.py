"""Tests for aind_code_ocean_pipeline_utils.log."""

from __future__ import annotations

import logging

import pytest

rich = pytest.importorskip("rich")
from rich.console import Console  # noqa: E402
from rich.logging import RichHandler  # noqa: E402

from aind_code_ocean_pipeline_utils.log import install_rich_handler  # noqa: E402


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
    from rich.progress import Progress

    console = install_rich_handler(isolated_logger)
    with Progress(console=console, transient=True) as progress:
        task = progress.add_task("x", total=1)
        progress.advance(task)
    # No assertion; the point is it must not raise.
