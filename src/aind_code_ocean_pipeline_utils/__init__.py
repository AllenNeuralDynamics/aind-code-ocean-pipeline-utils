"""Utilities for use in Code Ocean pipelines.

Core primitives are re-exported at the package level. The optional
:mod:`.log` module requires the ``[rich]`` extra and must be imported
explicitly::

    from aind_code_ocean_pipeline_utils.log import install_rich_handler
"""

from importlib.metadata import PackageNotFoundError, version

from .cache import canonical_params, input_fingerprint
from .io import (
    TRANSIENT_ERRNOS,
    atomic_json_write,
    atomic_write_text,
    retry_on_oserror,
)
from .process import (
    GracefulExit,
    check_shutdown,
    install_shutdown_handlers,
    is_shutdown_requested,
    reset_shutdown_state,
    shutdown_handler,
)
from .threading_utils import submit_with_context

try:
    __version__ = version("aind-code-ocean-pipeline-utils")
except PackageNotFoundError:
    __version__ = "0.0.0.dev0"

__all__ = [
    "TRANSIENT_ERRNOS",
    "GracefulExit",
    "__version__",
    "atomic_json_write",
    "atomic_write_text",
    "canonical_params",
    "check_shutdown",
    "input_fingerprint",
    "install_shutdown_handlers",
    "is_shutdown_requested",
    "reset_shutdown_state",
    "retry_on_oserror",
    "shutdown_handler",
    "submit_with_context",
]
