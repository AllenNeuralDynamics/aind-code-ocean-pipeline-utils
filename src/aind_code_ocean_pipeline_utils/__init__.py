"""Utilities for use in Code Ocean pipelines.

Core primitives are re-exported at the package level, including the schema-free
provenance breadcrumb layer from :mod:`.records` (``record_step``, ``make_record``,
``read_records``, ``read_processing_records``, ``frontier``, ``write_record``). The rich-aware helpers in
:mod:`.log` (``install_rich_handler``, ``build_progress``) require the optional
``[rich]`` extra and must be imported explicitly; :func:`attach_file_log` is
stdlib-only and re-exported here. The :mod:`.metadata` helpers
(``assemble_processing`` / ``write_assembled_processing`` / ``make_data_process``)
require the optional ``[metadata]`` extra (``aind-data-schema``) and must be imported
from their submodule; ``record_step`` uses them lazily and best-effort to author a
shard's opaque ``DataProcess`` payload.
"""

from importlib.metadata import PackageNotFoundError, version

from .cache import canonical_params, input_fingerprint
from .cli import parse_truthy
from .diagnostics import MemoryReporter, log_data_tree, start_memory_reporter
from .io import (
    TRANSIENT_ERRNOS,
    atomic_json_write,
    atomic_write_text,
    retry_on_oserror,
)
from .log import attach_file_log
from .metadata_files import (
    DEFAULT_INHERITED_METADATA,
    find_metadata_file,
    forward_metadata,
    read_data_description_fields,
)
from .process import (
    GracefulExit,
    check_shutdown,
    install_shutdown_handlers,
    is_shutdown_requested,
    reset_shutdown_state,
    shutdown_handler,
)
from .provenance import capsule_commit, package_version
from .records import (
    RECORD_VERSION,
    RecordContext,
    frontier,
    make_record,
    read_processing_records,
    read_records,
    record_step,
    write_record,
)
from .role_dispatch import (
    Role,
    StreamConfigError,
    default_sanitize,
    find_launcher_manifest,
    find_stream_config,
    find_worker_manifests,
    merge_manifests,
    write_stream_configs,
)
from .threading_utils import submit_with_context

try:
    __version__ = version("aind-code-ocean-pipeline-utils")
except PackageNotFoundError:
    __version__ = "0.0.0.dev0"

__all__ = [
    "DEFAULT_INHERITED_METADATA",
    "RECORD_VERSION",
    "TRANSIENT_ERRNOS",
    "GracefulExit",
    "MemoryReporter",
    "RecordContext",
    "Role",
    "StreamConfigError",
    "__version__",
    "atomic_json_write",
    "atomic_write_text",
    "attach_file_log",
    "canonical_params",
    "capsule_commit",
    "check_shutdown",
    "default_sanitize",
    "find_launcher_manifest",
    "find_metadata_file",
    "find_stream_config",
    "find_worker_manifests",
    "forward_metadata",
    "frontier",
    "input_fingerprint",
    "install_shutdown_handlers",
    "is_shutdown_requested",
    "log_data_tree",
    "make_record",
    "merge_manifests",
    "package_version",
    "parse_truthy",
    "read_data_description_fields",
    "read_processing_records",
    "read_records",
    "record_step",
    "reset_shutdown_state",
    "retry_on_oserror",
    "shutdown_handler",
    "start_memory_reporter",
    "submit_with_context",
    "write_record",
    "write_stream_configs",
]
