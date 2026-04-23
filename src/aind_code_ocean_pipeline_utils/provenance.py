"""Stamp capsule outputs with "what version of this code produced this.".

Two helpers that every capsule writing a manifest has been rewriting:
one that consults Code Ocean's pipeline-level env vars with a local
``git rev-parse`` fallback, and one that calls
:func:`importlib.metadata.version`. Centralizing them here means the
env-var list and the fallback behavior don't drift between capsules.

Both functions are **non-raising by design** — callers should be able
to stamp manifests unconditionally without wrapping every call in a
try/except. Missing info becomes ``None`` in the output, not a failure.
"""

from __future__ import annotations

import logging
import os
import subprocess
from collections.abc import Sequence
from importlib.metadata import PackageNotFoundError, version

__all__ = ["capsule_commit", "package_version"]

_logger = logging.getLogger(__name__)

# CO pipeline nodes get CO_COMMIT; other environments sometimes set
# GIT_COMMIT or COMMIT_ID (Jenkins, CI systems, etc.). Checked in order.
_DEFAULT_COMMIT_ENV_VARS: tuple[str, ...] = ("CO_COMMIT", "GIT_COMMIT", "COMMIT_ID")


def capsule_commit(
    *,
    code_dir: str = "/code",
    env_vars: Sequence[str] = _DEFAULT_COMMIT_ENV_VARS,
) -> str | None:
    """Return the capsule's git commit hash if discoverable, else ``None``.

    Resolution order:

    1. The first non-empty value among ``env_vars`` (in order).
    2. ``git -C <code_dir> rev-parse HEAD`` if git and the repo are
       available.
    3. ``None``.

    The return is the full 40-character hash — callers that want a
    short hash can slice. Never raises; any failure becomes ``None``
    so a manifest-emit path can stamp unconditionally.

    Parameters
    ----------
    code_dir : str, default ``"/code"``
        Directory passed to ``git -C``. Code Ocean capsules mount
        their source tree at ``/code``.
    env_vars : Sequence[str]
        Environment variable names to check, in priority order.

    Returns
    -------
    str or None
        40-character git hash, or ``None`` if undiscoverable.
    """
    for name in env_vars:
        val = os.environ.get(name, "").strip()
        if val:
            return val

    try:
        result = subprocess.run(
            ["git", "-C", code_dir, "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
            timeout=5.0,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        # OSError covers FileNotFoundError (git missing) and permission
        # issues. SubprocessError covers CalledProcessError (non-zero
        # exit, e.g. code_dir isn't a repo) and TimeoutExpired.
        _logger.debug("capsule_commit: git fallback failed: %s", exc)
        return None
    commit = result.stdout.strip()
    return commit or None


def package_version(package_name: str) -> str | None:
    """Return the installed version of ``package_name``, or ``None`` if absent.

    Thin wrapper around :func:`importlib.metadata.version` that
    swallows :class:`PackageNotFoundError`. Useful for optional
    dependencies where an absent stamp is preferable to a manifest
    emit crashing.

    Parameters
    ----------
    package_name : str
        Distribution name as used on PyPI (e.g. ``"numpy"``), which
        is not always the same as the import name.

    Returns
    -------
    str or None
    """
    try:
        return version(package_name)
    except PackageNotFoundError:
        return None
