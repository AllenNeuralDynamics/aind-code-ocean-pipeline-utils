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
import re
import subprocess
from collections.abc import Sequence
from importlib.metadata import PackageNotFoundError, version

__all__ = ["capsule_commit", "derive_code_url", "package_version"]

_logger = logging.getLogger(__name__)

# CO pipeline nodes get CO_COMMIT; other environments sometimes set
# GIT_COMMIT or COMMIT_ID (Jenkins, CI systems, etc.). Checked in order.
_DEFAULT_COMMIT_ENV_VARS: tuple[str, ...] = ("CO_COMMIT", "GIT_COMMIT", "COMMIT_ID")

# Code Ocean does not expose a single canonical "capsule repo URL" env var;
# these are the names seen across CO / CI environments, checked in order.
_DEFAULT_CODE_URL_ENV_VARS: tuple[str, ...] = ("CO_CAPSULE_URL", "GIT_URL", "REPO_URL")


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


def _normalize_remote_url(url: str) -> str:
    """Normalize a git remote URL to an https form, dropping a trailing ``.git``.

    ``git@github.com:org/repo.git`` -> ``https://github.com/org/repo`` and
    ``https://github.com/org/repo.git`` -> ``https://github.com/org/repo``.
    Anything unrecognized is returned unchanged.
    """
    url = url.strip()
    scp = re.match(r"^git@([^:]+):(.+)$", url)
    if scp:
        url = f"https://{scp.group(1)}/{scp.group(2)}"
    url = url.removesuffix(".git")
    return url


def derive_code_url(
    *,
    explicit: str | None = None,
    code_dir: str = "/code",
    env_vars: Sequence[str] = _DEFAULT_CODE_URL_ENV_VARS,
) -> str | None:
    """Best-effort repository URL for the capsule that ran this step.

    Resolution order: ``explicit`` override, then ``git -C <code_dir> remote
    get-url origin`` (normalized to https), then the first non-empty ``env_vars``
    value. Returns ``None`` if nothing resolves. Never raises.

    Parameters
    ----------
    explicit : str, optional
        Caller-supplied URL; wins outright when given.
    code_dir : str, default ``"/code"``
        Git checkout to inspect (Code Ocean mounts source at ``/code``).
    env_vars : Sequence[str]
        Env var names to consult, in priority order.

    Returns
    -------
    str or None
    """
    if explicit:
        return explicit
    try:
        result = subprocess.run(
            ["git", "-C", code_dir, "remote", "get-url", "origin"],
            capture_output=True,
            text=True,
            check=True,
            timeout=5.0,
        )
        remote = result.stdout.strip()
        if remote:
            return _normalize_remote_url(remote)
    except (OSError, subprocess.SubprocessError) as exc:
        _logger.debug("derive_code_url: git fallback failed: %s", exc)
    for name in env_vars:
        val = os.environ.get(name, "").strip()
        if val:
            return _normalize_remote_url(val)
    return None
