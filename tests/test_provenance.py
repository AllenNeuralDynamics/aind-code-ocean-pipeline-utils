"""Tests for aind_code_ocean_pipeline_utils.provenance."""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from aind_code_ocean_pipeline_utils.provenance import capsule_commit, package_version

_HASH_40 = "a" * 40


# ------------------------------------------------------------ capsule_commit --


def test_capsule_commit_prefers_env_var(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("CO_COMMIT", _HASH_40)
    # Even if git would find a different answer, env wins.
    assert capsule_commit() == _HASH_40


def test_capsule_commit_env_var_order(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("CO_COMMIT", raising=False)
    monkeypatch.setenv("GIT_COMMIT", "b" * 40)
    monkeypatch.setenv("COMMIT_ID", "c" * 40)
    # GIT_COMMIT wins because it is earlier than COMMIT_ID in the default list.
    assert capsule_commit() == "b" * 40


def test_capsule_commit_skips_empty_env_vars(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("CO_COMMIT", "")  # empty string -> skip
    monkeypatch.setenv("GIT_COMMIT", "  ")  # whitespace-only -> skip
    monkeypatch.setenv("COMMIT_ID", _HASH_40)
    assert capsule_commit() == _HASH_40


def test_capsule_commit_falls_back_to_git(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    for name in ("CO_COMMIT", "GIT_COMMIT", "COMMIT_ID"):
        monkeypatch.delenv(name, raising=False)
    # Initialize a real git repo with one commit.
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    subprocess.run(
        ["git", "-C", str(tmp_path), "config", "user.email", "test@example.com"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(tmp_path), "config", "user.name", "test"],
        check=True,
    )
    (tmp_path / "x.txt").write_text("hi")
    subprocess.run(["git", "-C", str(tmp_path), "add", "x.txt"], check=True)
    subprocess.run(
        ["git", "-C", str(tmp_path), "commit", "-q", "-m", "init", "--no-gpg-sign"],
        check=True,
    )

    commit = capsule_commit(code_dir=str(tmp_path))
    assert commit is not None
    assert len(commit) == 40
    assert all(c in "0123456789abcdef" for c in commit)


def test_capsule_commit_returns_none_when_not_a_repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    for name in ("CO_COMMIT", "GIT_COMMIT", "COMMIT_ID"):
        monkeypatch.delenv(name, raising=False)
    assert capsule_commit(code_dir=str(tmp_path)) is None


def test_capsule_commit_returns_none_when_git_missing(monkeypatch: pytest.MonkeyPatch):
    for name in ("CO_COMMIT", "GIT_COMMIT", "COMMIT_ID"):
        monkeypatch.delenv(name, raising=False)
    with patch("subprocess.run", side_effect=FileNotFoundError("no git here")):
        assert capsule_commit() is None


def test_capsule_commit_respects_custom_env_vars(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("CO_COMMIT", "should-not-be-used")
    monkeypatch.setenv("MY_COMMIT", _HASH_40)
    assert capsule_commit(env_vars=("MY_COMMIT",)) == _HASH_40


def test_capsule_commit_handles_timeout(monkeypatch: pytest.MonkeyPatch):
    for name in ("CO_COMMIT", "GIT_COMMIT", "COMMIT_ID"):
        monkeypatch.delenv(name, raising=False)
    with patch("subprocess.run", side_effect=subprocess.TimeoutExpired("git", timeout=5)):
        assert capsule_commit() is None


# ------------------------------------------------------------ package_version --


def test_package_version_returns_installed_version():
    # pytest itself is installed in the dev env.
    v = package_version("pytest")
    assert v is not None
    assert isinstance(v, str)
    # Basic shape: "X.Y[...]" — avoid over-fitting to the exact version.
    assert v.split(".")[0].isdigit()


def test_package_version_returns_none_for_missing_package():
    assert package_version("this-package-definitely-does-not-exist-zzz") is None


def test_package_version_uses_distribution_name_not_import_name():
    # Our own package is installed as "aind-code-ocean-pipeline-utils"
    # (note the hyphens; the import name uses underscores).
    v = package_version("aind-code-ocean-pipeline-utils")
    assert v is not None
