"""Utilities to for use in code ocean pipelines"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("aind-code-ocean-pipeline-utils")
except PackageNotFoundError:
    __version__ = "0.0.0.dev0"
