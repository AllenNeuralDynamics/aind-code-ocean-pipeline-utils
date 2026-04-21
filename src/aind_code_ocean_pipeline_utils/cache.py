"""Deterministic fingerprints for pipeline cache keys and resume validation.

A fingerprint is ``sha256:`` followed by the hex digest of a canonical JSON
encoding of the input parameters. Canonical JSON here means:

- keys are strings, sorted recursively
- no whitespace
- NaN / ``inf`` are rejected (non-standard JSON)
- UTF-8 encoded before hashing

Given the same Python ``Mapping`` — regardless of insertion order — the same
fingerprint comes out every time, on every Python version that supports the
type annotations.

Non-JSON values (ndarray, Path, dataclasses, ...) raise :class:`TypeError`
with a message identifying the offending key path. Callers are expected to
coerce at the call site (``Path → str``, ``ndarray → list`` with a size
ceiling). This keeps the fingerprint well-defined and reproducible rather
than hiding silent behavior changes inside a ``default=`` hook.

The ``sha256:`` prefix leaves room to change the algorithm later without
breaking consumers that string-compare fingerprints.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

__all__ = ["canonical_params", "input_fingerprint"]


def _validate_json(value: Any, path: str) -> None:
    """Walk ``value``; raise :class:`TypeError` on any non-JSON-encodable leaf.

    Raising early with an explicit key path beats the opaque
    ``"Object of type X is not JSON serializable"`` that :func:`json.dumps`
    emits without telling the caller *where* the bad value lives.
    """
    if value is None or isinstance(value, (bool, int, float, str)):
        if isinstance(value, float) and (value != value or value in (float("inf"), float("-inf"))):
            raise TypeError(f"value at {path} is not JSON-standard: {value!r}")
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"key at {path} is not a string: got {type(key).__name__}")
            _validate_json(item, f"{path}.{key}" if path != "<root>" else key)
        return
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _validate_json(item, f"{path}[{index}]")
        return
    raise TypeError(f"value at {path} is not JSON-serializable: got {type(value).__name__}")


def canonical_params(params: Mapping[str, Any]) -> str:
    """Return the canonical JSON encoding of ``params``.

    Parameters
    ----------
    params : Mapping[str, Any]
        The input parameters. Must be JSON-serializable; see module
        docstring for the allowed leaf types.

    Returns
    -------
    str
        Compact JSON with sorted keys, no whitespace, no NaN/inf.

    Raises
    ------
    TypeError
        If any value (or key) is not JSON-encodable. The message identifies
        the offending key path.
    """
    _validate_json(params, "<root>")
    return json.dumps(
        params,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def input_fingerprint(params: Mapping[str, Any]) -> str:
    """Return a stable ``sha256:`` fingerprint of ``params``.

    Equal inputs (as Python mappings, regardless of key insertion order)
    produce equal fingerprints. Distinct inputs almost-certainly produce
    distinct fingerprints (SHA256 collision resistance).

    Parameters
    ----------
    params : Mapping[str, Any]
        The input parameters. See :func:`canonical_params` for the
        serialization contract.

    Returns
    -------
    str
        ``"sha256:"`` followed by 64 hex characters.
    """
    canonical = canonical_params(params)
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return f"sha256:{digest}"
