"""Tests for aind_code_ocean_pipeline_utils.cache."""

from __future__ import annotations

import hashlib
import math
from pathlib import Path

import pytest

from aind_code_ocean_pipeline_utils.cache import canonical_params, input_fingerprint

# -------------------------------------------------------- canonical_params --


def test_canonical_sorts_top_level_keys():
    assert canonical_params({"b": 1, "a": 2}) == '{"a":2,"b":1}'


def test_canonical_sorts_nested_keys():
    out = canonical_params({"outer": {"z": 1, "a": 2}})
    assert out == '{"outer":{"a":2,"z":1}}'


def test_canonical_has_no_whitespace():
    out = canonical_params({"a": [1, 2, 3], "b": {"c": 4}})
    assert " " not in out
    assert "\n" not in out


def test_canonical_same_across_insertion_orders():
    assert canonical_params({"a": 1, "b": 2}) == canonical_params({"b": 2, "a": 1})


def test_canonical_handles_none_bool_float():
    assert canonical_params({"n": None, "b": True, "f": 1.5}) == '{"b":true,"f":1.5,"n":null}'


def test_canonical_handles_list_and_tuple_identically():
    # Tuples JSON-encode as arrays; fingerprint shouldn't distinguish.
    assert canonical_params({"x": [1, 2, 3]}) == canonical_params({"x": (1, 2, 3)})


def test_canonical_rejects_custom_object_with_key_path():
    class Opaque:
        pass

    with pytest.raises(TypeError) as excinfo:
        canonical_params({"weights": Opaque()})
    msg = str(excinfo.value)
    assert "weights" in msg
    assert "Opaque" in msg


def test_canonical_rejects_path_with_key_path():
    with pytest.raises(TypeError) as excinfo:
        canonical_params({"input_dir": Path("/tmp/foo")})
    assert "input_dir" in str(excinfo.value)


def test_canonical_rejects_nested_non_json_with_full_path():
    with pytest.raises(TypeError) as excinfo:
        canonical_params({"outer": {"inner": object()}})
    msg = str(excinfo.value)
    assert "outer" in msg
    assert "inner" in msg


def test_canonical_rejects_non_string_key():
    with pytest.raises(TypeError) as excinfo:
        canonical_params({1: "x"})  # type: ignore[dict-item]
    assert "not a string" in str(excinfo.value)


def test_canonical_rejects_nan():
    with pytest.raises(TypeError):
        canonical_params({"x": math.nan})


def test_canonical_rejects_inf():
    with pytest.raises(TypeError):
        canonical_params({"x": math.inf})


def test_canonical_identifies_bad_list_element_with_index():
    with pytest.raises(TypeError) as excinfo:
        canonical_params({"items": [1, 2, object()]})
    msg = str(excinfo.value)
    assert "items" in msg
    assert "[2]" in msg


# ------------------------------------------------------ input_fingerprint --


def test_fingerprint_has_sha256_prefix():
    fp = input_fingerprint({"a": 1})
    assert fp.startswith("sha256:")
    assert len(fp) == len("sha256:") + 64


def test_fingerprint_hex_only_after_prefix():
    fp = input_fingerprint({"a": 1})
    hex_part = fp.removeprefix("sha256:")
    int(hex_part, 16)  # must parse as hex


def test_fingerprint_stable_across_insertion_orders():
    assert input_fingerprint({"a": 1, "b": 2}) == input_fingerprint({"b": 2, "a": 1})


def test_fingerprint_differs_for_different_values():
    assert input_fingerprint({"a": 1}) != input_fingerprint({"a": 2})


def test_fingerprint_matches_known_vector():
    # Sanity check: manually compute the expected digest.
    canonical = '{"a":1,"b":2}'
    expected = "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    assert input_fingerprint({"a": 1, "b": 2}) == expected


def test_fingerprint_propagates_type_errors():
    with pytest.raises(TypeError):
        input_fingerprint({"bad": object()})
