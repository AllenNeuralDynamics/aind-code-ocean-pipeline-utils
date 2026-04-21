"""Tests for aind_code_ocean_pipeline_utils.cli."""

from __future__ import annotations

import pytest

from aind_code_ocean_pipeline_utils.cli import parse_truthy


@pytest.mark.parametrize(
    "value",
    [
        "true",
        "TRUE",
        "True",
        "yes",
        "Yes",
        "YES",
        "y",
        "Y",
        "t",
        "T",
    ],
)
def test_parse_truthy_accepts_words(value: str):
    assert parse_truthy(value) is True


@pytest.mark.parametrize(
    "value,expected",
    [
        ("1", True),
        ("42", True),
        ("-1", True),
        ("3.14", True),
        ("1.5e2", True),
        ("0", False),
        ("0.0", False),
        ("-0", False),
        ("-0.0", False),
    ],
)
def test_parse_truthy_handles_numeric_strings(value: str, expected: bool):
    assert parse_truthy(value) is expected


@pytest.mark.parametrize(
    "value",
    [
        "",
        " ",
        "false",
        "FALSE",
        "no",
        "n",
        "f",
        "off",
        "nope",
        "null",
        "None",
        "foo",
    ],
)
def test_parse_truthy_rejects_everything_else(value: str):
    assert parse_truthy(value) is False


def test_parse_truthy_strips_whitespace():
    assert parse_truthy("  true  ") is True
    assert parse_truthy("  42  ") is True
    assert parse_truthy("  0  ") is False
