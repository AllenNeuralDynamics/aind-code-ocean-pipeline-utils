"""Regenerate the checked-in provenance-record JSON Schema artifact.

Run after editing :func:`aind_code_ocean_pipeline_utils.record_schema.build_record_schema`::

    uv run python scripts/export_record_schema.py

Writes ``src/aind_code_ocean_pipeline_utils/<SCHEMA_ARTIFACT_RELPATH>``. A test
(``tests/test_record_schema.py``) fails if the checked-in file drifts from the
builder, so this must be re-run and committed whenever the schema changes.
"""

from __future__ import annotations

import json
from pathlib import Path

from aind_code_ocean_pipeline_utils.record_schema import (
    SCHEMA_ARTIFACT_RELPATH,
    build_record_schema,
)


def main() -> None:
    """Serialize the schema to the checked-in artifact path."""
    schema = build_record_schema()
    package_root = Path(__file__).resolve().parents[1] / "src" / "aind_code_ocean_pipeline_utils"
    out = package_root / SCHEMA_ARTIFACT_RELPATH
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(schema, indent=2, sort_keys=True) + "\n")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
