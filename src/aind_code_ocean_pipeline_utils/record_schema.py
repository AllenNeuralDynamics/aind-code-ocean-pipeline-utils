"""Versioned JSON Schema for the provenance breadcrumb envelope.

This describes the **owned** envelope only — the keys this library reads on the
routing hot path (:mod:`.records`). The ``data_process`` and ``pipeline.code``
payloads are typed as **opaque objects** on purpose: each carries an
aind-data-schema ``DataProcess`` / pipeline ``Code`` whose version is chosen by
whichever capsule authored the shard, and this library never parses them.
``additionalProperties`` is ``true`` throughout so the format is additive-only:
unknown keys are always tolerated.

Why version this on its own axis
--------------------------------
aind-data-schema couples the schema version to the *generating library release*
(``schema_version`` is a ``Literal["2.2.2"]``-style pin), which forces a schema
bump on every release and makes any vendored copy brittle. That coupling is the
thing to avoid. Here the envelope contract has its **own** version
(:data:`SCHEMA_VERSION`), independent of ``aind-code-ocean-pipeline-utils``'s
package version, and is shipped as a standalone JSON Schema artifact under
``schemas/`` (also installed as package data) so other libraries can **vendor a
frozen copy** without importing — or depending on — this package at all.

The wire field ``v`` (:data:`aind_code_ocean_pipeline_utils.records.RECORD_VERSION`)
is the schema **major** version. Additive changes bump :data:`SCHEMA_VERSION`'s
minor/patch and never ``v``; a breaking change bumps the major and ``v`` together
(and lands under a new ``schemas/provenance_record/v<major>/`` directory).
"""

from __future__ import annotations

import json
from importlib.resources import files
from typing import Any

from .records import RECORD_VERSION

__all__ = [
    "SCHEMA_ARTIFACT_RELPATH",
    "SCHEMA_ID",
    "SCHEMA_VERSION",
    "build_record_schema",
    "record_schema",
]

#: Semantic version of the envelope contract. Independent of the package version.
#: Minor/patch = additive; major matches the wire ``v`` and the ``v<major>`` dir.
SCHEMA_VERSION = "1.1.0"

#: Location of the checked-in artifact, relative to the package root. Used by both
#: the loader (:func:`record_schema`) and ``scripts/export_record_schema.py``.
SCHEMA_ARTIFACT_RELPATH = f"schemas/provenance_record/v{RECORD_VERSION}/provenance_record.schema.json"

#: Stable ``$id`` for the artifact. A URL by convention; it need not resolve.
SCHEMA_ID = (
    "https://raw.githubusercontent.com/AllenNeuralDynamics/aind-code-ocean-pipeline-utils/"
    f"main/src/aind_code_ocean_pipeline_utils/{SCHEMA_ARTIFACT_RELPATH}"
)

_OPAQUE_PAYLOAD = {
    "type": "object",
    "description": "Opaque payload; carried verbatim and never parsed by this library.",
}


def build_record_schema() -> dict[str, Any]:
    """Return the provenance-record JSON Schema as a plain dict (the source of truth).

    Pure and stdlib-only. ``scripts/export_record_schema.py`` serializes this to the
    checked-in artifact; :func:`record_schema` loads that artifact back. A test
    asserts the two agree, so the file can never silently drift from this definition.

    Returns
    -------
    dict[str, Any]
        A JSON Schema (draft 2020-12) for a single provenance breadcrumb envelope.
    """
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": SCHEMA_ID,
        "title": "Provenance breadcrumb record",
        "description": (
            "One node's provenance shard: an owned routing envelope wrapping opaque, "
            "author-only payloads. This library reads only 'v', 'node', and 'parents'."
        ),
        "x-schema-version": SCHEMA_VERSION,
        "type": "object",
        "required": ["v", "node", "parents"],
        "additionalProperties": True,
        "properties": {
            "v": {
                "const": RECORD_VERSION,
                "description": "Envelope major version. Additive changes do not bump it.",
            },
            "node": {
                "type": "string",
                "minLength": 1,
                "description": "Unique node id; also the authored DataProcess.name (the dependency_graph key space).",
            },
            "parents": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Direct-parent node ids ([] for a source node). The adjacency list IS the DAG.",
            },
            "data_process": {
                **_OPAQUE_PAYLOAD,
                "description": "Opaque serialized aind-data-schema DataProcess; never parsed by this library.",
            },
            "data_process_schema_version": {
                "type": "string",
                "description": "Version of aind-data-schema that authored 'data_process', for later assembly.",
            },
            "pipeline": {
                "type": "object",
                "description": "Run-level pipeline block; launcher-owned. Only 'name' is read.",
                "required": ["name"],
                "additionalProperties": True,
                "properties": {
                    "name": {
                        "type": "string",
                        "minLength": 1,
                        "description": "Pipeline name; links DataProcess.pipeline_name to Processing.pipelines.",
                    },
                    "code": {
                        **_OPAQUE_PAYLOAD,
                        "description": "Opaque serialized aind-data-schema pipeline Code payload.",
                    },
                },
            },
            "experimenters": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Run-level default responsible people; launcher-owned.",
            },
            "label": {
                "type": "string",
                "minLength": 1,
                "description": (
                    "Display name for the assembled DataProcess.name when it differs from 'node'; "
                    "set on records converted from a processing.json."
                ),
            },
        },
    }


def record_schema() -> dict[str, Any]:
    """Load the checked-in provenance-record JSON Schema shipped as package data.

    Returns
    -------
    dict[str, Any]
        The parsed artifact. Equal to :func:`build_record_schema` (a test enforces
        this); prefer this loader when you want the exact bytes other libraries vendor.
    """
    resource = files("aind_code_ocean_pipeline_utils").joinpath(SCHEMA_ARTIFACT_RELPATH)
    schema: dict[str, Any] = json.loads(resource.read_text())
    return schema
