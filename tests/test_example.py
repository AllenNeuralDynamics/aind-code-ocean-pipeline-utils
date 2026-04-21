"""Example test module."""

import aind_code_ocean_pipeline_utils


def test_version():
    """Test that version is defined."""
    assert aind_code_ocean_pipeline_utils.__version__ is not None
    assert isinstance(aind_code_ocean_pipeline_utils.__version__, str)
