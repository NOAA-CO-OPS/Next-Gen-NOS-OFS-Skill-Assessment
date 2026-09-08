"""Shared pytest fixtures for the ofs-skill test suite."""

from __future__ import annotations

import pytest


@pytest.fixture()
def fvcom_minimal_dataset(tmp_path):
    """Synthetic multi-file FVCOM stations dataset (data_vars='minimal')."""
    # Import inside the fixture so collecting unrelated tests does not
    # pull helper deps at conftest load time.
    from tests.helpers.fvcom_minimal import build_fvcom_minimal_dataset

    return build_fvcom_minimal_dataset(tmp_path)
