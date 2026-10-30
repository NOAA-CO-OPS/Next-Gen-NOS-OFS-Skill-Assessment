"""Shared pytest fixtures for the ofs-skill test suite."""

from __future__ import annotations

import re

import pytest

_UNICODE_ESCAPE = re.compile(r'\\u([0-9a-fA-F]{4})')


@pytest.fixture()
def fvcom_minimal_dataset(tmp_path):
    """Synthetic multi-file FVCOM stations dataset (data_vars='minimal')."""
    # Import inside the fixture so collecting unrelated tests does not
    # pull helper deps at conftest load time.
    from tests.helpers.fvcom_minimal import build_fvcom_minimal_dataset

    return build_fvcom_minimal_dataset(tmp_path)


def decode_plotly_escapes(text: str) -> str:
    """Decode every ``\\uXXXX`` escape in a written-out plotly HTML file.

    Plotly serialises the figure as JSON embedded in the HTML, and which
    characters come out escaped depends on the JSON engine it picks up:
    ``orjson``, when installed, writes raw UTF-8, while the stdlib
    ``json`` fallback defaults to ``ensure_ascii=True`` and escapes
    every non-ASCII character.  ``orjson`` is not a declared dependency,
    so a developer environment that happens to have it and a CI runner
    that does not will disagree on whether the degree sign in
    ``RMSE (°C)`` is a literal ``°`` or ``\\u00b0``.

    Decoding all escapes uniformly keeps a substring assertion valid
    under either engine.  Both renderings display identically in a
    browser, so this is purely about making the assertion engine-blind.
    """
    return _UNICODE_ESCAPE.sub(lambda m: chr(int(m.group(1), 16)), text)
