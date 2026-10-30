"""Issue #217: the ice RMSE box/violin plot y-axis label carries '%'.

The y-axis title on this figure comes from the ``RMSE`` dataframe column
name that ``px.violin`` picks up, so the unit has to be re-applied on the
layout.  It has to be applied with the leaf-level ``yaxis_title_text``
key: a bare ``yaxis_title`` replaces the whole ``layout.yaxis.title``
object and silently drops the font size set alongside it, leaving the
y-axis label rendering ~8px smaller than the x-axis label next to it.
"""
from __future__ import annotations

import logging
import os
import types

import numpy as np
import pandas as pd
import pytest

from ofs_skill.visualization import make_ice_boxplots


@pytest.fixture
def _prop(tmp_path):
    prop = types.SimpleNamespace()
    prop.ofs = 'loofs2'
    prop.whichcast = 'nowcast'
    prop.visuals_stats_ice_path = str(tmp_path / 'ice')
    os.makedirs(prop.visuals_stats_ice_path, exist_ok=True)
    return prop


def _ice_fields():
    rng = np.random.default_rng(0)
    shape = (6, 8, 8)
    obs = rng.uniform(0.0, 100.0, size=shape)
    model = obs + rng.normal(0.0, 5.0, size=shape)
    return obs, model


def _capture_figure(monkeypatch):
    captured: dict = {}

    def _write_html(self, *args, **kwargs):
        captured['fig'] = self

    monkeypatch.setattr(
        'plotly.graph_objects.Figure.write_html', _write_html, raising=True)
    return captured


def test_ice_rmse_axis_titles_share_one_font_size(monkeypatch, _prop):
    captured = _capture_figure(monkeypatch)
    obs, model = _ice_fields()
    times = pd.date_range('2026-03-28', periods=obs.shape[0], freq='D')
    make_ice_boxplots.make_ice_boxplots(
        obs, model, list(times), _prop, logging.getLogger('test'))

    fig = captured['fig']
    assert fig.layout.yaxis.title.text == 'RMSE (%)'
    assert fig.layout.yaxis.title.font.size == 20
    assert fig.layout.xaxis.title.font.size == 20
