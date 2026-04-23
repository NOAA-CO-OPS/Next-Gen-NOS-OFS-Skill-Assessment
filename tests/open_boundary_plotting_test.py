"""Smoke tests for ofs_skill.open_boundary.obc_plotting.plot_fvcom_obc.

Exercises the full plot pipeline (T transect, S transect, WL transect,
WL node time series, OBC node map) on a synthetic FVCOM-like OBC
xarray.Dataset and verifies each HTML artifact lands on disk.
"""
from __future__ import annotations

# Reuse the dataset builder from the processing test module (same directory,
# loaded by path since tests/ is not a package).
import importlib.util as _ilu
import logging
import os
import types
from pathlib import Path as _Path

import numpy as np
import pytest

from ofs_skill.open_boundary import obc_plotting

_proc_path = _Path(__file__).parent / 'open_boundary_processing_test.py'
_spec = _ilu.spec_from_file_location('_obc_proc_fixtures', _proc_path)
assert _spec is not None and _spec.loader is not None
_mod = _ilu.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
_make_ds = _mod._make_ds


@pytest.fixture()
def logger():
    lg = logging.getLogger('obc_plot_test')
    lg.setLevel(logging.INFO)
    return lg


@pytest.fixture()
def prop(tmp_path):
    p = types.SimpleNamespace()
    p.ofs = 'leofs'
    p.model_cycle = '00'
    p.start_date_full = '2026-04-01T00:00:00Z'
    p.visuals_1d_station_path = str(tmp_path)
    return p


class TestPlotFvcomObc:
    def test_writes_all_htmls(self, prop, logger):
        ds = _make_ds()
        # Add obc_nodes so the dedicated branch is hit
        ds['obc_nodes'] = (('node',), np.arange(1, ds.sizes['node'] + 1))
        obc_plotting.plot_fvcom_obc(prop, ds, logger)

        expected = [
            'leofs_temp_OBC.html',
            'leofs_salinity_OBC.html',
            'leofs_water_level_OBC.html',
            'leofs_water_level_OBC_node_series.html',
            'leofs_OBC_node_map.html',
        ]
        for fname in expected:
            fp = os.path.join(prop.visuals_1d_station_path, fname)
            assert os.path.isfile(fp), f'missing: {fname}'
            assert os.path.getsize(fp) > 0, f'empty: {fname}'

    def test_elevation_fallback_and_no_obc_nodes(self, prop, logger):
        # Dataset has 'elevation' instead of 'zeta' → exercises that branch.
        # Remove 'obc_nodes' to exercise the linspace fallback.
        ds = _make_ds()
        ds = ds.rename({'zeta': 'elevation'})
        obc_plotting.plot_fvcom_obc(prop, ds, logger)
        # Only check one artifact for smoke
        fp = os.path.join(prop.visuals_1d_station_path, 'leofs_temp_OBC.html')
        assert os.path.isfile(fp)

    def test_missing_variable_skips_but_continues(self, prop, logger, caplog):
        # Drop 'temp' to trigger the "variable not found; skipping" branch
        ds = _make_ds()
        ds = ds.drop_vars('temp')
        with caplog.at_level(logging.ERROR):
            obc_plotting.plot_fvcom_obc(prop, ds, logger)
        # Water level artifacts should still be written
        fp = os.path.join(prop.visuals_1d_station_path,
                          'leofs_water_level_OBC.html')
        assert os.path.isfile(fp)
        assert any("'temp'" in r.message or 'temp' in r.message
                   for r in caplog.records)
