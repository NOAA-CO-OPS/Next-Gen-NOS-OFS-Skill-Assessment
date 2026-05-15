"""Regression tests for ``_preload_static_coords``.

This helper exists to prevent a netCDF4-engine thread-safety hang seen on
NECOFS runs with ``parallel_variables=true``: 4 variable threads each
called ``np.array(model[lon])`` etc. inside ``index_nearest_node``,
contending on HDF5's global file lock and pinning one core with the
other three idle.

We can't reproduce the live deadlock in a unit test, but we can verify:
- The helper materializes the right vars without raising.
- It tolerates missing coords (e.g. FVCOM datasets without a 'z' coord).
- It works for each supported model_source dispatch.
"""

import logging

import numpy as np
import xarray as xr

from ofs_skill.model_processing.get_node_ofs import _preload_static_coords


def _logger():
    return logging.getLogger('preload_static_coords_test')


def _fvcom_dataset(include_z=True):
    """Minimal FVCOM-shaped stations dataset."""
    n_node, n_siglay, n_time = 50, 10, 6
    coords = {
        'lon': (('node',), np.linspace(-70, -65, n_node)),
        'lat': (('node',), np.linspace(40, 45, n_node)),
        'lonc': (('node',), np.linspace(-70, -65, n_node)),
        'latc': (('node',), np.linspace(40, 45, n_node)),
        'siglay': (('siglay', 'node'),
                   np.tile(np.linspace(-1, 0, n_siglay)[:, None],
                           (1, n_node))),
        'h': (('node',), np.full(n_node, 25.0)),
    }
    if include_z:
        coords['z'] = (('siglay', 'node'),
                       np.tile(np.linspace(-25, 0, n_siglay)[:, None],
                               (1, n_node)))
    data_vars = {
        'zeta': (('time', 'node'), np.zeros((n_time, n_node))),
    }
    return xr.Dataset(data_vars, coords=coords)


def test_preload_fvcom_loads_known_coords(caplog):
    ds = _fvcom_dataset(include_z=True)
    with caplog.at_level(logging.INFO, logger='preload_static_coords_test'):
        _preload_static_coords(ds, 'fvcom', _logger())

    msgs = [r.getMessage() for r in caplog.records]
    summary = next(m for m in msgs if 'Pre-loaded' in m)
    # All 7 FVCOM candidates present in the fixture (including z) should land.
    for name in ('lon', 'lat', 'lonc', 'latc', 'siglay', 'h', 'z'):
        assert name in summary


def test_preload_fvcom_tolerates_missing_z(caplog):
    """Some FVCOM outputs (e.g. NECOFS) have no pre-computed 'z' — fine."""
    ds = _fvcom_dataset(include_z=False)
    with caplog.at_level(logging.INFO, logger='preload_static_coords_test'):
        _preload_static_coords(ds, 'fvcom', _logger())

    summary = next(r.getMessage() for r in caplog.records
                   if 'Pre-loaded' in r.getMessage())
    # 6 of 7 loaded; 'z' simply missing from variables — no error path.
    assert "'z'" not in summary
    for name in ('lon', 'lat', 'lonc', 'latc', 'siglay', 'h'):
        assert name in summary


def test_preload_unknown_source_no_error(caplog):
    """Unknown model_source should log 0 loaded coords and not raise."""
    ds = _fvcom_dataset()
    with caplog.at_level(logging.INFO, logger='preload_static_coords_test'):
        _preload_static_coords(ds, 'made_up_model', _logger())

    summary = next(r.getMessage() for r in caplog.records
                   if 'Pre-loaded' in r.getMessage())
    assert 'Pre-loaded 0 static coord' in summary


def test_preload_roms_skips_when_coords_absent(caplog):
    """ROMS candidate list applied to an FVCOM dataset finds 'h' only."""
    ds = _fvcom_dataset()
    with caplog.at_level(logging.INFO, logger='preload_static_coords_test'):
        _preload_static_coords(ds, 'roms', _logger())

    summary = next(r.getMessage() for r in caplog.records
                   if 'Pre-loaded' in r.getMessage())
    assert "'h'" in summary
    for name in ('lon_rho', 'lat_rho', 'mask_rho', 's_rho'):
        assert name not in summary
