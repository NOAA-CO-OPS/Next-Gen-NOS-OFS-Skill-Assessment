"""Regression tests for ``_precompute_scalar_data`` 2-D variable handling.

Reproduces the bug where the FVCOM/ROMS stations-file batch precompute
called ``model[zeta][:, depth, station]`` (3-D indexing) on a variable
that's only ``(time, station)``, raising ``IndexError: too many indices
for array``. The fallback path was per-station Dask compute, which made
the WL stage run several hours on multi-month windows instead of seconds.

After the fix, ``_precompute_scalar_data`` dispatches based on
``model[var].ndim`` so 2-D zeta extracts in one batched compute.
"""

import logging
from types import SimpleNamespace

import numpy as np
import xarray as xr

from ofs_skill.model_processing.get_node_ofs import _precompute_scalar_data


def _logger():
    return logging.getLogger('precompute_scalar_2d_test')


def _hour_times(n_time):
    start = np.datetime64('2026-02-16')
    return start + np.arange(n_time) * np.timedelta64(1, 'h')


def _make_fvcom_dataset(n_time=24, n_siglay=10, n_station=8):
    """FVCOM-like stations dataset with 2-D zeta + 3-D temp."""
    rng = np.random.default_rng(0)
    return xr.Dataset(
        {
            'zeta': (
                ('time', 'station'),
                rng.standard_normal((n_time, n_station)),
            ),
            'temp': (
                ('time', 'siglay', 'station'),
                rng.standard_normal((n_time, n_siglay, n_station)),
            ),
        },
        coords={
            'time': _hour_times(n_time),
        },
    )


def _ctlfile(n_station, depth_idx=0):
    """Mimic ``ofs_ctlfile`` tuple shape used by precompute helpers."""
    lines = []
    nodes = list(range(n_station))
    depths = [depth_idx] * n_station
    shifts = [0.0] * n_station
    ids = [f's{i}' for i in range(n_station)]
    return (lines, nodes, depths, shifts, ids)


def _fvcom_props():
    return SimpleNamespace(
        model_source='fvcom',
        ofs='necofs',
        ofsfiletype='stations',
        whichcast='nowcast',
    )


def test_precompute_zeta_2d_batches_successfully():
    """FVCOM 2-D zeta should batch without "too many indices"."""
    n_station = 6
    ds = _make_fvcom_dataset(n_station=n_station)
    ofs_ctl = _ctlfile(n_station)

    result = _precompute_scalar_data(
        _fvcom_props(), ds, ofs_ctl, 'zeta', _logger())

    arr = result['scalar_data']
    assert arr.shape == (ds.sizes['time'], n_station)
    # Spot-check: stacked output matches direct per-station selection.
    for i in range(n_station):
        np.testing.assert_array_equal(arr[:, i], ds['zeta'][:, i].values)


def test_precompute_temp_3d_still_works():
    """3-D temp path must continue to use depth indexing after the fix."""
    n_station = 6
    depth_idx = 3
    ds = _make_fvcom_dataset(n_station=n_station)
    ofs_ctl = _ctlfile(n_station, depth_idx=depth_idx)

    result = _precompute_scalar_data(
        _fvcom_props(), ds, ofs_ctl, 'temp', _logger())

    arr = result['scalar_data']
    assert arr.shape == (ds.sizes['time'], n_station)
    for i in range(n_station):
        np.testing.assert_array_equal(
            arr[:, i], ds['temp'][:, depth_idx, i].values)


def test_precompute_zeta_2d_roms_path():
    """ROMS stations zeta is also 2-D; same fix applies on idx_first branch."""
    n_time, n_station = 12, 5
    rng = np.random.default_rng(1)
    ds = xr.Dataset(
        {
            'zeta': (
                ('ocean_time', 'station'),
                rng.standard_normal((n_time, n_station)),
            ),
        },
        coords={'ocean_time': _hour_times(n_time)},
    )
    prop = SimpleNamespace(
        model_source='roms',
        ofs='cbofs',
        ofsfiletype='stations',
        whichcast='nowcast',
    )
    ofs_ctl = _ctlfile(n_station)

    result = _precompute_scalar_data(prop, ds, ofs_ctl, 'zeta', _logger())

    arr = result['scalar_data']
    assert arr.shape == (n_time, n_station)
    for i in range(n_station):
        np.testing.assert_array_equal(arr[:, i], ds['zeta'][:, i].values)
