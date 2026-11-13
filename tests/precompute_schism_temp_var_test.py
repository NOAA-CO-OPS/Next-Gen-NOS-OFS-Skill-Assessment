"""Regression tests for SCHISM temperature naming in the batch precompute.

Reproduces the bug where ``_precompute_scalar_data`` unconditionally
renamed ``temp`` to ``temperature`` for SCHISM models. STOFS files do
carry ``temperature``, but SECOFS stations files keep SCHISM's native
``temp`` — so the batch precompute raised ``KeyError`` and the caller's
broad fallback silently dropped the whole variable to per-station
extraction (~12 h instead of minutes on a 3-month SECOFS run).

After the fix, the variable name is probed against the dataset, so both
namings batch successfully.
"""

import logging
from types import SimpleNamespace

import numpy as np
import xarray as xr

from ofs_skill.model_processing.get_node_ofs import _precompute_scalar_data


def _logger():
    return logging.getLogger('precompute_schism_temp_var_test')


def _hour_times(n_time):
    start = np.datetime64('2026-05-07')
    return start + np.arange(n_time) * np.timedelta64(1, 'h')


def _make_schism_dataset(temp_name, n_time=24, n_siglay=10, n_station=6):
    """SCHISM stations dataset (time, siglay, station) with one temp var."""
    rng = np.random.default_rng(0)
    return xr.Dataset(
        {
            temp_name: (
                ('time', 'siglay', 'station'),
                rng.standard_normal((n_time, n_siglay, n_station)),
            ),
        },
        coords={'time': _hour_times(n_time)},
    )


def _ctlfile(n_station, depth_idx=0):
    lines = []
    nodes = list(range(n_station))
    depths = [depth_idx] * n_station
    shifts = [0.0] * n_station
    ids = [f's{i}' for i in range(n_station)]
    return (lines, nodes, depths, shifts, ids)


def _schism_props(ofs):
    return SimpleNamespace(
        model_source='schism',
        ofs=ofs,
        ofsfiletype='stations',
        whichcast='nowcast',
    )


def test_secofs_temp_batches_without_keyerror():
    """SECOFS stations files carry 'temp'; the batch path must use it."""
    n_station, depth_idx = 6, 3
    ds = _make_schism_dataset('temp', n_station=n_station)
    ofs_ctl = _ctlfile(n_station, depth_idx=depth_idx)

    result = _precompute_scalar_data(
        _schism_props('secofs'), ds, ofs_ctl, 'temp', _logger())

    arr = result['scalar_data']
    assert arr.shape == (ds.sizes['time'], n_station)
    for i in range(n_station):
        np.testing.assert_array_equal(
            arr[:, i], ds['temp'][:, depth_idx, i].values)


def test_stofs_temperature_rename_still_applies():
    """STOFS files carry 'temperature'; 'temp' input must still resolve."""
    n_station = 5
    n_time = 12
    rng = np.random.default_rng(1)
    # STOFS stations temperature is 2-D (time, station).
    ds = xr.Dataset(
        {
            'temperature': (
                ('time', 'station'),
                rng.standard_normal((n_time, n_station)),
            ),
        },
        coords={'time': _hour_times(n_time)},
    )
    ofs_ctl = _ctlfile(n_station)

    result = _precompute_scalar_data(
        _schism_props('stofs_3d_atl'), ds, ofs_ctl, 'temp', _logger())

    arr = result['scalar_data']
    assert arr.shape == (n_time, n_station)
    for i in range(n_station):
        np.testing.assert_array_equal(
            arr[:, i], ds['temperature'][:, i].values)
