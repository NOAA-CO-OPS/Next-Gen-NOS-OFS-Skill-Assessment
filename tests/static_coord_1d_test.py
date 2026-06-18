"""Tests for ``_static_coord_1d`` in indexing.py.

The helper normalises static mesh vars to their non-time-replicated form
regardless of whether intake combined the source files with
``data_vars='all'`` (replicates static vars along the concat time dim)
or ``data_vars='minimal'`` (leaves them as-is).

These tests pin the contract: 1-D when the source is 1-D, 2-D when the
source was always 2-D (e.g. FVCOM siglay), and the leading time dim is
dropped when present.
"""

import numpy as np
import pytest
import xarray as xr

from ofs_skill.model_processing.indexing import _static_coord_1d


def _ds_minimal_form():
    """Mimic an FVCOM stations dataset combined with data_vars='minimal'."""
    n_station = 8
    n_siglay = 4
    return xr.Dataset(
        data_vars={
            'lon': (('station',), np.linspace(-71.0, -68.0, n_station)),
            'lat': (('station',), np.linspace(41.0, 44.0, n_station)),
            'h': (('station',), np.full(n_station, 25.0)),
            'siglay': (('siglay', 'station'),
                       np.tile(np.linspace(-1, 0, n_siglay)[:, None],
                               (1, n_station))),
            'zeta': (('time', 'station'),
                     np.zeros((6, n_station))),
        },
        coords={'time': np.datetime64('2026-02-16T00')
                + np.arange(6) * np.timedelta64(1, 'h')},
    )


def _ds_all_form():
    """Mimic an FVCOM stations dataset combined with data_vars='all'.

    Lon/lat/h get replicated along the concat time dim. Siglay was
    already 2-D in the source file so it doesn't grow extra dims.
    """
    n_station = 8
    n_siglay = 4
    n_time = 6
    lon_1d = np.linspace(-71.0, -68.0, n_station)
    lat_1d = np.linspace(41.0, 44.0, n_station)
    h_1d = np.full(n_station, 25.0)
    return xr.Dataset(
        data_vars={
            'lon': (('time', 'station'),
                    np.broadcast_to(lon_1d, (n_time, n_station)).copy()),
            'lat': (('time', 'station'),
                    np.broadcast_to(lat_1d, (n_time, n_station)).copy()),
            'h': (('time', 'station'),
                  np.broadcast_to(h_1d, (n_time, n_station)).copy()),
            'siglay': (('siglay', 'station'),
                       np.tile(np.linspace(-1, 0, n_siglay)[:, None],
                               (1, n_station))),
            'zeta': (('time', 'station'),
                     np.zeros((n_time, n_station))),
        },
        coords={'time': np.datetime64('2026-02-16T00')
                + np.arange(n_time) * np.timedelta64(1, 'h')},
    )


def _ds_roms_minimal_form():
    """Mimic a ROMS (CBOFS) stations dataset under data_vars='minimal'."""
    n_station = 5
    n_srho = 4
    return xr.Dataset(
        data_vars={
            'lon_rho': (('station',), np.linspace(-77.0, -75.0, n_station)),
            'lat_rho': (('station',), np.linspace(36.5, 39.0, n_station)),
            'h': (('station',), np.linspace(5.0, 30.0, n_station)),
            's_rho': (('s_rho',), np.linspace(-1, 0, n_srho)),
            'zeta': (('ocean_time', 'station'),
                     np.zeros((4, n_station))),
        },
        coords={'ocean_time': np.datetime64('2026-02-16T00')
                + np.arange(4) * np.timedelta64(1, 'h')},
    )


# ---------------------------------------------------------------------------
# Minimal form (1-D source) returns unchanged
# ---------------------------------------------------------------------------


def test_minimal_form_returns_unchanged():
    ds = _ds_minimal_form()
    lon = _static_coord_1d(ds, 'lon')
    lat = _static_coord_1d(ds, 'lat')
    h = _static_coord_1d(ds, 'h')

    assert lon.shape == (8,)
    assert lat.shape == (8,)
    assert h.shape == (8,)
    np.testing.assert_array_equal(lon, ds['lon'].values)
    np.testing.assert_array_equal(lat, ds['lat'].values)
    np.testing.assert_array_equal(h, ds['h'].values)


def test_2d_static_var_passthrough():
    """siglay was always (siglay, station) — must NOT be reduced."""
    ds = _ds_minimal_form()
    sig = _static_coord_1d(ds, 'siglay')
    assert sig.shape == (4, 8)
    np.testing.assert_array_equal(sig, ds['siglay'].values)


# ---------------------------------------------------------------------------
# Replicated form (data_vars='all') drops the leading time dim
# ---------------------------------------------------------------------------


def test_all_form_drops_leading_time_dim():
    ds = _ds_all_form()
    # Pre-condition: lon really is 2-D in the test fixture
    assert ds['lon'].dims == ('time', 'station')
    assert ds['lon'].shape == (6, 8)

    lon = _static_coord_1d(ds, 'lon')
    assert lon.shape == (8,), \
        f'leading time dim should be dropped, got {lon.shape}'
    # Values across time were identical (replicated), so any slice equals the others
    np.testing.assert_array_equal(lon, ds['lon'].values[0])


def test_both_forms_return_identical_values():
    """The helper's whole point: same shape + values regardless of intake mode."""
    ds_min = _ds_minimal_form()
    ds_all = _ds_all_form()

    for name in ('lon', 'lat', 'h'):
        a = _static_coord_1d(ds_min, name)
        b = _static_coord_1d(ds_all, name)
        assert a.shape == b.shape, \
            f'{name}: shape differs {a.shape} vs {b.shape}'
        np.testing.assert_array_equal(a, b)


# ---------------------------------------------------------------------------
# ROMS shape
# ---------------------------------------------------------------------------


def test_roms_minimal_form():
    ds = _ds_roms_minimal_form()
    lon_rho = _static_coord_1d(ds, 'lon_rho')
    lat_rho = _static_coord_1d(ds, 'lat_rho')
    h = _static_coord_1d(ds, 'h')
    s_rho = _static_coord_1d(ds, 's_rho')

    assert lon_rho.shape == (5,)
    assert lat_rho.shape == (5,)
    assert h.shape == (5,)
    assert s_rho.shape == (4,)
    # s_rho is on the s_rho dim, not station — must not be touched
    np.testing.assert_array_equal(s_rho, ds['s_rho'].values)


def test_ocean_time_recognised_as_concat_dim():
    """ROMS uses 'ocean_time', not 'time'. Helper must recognise both."""
    n_time, n_station = 3, 5
    ds = xr.Dataset(
        data_vars={
            'lon_rho': (('ocean_time', 'station'),
                        np.broadcast_to(np.arange(n_station, dtype=float),
                                        (n_time, n_station)).copy()),
        },
        coords={'ocean_time': np.datetime64('2026-02-16T00')
                + np.arange(n_time) * np.timedelta64(1, 'h')},
    )
    lon = _static_coord_1d(ds, 'lon_rho')
    assert lon.shape == (n_station,)


def test_3d_static_with_leading_time_drops_only_time():
    """A 3-D static (e.g. siglay if it ever got replicated) drops the time
    axis but keeps the rest."""
    n_time, n_siglay, n_station = 4, 5, 7
    ds = xr.Dataset(
        data_vars={
            'siglay': (('time', 'siglay', 'station'),
                       np.broadcast_to(
                           np.linspace(-1, 0, n_siglay)[:, None],
                           (n_time, n_siglay, n_station)).copy()),
        },
        coords={'time': np.datetime64('2026-02-16T00')
                + np.arange(n_time) * np.timedelta64(1, 'h')},
    )
    siglay = _static_coord_1d(ds, 'siglay')
    assert siglay.shape == (n_siglay, n_station), \
        f'expected (siglay, station), got {siglay.shape}'


# ---------------------------------------------------------------------------
# Shape guard: reject pathological inputs that would mis-index downstream
# ---------------------------------------------------------------------------


def test_shape_guard_rejects_time_only_1d_var():
    """A 1-D var on the time dim alone (no station dim) yields a 0-D scalar
    after the time-dim drop — the helper must refuse rather than hand a
    useless scalar to downstream nearest-node code."""
    n_time = 6
    ds = xr.Dataset(
        data_vars={
            'bogus': (('time',), np.zeros(n_time)),
        },
        coords={'time': np.datetime64('2026-02-16T00')
                + np.arange(n_time) * np.timedelta64(1, 'h')},
    )
    with pytest.raises(ValueError, match='unexpected shape after dropping'):
        _static_coord_1d(ds, 'bogus')
