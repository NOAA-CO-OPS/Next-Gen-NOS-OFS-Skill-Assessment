"""Regression tests for ``_node_value`` in ``get_datum_offset.py``.

The datum-offset code path reads per-station coords (lon/lat/x/y/Jpos/
Ipos) using ``model[var][0, node]``. Under the legacy
``data_vars='all'`` intake those vars were replicated to
``(time, station)`` and ``[0, node]`` returned a scalar. Under the
current ``data_vars='minimal'`` intake the same vars stay
``(station,)`` and ``[0, node]`` raises ``IndexError: too many indices``
— which then triggers the bare-``except`` in ``get_datum_offset`` and
silently returns the ``-9992`` sentinel, causing every WL ``.prd`` to
inherit a garbage datum offset.

The new ``_node_value`` helper inspects ``var.dims`` and reads with the
right rank. These tests verify it works under both shapes.
"""

import numpy as np
import xarray as xr

from ofs_skill.model_processing.get_datum_offset import _node_value


def _ds_fvcom_minimal():
    """FVCOM stations dataset as intake produces under data_vars='minimal'."""
    n_station = 6
    return xr.Dataset(
        data_vars={
            'lon': (('station',),
                    np.linspace(-71.0, -68.0, n_station, dtype=np.float64)),
            'lat': (('station',),
                    np.linspace(41.0, 44.0, n_station, dtype=np.float64)),
        },
        coords={'time': np.array(['2026-02-16T00:00'], dtype='datetime64[m]')},
    )


def _ds_fvcom_legacy():
    """Same dataset under legacy data_vars='all' (lon/lat replicated)."""
    ds = _ds_fvcom_minimal()
    n_time = 4
    lon_1d = ds['lon'].values
    lat_1d = ds['lat'].values
    n_station = lon_1d.shape[0]
    lon_2d = np.broadcast_to(lon_1d, (n_time, n_station)).copy()
    lat_2d = np.broadcast_to(lat_1d, (n_time, n_station)).copy()
    return xr.Dataset(
        data_vars={
            'lon': (('time', 'station'), lon_2d),
            'lat': (('time', 'station'), lat_2d),
        },
        coords={
            'time': np.datetime64('2026-02-16T00')
                    + np.arange(n_time) * np.timedelta64(6, 'm'),
        },
    )


def _ds_roms_minimal():
    """ROMS stations dataset as intake produces under data_vars='minimal'."""
    n_station = 4
    return xr.Dataset(
        data_vars={
            'Jpos': (('station',),
                     np.arange(10, 10 + n_station, dtype=np.int32)),
            'Ipos': (('station',),
                     np.arange(20, 20 + n_station, dtype=np.int32)),
        },
        coords={'ocean_time': np.array(['2026-02-16T00:00'],
                                       dtype='datetime64[m]')},
    )


def _ds_roms_legacy():
    """Same ROMS dataset under legacy ocean_time-replicated shape."""
    ds = _ds_roms_minimal()
    n_time = 3
    j_1d = ds['Jpos'].values
    i_1d = ds['Ipos'].values
    n_station = j_1d.shape[0]
    j_2d = np.broadcast_to(j_1d, (n_time, n_station)).copy()
    i_2d = np.broadcast_to(i_1d, (n_time, n_station)).copy()
    return xr.Dataset(
        data_vars={
            'Jpos': (('ocean_time', 'station'), j_2d),
            'Ipos': (('ocean_time', 'station'), i_2d),
        },
        coords={
            'ocean_time': np.datetime64('2026-02-16T00')
                          + np.arange(n_time) * np.timedelta64(6, 'm'),
        },
    )


# ---------------------------------------------------------------------------
# minimal-form (1-D) intake
# ---------------------------------------------------------------------------


def test_fvcom_minimal_lon():
    ds = _ds_fvcom_minimal()
    for node in range(ds.sizes['station']):
        assert _node_value(ds, 'lon', node) == float(ds['lon'].values[node])


def test_fvcom_minimal_lat():
    ds = _ds_fvcom_minimal()
    for node in range(ds.sizes['station']):
        assert _node_value(ds, 'lat', node) == float(ds['lat'].values[node])


def test_roms_minimal_jpos_ipos():
    ds = _ds_roms_minimal()
    for node in range(ds.sizes['station']):
        assert _node_value(ds, 'Jpos', node) == float(ds['Jpos'].values[node])
        assert _node_value(ds, 'Ipos', node) == float(ds['Ipos'].values[node])


# ---------------------------------------------------------------------------
# legacy-form (2-D, time-replicated) intake
# ---------------------------------------------------------------------------


def test_fvcom_legacy_strips_leading_time():
    ds = _ds_fvcom_legacy()
    assert ds['lon'].dims == ('time', 'station')
    for node in range(ds.sizes['station']):
        # Helper picks time=0 row implicitly; values are identical across
        # the replicated axis, so any row would do.
        assert _node_value(ds, 'lon', node) == float(ds['lon'].values[0, node])


def test_roms_legacy_strips_leading_ocean_time():
    ds = _ds_roms_legacy()
    assert ds['Jpos'].dims == ('ocean_time', 'station')
    for node in range(ds.sizes['station']):
        assert _node_value(ds, 'Jpos', node) == float(ds['Jpos'].values[0, node])
        assert _node_value(ds, 'Ipos', node) == float(ds['Ipos'].values[0, node])


# ---------------------------------------------------------------------------
# Both intakes return identical values for the same node
# ---------------------------------------------------------------------------


def test_both_intake_modes_match_for_fvcom():
    """Whatever shape lon/lat carry, _node_value picks the same number
    for the same station index. This is the contract that protects
    downstream datum lookups."""
    ds_min = _ds_fvcom_minimal()
    ds_all = _ds_fvcom_legacy()
    for node in range(ds_min.sizes['station']):
        assert _node_value(ds_min, 'lon', node) == \
               _node_value(ds_all, 'lon', node)
        assert _node_value(ds_min, 'lat', node) == \
               _node_value(ds_all, 'lat', node)


def test_returns_scalar_float():
    """Downstream callers do arithmetic on the return value; must be float."""
    ds = _ds_fvcom_minimal()
    out = _node_value(ds, 'lon', 0)
    assert isinstance(out, float)
    out_legacy = _node_value(_ds_fvcom_legacy(), 'lon', 0)
    assert isinstance(out_legacy, float)
