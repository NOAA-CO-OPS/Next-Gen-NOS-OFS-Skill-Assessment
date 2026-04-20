"""Tests for ofs_skill.open_boundary.obc_processing.

Covers haversine, _decode_time (CF / Itime-Itime2 / MJD fallback),
mask_distance_gaps, transform_to_z (including sigma non-uniformity and
seafloor/surface masking), make_x_labels, parameter_validation, and
load_obc_file. Synthetic xarray.Dataset fixtures avoid requiring real
FVCOM OBC .nc files.
"""
from __future__ import annotations

import logging
import os
import types
from datetime import timedelta

import numpy as np
import pytest
import xarray as xr

from ofs_skill.open_boundary import obc_processing as op

# ---------------------------------------------------------------------------
# Synthetic dataset fixtures
# ---------------------------------------------------------------------------
N_TIME = 4
N_SIGLAY = 10
N_NODE = 20


def _stretched_siglay(n_sig: int, n_node: int) -> np.ndarray:
    """Generate a stretched sigma layer array in [-1, 0].

    s * sqrt(|s|) preserves the sign and produces finer spacing near s=0
    (surface) and coarser near s=-1 (bottom), mimicking FVCOM generalized
    sigma. Values live in [-1, 0] by construction.
    """
    s = np.linspace(-1.0, 0.0, n_sig)
    stretched = s * np.sqrt(np.abs(s))
    arr = np.tile(stretched[:, None], (1, n_node))
    return np.clip(arr, -1.0, 0.0)


def _uniform_siglay(n_sig: int, n_node: int) -> np.ndarray:
    s = np.linspace(-1.0, 0.0, n_sig)
    return np.tile(s[:, None], (1, n_node))


def _make_ds(
    n_time: int = N_TIME,
    n_sig: int = N_SIGLAY,
    n_node: int = N_NODE,
    stretched: bool = True,
    with_zeta: bool = True,
    time_units: str | None = 'days since 2000-01-01 00:00:00',
    time_values: np.ndarray | None = None,
    add_itime: bool = False,
    gap: bool = False,
) -> xr.Dataset:
    """Build a minimal FVCOM-like OBC xarray.Dataset."""
    lats = np.linspace(41.0, 41.5, n_node)
    lons = np.linspace(-83.0, -82.5, n_node)
    if gap:
        # Create a big jump between nodes 9 and 10
        lons[10:] += 2.0  # +~170 km
    h = np.linspace(5.0, 200.0, n_node)
    siglay = _stretched_siglay(n_sig, n_node) if stretched else _uniform_siglay(n_sig, n_node)
    zeta = 0.2 * np.sin(np.linspace(0, 2 * np.pi, n_time))[:, None] * np.ones(n_node)[None, :]

    rng = np.random.default_rng(42)
    temp = 15.0 + rng.normal(scale=0.2, size=(n_time, n_sig, n_node))
    salinity = 32.0 + rng.normal(scale=0.1, size=(n_time, n_sig, n_node))

    if time_values is None:
        time_values = np.arange(n_time, dtype=float)

    data_vars: dict = {
        'h': (('node',), h),
        'siglay': (('siglay', 'node'), siglay),
        'temp': (('time', 'siglay', 'node'), temp),
        'salinity': (('time', 'siglay', 'node'), salinity),
    }
    if with_zeta:
        data_vars['zeta'] = (('time', 'node'), zeta)
    if add_itime:
        # Itime = integer days since MJD epoch; Itime2 = ms past midnight
        # Pick MJD of 2020-01-01 = 58849
        itime = np.full(n_time, 58849, dtype=np.int64) + np.arange(n_time, dtype=np.int64)
        itime2 = np.zeros(n_time, dtype=np.int64)
        data_vars['Itime'] = (('time',), itime)
        data_vars['Itime2'] = (('time',), itime2)

    ds = xr.Dataset(
        data_vars,
        coords={
            'lon': (('node',), lons),
            'lat': (('node',), lats),
            'time': (('time',), time_values),
        },
    )
    if time_units:
        ds['time'].attrs['units'] = time_units
        ds['time'].attrs['calendar'] = 'standard'
    return ds


@pytest.fixture()
def synth_ds():
    return _make_ds()


@pytest.fixture()
def synth_ds_no_zeta():
    return _make_ds(with_zeta=False)


@pytest.fixture()
def synth_ds_gap():
    return _make_ds(gap=True)


@pytest.fixture()
def logger():
    lg = logging.getLogger('obc_test')
    lg.setLevel(logging.DEBUG)
    return lg


# ---------------------------------------------------------------------------
# haversine
# ---------------------------------------------------------------------------
class TestHaversine:
    def test_nyc_to_la(self):
        # NYC 40.7128, -74.0060 — LA 34.0522, -118.2437 ~ 3935 km
        d = op.haversine(40.7128, -74.0060, 34.0522, -118.2437)
        assert d == pytest.approx(3935.0, abs=25.0)

    def test_symmetry(self):
        a = op.haversine(10.0, 20.0, 30.0, 40.0)
        b = op.haversine(30.0, 40.0, 10.0, 20.0)
        assert a == pytest.approx(b, abs=1e-9)

    def test_zero_for_identical(self):
        assert op.haversine(12.34, 56.78, 12.34, 56.78) == pytest.approx(0.0, abs=1e-9)

    def test_small_distance(self):
        # 1 deg lat ~ 111 km
        d = op.haversine(0.0, 0.0, 1.0, 0.0)
        assert d == pytest.approx(111.19, abs=0.5)


# ---------------------------------------------------------------------------
# _decode_time
# ---------------------------------------------------------------------------
class TestDecodeTime:
    def test_cf_units(self, synth_ds, logger):
        out = op._decode_time(synth_ds, logger)
        assert isinstance(out, list)
        assert len(out) == N_TIME
        assert out[0].year == 2000
        assert out[0].month == 1
        assert out[0].day == 1

    def test_itime_itime2_branch(self, logger):
        # Strip CF units so resolver falls through to Itime branch
        ds = _make_ds(time_units=None, add_itime=True)
        out = op._decode_time(ds, logger)
        assert len(out) == N_TIME
        # MJD 58849 = 2020-01-01
        assert out[0].year == 2020
        assert out[0].month == 1
        assert out[0].day == 1
        # Consecutive days
        assert out[1] == out[0] + timedelta(days=1)

    def test_mjd_fallback_warns(self, caplog, logger):
        # Reset one-shot flag so caplog catches the warning
        op._TIME_FALLBACK_WARNED = False
        ds = _make_ds(time_units=None, add_itime=False,
                      time_values=np.array([60000.0, 60001.0, 60002.0, 60003.0]))
        with caplog.at_level(logging.WARNING, logger=logger.name):
            out = op._decode_time(ds, logger)
        # MJD 60000 = 2023-02-25
        assert out[0].year == 2023
        # One-shot warning must fire on the first unguarded call
        assert any('MJD epoch' in r.message for r in caplog.records)

    def test_cf_decode_failure_falls_through(self, logger):
        # Bogus units — expect xarray decoder to raise, then fall through to MJD.
        op._TIME_FALLBACK_WARNED = False
        ds = _make_ds(time_units='not a real units string',
                      time_values=np.array([60000.0, 60001.0, 60002.0, 60003.0]))
        out = op._decode_time(ds, logger)
        assert len(out) == N_TIME


# ---------------------------------------------------------------------------
# mask_distance_gaps
# ---------------------------------------------------------------------------
class TestMaskDistanceGaps:
    def test_single_big_gap_masked(self, logger):
        # Regular spacing 1km, one big gap (10km) in the middle
        x_orig = np.array([0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 15.0, 16.0, 17.0, 18.0])
        x_interp = np.linspace(0.0, 18.0, 200)
        z = np.ones((2, 3, 200))
        out = op.mask_distance_gaps(x_orig, x_interp, z.copy(), logger)
        # Cells strictly inside (5, 15) must be NaN
        inside = (x_interp > 5.0) & (x_interp < 15.0)
        assert np.all(np.isnan(out[:, :, inside]))
        # Cells outside the gap should still be 1.0
        outside = x_interp <= 5.0
        assert np.all(out[:, :, outside] == 1.0)

    def test_regular_spacing_no_mask(self, logger):
        x_orig = np.linspace(0.0, 10.0, 11)
        x_interp = np.linspace(0.0, 10.0, 200)
        z = np.ones((2, 3, 200))
        out = op.mask_distance_gaps(x_orig, x_interp, z.copy(), logger)
        assert not np.any(np.isnan(out))

    def test_two_gaps_both_masked(self, logger):
        # Two big gaps — median+3IQR still catches both
        x_orig = np.array([0.0, 1.0, 2.0, 12.0, 13.0, 14.0, 24.0, 25.0, 26.0])
        x_interp = np.linspace(0.0, 26.0, 300)
        z = np.ones((1, 2, 300))
        out = op.mask_distance_gaps(x_orig, x_interp, z.copy(), logger)
        first_gap = (x_interp > 2.0) & (x_interp < 12.0)
        second_gap = (x_interp > 14.0) & (x_interp < 24.0)
        assert np.all(np.isnan(out[:, :, first_gap]))
        assert np.all(np.isnan(out[:, :, second_gap]))


# ---------------------------------------------------------------------------
# transform_to_z
# ---------------------------------------------------------------------------
class TestTransformToZ:
    def test_happy_path_shape(self, synth_ds, logger):
        x_labels = op.make_x_labels(synth_ds, logger)
        z, ref_depth, x_grid = op.transform_to_z(synth_ds, 'temp', x_labels, logger)
        # (n_time_subset, siglay_len, len(x_grid))
        assert z.ndim == 3
        assert z.shape[0] == N_TIME  # n_time <= 55, no time subsample
        assert z.shape[2] == len(x_grid)
        assert ref_depth.ndim == 1
        assert ref_depth[0] == pytest.approx(0.0)
        assert ref_depth[-1] == pytest.approx(200.0, abs=1e-6)

    def test_seafloor_mask_at_shallow_node(self, synth_ds, logger):
        # Node 0 has h=5 m. Rows where ref_depth > 5+zeta should be NaN.
        x_labels = op.make_x_labels(synth_ds, logger)
        z, ref_depth, x_grid = op.transform_to_z(synth_ds, 'temp', x_labels, logger)
        # Nearest column index to x_grid[0]
        col0 = 0
        deep_rows = ref_depth > 6.0  # well below max zeta amplitude (0.2 m)
        assert np.all(np.isnan(z[:, deep_rows, col0]))

    def test_no_zeta_path_warns(self, synth_ds_no_zeta, logger, caplog):
        op._ZETA_MISSING_WARNED = False
        x_labels = op.make_x_labels(synth_ds_no_zeta, logger)
        with caplog.at_level(logging.WARNING, logger=logger.name):
            z, ref_depth, x_grid = op.transform_to_z(
                synth_ds_no_zeta, 'temp', x_labels, logger)
        assert z.ndim == 3
        assert any('free surface = 0' in r.message for r in caplog.records)

    def test_nonuniform_siglay_differs_from_uniform(self, logger):
        ds_u = _make_ds(stretched=False)
        ds_s = _make_ds(stretched=True)
        # Use identical temp/salt in both datasets for a fair comparison.
        rng = np.random.default_rng(7)
        temp_fixed = 15.0 + rng.normal(scale=0.2, size=(N_TIME, N_SIGLAY, N_NODE))
        ds_u['temp'] = (('time', 'siglay', 'node'), temp_fixed)
        ds_s['temp'] = (('time', 'siglay', 'node'), temp_fixed)

        x_u = op.make_x_labels(ds_u, logger)
        x_s = op.make_x_labels(ds_s, logger)
        z_u, _, _ = op.transform_to_z(ds_u, 'temp', x_u, logger)
        z_s, _, _ = op.transform_to_z(ds_s, 'temp', x_s, logger)

        # Different siglay → different interpolated z values somewhere.
        diff = np.nanmean(np.abs(z_u - z_s))
        assert diff > 0.0

    def test_surface_mask_with_negative_zeta(self, logger):
        # Force zeta negative (depression). Rows where ref_depth < -zeta → NaN.
        ds = _make_ds()
        # All zeta = -0.5 so -zeta = +0.5; ref_depth < 0.5 masked.
        ds['zeta'].values[...] = -0.5
        x_labels = op.make_x_labels(ds, logger)
        z, ref_depth, x_grid = op.transform_to_z(ds, 'temp', x_labels, logger)
        shallow_rows = ref_depth < 0.49
        # All shallow rows must be NaN (above free surface)
        assert np.all(np.isnan(z[:, shallow_rows, :]))


# ---------------------------------------------------------------------------
# make_x_labels
# ---------------------------------------------------------------------------
class TestMakeXLabels:
    def test_starts_at_zero(self, synth_ds, logger):
        x = op.make_x_labels(synth_ds, logger)
        assert x[0] == 0.0

    def test_length_matches_nodes(self, synth_ds, logger):
        x = op.make_x_labels(synth_ds, logger)
        assert len(x) == N_NODE

    def test_monotone(self, synth_ds, logger):
        x = op.make_x_labels(synth_ds, logger)
        assert np.all(np.diff(x) >= 0.0)

    def test_three_point_triangle(self, logger):
        # (0,0) -> (0,1) -> (1,1) : each leg ~ 111 km
        ds = xr.Dataset(
            {'h': (('node',), np.ones(3))},
            coords={
                'lon': (('node',), np.array([0.0, 1.0, 1.0])),
                'lat': (('node',), np.array([0.0, 0.0, 1.0])),
            },
        )
        x = op.make_x_labels(ds, logger)
        assert len(x) == 3
        assert x[0] == pytest.approx(0.0)
        assert x[1] == pytest.approx(111.19, abs=0.5)
        assert x[2] == pytest.approx(222.38, abs=1.0)


# ---------------------------------------------------------------------------
# parameter_validation
# ---------------------------------------------------------------------------
class TestParameterValidation:
    def _make_prop(self, tmp_path, ofs='leofs'):
        prop = types.SimpleNamespace()
        prop.path = str(tmp_path)
        prop.ofs = ofs
        prop.model_source = 'fvcom'
        prop.model_cycle = '00'
        prop.start_date_full = '2026-04-01T00:00:00Z'
        return prop

    def test_bad_date_exits(self, tmp_path, logger):
        prop = self._make_prop(tmp_path)
        prop.start_date_full = 'not-a-date'
        with pytest.raises(SystemExit):
            op.parameter_validation(prop, logger)

    def test_roms_rejected(self, tmp_path, logger):
        prop = self._make_prop(tmp_path)
        prop.model_source = 'roms'
        with pytest.raises(SystemExit):
            op.parameter_validation(prop, logger)

    def test_missing_ofs_extents_exits(self, tmp_path, logger):
        prop = self._make_prop(tmp_path)
        # No ofs_extents dir created
        with pytest.raises(SystemExit):
            op.parameter_validation(prop, logger)

    def test_missing_shapefile_exits(self, tmp_path, logger):
        prop = self._make_prop(tmp_path, ofs='no_such_ofs')
        os.makedirs(tmp_path / 'ofs_extents', exist_ok=True)
        with pytest.raises(SystemExit):
            op.parameter_validation(prop, logger)

    def test_happy_path_creates_dirs(self, tmp_path, logger):
        prop = self._make_prop(tmp_path, ofs='leofs')
        ofs_ext = tmp_path / 'ofs_extents'
        ofs_ext.mkdir()
        (ofs_ext / 'leofs.shp').write_bytes(b'')
        op.parameter_validation(prop, logger)
        assert os.path.isdir(prop.control_files_path)
        assert os.path.isdir(prop.visuals_1d_station_path)
        assert os.path.isdir(prop.data_model_1d_node_path)
        assert hasattr(prop, 'model_obc_path')


# ---------------------------------------------------------------------------
# load_obc_file
# ---------------------------------------------------------------------------
class TestLoadObcFile:
    def test_missing_file_exits(self, tmp_path, logger):
        prop = types.SimpleNamespace()
        prop.ofs = 'leofs'  # fvcom
        prop.model_cycle = '00'
        prop.start_date_full = '2026-04-01T00:00:00Z'
        prop.model_obc_path = str(tmp_path / 'nope')
        with pytest.raises(SystemExit):
            op.load_obc_file(prop, logger)

    def test_happy_path_loads(self, tmp_path, synth_ds, logger):
        prop = types.SimpleNamespace()
        prop.ofs = 'leofs'
        prop.model_cycle = '00'
        prop.start_date_full = '2026-04-01T00:00:00Z'
        prop.model_obc_path = str(tmp_path)

        # Write the dataset at the exact expected path:
        #   <model_obc_path>/YYYYMM/<ofs>.t<cycle>z.YYYYMMDD.obc.nc
        subdir = tmp_path / '202604'
        subdir.mkdir()
        # Strip the CF units so xarray open with decode_times=False doesn't blow up
        ds_write = synth_ds.copy()
        ds_write['time'].attrs.pop('units', None)
        ds_write['time'].attrs.pop('calendar', None)
        filepath = subdir / 'leofs.t00z.20260401.obc.nc'
        ds_write.to_netcdf(filepath)

        ds = op.load_obc_file(prop, logger)
        assert 'temp' in ds
        assert 'h' in ds
        ds.close()


# ---------------------------------------------------------------------------
# __init__ re-exports
# ---------------------------------------------------------------------------
def test_init_exports():
    from ofs_skill import open_boundary
    for name in ('haversine', 'load_obc_file', 'make_x_labels',
                 'mask_distance_gaps', 'parameter_validation',
                 'transform_to_z', 'plot_fvcom_obc'):
        assert hasattr(open_boundary, name), f'missing: {name}'
    assert 'plot_fvcom_obc' in open_boundary.__all__
