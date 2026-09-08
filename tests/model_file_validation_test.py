"""
Tests for pre-open model file validation (model_file_validation.py,
issue #194): unreadable, incomplete, or dimensionally inconsistent
files must be dropped with a warning instead of crashing the
multi-file open.
"""

import logging
from types import SimpleNamespace

import numpy as np
import pytest
import xarray as xr

from ofs_skill.model_processing.model_file_validation import (
    scrub_cached_copies,
    validate_model_files,
)

LOG = logging.getLogger('model_file_validation_test')

N_TIME = 24
N_STATION = 12
N_SIGLAY = 4


def _stations_ds(n_time=N_TIME, n_station=N_STATION, n_siglay=N_SIGLAY,
                 t0='2026-05-14T00'):
    n_siglev = n_siglay + 1
    return xr.Dataset(
        data_vars={
            'h': (('station',),
                  np.linspace(5.0, 30.0, n_station)),
            'siglay': (('siglay', 'station'),
                       np.zeros((n_siglay, n_station))),
            'siglev': (('siglev', 'station'),
                       np.zeros((n_siglev, n_station))),
            'zeta': (('time', 'station'),
                     np.random.default_rng(0).normal(
                         0, 1, (n_time, n_station))),
        },
        coords={'time': np.datetime64(t0)
                + np.arange(n_time) * np.timedelta64(6, 'm')},
    )


def _write_nc3(tmp_path, name, ds=None, unlimited=True):
    ds = _stations_ds() if ds is None else ds
    path = tmp_path / name
    kwargs = {'format': 'NETCDF3_CLASSIC'}
    if unlimited:
        kwargs['unlimited_dims'] = ['time']
    ds.to_netcdf(path, **kwargs)
    return str(path)


def _write_nc4(tmp_path, name, ds=None):
    ds = _stations_ds() if ds is None else ds
    path = tmp_path / name
    ds.to_netcdf(path, format='NETCDF4')
    return str(path)


def _truncate(path, keep_fraction):
    import os
    size = os.path.getsize(path)
    with open(path, 'r+b') as fp:
        fp.truncate(int(size * keep_fraction))
    return path


# ---------------------------------------------------------------------
# per-file checks
# ---------------------------------------------------------------------

def test_all_good_files_pass(tmp_path):
    files = [_write_nc3(tmp_path, f'f{i}.nc') for i in range(3)]
    valid, dropped, _ = validate_model_files(
        files, 'netcdf4', 'time', 'stations', LOG)
    assert valid == files
    assert dropped == []


def test_zero_byte_file_dropped(tmp_path):
    good = _write_nc3(tmp_path, 'good.nc')
    bad = tmp_path / 'empty.nc'
    bad.write_bytes(b'')
    valid, dropped, _ = validate_model_files(
        [good, str(bad)], 'netcdf4', 'time', 'stations', LOG)
    assert valid == [good]
    assert len(dropped) == 1 and dropped[0][0] == str(bad)
    assert 'unreadable' in dropped[0][1]


def test_garbage_file_dropped(tmp_path):
    good = _write_nc3(tmp_path, 'good.nc')
    bad = tmp_path / 'garbage.nc'
    bad.write_bytes(b'this is not a netcdf file' * 100)
    valid, dropped, _ = validate_model_files(
        [good, str(bad)], 'netcdf4', 'time', 'stations', LOG)
    assert valid == [good]
    assert dropped[0][0] == str(bad)


def test_truncated_netcdf3_dropped(tmp_path):
    good = _write_nc3(tmp_path, 'good.nc')
    bad = _truncate(_write_nc3(tmp_path, 'trunc.nc'), 0.4)
    valid, dropped, _ = validate_model_files(
        [good, bad], 'netcdf4', 'time', 'stations', LOG)
    assert valid == [good]
    assert dropped[0][0] == bad


def test_truncated_hdf5_dropped(tmp_path):
    good = _write_nc4(tmp_path, 'good.nc')
    bad = _truncate(_write_nc4(tmp_path, 'trunc.nc'), 0.4)
    valid, dropped, _ = validate_model_files(
        [good, bad], 'h5netcdf', 'time', 'stations', LOG)
    assert valid == [good]
    assert dropped[0][0] == bad


def test_empty_time_dimension_dropped(tmp_path):
    good = _write_nc3(tmp_path, 'good.nc')
    bad = _write_nc3(tmp_path, 'notime.nc', ds=_stations_ds(n_time=0))
    valid, dropped, _ = validate_model_files(
        [good, bad], 'netcdf4', 'time', 'stations', LOG)
    assert valid == [good]
    assert "empty 'time'" in dropped[0][1]


def test_missing_time_dimension_dropped(tmp_path):
    good = _write_nc3(tmp_path, 'good.nc')
    static_only = xr.Dataset(
        {'h': (('station',), np.zeros(N_STATION))})
    bad = _write_nc3(tmp_path, 'static.nc', ds=static_only,
                     unlimited=False)
    valid, dropped, _ = validate_model_files(
        [good, bad], 'netcdf4', 'time', 'stations', LOG)
    assert valid == [good]
    assert "missing 'time'" in dropped[0][1]


def test_zero_filled_time_tail_dropped(tmp_path):
    # The actual issue #194 signature: libnetCDF silently zero-fills
    # NetCDF-3 reads past EOF, so a truncated file reads back with a
    # zeroed time tail instead of raising. The strictly-increasing
    # check must reject it.
    good = _write_nc3(tmp_path, 'good.nc')
    ds = _stations_ds()
    tvals = ds['time'].values.copy()
    tvals[N_TIME // 2:] = tvals[0]  # simulate the zero-filled tail
    ds = ds.assign_coords(time=tvals)
    bad = _write_nc3(tmp_path, 'zerofill.nc', ds=ds)
    valid, dropped, _ = validate_model_files(
        [good, bad], 'netcdf4', 'time', 'stations', LOG)
    assert valid == [good]
    assert 'not strictly increasing' in dropped[0][1]


# ---------------------------------------------------------------------
# batch consistency
# ---------------------------------------------------------------------

def test_minority_siglay_mismatch_dropped(tmp_path):
    files = [_write_nc3(tmp_path, f'f{i}.nc') for i in range(3)]
    odd = _write_nc3(tmp_path, 'odd.nc',
                     ds=_stations_ds(n_siglay=N_SIGLAY + 2))
    valid, dropped, _ = validate_model_files(
        files + [odd], 'netcdf4', 'time', 'stations', LOG)
    assert valid == files
    assert dropped[0][0] == odd
    assert 'dimension mismatch' in dropped[0][1]


def test_station_dim_mismatch_exempt_for_stations(tmp_path):
    # Differing station counts are legitimate for stations files and
    # handled downstream by remove_extra_stations — must NOT be dropped.
    files = [_write_nc3(tmp_path, f'f{i}.nc') for i in range(2)]
    fewer = _write_nc3(tmp_path, 'fewer.nc',
                       ds=_stations_ds(n_station=N_STATION - 4))
    valid, dropped, _ = validate_model_files(
        files + [fewer], 'netcdf4', 'time', 'stations', LOG)
    assert valid == files + [fewer]
    assert dropped == []


def test_station_dim_mismatch_dropped_for_fields(tmp_path):
    files = [_write_nc3(tmp_path, f'f{i}.nc') for i in range(2)]
    fewer = _write_nc3(tmp_path, 'fewer.nc',
                       ds=_stations_ds(n_station=N_STATION - 4))
    valid, dropped, _ = validate_model_files(
        files + [fewer], 'netcdf4', 'time', 'fields', LOG)
    assert valid == files
    assert dropped[0][0] == fewer


def test_differing_time_lengths_are_fine(tmp_path):
    # Time is the concat dim — an incomplete-but-readable forecast with
    # fewer steps is usable data and must be kept.
    full = _write_nc3(tmp_path, 'full.nc')
    short = _write_nc3(tmp_path, 'short.nc',
                       ds=_stations_ds(n_time=N_TIME // 3))
    valid, dropped, _ = validate_model_files(
        [full, short], 'netcdf4', 'time', 'stations', LOG)
    assert valid == [full, short]
    assert dropped == []


def test_static_metadata_minority_dropped(tmp_path):
    # Same dims everywhere, but one file carries different static
    # values (e.g. relocated stations after a model update) — the
    # exact case behind "MergeError: conflicting values for variable
    # 'x'" on multi-month runs. The minority deployment is dropped.
    files = [_write_nc3(tmp_path, f'f{i}.nc') for i in range(3)]
    ds = _stations_ds()
    ds['h'] = (('station',), np.linspace(6.0, 31.0, N_STATION))
    moved = _write_nc3(tmp_path, 'moved.nc', ds=ds)
    valid, dropped, _ = validate_model_files(
        files + [moved], 'netcdf4', 'time', 'stations', LOG)
    assert valid == files
    assert dropped[0][0] == moved
    assert 'static station metadata' in dropped[0][1]


def test_static_metadata_no_majority_warns_but_keeps(tmp_path, caplog):
    # Every file differs (no clear majority): don't guess — keep all
    # files and warn that the combine may fail.
    files = []
    for i in range(3):
        ds = _stations_ds()
        ds['h'] = (('station',),
                   np.linspace(5.0 + i, 30.0 + i, N_STATION))
        files.append(_write_nc3(tmp_path, f'f{i}.nc', ds=ds))
    with caplog.at_level(logging.WARNING):
        valid, dropped, _ = validate_model_files(
            files, 'netcdf4', 'time', 'stations', LOG)
    assert valid == files
    assert dropped == []
    assert any('no clear majority' in r.message for r in caplog.records)


def test_static_metadata_not_checked_for_fields(tmp_path):
    # Fields batches are combined differently; the fingerprint check
    # only applies to stations files.
    files = [_write_nc3(tmp_path, f'f{i}.nc') for i in range(2)]
    ds = _stations_ds()
    ds['h'] = (('station',), np.linspace(6.0, 31.0, N_STATION))
    moved = _write_nc3(tmp_path, 'moved.nc', ds=ds)
    valid, dropped, _ = validate_model_files(
        files + [moved], 'netcdf4', 'time', 'fields', LOG)
    assert valid == files + [moved]
    assert dropped == []


# ---------------------------------------------------------------------
# batch-level behavior
# ---------------------------------------------------------------------

def test_all_files_bad_raises_system_exit(tmp_path):
    bad1 = tmp_path / 'a.nc'
    bad1.write_bytes(b'nope')
    bad2 = tmp_path / 'b.nc'
    bad2.write_bytes(b'')
    with pytest.raises(SystemExit):
        validate_model_files(
            [str(bad1), str(bad2)], 'netcdf4', 'time', 'stations', LOG)


def test_remote_urls_pass_through(tmp_path):
    urls = ['https://noaa-nos-ofs-pds.s3.amazonaws.com/x.nc',
            'simplecache::https://noaa-nos-ofs-pds.s3.amazonaws.com/y.nc',
            's3://bucket/z.nc']
    valid, dropped, _ = validate_model_files(
        urls, 'h5netcdf', 'time', 'stations', LOG)
    assert valid == urls
    assert dropped == []


def test_mixed_remote_and_bad_local(tmp_path):
    url = 'https://noaa-nos-ofs-pds.s3.amazonaws.com/x.nc'
    bad = tmp_path / 'bad.nc'
    bad.write_bytes(b'garbage')
    valid, dropped, _ = validate_model_files(
        [url, str(bad)], 'netcdf4', 'time', 'stations', LOG)
    assert valid == [url]
    assert dropped[0][0] == str(bad)


def test_order_preserved_after_drop(tmp_path):
    f1 = _write_nc3(tmp_path, 'a.nc')
    bad = tmp_path / 'b.nc'
    bad.write_bytes(b'junk')
    f3 = _write_nc3(tmp_path, 'c.nc')
    valid, _, _ = validate_model_files(
        [f1, str(bad), f3], 'netcdf4', 'time', 'stations', LOG)
    assert valid == [f1, f3]


def test_dims_returned_for_surviving_local_files(tmp_path):
    # Callers reuse the probed dimensions (e.g. the station-dimension
    # compatibility check) instead of re-opening every file.
    files = [_write_nc3(tmp_path, f'f{i}.nc') for i in range(2)]
    url = 'https://noaa-nos-ofs-pds.s3.amazonaws.com/x.nc'
    bad = tmp_path / 'bad.nc'
    bad.write_bytes(b'junk')
    valid, dropped, dims = validate_model_files(
        files + [url, str(bad)], 'netcdf4', 'time', 'stations', LOG)
    assert set(dims) == set(files)          # local survivors only
    assert dims[files[0]]['station'] == N_STATION
    assert dims[files[0]]['time'] == N_TIME
    assert url in valid and str(bad) not in valid


# ---------------------------------------------------------------------
# cache scrub (issues #176 / #193)
# ---------------------------------------------------------------------

BUCKET = 'https://noaa-nos-ofs-pds.s3.amazonaws.com/cbofs/netcdf'


def test_scrub_removes_partial_cached_copy(tmp_path):
    # Cached copies live under the URL's parent-directory key
    # (cached_path_for_url, issue #267) — here the URL parent is
    # 'netcdf'.
    cache = tmp_path / 'cache'
    subdir = cache / 'netcdf'
    subdir.mkdir(parents=True)
    good = _write_nc3(subdir, 'a.nc')
    partial = _truncate(_write_nc3(subdir, 'b.nc'), 0.4)
    urls = [f'{BUCKET}/a.nc', f'{BUCKET}/b.nc', f'{BUCKET}/notcached.nc']
    removed = scrub_cached_copies(urls, str(cache), 'time', LOG)
    assert removed == 1
    import os
    assert os.path.isfile(good)          # healthy copy kept
    assert not os.path.isfile(partial)   # partial copy deleted


def test_scrub_handles_simplecache_prefix_and_local_entries(tmp_path):
    cache = tmp_path / 'cache'
    subdir = cache / 'netcdf'
    subdir.mkdir(parents=True)
    partial = _truncate(_write_nc3(subdir, 'c.nc'), 0.4)
    local_file = _write_nc3(tmp_path, 'local.nc')
    urls = [f'simplecache::{BUCKET}/c.nc', local_file]
    removed = scrub_cached_copies(urls, str(cache), 'time', LOG)
    assert removed == 1
    import os
    assert not os.path.isfile(partial)
    # Local (non-remote) entries are never touched by the scrub.
    assert os.path.isfile(local_file)


def test_scrub_noop_when_cache_healthy(tmp_path):
    cache = tmp_path / 'cache'
    subdir = cache / 'netcdf'
    subdir.mkdir(parents=True)
    good = _write_nc3(subdir, 'a.nc')
    removed = scrub_cached_copies(
        [f'{BUCKET}/a.nc'], str(cache), 'time', LOG)
    assert removed == 0
    import os
    assert os.path.isfile(good)


# ---------------------------------------------------------------------
# integration: intake_model survives a corrupt file in the batch
# ---------------------------------------------------------------------

def test_intake_model_drops_corrupt_file_and_continues(tmp_path):
    from ofs_skill.model_processing.intake_scisa import intake_model

    good1 = _write_nc3(tmp_path, 'necofs.t00z.stations.nowcast.nc',
                       ds=_stations_ds(t0='2026-05-14T00'))
    good2 = _write_nc3(tmp_path, 'necofs.t06z.stations.nowcast.nc',
                       ds=_stations_ds(t0='2026-05-14T02:24'))
    bad = _truncate(
        _write_nc3(tmp_path, 'necofs.t12z.stations.nowcast.nc',
                   ds=_stations_ds(t0='2026-05-14T04:48')), 0.4)

    prop = SimpleNamespace(ofs='necofs', model_source='fvcom',
                           ofsfiletype='stations', whichcast='nowcast',
                           config_file=None)
    ds = intake_model([good1, good2, bad], prop, LOG)

    # The two good files supply 2 * N_TIME unique timesteps; the
    # corrupt third file is dropped rather than crashing the open.
    assert ds.sizes['time'] == 2 * N_TIME
    assert prop.netcdf_engine == 'netcdf4'
    ds.close()


def test_intake_model_exits_when_all_files_corrupt(tmp_path):
    from ofs_skill.model_processing.intake_scisa import intake_model

    bad1 = _truncate(_write_nc3(tmp_path, 'a.nc'), 0.3)
    bad2 = _truncate(_write_nc3(tmp_path, 'b.nc'), 0.3)
    prop = SimpleNamespace(ofs='necofs', model_source='fvcom',
                           ofsfiletype='stations', whichcast='nowcast',
                           config_file=None)
    with pytest.raises(SystemExit):
        intake_model([bad1, bad2], prop, LOG)
