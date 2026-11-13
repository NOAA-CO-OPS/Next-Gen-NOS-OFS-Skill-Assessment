"""
Unit tests for NODD downloader robustness in `bin/utils/get_model_data.py`.

Covers four defects found while verifying the STOFS station download path:

(a) `urlretrieve` wrote directly to the final path, so an interrupted
    transfer left a truncated file that the skip-if-exists check treated
    as complete on every later run. Downloads now go to a '.part' name
    and are promoted with os.replace() only on success (same pattern as
    the S3 cache path in intake_scisa.py, issues #176/#193).

(b) The bucket key was extracted with `url.split('.com')[-1]`, which
    breaks for endpoints whose host has no '.com'. Now urlparse().path.

(c) `savepath` was derived by splitting the first save directory on the
    FIRST occurrence of the OFS name anywhere in the string, which broke
    for working directories containing the OFS name (e.g.
    /home/user/cbofs_runs/). Now anchored on the trailing
    {ofs}/{netcdf_dir}/ layout component.

(d) For STOFS-3D stations nowcast, the read side lists the day-(end+1)
    12Z points file (the t12z segment covers the PRECEDING 24 hours) but
    the downloader never fetched it, so pre-provisioned runs
    (use_s3_fallback=False) could not cover post-12Z hours of the last
    day. The downloader's dates_range now mirrors the read side.
"""

from types import SimpleNamespace
from unittest.mock import patch
from urllib.error import ContentTooShortError, HTTPError

import pytest

from bin.utils import get_model_data


def make_logger(record):
    """Return a logger stand-in appending (level, message) to record."""
    def log_as(level):
        return lambda msg, *args, **_kw: record.append(
            (level, msg % args if args else msg))
    return SimpleNamespace(info=log_as('info'), warning=log_as('warning'),
                           error=log_as('error'), debug=log_as('debug'))


def make_props(tmp_path, ofs, whichcast, ofsfiletype='stations'):
    """Return a minimal ModelProperties stand-in with a written config."""
    conf_path = tmp_path / 'ofs_dps.conf'
    conf_path.write_text(
        '[directories]\n'
        f'home={tmp_path}\n'
        'netcdf_dir=netcdf\n'
        f'model_historical_dir={tmp_path / "example_data"}\n'
        '[urls]\n'
        'nodd_s3=https://noaa-nos-ofs-pds.s3.amazonaws.com/\n'
        'nodd_s3_stofs3d=https://noaa-nos-stofs3d-pds.s3.amazonaws.com/\n'
        'nodd_s3_stofs2d=https://noaa-gestofs-pds.s3.amazonaws.com/\n',
        encoding='utf-8',
    )
    return SimpleNamespace(
        ofs=ofs, whichcast=whichcast, ofsfiletype=ofsfiletype,
        config_file=str(conf_path), model_path='',
        start_date_full='2025-07-01T00:00:00Z',
        end_date_full='2025-07-02T00:00:00Z',
    )


def make_file_list_for(tmp_path, ofs, whichcast, ofsfiletype='stations'):
    """Run the list_of_dir -> make_file_list chain and return the file list."""
    logger = make_logger([])
    prop = make_props(tmp_path, ofs, whichcast, ofsfiletype)
    dir_list, dates = get_model_data.list_of_dir(
        prop, f'{ofs}/netcdf', logger)
    return get_model_data.make_file_list(prop, dates, dir_list, logger)


CBOFS_URL = ('https://noaa-nos-ofs-pds.s3.amazonaws.com/cbofs/netcdf/'
             '2025/07/01/cbofs.t00z.20250701.stations.nowcast.nc')


def _download(tmp_path, side_effect, url=CBOFS_URL):
    """Call _download_single_file with a mocked urlretrieve; return results."""
    logged = []
    logger = make_logger(logged)
    savepath = f'{tmp_path.as_posix()}/'
    target_dir = tmp_path / 'cbofs' / 'netcdf' / '2025' / '07' / '01'
    target_dir.mkdir(parents=True, exist_ok=True)
    prefix_map = ('cbofs/netcdf/', 'cbofs/netcdf/')
    with patch('bin.utils.get_model_data.urllib.request.urlretrieve',
               side_effect=side_effect) as mock_get, \
            patch('bin.utils.get_model_data.time.sleep'):
        # pylint: disable-next=protected-access
        local_path = get_model_data._download_single_file(
            url, savepath, logger, prefix_map=prefix_map)
    final = target_dir / 'cbofs.t00z.20250701.stations.nowcast.nc'
    part = target_dir / 'cbofs.t00z.20250701.stations.nowcast.nc.part'
    return local_path, final, part, mock_get, logged


# ---------------------------------------------------------------------------
# (a) Atomic downloads: .part never promoted, interrupted runs recover
# ---------------------------------------------------------------------------

def test_truncated_download_never_promoted(tmp_path):
    """A transfer that dies mid-download leaves neither final nor .part."""
    def truncate(_url, dst):
        with open(dst, 'wb') as fil:
            fil.write(b'trunc')
        raise ContentTooShortError('connection dropped', b'trunc')

    local_path, final, part, _, logged = _download(tmp_path, truncate)
    assert local_path is None
    assert not final.exists(), 'truncated file must not reach the final path'
    assert not part.exists(), 'failed .part file must be cleaned up'
    assert any(lvl == 'error' and 'Download failed' in msg
               for lvl, msg in logged)


def test_interrupted_download_retried_next_run(tmp_path):
    """A stale .part from a killed run is removed and the file re-fetched."""
    def fetch(_url, dst):
        with open(dst, 'wb') as fil:
            fil.write(b'complete-data')

    target_dir = tmp_path / 'cbofs' / 'netcdf' / '2025' / '07' / '01'
    target_dir.mkdir(parents=True)
    stale = target_dir / 'cbofs.t00z.20250701.stations.nowcast.nc.part'
    stale.write_bytes(b'trunc')

    local_path, final, part, mock_get, logged = _download(tmp_path, fetch)
    assert local_path == final.as_posix()
    assert final.read_bytes() == b'complete-data'
    assert not part.exists()
    assert mock_get.call_count == 1
    assert any(lvl == 'warning' and 'stale partial' in msg
               for lvl, msg in logged)


def test_completed_file_still_skipped(tmp_path):
    """A file already at the final path is skipped without re-download."""
    target_dir = tmp_path / 'cbofs' / 'netcdf' / '2025' / '07' / '01'
    target_dir.mkdir(parents=True)
    final_pre = target_dir / 'cbofs.t00z.20250701.stations.nowcast.nc'
    final_pre.write_bytes(b'already-here')

    local_path, final, _, mock_get, logged = _download(tmp_path, None)
    assert local_path == final.as_posix()
    assert final.read_bytes() == b'already-here'
    mock_get.assert_not_called()
    assert any(lvl == 'info' and 'already exists' in msg
               for lvl, msg in logged)


def test_http_503_retry_semantics_preserved(tmp_path):
    """Two 503s then success: three attempts, file promoted, no .part."""
    calls = {'n': 0}

    def flaky(url, dst):
        calls['n'] += 1
        if calls['n'] < 3:
            raise HTTPError(url, 503, 'Service Unavailable', None, None)
        with open(dst, 'wb') as fil:
            fil.write(b'third-time-lucky')

    local_path, final, part, mock_get, _ = _download(tmp_path, flaky)
    assert local_path == final.as_posix()
    assert final.read_bytes() == b'third-time-lucky'
    assert not part.exists()
    assert mock_get.call_count == 3


def test_retrieve_atomic_promotes_on_success(tmp_path):
    """_retrieve_atomic downloads to .part and renames into place."""
    final = tmp_path / 'file.nc'

    def fetch(_url, dst):
        assert dst == f'{final.as_posix()}.part'
        with open(dst, 'wb') as fil:
            fil.write(b'payload')

    with patch('bin.utils.get_model_data.urllib.request.urlretrieve',
               side_effect=fetch):
        # pylint: disable-next=protected-access
        get_model_data._retrieve_atomic('http://x/file.nc', final.as_posix())
    assert final.read_bytes() == b'payload'
    assert not (tmp_path / 'file.nc.part').exists()


# ---------------------------------------------------------------------------
# (b) Bucket key extraction via urlparse (no '.com' assumption)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('url', [
    'https://noaa-nos-ofs-pds.s3.amazonaws.com/cbofs/netcdf/2025/07/01/f.nc',
    'https://ofs-mirror.s3.us-gov-east-1.example.org/cbofs/netcdf/'
    '2025/07/01/f.nc',
    'http://localhost:9000/cbofs/netcdf/2025/07/01/f.nc',
])
def test_url_to_local_path_any_endpoint(url):
    """The key is the URL path for .com and non-.com endpoints alike."""
    # pylint: disable-next=protected-access
    local = get_model_data._url_to_local_path(
        url, '/data/', ('cbofs/netcdf/', 'cbofs/netcdf/'))
    assert local == '/data/cbofs/netcdf/2025/07/01/f.nc'


def test_url_to_local_path_swaps_bucket_prefix_non_com():
    """STOFS bucket prefix translation works without a '.com' host."""
    url = ('https://stofs-mirror.example.net/STOFS-3D-Atl/'
           'stofs_3d_atl.20250701/stofs_3d_atl.t12z.points.cwl.temp.salt.vel.nc')
    # pylint: disable-next=protected-access
    local = get_model_data._url_to_local_path(
        url, '/data/', ('stofs_3d_atl/netcdf/', 'STOFS-3D-Atl/'))
    assert local == ('/data/stofs_3d_atl/netcdf/stofs_3d_atl.20250701/'
                     'stofs_3d_atl.t12z.points.cwl.temp.salt.vel.nc')


# ---------------------------------------------------------------------------
# (c) savepath anchored on the trailing {ofs}/{netcdf_dir}/ component
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('first_dir,prefix,expected', [
    # OFS name in the working directory must not truncate the base path
    ('/home/user/cbofs_runs/data/model/cbofs/netcdf/2025/07/01',
     'cbofs/netcdf/', '/home/user/cbofs_runs/data/model/'),
    ('/base/example_data/cbofs/netcdf/2025/07/01',
     'cbofs/netcdf/', '/base/example_data/'),
    ('/base/example_data/stofs_3d_atl/netcdf/stofs_3d_atl.20250701',
     'stofs_3d_atl/netcdf/', '/base/example_data/'),
    # Relative base path
    ('example_data/cbofs/netcdf/2025/07/01',
     'cbofs/netcdf/', 'example_data/'),
    # Layout rooted directly at {ofs}/{netcdf_dir}/
    ('cbofs/netcdf/2025/07/01', 'cbofs/netcdf/', ''),
])
def test_derive_savepath(first_dir, prefix, expected):
    """savepath is everything above the anchored {ofs}/{netcdf_dir}/."""
    # pylint: disable-next=protected-access
    assert get_model_data._derive_savepath([first_dir], prefix) == expected


def test_derive_savepath_missing_layout_raises():
    """A save directory without the layout component is an error."""
    with pytest.raises(ValueError, match='cbofs/netcdf/'):
        # pylint: disable-next=protected-access
        get_model_data._derive_savepath(['/somewhere/else'], 'cbofs/netcdf/')


# ---------------------------------------------------------------------------
# (d) STOFS-3D stations nowcast fetches the day-(end+1) 12Z cycle
# ---------------------------------------------------------------------------

def test_stofs3d_stations_nowcast_includes_end_plus_one(tmp_path):
    """The downloader lists the same end+1 points file the reader needs."""
    files = make_file_list_for(tmp_path, 'stofs_3d_atl', 'nowcast')
    # 06/30 (start-1) through 07/03 (end+1), one t12z points file per day
    assert len(files) == 4
    assert any('stofs_3d_atl.20250703' in f for f in files)
    assert files[-1].endswith(
        'stofs_3d_atl.20250703/stofs_3d_atl.t12z.points.cwl.temp.salt.vel.nc')


def test_stofs3d_stations_forecast_b_unchanged(tmp_path):
    """forecast_b keeps the start-1..end range (no look-ahead day)."""
    files = make_file_list_for(tmp_path, 'stofs_3d_atl', 'forecast_b')
    assert len(files) == 3
    assert not any('20250703' in f for f in files)


def test_stofs3d_fields_nowcast_unchanged(tmp_path):
    """The look-ahead applies to stations only, not fields files."""
    files = make_file_list_for(tmp_path, 'stofs_3d_atl', 'nowcast',
                               ofsfiletype='fields')
    assert files
    assert not any('20250703' in f for f in files)


def test_cbofs_stations_nowcast_unchanged(tmp_path):
    """Non-STOFS nowcast file lists keep the start-1..end range."""
    files = make_file_list_for(tmp_path, 'cbofs', 'nowcast')
    # 3 days (06/30-07/02) x 4 cycles
    assert len(files) == 12
    assert not any('/07/03/' in f for f in files)
