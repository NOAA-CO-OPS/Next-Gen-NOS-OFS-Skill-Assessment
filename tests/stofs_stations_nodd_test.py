"""
Unit tests for STOFS station-file download paths on the NODD (issue #205).

Regression protection for two defects that made STOFS station downloads
return HTTP 404 from the NODD S3 buckets:

1. `make_file_list()` in `bin/utils/get_model_data.py` had no
   stofs_3d_atl/stofs_3d_pac stations branch, so it built NOS-OFS-style
   names (e.g. `stofs_3d_atl.t12z.20250630.stations.nowcast.nc`) that do
   not exist on the bucket. The station product is a single points file
   per cycle: `{ofs}.t{cycle}z.points.cwl.temp.salt.vel.nc`.

2. URL builders kept the local `netcdf/` subdirectory in bucket paths,
   but the STOFS buckets have no `netcdf/` level. With the shipped
   default `netcdf_dir=netcdf`, every STOFS URL 404'd. Affected both
   `list_of_urls()` (get_model_data.py) and `construct_s3_url()`
   (list_of_files.py).
"""

import configparser
from unittest.mock import patch

import pytest

from bin.utils import get_model_data
from ofs_skill.model_processing.list_of_files import (
    construct_s3_url,
    get_nodd_prefix_map,
)


class MockLogger:
    """Collects log messages so tests can assert on them."""

    def __init__(self):
        self.infos = []
        self.warnings = []
        self.errors = []

    def info(self, msg, *args, **_kwargs):
        """Record an info message."""
        self.infos.append(msg % args if args else msg)

    def warning(self, msg, *args, **_kwargs):
        """Record a warning message."""
        self.warnings.append(msg % args if args else msg)

    def error(self, msg, *args, **_kwargs):
        """Record an error message."""
        self.errors.append(msg % args if args else msg)


class MockProps:  # pylint: disable=too-few-public-methods
    """Minimal ModelProperties stand-in for get_model_data functions."""

    def __init__(self, ofs='stofs_3d_atl', whichcast='nowcast',
                 ofsfiletype='stations', config_file=None):
        self.ofs = ofs
        self.whichcast = whichcast
        self.ofsfiletype = ofsfiletype
        self.config_file = config_file
        self.model_path = ''
        self.start_date_full = '20250701-00:00:00'
        self.end_date_full = '20250702-00:00:00'


def _fake_urlretrieve(_url, dst):
    """Stand-in for urlretrieve: writes bytes to the destination path."""
    with open(dst, 'wb') as fil:
        fil.write(b'data')


def write_conf(tmp_path, netcdf_dir):
    """Write a minimal config with the given netcdf_dir and return its path."""
    conf = configparser.ConfigParser()
    conf['directories'] = {
        'home': str(tmp_path),
        'netcdf_dir': netcdf_dir,
        'model_historical_dir': str(tmp_path / 'example_data'),
    }
    conf['urls'] = {
        'nodd_s3': 'https://noaa-nos-ofs-pds.s3.amazonaws.com/',
        'nodd_s3_stofs3d': 'https://noaa-nos-stofs3d-pds.s3.amazonaws.com/',
        'nodd_s3_stofs2d': 'https://noaa-gestofs-pds.s3.amazonaws.com/',
    }
    conf_path = tmp_path / 'ofs_dps.conf'
    with open(conf_path, 'w', encoding='utf-8') as fil:
        conf.write(fil)
    return str(conf_path)


def build_urls(tmp_path, ofs, whichcast, netcdf_dir):
    """Run the list_of_dir -> make_file_list -> list_of_urls chain."""
    logger = MockLogger()
    prop = MockProps(ofs=ofs, whichcast=whichcast,
                     config_file=write_conf(tmp_path, netcdf_dir))
    nodd_path = ofs if not netcdf_dir else f'{ofs}/{netcdf_dir}'
    dir_list, dates = get_model_data.list_of_dir(prop, nodd_path, logger)
    file_list = get_model_data.make_file_list(prop, dates, dir_list, logger)
    return get_model_data.list_of_urls(file_list, prop, logger)


# ---------------------------------------------------------------------------
# make_file_list: STOFS-3D stations must use the points file name
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('whichcast', ['nowcast', 'forecast_b'])
def test_make_file_list_stofs3d_stations_points_name(tmp_path, whichcast):
    """STOFS-3D station file names must be the per-cycle points file."""
    urls = build_urls(tmp_path, 'stofs_3d_atl', whichcast, 'netcdf')
    assert urls, 'expected at least one URL'
    for url in urls:
        assert url.endswith('.points.cwl.temp.salt.vel.nc')
        assert 'stations.nowcast' not in url
        assert 'stations.forecast' not in url


# ---------------------------------------------------------------------------
# list_of_urls: no netcdf/ level on STOFS buckets, bucket prefix swapped
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("netcdf_dir", ["netcdf", "", "model_output"])
def test_list_of_urls_stofs3d_bucket_layout(tmp_path, netcdf_dir):
    """STOFS-3D URLs use the STOFS-3D-Atl/ prefix plus netcdf_dir when configured."""
    urls = build_urls(tmp_path, "stofs_3d_atl", "nowcast", netcdf_dir)
    sub = f"{netcdf_dir}/" if netcdf_dir else ""
    base = f"https://noaa-nos-stofs3d-pds.s3.amazonaws.com/STOFS-3D-Atl/{sub}"
    for url in urls:
        assert url.startswith((
            f"{base}stofs_3d_atl.202507",
            f"{base}stofs_3d_atl.202506",
        ))


@pytest.mark.parametrize('netcdf_dir', ['netcdf', '', 'model_output'])
def test_list_of_urls_stofs2d_bucket_layout(tmp_path, netcdf_dir):
    """STOFS-2D-Global URLs have date directories at the bucket root."""
    urls = build_urls(tmp_path, 'stofs_2d_glo', 'nowcast', netcdf_dir)
    sub = f'{netcdf_dir}/' if netcdf_dir else ''
    for url in urls:
        assert url.startswith(
            f'https://noaa-gestofs-pds.s3.amazonaws.com/{sub}stofs_2d_glo.202'
        )


def test_list_of_urls_non_stofs_unchanged(tmp_path):
    """Non-STOFS URLs keep the {ofs}/netcdf/ bucket layout."""
    urls = build_urls(tmp_path, 'cbofs', 'nowcast', 'netcdf')
    for url in urls:
        assert url.startswith(
            'https://noaa-nos-ofs-pds.s3.amazonaws.com/cbofs/netcdf/')


# ---------------------------------------------------------------------------
# _download_single_file: bucket prefix mapped back to the local layout
# ---------------------------------------------------------------------------

def test_download_single_file_local_path_restores_netcdf_dir(tmp_path):
    """Downloads land in the local {ofs}/{netcdf_dir}/ layout."""
    logger = MockLogger()
    url = ('https://noaa-nos-stofs3d-pds.s3.amazonaws.com/STOFS-3D-Atl/'
           'stofs_3d_atl.20250701/stofs_3d_atl.t12z.points.cwl.temp.salt.vel.nc')
    savepath = f'{tmp_path.as_posix()}/'
    target_dir = tmp_path / 'stofs_3d_atl' / 'netcdf' / 'stofs_3d_atl.20250701'
    target_dir.mkdir(parents=True)
    with patch('bin.utils.get_model_data.urllib.request.urlretrieve',
               side_effect=_fake_urlretrieve) as mock_get:
        # pylint: disable-next=protected-access
        local_path = get_model_data._download_single_file(
            url, savepath, logger,
            prefix_map=('stofs_3d_atl/netcdf/', 'STOFS-3D-Atl/'),
        )
    assert local_path == (
        f'{tmp_path.as_posix()}/stofs_3d_atl/netcdf/stofs_3d_atl.20250701/'
        'stofs_3d_atl.t12z.points.cwl.temp.salt.vel.nc'
    )
    # Downloads go to a temporary .part name and are promoted on success
    mock_get.assert_called_once_with(url, f'{local_path}.part')


def test_download_single_file_savepath_with_bucket_prefix(tmp_path):
    """A savepath that itself contains 'STOFS-3D-Atl/' is never rewritten."""
    logger = MockLogger()
    url = ('https://noaa-nos-stofs3d-pds.s3.amazonaws.com/STOFS-3D-Atl/'
           'stofs_3d_atl.20250701/stofs_3d_atl.t12z.points.cwl.temp.salt.vel.nc')
    base = tmp_path / 'STOFS-3D-Atl' / 'data'
    savepath = f'{base.as_posix()}/'
    target_dir = base / 'stofs_3d_atl' / 'netcdf' / 'stofs_3d_atl.20250701'
    target_dir.mkdir(parents=True)
    with patch('bin.utils.get_model_data.urllib.request.urlretrieve',
               side_effect=_fake_urlretrieve) as mock_get:
        # pylint: disable-next=protected-access
        local_path = get_model_data._download_single_file(
            url, savepath, logger,
            prefix_map=('stofs_3d_atl/netcdf/', 'STOFS-3D-Atl/'),
        )
    assert local_path == (
        f'{base.as_posix()}/stofs_3d_atl/netcdf/stofs_3d_atl.20250701/'
        'stofs_3d_atl.t12z.points.cwl.temp.salt.vel.nc'
    )
    # Downloads go to a temporary .part name and are promoted on success
    mock_get.assert_called_once_with(url, f'{local_path}.part')


# ---------------------------------------------------------------------------
# get_nodd_prefix_map: prefix pairs and netcdf_dir validation
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "ofs,netcdf_dir,expected",
    [
        (
            "stofs_3d_atl",
            "netcdf",
            ("stofs_3d_atl/netcdf/", "STOFS-3D-Atl/netcdf/"),
        ),
        ("stofs_3d_atl", "", ("stofs_3d_atl/", "STOFS-3D-Atl/")),
        (
            "stofs_3d_pac",
            "netcdf",
            ("stofs_3d_pac/netcdf/", "STOFS-3D-Pac/netcdf/"),
        ),
        ("stofs_2d_glo", "netcdf", ("stofs_2d_glo/netcdf/", "netcdf/")),
        ("cbofs", "netcdf", ("cbofs/netcdf/", "cbofs/netcdf/")),
    ],
)
def test_get_nodd_prefix_map_pairs(tmp_path, ofs, netcdf_dir, expected):
    """Prefix pairs reflect the configured netcdf_dir and the OFS bucket."""
    logger = MockLogger()
    prop = MockProps(ofs=ofs, config_file=write_conf(tmp_path, netcdf_dir))
    assert get_nodd_prefix_map(prop, logger) == expected


@pytest.mark.parametrize('bad_dir', [
    '/absolute/path',
    '..',
    '../up',
    'a/../b',
    '..\\up',
    'C:/absolute',
])
def test_get_nodd_prefix_map_rejects_unsafe_netcdf_dir(tmp_path, bad_dir):
    """Absolute or parent-traversing netcdf_dir values raise ValueError."""
    logger = MockLogger()
    prop = MockProps(config_file=write_conf(tmp_path, bad_dir))
    with pytest.raises(ValueError, match='netcdf_dir'):
        get_nodd_prefix_map(prop, logger)


# ---------------------------------------------------------------------------
# construct_s3_url: netcdf/ stripped from STOFS bucket paths
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "ofs,local,expected",
    [
        (
            "stofs_3d_atl",
            "./example_data/stofs_3d_atl/netcdf/stofs_3d_atl.20250701/"
            "stofs_3d_atl.t12z.points.cwl.temp.salt.vel.nc",
            "https://noaa-nos-stofs3d-pds.s3.amazonaws.com/STOFS-3D-Atl/netcdf/"
            "stofs_3d_atl.20250701/stofs_3d_atl.t12z.points.cwl.temp.salt.vel.nc",
        ),
        (
            "stofs_2d_glo",
            "./example_data/stofs_2d_glo/netcdf/stofs_2d_glo.20250701/"
            "stofs_2d_glo.t00z.points.cwl.nc",
            "https://noaa-gestofs-pds.s3.amazonaws.com/netcdf/"
            "stofs_2d_glo.20250701/stofs_2d_glo.t00z.points.cwl.nc",
        ),
        (
            "cbofs",
            "./example_data/cbofs/netcdf/2025/07/01/"
            "cbofs.t00z.20250701.stations.nowcast.nc",
            "https://noaa-nos-ofs-pds.s3.amazonaws.com/cbofs/netcdf/2025/07/01/"
            "cbofs.t00z.20250701.stations.nowcast.nc",
        ),
    ],
)
def test_construct_s3_url_bucket_paths(tmp_path, ofs, local, expected):
    """construct_s3_url maps local paths to each bucket's layout."""
    logger = MockLogger()
    prop = MockProps(ofs=ofs, config_file=write_conf(tmp_path, "netcdf"))
    with patch(
        "ofs_skill.model_processing.list_of_files.check_s3_for_file",
        return_value=True,
    ):
        url = construct_s3_url(local, prop, logger)
    assert url == expected


@pytest.mark.parametrize(
    "netcdf_dir,subdir",
    [
        ("model_output", "model_output/"),
        ("", ""),
    ],
)
def test_construct_s3_url_custom_netcdf_dir(tmp_path, netcdf_dir, subdir):
    """A custom netcdf_dir is appended to STOFS paths when present."""
    logger = MockLogger()
    prop = MockProps(
        ofs="stofs_3d_atl", config_file=write_conf(tmp_path, netcdf_dir)
    )
    local = (
        f"./example_data/stofs_3d_atl/{subdir}stofs_3d_atl.20250701/"
        "stofs_3d_atl.t12z.points.cwl.temp.salt.vel.nc"
    )
    with patch(
        "ofs_skill.model_processing.list_of_files.check_s3_for_file",
        return_value=True,
    ):
        url = construct_s3_url(local, prop, logger)
    assert url == (
        f"https://noaa-nos-stofs3d-pds.s3.amazonaws.com/STOFS-3D-Atl/{subdir}"
        "stofs_3d_atl.20250701/stofs_3d_atl.t12z.points.cwl.temp.salt.vel.nc"
    )