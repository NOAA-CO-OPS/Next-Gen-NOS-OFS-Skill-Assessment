"""
Unit tests for STOFS-3D points-file parsing in list_of_files().

Regression protection for the bug where the local directory-listing
parsers could not parse the STOFS-3D points filename
(e.g. ``stofs_3d_atl.t12z.points.cwl.temp.salt.vel.nc``): the
underscore-split logic written for the fields names ended up doing
``int('tl')`` -> ValueError, so every locally present points file was
skipped as unparseable. With ``use_s3_fallback=False`` a run over fully
pre-downloaded station files exited with "No model files found" even
though the files were on disk.

Also covers the companion fix: the STOFS-3D *fields* sub-branches never
checked ``prop.ofsfiletype``, so fields files could leak into a stations
listing.

All tests are in-process (no subprocesses) and use real temp
directories/config files so the exact production code path is exercised
with the S3 fallback disabled.
"""

import pytest

from ofs_skill.model_processing.list_of_files import list_of_files

POINTS_FILE = 'stofs_3d_atl.t12z.points.cwl.temp.salt.vel.nc'
FIELDS_FILES = [
    'stofs_3d_atl.t12z.fields.temperature_n001_012.nc',
    'stofs_3d_atl.t12z.fields.temperature_n013_024.nc',
]
FORECAST_FIELDS_FILES = [
    'stofs_3d_atl.t12z.fields.temperature_f001_012.nc',
    'stofs_3d_atl.t12z.fields.temperature_f013_024.nc',
]


class MockLogger:
    """Minimal logger capturing messages per level for assertions."""

    def __init__(self):
        """Create empty per-level message buffers."""
        self.infos = []
        self.warnings = []
        self.errors = []
        self.debugs = []

    def info(self, msg, *args, **_kwargs):
        """Record an info message."""
        self.infos.append(msg % args if args else msg)

    def warning(self, msg, *args, **_kwargs):
        """Record a warning message."""
        self.warnings.append(msg % args if args else msg)

    def error(self, msg, *args, **_kwargs):
        """Record an error message."""
        self.errors.append(msg % args if args else msg)

    def debug(self, msg, *args, **_kwargs):
        """Record a debug message."""
        self.debugs.append(msg % args if args else msg)


class MockProps:  # pylint: disable=too-few-public-methods
    """Minimal ModelProperties stand-in for list_of_files()."""

    def __init__(self, config_file, **overrides):
        """Store the attributes list_of_files() reads."""
        self.ofs = 'stofs_3d_atl'
        self.whichcast = 'nowcast'
        self.ofsfiletype = 'stations'
        self.forecast_hr = '12z'
        self.startdate = '2025070100'
        self.enddate = '2025070223'
        self.config_file = config_file
        for key, value in overrides.items():
            setattr(self, key, value)


@pytest.fixture(name='logger')
def logger_fixture():
    """Provide a fresh MockLogger per test."""
    return MockLogger()


@pytest.fixture(name='no_fallback_conf')
def no_fallback_conf_fixture(tmp_path):
    """Write a real config file with the S3 fallback disabled."""
    conf = tmp_path / 'ofs_dps.conf'
    conf.write_text('[settings]\nuse_s3_fallback = False\n',
                    encoding='utf-8')
    return str(conf)


def make_stofs_dir(tmp_path, date_str, filenames):
    """Create a STOFS-style dated directory populated with dummy files."""
    stofs_dir = tmp_path / f'stofs_3d_atl.{date_str}'
    stofs_dir.mkdir()
    for name in filenames:
        (stofs_dir / name).write_bytes(b'')
    return stofs_dir


def basenames(result):
    """Reduce list_of_files() output to plain file names."""
    return [f.replace('\\', '/').split('/')[-1] for f in result]


@pytest.mark.parametrize('whichcast', ['nowcast', 'forecast_a', 'forecast_b'])
def test_stations_run_lists_points_file(tmp_path, no_fallback_conf, logger,
                                        whichcast):
    """Points files must be listed for stations runs in all whichcasts.

    Pre-fix, the underscore-split parse raised ValueError (int('tl')) and
    the file was skipped, leaving the listing empty and the run dead with
    SystemExit(1) when the S3 fallback is off.
    """
    stofs_dir = make_stofs_dir(tmp_path, '20250701', [POINTS_FILE])
    props = MockProps(no_fallback_conf, whichcast=whichcast)

    result = list_of_files(props, [str(stofs_dir)], logger)

    assert basenames(result) == [POINTS_FILE]
    assert not any('unparseable' in w for w in logger.warnings), (
        f'points file still hit the unparseable path: {logger.warnings}'
    )


@pytest.mark.parametrize(
    'whichcast,fields_names',
    [('nowcast', FIELDS_FILES),
     ('forecast_a', FORECAST_FIELDS_FILES),
     ('forecast_b', FORECAST_FIELDS_FILES)])
def test_stations_run_excludes_fields_files(tmp_path, no_fallback_conf,
                                            logger, whichcast, fields_names):
    """Fields files in the same directory must not leak into stations runs.

    Pre-fix, the fields sub-branch had no ofsfiletype guard, so fields
    files were appended to stations listings whenever they parsed.
    """
    stofs_dir = make_stofs_dir(tmp_path, '20250701',
                               [POINTS_FILE] + fields_names)
    props = MockProps(no_fallback_conf, whichcast=whichcast)

    result = list_of_files(props, [str(stofs_dir)], logger)

    assert basenames(result) == [POINTS_FILE]


def test_stations_run_with_only_fields_files_exits(tmp_path,
                                                   no_fallback_conf, logger):
    """A stations run over a fields-only directory must find nothing.

    Pre-fix, the missing ofsfiletype guard let the fields files through;
    post-fix the listing is empty and, with the fallback off, the run
    exits via SystemExit.
    """
    stofs_dir = make_stofs_dir(tmp_path, '20250701', FIELDS_FILES)
    props = MockProps(no_fallback_conf, whichcast='nowcast')

    with pytest.raises(SystemExit):
        list_of_files(props, [str(stofs_dir)], logger)


@pytest.mark.parametrize('whichcast', ['nowcast', 'forecast_a', 'forecast_b'])
def test_fields_run_excludes_points_files(tmp_path, no_fallback_conf, logger,
                                          whichcast):
    """Fields runs keep listing fields files and never the points file."""
    fields_names = (FIELDS_FILES if whichcast == 'nowcast'
                    else FORECAST_FIELDS_FILES)
    stofs_dir = make_stofs_dir(tmp_path, '20250701',
                               [POINTS_FILE] + fields_names)
    props = MockProps(no_fallback_conf, whichcast=whichcast,
                      ofsfiletype='fields')

    result = list_of_files(props, [str(stofs_dir)], logger)

    assert sorted(basenames(result)) == sorted(fields_names)


def test_forecast_a_points_filtered_by_cycle(tmp_path, no_fallback_conf,
                                             logger):
    """forecast_a must keep only the requested cycle's points file."""
    stofs_dir = make_stofs_dir(
        tmp_path, '20250701',
        [POINTS_FILE, 'stofs_3d_atl.t00z.points.cwl.temp.salt.vel.nc'])
    props = MockProps(no_fallback_conf, whichcast='forecast_a',
                      forecast_hr='12z')

    result = list_of_files(props, [str(stofs_dir)], logger)

    assert basenames(result) == [POINTS_FILE]


def test_multi_day_points_files_listed_in_directory_order(
        tmp_path, no_fallback_conf, logger):
    """One points file per dated directory, in dir_list (date) order.

    This matches the ordering construct_expected_files() produces when
    the S3 fallback resolves the same window.
    """
    dir1 = make_stofs_dir(tmp_path, '20250701', [POINTS_FILE])
    dir2 = make_stofs_dir(tmp_path, '20250702', [POINTS_FILE])
    props = MockProps(no_fallback_conf, whichcast='nowcast')

    result = list_of_files(props, [str(dir1), str(dir2)], logger)

    assert len(result) == 2
    assert '20250701' in result[0].replace('\\', '/')
    assert '20250702' in result[1].replace('\\', '/')


@pytest.mark.parametrize('whichcast,cast_names', [
    ('nowcast', ['cbofs.t00z.20250701.stations.n001.nc',
                 'cbofs.t06z.20250701.stations.n001.nc']),
    ('forecast_b', ['cbofs.t00z.20250701.stations.f001.nc',
                    'cbofs.t06z.20250701.stations.f001.nc']),
])
def test_cbofs_style_names_unchanged(tmp_path, no_fallback_conf, logger,
                                     whichcast, cast_names):
    """Non-STOFS naming conventions must parse exactly as before."""
    cbofs_dir = tmp_path / 'cbofs' / '2025' / '07' / '01'
    cbofs_dir.mkdir(parents=True)
    for name in cast_names:
        (cbofs_dir / name).write_bytes(b'')
    props = MockProps(no_fallback_conf, whichcast=whichcast, ofs='cbofs')

    result = list_of_files(props, [str(cbofs_dir)], logger)

    assert sorted(basenames(result)) == sorted(cast_names)
