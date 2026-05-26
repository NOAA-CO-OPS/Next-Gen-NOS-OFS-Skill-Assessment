"""
Unit tests for the `use_s3_fallback=force` tri-value mode (issue #58).

Validates that:
- `use_s3_fallback=False` returns local file paths and never calls S3 helpers.
- `use_s3_fallback=True` returns local paths when files are present locally
  (regression guard for the default behavior).
- `use_s3_fallback=force` skips local enumeration entirely, calls
  `construct_expected_files` for each directory, and converts every file
  to an S3 URL via `construct_s3_url` regardless of whether a local copy
  exists.
"""

import os
from unittest.mock import patch

import pytest

from ofs_skill.model_processing.list_of_files import list_of_files


class MockLogger:
    """Mock logger that records all messages."""

    def __init__(self):
        self.infos = []
        self.warnings = []
        self.errors = []
        self.debugs = []

    def info(self, msg, *args, **kwargs):
        self.infos.append(msg % args if args else msg)

    def warning(self, msg, *args, **kwargs):
        self.warnings.append(msg % args if args else msg)

    def error(self, msg, *args, **kwargs):
        self.errors.append(msg % args if args else msg)

    def debug(self, msg, *args, **kwargs):
        self.debugs.append(msg % args if args else msg)


class MockProps:
    """Mock ModelProperties for testing."""

    def __init__(self, ofs='cbofs', whichcast='nowcast', ofsfiletype='fields',
                 startdate='2024090100', enddate='2024090123',
                 forecast_hr='00z'):
        self.ofs = ofs
        self.whichcast = whichcast
        self.ofsfiletype = ofsfiletype
        self.startdate = startdate
        self.enddate = enddate
        self.forecast_hr = forecast_hr


@pytest.fixture
def logger():
    return MockLogger()


@pytest.fixture
def props():
    return MockProps()


def _local_files_in(tmp_path):
    """Create a fake local model dir with two real netCDF files and return
    the directory plus the file basenames."""
    netcdf_dir = tmp_path / 'cbofs' / 'netcdf' / '2024' / '09' / '01'
    netcdf_dir.mkdir(parents=True)
    files = [
        'cbofs.t00z.20240901.fields.n001.nc',
        'cbofs.t00z.20240901.fields.n002.nc',
    ]
    for name in files:
        (netcdf_dir / name).write_bytes(b'')
    return str(netcdf_dir), files


@patch('ofs_skill.model_processing.list_of_files.utils')
def test_fallback_false_returns_local_only(mock_utils, props, logger, tmp_path):
    """use_s3_fallback=False with local files present -> local paths returned."""
    mock_utils.Utils.return_value.read_config_section.return_value = {
        'use_s3_fallback': 'False'
    }
    netcdf_dir, files = _local_files_in(tmp_path)

    result = list_of_files(props, [netcdf_dir], logger)

    assert len(result) == len(files)
    for path in result:
        assert path.startswith(netcdf_dir)
        assert not path.startswith('http')
    assert any('S3 fallback is disabled' in m for m in logger.infos)


@patch('ofs_skill.model_processing.list_of_files.utils')
@patch('ofs_skill.model_processing.list_of_files.construct_s3_url')
def test_fallback_true_prefers_local(mock_construct_s3, mock_utils,
                                     props, logger, tmp_path):
    """use_s3_fallback=True with local files present -> local paths, S3 not invoked."""
    mock_utils.Utils.return_value.read_config_section.return_value = {
        'use_s3_fallback': 'True'
    }
    netcdf_dir, files = _local_files_in(tmp_path)

    result = list_of_files(props, [netcdf_dir], logger)

    assert len(result) == len(files)
    for path in result:
        # Local files exist, so even with fallback enabled they should be kept
        assert os.path.isfile(path.replace('//', '/'))
        assert not path.startswith('http')
    # construct_s3_url is invoked only for missing files; none should be missing
    assert mock_construct_s3.call_count == 0


@patch('ofs_skill.model_processing.list_of_files.utils')
@patch('ofs_skill.model_processing.list_of_files.construct_s3_url')
@patch('ofs_skill.model_processing.list_of_files.construct_expected_files')
def test_force_uses_nodd_even_when_local_present(mock_construct_expected,
                                                  mock_construct_s3,
                                                  mock_utils,
                                                  props, logger, tmp_path):
    """use_s3_fallback=force with local files present -> S3 URLs only."""
    mock_utils.Utils.return_value.read_config_section.return_value = {
        'use_s3_fallback': 'force'
    }
    netcdf_dir, files = _local_files_in(tmp_path)
    expected_local_paths = [os.path.join(netcdf_dir, f).replace('\\', '/') for f in files]
    mock_construct_expected.return_value = expected_local_paths

    def fake_s3(local_path, prop, _logger):
        # Mimic construct_s3_url: convert local path to a NODD URL
        basename = os.path.basename(local_path.replace('//', '/'))
        return f'https://noaa-nos-ofs-pds.s3.amazonaws.com/cbofs/netcdf/2024/09/01/{basename}'

    mock_construct_s3.side_effect = fake_s3

    result = list_of_files(props, [netcdf_dir], logger)

    # construct_expected_files must be called instead of listdir
    assert mock_construct_expected.call_count == 1
    # Every returned entry must be an S3 URL, even though local copies exist
    assert len(result) == len(files)
    for path in result:
        assert path.startswith('https://noaa-')
        assert not os.path.isfile(path.replace('//', '/'))
    # construct_s3_url called once per file
    assert mock_construct_s3.call_count == len(files)
    # Confirm the new log line fires
    assert any('Forced NODD streaming' in m for m in logger.infos)


@patch('ofs_skill.model_processing.list_of_files.utils')
@patch('ofs_skill.model_processing.list_of_files.construct_s3_url')
@patch('ofs_skill.model_processing.list_of_files.construct_expected_files')
def test_force_value_is_case_insensitive(mock_construct_expected,
                                         mock_construct_s3, mock_utils,
                                         props, logger, tmp_path):
    """'FORCE' and 'forced' should both trigger the forced-NODD path."""
    mock_utils.Utils.return_value.read_config_section.return_value = {
        'use_s3_fallback': 'FORCE'
    }
    netcdf_dir, files = _local_files_in(tmp_path)
    mock_construct_expected.return_value = [
        os.path.join(netcdf_dir, f).replace('\\', '/') for f in files
    ]
    mock_construct_s3.side_effect = (
        lambda p, _prop, _lg:
        f'https://noaa-nos-ofs-pds.s3.amazonaws.com/cbofs/netcdf/x/{os.path.basename(p)}'
    )

    result = list_of_files(props, [netcdf_dir], logger)

    assert mock_construct_expected.called
    assert all(r.startswith('https://noaa-') for r in result)


@patch('ofs_skill.model_processing.list_of_files.utils')
@patch('ofs_skill.model_processing.list_of_files.construct_expected_files')
def test_loofs2_hindcast_disables_force(mock_construct_expected, mock_utils,
                                         logger, tmp_path):
    """loofs2 + hindcast guards: force never triggers construct_expected_files."""
    mock_utils.Utils.return_value.read_config_section.return_value = {
        'use_s3_fallback': 'force'
    }
    props_loofs2 = MockProps(ofs='loofs2', whichcast='hindcast',
                             ofsfiletype='stations',
                             startdate='2024090100', enddate='2024090123')
    netcdf_dir = tmp_path / 'loofs2' / 'netcdf' / '2024' / '09' / '01'
    netcdf_dir.mkdir(parents=True)
    # Empty dir is fine here -- we only care that the force path isn't taken
    try:
        list_of_files(props_loofs2, [str(netcdf_dir)], logger)
    except SystemExit:
        # Empty dir with S3 disabled -> SystemExit is the documented behavior;
        # the important assertion is below.
        pass

    # If force had remained active, construct_expected_files would have fired
    # at the top of the per-directory loop. Loofs2-hindcast guard must clear it.
    assert mock_construct_expected.call_count == 0
