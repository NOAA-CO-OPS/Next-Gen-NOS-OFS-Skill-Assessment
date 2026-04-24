"""
Unit tests for the forecast_a cycle filter in list_of_files().

Regression protection for the bug where `list_of_files.py` derived the
forecast cycle from `prop.startdate[-2:]` — a value that is always '00'
because `get_node_ofs.py:914-916` strips the hour. The filter therefore
matched t00z files regardless of the user's -f flag.

The fix reads `prop.forecast_hr` directly, which is set canonically by
`get_skill.py:555-556` before any list_of_files call on the forecast_a
path and is validated non-None at `get_skill.py:634`.
"""

from unittest.mock import patch

import pytest

from ofs_skill.model_processing.list_of_files import list_of_files


class MockLogger:
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
    """Minimal ModelProperties mock for list_of_files.

    Note `startdate` always ends in '00' to match the real-world behavior
    from get_node_ofs.py:914-916 — the fix must not depend on this being
    anything else.
    """

    def __init__(self, whichcast='forecast_a', ofsfiletype='stations',
                 forecast_hr='06z', ofs='cbofs',
                 startdate='2026032800', enddate='2026032823'):
        self.ofs = ofs
        self.whichcast = whichcast
        self.ofsfiletype = ofsfiletype
        self.forecast_hr = forecast_hr
        self.startdate = startdate
        self.enddate = enddate
        self.config_file = None


@pytest.fixture
def logger():
    return MockLogger()


def _all_cycle_station_files():
    """Directory contents spanning all 4 standard cbofs forecast cycles."""
    return [
        'cbofs.t00z.20260328.stations.f001.nc',
        'cbofs.t06z.20260328.stations.f001.nc',
        'cbofs.t12z.20260328.stations.f001.nc',
        'cbofs.t18z.20260328.stations.f001.nc',
    ]


def _all_cycle_fields_files():
    """Directory contents spanning all 4 cycles for fields ofsfiletype."""
    return [
        'cbofs.t00z.20260328.fields.f001.nc',
        'cbofs.t06z.20260328.fields.f001.nc',
        'cbofs.t12z.20260328.fields.f001.nc',
        'cbofs.t18z.20260328.fields.f001.nc',
    ]


@patch('ofs_skill.model_processing.list_of_files.utils')
@patch('os.path.exists', return_value=True)
@patch('ofs_skill.model_processing.list_of_files.listdir')
def test_forecast_a_filters_by_forecast_hr_not_startdate(
        mock_listdir, mock_exists, mock_utils, logger):
    """Bug reproducer: prop.startdate ends in '00' but user asked for 06z.

    Pre-fix, this returned the t00z file (wrong). Post-fix, only the
    t06z file is returned because `cycle_z = prop.forecast_hr.lower()`.
    """
    mock_utils.Utils.return_value.read_config_section.return_value = {
        'use_s3_fallback': 'False'
    }
    mock_listdir.return_value = _all_cycle_station_files()

    props = MockProps(
        whichcast='forecast_a',
        ofsfiletype='stations',
        forecast_hr='06z',
        startdate='2026032800',  # Hour stripped by get_node_ofs — the bug input.
    )

    result = list_of_files(props, ['/fake/dir'], logger)
    filenames = [f.split('/')[-1] for f in result]

    assert len(filenames) == 1, (
        f'expected exactly one file matching the 06z cycle, got {filenames}'
    )
    assert 't06z' in filenames[0]
    assert 't00z' not in filenames[0]


@pytest.mark.parametrize('cycle_hour', ['00', '06', '12', '18'])
@patch('ofs_skill.model_processing.list_of_files.utils')
@patch('os.path.exists', return_value=True)
@patch('ofs_skill.model_processing.list_of_files.listdir')
def test_forecast_a_selects_each_cycle_correctly(
        mock_listdir, mock_exists, mock_utils, cycle_hour, logger):
    """Every cycle — including 00z — must be selectable via -f."""
    mock_utils.Utils.return_value.read_config_section.return_value = {
        'use_s3_fallback': 'False'
    }
    mock_listdir.return_value = _all_cycle_station_files()

    props = MockProps(
        whichcast='forecast_a',
        ofsfiletype='stations',
        forecast_hr=f'{cycle_hour}z',
        startdate='2026032800',  # Always '00' — the bug input.
    )

    result = list_of_files(props, ['/fake/dir'], logger)
    filenames = [f.split('/')[-1] for f in result]

    assert len(filenames) == 1
    assert f't{cycle_hour}z' in filenames[0]


@patch('ofs_skill.model_processing.list_of_files.utils')
@patch('os.path.exists', return_value=True)
@patch('ofs_skill.model_processing.list_of_files.listdir')
def test_forecast_a_fields_filter_uses_forecast_hr(
        mock_listdir, mock_exists, mock_utils, logger):
    """Same filter applies to the fields ofsfiletype branch (line 944)."""
    mock_utils.Utils.return_value.read_config_section.return_value = {
        'use_s3_fallback': 'False'
    }
    mock_listdir.return_value = _all_cycle_fields_files()

    props = MockProps(
        whichcast='forecast_a',
        ofsfiletype='fields',
        forecast_hr='12z',
        startdate='2026032800',
    )

    result = list_of_files(props, ['/fake/dir'], logger)
    filenames = [f.split('/')[-1] for f in result]

    assert len(filenames) == 1
    assert 't12z' in filenames[0]
    assert 'fields.f001' in filenames[0]


@patch('ofs_skill.model_processing.list_of_files.utils')
@patch('os.path.exists', return_value=True)
@patch('ofs_skill.model_processing.list_of_files.listdir')
def test_forecast_a_uppercase_forecast_hr_normalized(
        mock_listdir, mock_exists, mock_utils, logger):
    """Defensive: the fix uses `.lower()`, so uppercase -f survives."""
    mock_utils.Utils.return_value.read_config_section.return_value = {
        'use_s3_fallback': 'False'
    }
    mock_listdir.return_value = _all_cycle_station_files()

    props = MockProps(
        whichcast='forecast_a',
        ofsfiletype='stations',
        forecast_hr='06Z',  # uppercase
        startdate='2026032800',
    )

    result = list_of_files(props, ['/fake/dir'], logger)
    filenames = [f.split('/')[-1] for f in result]

    assert len(filenames) == 1
    assert 't06z' in filenames[0]
