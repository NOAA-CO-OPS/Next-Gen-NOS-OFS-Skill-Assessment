"""
Tests for the virtual-bin fallback hardening in
``_fetch_and_format_station``.

A ctl row with a virtual bin ID (``{parent}_b{NN}``) pins its depth and
identity to bin NN. When the CO-OPS retrieval — including the
unrestricted all-bins retry — still lacks that exact bin, the old code
substituted the first available bin's data under bin NN's virtual ID
and depth, silently corrupting skill statistics. Virtual rows must now
skip the station with an ERROR instead; legacy non-virtual IDs (no
``_b`` suffix) keep the first-available-bin fallback.
"""

import importlib
import logging
from unittest.mock import patch

import pandas as pd

from ofs_skill.utils.file_headers import series_header

gso = importlib.import_module(
    'ofs_skill.obs_retrieval.get_station_observations')

_LOGGER_NAME = 'obs_virtual_bin_fallback_test'


def _bin_df(value):
    return pd.DataFrame({
        'DateTime': pd.date_range('2026-02-16', periods=3, freq='6min'),
        'DEP01': [4.0, 4.0, 4.0],
        'OBS': [value, value, value],
    })


def _call(station_id, retrieve_mock, tmp_path):
    """Invoke _fetch_and_format_station with retrieval + formatting mocked."""
    with patch.object(gso, 'retrieve_t_and_c_station',
                      side_effect=retrieve_mock) as mock_retrieve, \
            patch.object(gso, '_format_timeseries',
                         return_value=['formatted-line']) as mock_format:
        result = gso._fetch_and_format_station(
            station_info=[station_id, 'x', 'y', 'CO-OPS', 'z'],
            station_metadata=['MLLW', 0.0, 0.0],
            variable='currents',
            name_var='cu',
            datum='MLLW',
            datum_list=['MLLW'],
            start_date='20260216',
            end_date='20260218',
            start_date_full='2026-02-16-00:00:00',
            end_date_full='2026-02-18-00:00:00',
            ofs='necofs',
            data_observations_1d_station_path=str(tmp_path),
            logger=logging.getLogger(_LOGGER_NAME),
            control_files_path=str(tmp_path),
        )
    return result, mock_retrieve, mock_format


def test_virtual_row_missing_bin_skips_with_error(tmp_path, caplog):
    """Bin absent even after the retry -> ERROR log, no .obs, no substitute."""
    def fake_retrieve(retrieve_input, logger, control_files_path,
                      config_file=None, only_bins=None):
        # Neither the restricted fetch nor the unrestricted retry has
        # bin 3 — only bins 5 and 7 exist.
        return {5: _bin_df(2.0), 7: _bin_df(2.5)}

    with caplog.at_level(logging.ERROR, logger=_LOGGER_NAME):
        result, _, mock_format = _call(
            '1234567_b03', fake_retrieve, tmp_path)

    assert result is None
    mock_format.assert_not_called()
    assert list(tmp_path.glob('*.obs')) == []
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1
    message = errors[0].getMessage()
    assert '1234567_b03' in message
    assert '[5, 7]' in message
    assert 'skipping' in message


def test_virtual_row_bin_present_after_retry_emits(tmp_path, caplog):
    """Bin found by the unrestricted retry -> station emits normally."""
    calls = []

    def fake_retrieve(retrieve_input, logger, control_files_path,
                      config_file=None, only_bins=None):
        calls.append(only_bins)
        if only_bins is not None:
            return {}
        return {3: _bin_df(1.0)}

    with caplog.at_level(logging.DEBUG, logger=_LOGGER_NAME):
        result, _, mock_format = _call(
            '1234567_b03', fake_retrieve, tmp_path)

    assert calls == [{3}, None]
    assert result == '1234567_b03'
    mock_format.assert_called_once()
    obs_file = tmp_path / '1234567_b03_necofs_cu_station.obs'
    assert obs_file.read_text(encoding='utf-8') == (
        series_header('cu') + 'formatted-line\n')
    assert not [r for r in caplog.records if r.levelno == logging.ERROR]


def test_legacy_id_keeps_first_available_bin_fallback(tmp_path, caplog):
    """A non-virtual ID still falls back to the first available bin."""
    def fake_retrieve(retrieve_input, logger, control_files_path,
                      config_file=None, only_bins=None):
        return {7: _bin_df(3.0), 2: _bin_df(3.5)}

    with caplog.at_level(logging.DEBUG, logger=_LOGGER_NAME):
        result, mock_retrieve, mock_format = _call(
            '1234567', fake_retrieve, tmp_path)

    # Legacy rows fetch unrestricted once and use the lowest bin number.
    assert [c.kwargs.get('only_bins')
            for c in mock_retrieve.call_args_list] == [None]
    assert result == '1234567'
    mock_format.assert_called_once()
    assert (tmp_path / '1234567_necofs_cu_station.obs').exists()
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any('using bin 2' in r.getMessage() for r in warnings)
    assert not [r for r in caplog.records if r.levelno == logging.ERROR]


def test_virtual_row_empty_dict_after_retry_skips_quietly(tmp_path, caplog):
    """No bins at all after the retry -> plain no-data skip, no ERROR."""
    def fake_retrieve(retrieve_input, logger, control_files_path,
                      config_file=None, only_bins=None):
        return {}

    with caplog.at_level(logging.DEBUG, logger=_LOGGER_NAME):
        result, _, mock_format = _call(
            '1234567_b03', fake_retrieve, tmp_path)

    # An empty dict means the station is simply dead in the window —
    # the pre-existing info-level skip applies, not the ERROR path.
    assert result is None
    mock_format.assert_not_called()
    assert list(tmp_path.glob('*.obs')) == []
    assert not [r for r in caplog.records if r.levelno == logging.ERROR]
