"""
Tests for the CO-OPS currents ``only_bins`` restriction in
``_fetch_and_format_station``.

The station ctl file carries one row per virtual ADCP bin
(``{parent}_b{NN}``). The obs fetch for such a row must restrict the
CO-OPS retrieval to that one bin (``only_bins={NN}``) instead of
fetching every bin of the parent station and discarding all but one —
K bins per station previously meant K× the CO-OPS requests per row.
"""

import importlib
from unittest.mock import patch

import pandas as pd

# The package __init__ re-exports the function under the same name as
# its module, so attribute-style imports resolve to the function.
gso = importlib.import_module(
    'ofs_skill.obs_retrieval.get_station_observations')


class _MockLogger:
    def info(self, *a, **k):
        pass

    def warning(self, *a, **k):
        pass

    def error(self, *a, **k):
        pass


def _bin_df(value):
    return pd.DataFrame({
        'DateTime': pd.date_range('2026-02-16', periods=3, freq='6min'),
        'DEP01': [4.0, 4.0, 4.0],
        'OBS': [value, value, value],
    })


def _call(station_id, variable='currents', retrieve_mock=None,
          tmp_path=None):
    """Invoke _fetch_and_format_station with retrieval + formatting mocked."""
    with patch.object(gso, 'retrieve_t_and_c_station',
                      side_effect=retrieve_mock) as mock_retrieve, \
            patch.object(gso, '_format_timeseries',
                         return_value=['formatted-line']):
        result = gso._fetch_and_format_station(
            station_info=[station_id, 'x', 'y', 'CO-OPS', 'z'],
            station_metadata=['MLLW', 0.0, 0.0],
            variable=variable,
            name_var='cu' if variable == 'currents' else 'wl',
            datum='MLLW',
            datum_list=['MLLW'],
            start_date='20260216',
            end_date='20260218',
            start_date_full='2026-02-16-00:00:00',
            end_date_full='2026-02-18-00:00:00',
            ofs='necofs',
            data_observations_1d_station_path=str(tmp_path),
            logger=_MockLogger(),
            control_files_path=str(tmp_path),
        )
    return result, mock_retrieve


def test_virtual_bin_row_restricts_fetch(tmp_path):
    """A ``{parent}_b{NN}`` row passes only_bins={NN} and fetches once."""
    calls = []

    def fake_retrieve(retrieve_input, logger, control_files_path,
                      config_file=None, only_bins=None):
        calls.append(only_bins)
        return {3: _bin_df(1.0)}

    result, _ = _call('1234567_b03', retrieve_mock=fake_retrieve,
                      tmp_path=tmp_path)

    assert result == '1234567_b03'
    assert calls == [{3}]


def test_missing_bin_retries_unrestricted(tmp_path):
    """Empty restricted fetch triggers the unrestricted all-bins retry."""
    calls = []

    def fake_retrieve(retrieve_input, logger, control_files_path,
                      config_file=None, only_bins=None):
        calls.append(only_bins)
        if only_bins is not None:
            return {}
        return {5: _bin_df(2.0)}

    result, _ = _call('1234567_b03', retrieve_mock=fake_retrieve,
                      tmp_path=tmp_path)

    # Restricted first, unrestricted retry second. The retry still lacks
    # bin 3, so the virtual row is skipped rather than fed bin 5's data.
    assert calls == [{3}, None]
    assert result is None


def test_legacy_id_fetches_all_bins(tmp_path):
    """A non-virtual station ID keeps the unrestricted fetch."""
    calls = []

    def fake_retrieve(retrieve_input, logger, control_files_path,
                      config_file=None, only_bins=None):
        calls.append(only_bins)
        return {1: _bin_df(3.0)}

    result, _ = _call('1234567', retrieve_mock=fake_retrieve,
                      tmp_path=tmp_path)

    assert calls == [None]
    assert result == '1234567'
