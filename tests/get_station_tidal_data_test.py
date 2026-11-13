"""Tests for issue #271 tide-lookup tightening in get_station_tidal_data.

Default config disables nearby CO-OPS fallback. When nearby search is
enabled, each candidate is queried once; a False (no-predictions) response
must not walk the rest of datum_list for that station.
"""
from __future__ import annotations

import importlib
import logging
from datetime import datetime
from types import SimpleNamespace
from unittest import mock

import pandas as pd
import pytest

# Package __init__ re-exports the function and shadows the submodule.
gstd = importlib.import_module(
    'ofs_skill.obs_retrieval.get_station_tidal_data')


def _prop(tmp_path, datum='MLLW', ofs='cbofs'):
    ctl_dir = tmp_path / 'control_files'
    ctl_dir.mkdir()
    # CO-OPS-style and USGS-style rows for coordinate lookup.
    (ctl_dir / f'{ofs}_wl_station.ctl').write_text(
        '8574680 8574680_COOPS "Annapolis"\n'
        '38.983 -76.480 0.0 MLLW\n'
        '01646500 01646500_USGS "Potomac"\n'
        '38.950 -77.130 0.0 MLLW\n'
        '44009 44009_NDBC "Delaware Bay"\n'
        '38.460 -74.700 0.0 MLLW\n',
        encoding='utf-8',
    )
    return SimpleNamespace(
        datum=datum,
        ofs=ofs,
        control_files_path=str(ctl_dir),
    )


def _tide_df():
    return pd.DataFrame({
        'DateTime': pd.to_datetime(['2026-07-15T00:00:00', '2026-07-15T01:00:00']),
        'TIDE': [0.1, 0.2],
    })


@pytest.fixture
def logger():
    return logging.getLogger('get_station_tidal_data_test')


def test_api_datum_maps_model_only_to_mllw():
    assert gstd._api_datum_for_request('MLLW') == 'MLLW'
    assert gstd._api_datum_for_request('mhw') == 'MHW'
    assert gstd._api_datum_for_request('IGLD85') == 'MLLW'
    assert gstd._api_datum_for_request('XGEOID20B') == 'MLLW'
    assert gstd._api_datum_for_request('LWD') == 'MLLW'


def test_non_coops_skips_nearby_when_flag_false(tmp_path, logger):
    """USGS/NDBC IDs must not trigger nearby search when the flag is false."""
    prop = _prop(tmp_path)
    with mock.patch.object(gstd, '_allow_nearby_tidal_stations', return_value=False), \
            mock.patch.object(gstd, 'retrieve_tidal_predictions') as retrieve, \
            mock.patch.object(gstd, 'find_nearest_tidal_stations') as nearest:
        tidal_data, info = gstd.get_station_tidal_data(
            datetime(2026, 7, 15), datetime(2026, 7, 15, 23, 59),
            prop, '01646500', logger)

    assert tidal_data is None
    assert info['tidal_station_id'] is None
    retrieve.assert_not_called()
    nearest.assert_not_called()


def test_coops_station_one_api_call_on_success(tmp_path, logger):
    """A CO-OPS ID should query itself once and stop on success."""
    prop = _prop(tmp_path, datum='MLLW')
    with mock.patch.object(gstd, '_allow_nearby_tidal_stations', return_value=False), \
            mock.patch.object(
                gstd, 'retrieve_tidal_predictions', return_value=_tide_df()
            ) as retrieve, \
            mock.patch.object(gstd, 'find_nearest_tidal_stations') as nearest:
        tidal_data, info = gstd.get_station_tidal_data(
            datetime(2026, 7, 15), datetime(2026, 7, 15, 23, 59),
            prop, '8574680', logger)

    assert tidal_data is not None
    assert info['tidal_station_id'] == '8574680'
    assert info['used_datum'] == 'MLLW'
    assert retrieve.call_count == 1
    assert retrieve.call_args.args[0].datum == 'MLLW'
    nearest.assert_not_called()


def test_no_predictions_does_not_walk_datums_on_nearby(tmp_path, logger):
    """False from the API must move to the next station, not the next datum."""
    prop = _prop(tmp_path, datum='MLLW')
    nearby = [
        ('8590111', 'Near A', 5.0),
        ('8634214', 'Near B', 8.0),
    ]
    good = _tide_df()

    def _retrieve(retrieve_input, _logger):
        if retrieve_input.station == '8590111':
            return False
        if retrieve_input.station == '8634214':
            return good
        return None

    with mock.patch.object(gstd, '_allow_nearby_tidal_stations', return_value=True), \
            mock.patch.object(
                gstd, 'find_nearest_tidal_stations', return_value=nearby
            ) as nearest, \
            mock.patch.object(
                gstd, 'retrieve_tidal_predictions', side_effect=_retrieve
            ) as retrieve:
        tidal_data, info = gstd.get_station_tidal_data(
            datetime(2026, 7, 15), datetime(2026, 7, 15, 23, 59),
            prop, '44009', logger)

    assert tidal_data is not None
    assert info['tidal_station_id'] == '8634214'
    assert info['tidal_station_distance'] == 8.0
    nearest.assert_called_once()
    # One call for 8590111 (False) + one for 8634214 (hit). No datum walk.
    assert retrieve.call_count == 2
    assert [c.args[0].datum for c in retrieve.call_args_list] == ['MLLW', 'MLLW']


def test_model_only_datum_uses_mllw_for_api(tmp_path, logger):
    """IGLD85 must not be sent to the predictions API."""
    prop = _prop(tmp_path, datum='IGLD85')
    with mock.patch.object(gstd, '_allow_nearby_tidal_stations', return_value=False), \
            mock.patch.object(
                gstd, 'retrieve_tidal_predictions', return_value=_tide_df()
            ) as retrieve, \
            mock.patch.object(
                gstd, '_convert_tidal_datum',
                side_effect=lambda data, used, req, lat, lon, log: (data, req),
            ):
        tidal_data, info = gstd.get_station_tidal_data(
            datetime(2026, 7, 15), datetime(2026, 7, 15, 23, 59),
            prop, '8574680', logger)

    assert tidal_data is not None
    assert retrieve.call_args.args[0].datum == 'MLLW'
    assert info['used_datum'] == 'IGLD85'
