"""
Test suite for CHS (Canadian Hydrographic Service) observation retrieval.

This module tests the CHS data retrieval functionality including:
- Inventory retrieval with dynamic variable capability flags
- Data retrieval for water level, temperature, salinity, and currents
- Fallback time series code logic
- Current speed/direction merge
"""

import logging
from datetime import datetime
from unittest.mock import Mock, patch

import pandas as pd
import pytest
import requests

from ofs_skill.obs_retrieval import retrieve_chs_station as retrieve_chs_station_module
from ofs_skill.obs_retrieval.inventory_chs_station import (
    inventory_chs_station,
)
from ofs_skill.obs_retrieval.retrieve_chs_station import (
    retrieve_chs_station,
)


@pytest.fixture
def logger():
    """Create a logger for tests."""
    logging.basicConfig(level=logging.INFO)
    return logging.getLogger('test_chs')


@pytest.fixture(autouse=True)
def mock_chs_uuid():
    """Bypass network UUID resolution and echo back the test station ID."""
    with patch(
        'ofs_skill.obs_retrieval.retrieve_chs_station._get_chs_uuid',
        side_effect=lambda station_id, logger: station_id,
    ):
        yield


def _make_chs_api_response(values, start='2025-01-01', minutes=3):
    """Helper to create a fake CHS API response DataFrame."""
    n = len(values)
    dates = pd.date_range(start, periods=n, freq=f'{minutes}min')
    return pd.DataFrame({
        'eventDate': [d.strftime('%Y-%m-%dT%H:%M:%SZ') for d in dates],
        'qcFlagCode': ['1'] * n,
        'value': values,
        'timeSeriesId': ['fake_ts_id'] * n,
        'reviewed': [False] * n,
    })


def _make_station_metadata(station_id, name, lat, lon, codes):
    """Helper to create a fake station metadata row."""
    ts_list = [
        {'id': f'fake_{c}', 'code': c, 'nameEn': f'Fake {c}',
         'nameFr': f'Faux {c}', 'phenomenonId': 'fake', 'owner': 'CHS-SHC'}
        for c in codes
    ]
    return {
        'id': station_id,
        'code': '99999',
        'officialName': name,
        'alternativeName': None,
        'operating': True,
        'latitude': lat,
        'longitude': lon,
        'type': 'PERMANENT',
        'timeSeries': ts_list,
    }


class TestCHSImports:
    """Test that CHS modules can be imported correctly."""

    def test_retrieve_chs_station_import(self):
        from ofs_skill.obs_retrieval.retrieve_chs_station import (
            retrieve_chs_station,
        )
        assert callable(retrieve_chs_station)

    def test_inventory_chs_station_import(self):
        from ofs_skill.obs_retrieval.inventory_chs_station import (
            inventory_chs_station,
        )
        assert callable(inventory_chs_station)


class TestInventoryCapabilityFlags:
    """Test dynamic capability flag extraction from timeSeries metadata."""

    @patch('ofs_skill.obs_retrieval.inventory_chs_station.get_chs_stations')
    def test_wl_only_station(self, mock_get_stations, logger):
        """Station with only wlo should have has_wl=True, others False."""
        import geopandas as gpd
        from shapely.geometry import Point

        meta = _make_station_metadata(
            'st1', 'Test WL', 45.0, -70.0, ['wlp', 'wlo', 'wlp-hilo'])
        df = gpd.GeoDataFrame([meta],
                              geometry=[Point(meta['longitude'],
                                              meta['latitude'])],
                              crs='EPSG:4326')
        mock_get_stations.return_value = df

        result = inventory_chs_station(44.0, 46.0, -71.0, -69.0, logger)

        assert result is not None
        assert len(result) == 1
        assert result.iloc[0]['has_wl']
        assert not result.iloc[0]['has_temp']
        assert not result.iloc[0]['has_salt']
        assert not result.iloc[0]['has_cu']

    @patch('ofs_skill.obs_retrieval.inventory_chs_station.get_chs_stations')
    def test_full_capability_station(self, mock_get_stations, logger):
        """Station with all variable codes should have all flags True."""
        import geopandas as gpd
        from shapely.geometry import Point

        meta = _make_station_metadata(
            'st2', 'Test Full', 45.0, -70.0,
            ['wlo', 'wt1', 'ws1', 'wcs1', 'wcd1'])
        df = gpd.GeoDataFrame([meta],
                              geometry=[Point(meta['longitude'],
                                              meta['latitude'])],
                              crs='EPSG:4326')
        mock_get_stations.return_value = df

        result = inventory_chs_station(44.0, 46.0, -71.0, -69.0, logger)

        assert result.iloc[0]['has_wl']
        assert result.iloc[0]['has_temp']
        assert result.iloc[0]['has_salt']
        assert result.iloc[0]['has_cu']

    @patch('ofs_skill.obs_retrieval.inventory_chs_station.get_chs_stations')
    def test_speed_only_no_currents(self, mock_get_stations, logger):
        """Station with speed but no direction should have has_cu=False."""
        import geopandas as gpd
        from shapely.geometry import Point

        meta = _make_station_metadata(
            'st3', 'Speed Only', 45.0, -70.0, ['wlo', 'wcs1'])
        df = gpd.GeoDataFrame([meta],
                              geometry=[Point(meta['longitude'],
                                              meta['latitude'])],
                              crs='EPSG:4326')
        mock_get_stations.return_value = df

        result = inventory_chs_station(44.0, 46.0, -71.0, -69.0, logger)

        assert not result.iloc[0]['has_cu']

    @patch('ofs_skill.obs_retrieval.inventory_chs_station.get_chs_stations')
    def test_fallback_codes(self, mock_get_stations, logger):
        """Station with wt2 (not wt1) should still have has_temp=True."""
        import geopandas as gpd
        from shapely.geometry import Point

        meta = _make_station_metadata(
            'st4', 'Fallback', 45.0, -70.0, ['wlo', 'wt2', 'ws2'])
        df = gpd.GeoDataFrame([meta],
                              geometry=[Point(meta['longitude'],
                                              meta['latitude'])],
                              crs='EPSG:4326')
        mock_get_stations.return_value = df

        result = inventory_chs_station(44.0, 46.0, -71.0, -69.0, logger)

        assert result.iloc[0]['has_temp']
        assert result.iloc[0]['has_salt']

    @patch('ofs_skill.obs_retrieval.inventory_chs_station.get_chs_stations')
    def test_no_wlo_station(self, mock_get_stations, logger):
        """Station without wlo should have has_wl=False."""
        import geopandas as gpd
        from shapely.geometry import Point

        meta = _make_station_metadata(
            'st5', 'Predictions Only', 45.0, -70.0, ['wlp', 'wlp-hilo'])
        df = gpd.GeoDataFrame([meta],
                              geometry=[Point(meta['longitude'],
                                              meta['latitude'])],
                              crs='EPSG:4326')
        mock_get_stations.return_value = df

        result = inventory_chs_station(44.0, 46.0, -71.0, -69.0, logger)

        assert not result.iloc[0]['has_wl']

    @patch('ofs_skill.obs_retrieval.inventory_chs_station.get_chs_stations')
    def test_geo_filtering_passed_to_searvey(self, mock_get_stations, logger):
        """Verify bbox params are passed to get_chs_stations."""
        import geopandas as gpd
        mock_get_stations.return_value = gpd.GeoDataFrame()

        inventory_chs_station(44.0, 46.0, -71.0, -69.0, logger)

        mock_get_stations.assert_called_once_with(
            lon_min=-71.0, lon_max=-69.0,
            lat_min=44.0, lat_max=46.0,
        )


class TestRetrieveScalar:
    """Test scalar variable retrieval (water level, temp, salinity)."""

    @patch('ofs_skill.obs_retrieval.retrieve_chs_station._fetch_chs_window')
    def test_water_level(self, mock_fetch, logger):
        """Water level should use wlo code and set Datum=IGLD."""
        mock_fetch.return_value = _make_chs_api_response([1.0, 1.5, 2.0])

        result = retrieve_chs_station(
            '20250101', '20250102', 'test_st', 'water_level', logger)

        assert result is not None
        assert 'DateTime' in result.columns
        assert 'OBS' in result.columns
        assert 'DEP01' in result.columns
        assert 'Datum' in result.columns
        assert (result['Datum'] == 'IGLD').all()
        assert (result['DEP01'] == 0.0).all()
        args = mock_fetch.call_args[0]
        assert args[0] == 'test_st'
        assert args[1] == 'wlo'

    @patch('ofs_skill.obs_retrieval.retrieve_chs_station._fetch_chs_window')
    def test_temperature(self, mock_fetch, logger):
        """Temperature should use wt1 code and NOT set Datum."""
        mock_fetch.return_value = _make_chs_api_response([5.0, 5.5, 6.0])

        result = retrieve_chs_station(
            '20250101', '20250102', 'test_st', 'water_temperature', logger)

        assert result is not None
        assert 'Datum' not in result.columns
        assert (result['DEP01'] == 0.0).all()
        args = mock_fetch.call_args[0]
        assert args[0] == 'test_st'
        assert args[1] == 'wt1'

    @patch('ofs_skill.obs_retrieval.retrieve_chs_station._fetch_chs_window')
    def test_temperature_fallback_wt2(self, mock_fetch, logger):
        """If wt1 returns empty, should fallback to wt2."""
        empty_df = pd.DataFrame(columns=[
            'eventDate', 'qcFlagCode', 'value', 'timeSeriesId', 'reviewed'])
        good_df = _make_chs_api_response([5.0, 5.5])

        mock_fetch.side_effect = [empty_df, good_df]

        result = retrieve_chs_station(
            '20250101', '20250102', 'test_st', 'water_temperature', logger)

        assert result is not None
        assert len(result) == 2
        calls = mock_fetch.call_args_list
        assert calls[0][0][1] == 'wt1'
        assert calls[1][0][1] == 'wt2'

    @patch('ofs_skill.obs_retrieval.retrieve_chs_station._fetch_chs_window')
    def test_salinity(self, mock_fetch, logger):
        """Salinity should use ws1 code and NOT set Datum."""
        mock_fetch.return_value = _make_chs_api_response([23.0, 23.5])

        result = retrieve_chs_station(
            '20250101', '20250102', 'test_st', 'salinity', logger)

        assert result is not None
        assert 'Datum' not in result.columns
        args = mock_fetch.call_args[0]
        assert args[0] == 'test_st'
        assert args[1] == 'ws1'

    @patch('ofs_skill.obs_retrieval.retrieve_chs_station._fetch_chs_window')
    def test_no_data_returns_none(self, mock_fetch, logger):
        """Should return None when no data available."""
        empty_df = pd.DataFrame(columns=[
            'eventDate', 'qcFlagCode', 'value', 'timeSeriesId', 'reviewed'])
        mock_fetch.return_value = empty_df

        result = retrieve_chs_station(
            '20250101', '20250102', 'test_st', 'water_temperature', logger)

        assert result is None

    @patch('ofs_skill.obs_retrieval.retrieve_chs_station._fetch_chs_window')
    def test_qc_filtering_rejects_suspect_data(self, mock_fetch, logger):
        """Records with qcFlagCode 3 (suspect) or 4 (erroneous) are filtered."""
        n = 5
        dates = pd.date_range('2025-01-01', periods=n, freq='3min')
        df = pd.DataFrame({
            'eventDate': [d.strftime('%Y-%m-%dT%H:%M:%SZ') for d in dates],
            'qcFlagCode': ['1', '2', '3', '4', '1'],
            'value': [5.0, 5.5, 999.0, -99.0, 6.0],
            'timeSeriesId': ['ts'] * n,
            'reviewed': [False] * n,
        })
        mock_fetch.return_value = df

        result = retrieve_chs_station(
            '20250101', '20250102', 'test_st', 'water_temperature', logger)

        assert result is not None
        assert len(result) == 3  # only codes '1' and '2' accepted
        assert 999.0 not in result['OBS'].values
        assert -99.0 not in result['OBS'].values


class TestRetrieveCurrents:
    """Test current speed/direction retrieval and merge."""

    @patch('ofs_skill.obs_retrieval.retrieve_chs_station._fetch_chs_window')
    def test_currents_merge(self, mock_fetch, logger):
        """Speed and direction should be merged on DateTime."""
        speed_df = _make_chs_api_response([1.0, 1.5, 2.0])
        dir_df = _make_chs_api_response([90.0, 135.0, 180.0])

        mock_fetch.side_effect = [speed_df, dir_df]

        result = retrieve_chs_station(
            '20250101', '20250102', 'test_st', 'currents', logger)

        assert result is not None
        assert 'OBS' in result.columns
        assert 'DIR' in result.columns
        assert 'DEP01' in result.columns
        assert 'Datum' not in result.columns
        assert len(result) == 3
        assert list(result['OBS']) == [1.0, 1.5, 2.0]
        assert list(result['DIR']) == [90.0, 135.0, 180.0]

    @patch('ofs_skill.obs_retrieval.retrieve_chs_station._fetch_chs_window')
    def test_currents_partial_overlap(self, mock_fetch, logger):
        """Only timestamps with both speed and direction should be kept."""
        speed_df = _make_chs_api_response([1.0, 1.5, 2.0])
        # Direction has only 2 timestamps (same start)
        dir_df = _make_chs_api_response([90.0, 135.0])

        mock_fetch.side_effect = [speed_df, dir_df]

        result = retrieve_chs_station(
            '20250101', '20250102', 'test_st', 'currents', logger)

        assert result is not None
        assert len(result) == 2  # inner merge

    @patch('ofs_skill.obs_retrieval.retrieve_chs_station._fetch_chs_window')
    def test_currents_no_speed_returns_none(self, mock_fetch, logger):
        """If speed data is missing from all pairs, should return None."""
        empty_df = pd.DataFrame(columns=[
            'eventDate', 'qcFlagCode', 'value', 'timeSeriesId', 'reviewed'])

        # Pair 1: wcs1 empty -> skip direction, continue to pair 2
        # Pair 2: wcs2 empty -> skip direction, no pairs left
        mock_fetch.side_effect = [empty_df, empty_df]

        result = retrieve_chs_station(
            '20250101', '20250102', 'test_st', 'currents', logger)

        assert result is None
        # Only speed codes tried (direction skipped due to early continue)
        calls = mock_fetch.call_args_list
        assert len(calls) == 2
        assert calls[0][0][1] == 'wcs1'
        assert calls[1][0][1] == 'wcs2'

    @patch('ofs_skill.obs_retrieval.retrieve_chs_station._fetch_chs_window')
    def test_currents_matched_sensor_pair(self, mock_fetch, logger):
        """Speed and direction must come from same sensor number."""
        empty_df = pd.DataFrame(columns=[
            'eventDate', 'qcFlagCode', 'value', 'timeSeriesId', 'reviewed'])
        speed_df = _make_chs_api_response([1.0])
        dir_df = _make_chs_api_response([90.0])

        # Pair 1: wcs1 empty -> skip wcd1 (early continue)
        # Pair 2: wcs2 has data, wcd2 has data -> pair succeeds
        mock_fetch.side_effect = [empty_df, speed_df, dir_df]

        result = retrieve_chs_station(
            '20250101', '20250102', 'test_st', 'currents', logger)

        assert result is not None
        calls = mock_fetch.call_args_list
        assert calls[0][0][1] == 'wcs1'
        # wcd1 skipped because wcs1 was empty
        assert calls[1][0][1] == 'wcs2'
        assert calls[2][0][1] == 'wcd2'

    @patch('ofs_skill.obs_retrieval.retrieve_chs_station._fetch_chs_window')
    def test_currents_api_codes(self, mock_fetch, logger):
        """Should call wcs1 for speed and wcd1 for direction."""
        speed_df = _make_chs_api_response([1.0])
        dir_df = _make_chs_api_response([90.0])

        mock_fetch.side_effect = [speed_df, dir_df]

        retrieve_chs_station(
            '20250101', '20250102', 'test_st', 'currents', logger)

        calls = mock_fetch.call_args_list
        assert calls[0][0][1] == 'wcs1'
        assert calls[1][0][1] == 'wcd1'


class TestDateChunking:
    """Test 21-day date chunking behavior."""

    @patch('ofs_skill.obs_retrieval.retrieve_chs_station._fetch_chs_window')
    def test_short_range_no_chunking(self, mock_fetch, logger):
        """Ranges <= 21 days should result in a single API call."""
        mock_fetch.return_value = _make_chs_api_response([1.0])

        retrieve_chs_station(
            '20250101', '20250105', 'test_st', 'water_level', logger)

        assert mock_fetch.call_count == 1

    @patch('ofs_skill.obs_retrieval.retrieve_chs_station._fetch_chs_window')
    def test_range_within_three_weeks_not_chunked(self, mock_fetch, logger):
        """A 19-day range fits one request at THREE_MINUTES resolution.

        Under the previous ONE_MINUTE default this needed three requests.
        """
        mock_fetch.return_value = _make_chs_api_response([1.0])

        retrieve_chs_station(
            '20250101', '20250120', 'test_st', 'water_level', logger)

        assert mock_fetch.call_count == 1

    @patch('ofs_skill.obs_retrieval.retrieve_chs_station._fetch_chs_window')
    def test_long_range_chunked(self, mock_fetch, logger):
        """Ranges > 21 days should be split into multiple API calls."""
        mock_fetch.return_value = _make_chs_api_response([1.0])

        retrieve_chs_station(
            '20250101', '20250401', 'test_st', 'water_level', logger)

        assert mock_fetch.call_count > 1

    @patch('ofs_skill.obs_retrieval.retrieve_chs_station._fetch_chs_window')
    def test_six_month_window_request_count(self, mock_fetch, logger):
        """A ~6-month window costs 10 requests with THREE_MINUTES (21-day chunks).

        This is significantly fewer than the 28 requests required at
        the API's default ONE_MINUTE resolution.
        """
        mock_fetch.return_value = _make_chs_api_response([1.0])

        retrieve_chs_station(
            '20260226', '20260904', 'test_st', 'water_level', logger)

        assert mock_fetch.call_count == 10


class TestRequestResolution:
    """The explicit resolution parameter must reach the CHS API."""

    @patch('ofs_skill.obs_retrieval.retrieve_chs_station.chs_get')
    def test_window_request_sends_three_minute_resolution(self, mock_get):
        """Omitting resolution silently caps the window at 7 days."""
        mock_get.return_value = Mock(
            json=Mock(return_value=[]), raise_for_status=Mock()
        )

        retrieve_chs_station_module._fetch_chs_window(
            'abc123', 'wlo',
            datetime(2026, 3, 1), datetime(2026, 3, 22),
            logging.getLogger('chs_resolution_test'),
        )

        url = mock_get.call_args[0][0]
        assert 'resolution=THREE_MINUTES' in url
        assert 'time-series-code=wlo' in url
        assert 'from=2026-03-01T00:00:00Z' in url
        assert 'to=2026-03-22T00:00:00Z' in url

    @patch('ofs_skill.obs_retrieval.retrieve_chs_station.chs_get')
    def test_error_object_is_not_treated_as_data(self, mock_get):
        """An error response is a JSON object, not a list of observations."""
        mock_get.return_value = Mock(
            json=Mock(return_value={'errors': ['nope']}),
            raise_for_status=Mock(),
        )

        result = retrieve_chs_station_module._fetch_chs_window(
            'abc123', 'wlo',
            datetime(2026, 3, 1), datetime(2026, 3, 22),
            logging.getLogger('chs_resolution_test'),
        )

        # None, not empty: an error object is a failed window, not an
        # absence of observations.
        assert result is None


class TestUnsupportedVariable:
    """Test behavior with unsupported variable names."""

    @patch('ofs_skill.obs_retrieval.retrieve_chs_station._fetch_chs_window')
    def test_invalid_variable(self, mock_fetch, logger):
        """Unsupported variable should return None."""
        result = retrieve_chs_station(
            '20250101', '20250102', 'test_st', 'ice_concentration', logger)

        assert result is None


class TestWindowHttpErrors:
    """A failed window must degrade to no data, not abort the station."""

    @staticmethod
    def _response(status):
        response = Mock()
        error = requests.HTTPError(f'{status}')
        error.response = Mock(status_code=status)
        response.raise_for_status = Mock(side_effect=error)
        return response

    @patch('ofs_skill.obs_retrieval.retrieve_chs_station.chs_get')
    def test_missing_time_series_returns_empty(self, mock_get, logger):
        """404 means the station does not publish that code at all.

        searvey returned a frame with an 'errors' column here; raising
        instead would abort every station lacking the code.
        """
        mock_get.return_value = self._response(404)

        result = retrieve_chs_station_module._fetch_chs_window(
            'abc123', 'wlo',
            datetime(2026, 3, 1), datetime(2026, 3, 22), logger,
        )

        assert result.empty

    @patch('ofs_skill.obs_retrieval.retrieve_chs_station.chs_get')
    def test_rate_limited_window_warns(self, mock_get, logger, caplog):
        """429 drops data, so unlike a 404 it must be visible in the log."""
        mock_get.return_value = self._response(429)

        with caplog.at_level(logging.WARNING):
            result = retrieve_chs_station_module._fetch_chs_window(
                'abc123', 'wlo',
                datetime(2026, 3, 1), datetime(2026, 3, 22), logger,
            )

        assert result is None
        assert any('429' in r.getMessage() for r in caplog.records)

    @patch('ofs_skill.obs_retrieval.retrieve_chs_station.chs_get')
    def test_station_survives_a_failed_window(self, mock_get, logger):
        """One bad window must not lose the rest of the station."""
        good = _make_chs_api_response([1.0, 1.5])
        good_response = Mock(
            json=Mock(return_value=good.to_dict('records')),
            raise_for_status=Mock(),
        )
        mock_get.side_effect = [self._response(500)] + [good_response] * 9

        result = retrieve_chs_station(
            '20260226', '20260904', '000000000000000000000065',
            'water_level', logger)

        assert result is not None
        assert not result.empty


class TestFailedWindowsAreReported:
    """A failed window must not be silently trimmed from the series."""

    @patch('ofs_skill.obs_retrieval.retrieve_chs_station._fetch_chs_window')
    def test_partial_series_warns_with_gap_size(self, mock_fetch, logger,
                                                caplog):
        """9 of 10 windows returning is not success; say what is missing.

        Concatenating the survivors and reporting data-found would write an
        .obs file and skill statistics over an undisclosed multi-week hole.
        """
        good = _make_chs_api_response([1.0, 1.5])
        mock_fetch.side_effect = [good, good, None] + [good] * 7

        with caplog.at_level(logging.WARNING):
            result = retrieve_chs_station(
                '20260226', '20260904', '000000000000000000000065',
                'water_level', logger)

        assert result is not None
        joined = '\n'.join(r.getMessage() for r in caplog.records)
        assert '1 of 10 window(s) could not be retrieved' in joined
        assert 'day(s) are missing' in joined

    @patch('ofs_skill.obs_retrieval.retrieve_chs_station._fetch_chs_window')
    def test_complete_series_does_not_warn(self, mock_fetch, logger, caplog):
        """No false alarm when every window came back."""
        good = _make_chs_api_response([1.0, 1.5])
        mock_fetch.return_value = good

        with caplog.at_level(logging.WARNING):
            retrieve_chs_station(
                '20260226', '20260904', '000000000000000000000065',
                'water_level', logger)

        joined = '\n'.join(r.getMessage() for r in caplog.records)
        assert 'could not be retrieved' not in joined

    @patch('ofs_skill.obs_retrieval.retrieve_chs_station.chs_get')
    def test_non_json_body_does_not_abort_the_station(self, mock_get,
                                                      logger):
        """A 200 carrying HTML must degrade, not propagate out."""
        mock_get.return_value = Mock(
            raise_for_status=Mock(),
            json=Mock(side_effect=ValueError('Expecting value')),
        )

        result = retrieve_chs_station_module._fetch_chs_window(
            'abc123', 'wlo',
            datetime(2026, 3, 1), datetime(2026, 3, 22), logger,
        )

        assert result is None


class TestRequestedWindowCoverage:
    """Chunk boundaries must tile the requested window exactly.

    The rewrite to patch _fetch_chs_window dropped the old per-call
    assertions on start_date/end_date, so nothing verified that the chunks
    actually span what the caller asked for. A gap loses observations
    silently; an overlap wastes rate-limit budget re-fetching.
    """

    @patch('ofs_skill.obs_retrieval.retrieve_chs_station._fetch_chs_window')
    def test_chunks_tile_the_window_without_gaps_or_overlaps(
            self, mock_fetch, logger):
        mock_fetch.return_value = _make_chs_api_response([1.0])

        retrieve_chs_station(
            '20260226', '20260904', 'test_st', 'water_level', logger)

        windows = [(c.args[2], c.args[3]) for c in mock_fetch.call_args_list]

        assert windows[0][0] == datetime(2026, 2, 26)
        assert windows[-1][1] == datetime(2026, 9, 4)
        for (_, prev_end), (next_start, _) in zip(windows, windows[1:]):
            assert prev_end == next_start, (
                f'gap or overlap at {prev_end} -> {next_start}')

    @patch('ofs_skill.obs_retrieval.retrieve_chs_station._fetch_chs_window')
    def test_no_chunk_exceeds_the_api_maximum(self, mock_fetch, logger):
        """CHS rejects a window longer than 21 days at this resolution."""
        mock_fetch.return_value = _make_chs_api_response([1.0])

        retrieve_chs_station(
            '20260101', '20261231', 'test_st', 'water_level', logger)

        for call in mock_fetch.call_args_list:
            span = (call.args[3] - call.args[2]).days
            assert span <= 21, f'chunk of {span} days exceeds the API cap'

    @patch('ofs_skill.obs_retrieval.retrieve_chs_station._fetch_chs_window')
    def test_short_window_is_requested_verbatim(self, mock_fetch, logger):
        """A sub-chunk window must not be rounded or padded."""
        mock_fetch.return_value = _make_chs_api_response([1.0])

        retrieve_chs_station(
            '20260301', '20260305', 'test_st', 'water_level', logger)

        assert mock_fetch.call_count == 1
        assert mock_fetch.call_args.args[2] == datetime(2026, 3, 1)
        assert mock_fetch.call_args.args[3] == datetime(2026, 3, 5)
