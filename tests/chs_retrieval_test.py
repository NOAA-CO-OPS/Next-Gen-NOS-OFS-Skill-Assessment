"""
Test suite for CHS (Canadian Hydrographic Service) observation retrieval.

This module tests the CHS data retrieval functionality including:
- Inventory retrieval with dynamic variable capability flags
- Data retrieval for water level, temperature, salinity, and currents
- Fallback time series code logic
- Current speed/direction merge
"""

import logging
from unittest.mock import patch

import pandas as pd
import pytest

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


def _make_chs_api_response(values, start='2025-01-01', minutes=5):
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

    @patch('ofs_skill.obs_retrieval.retrieve_chs_station.fetch_chs_station')
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
        mock_fetch.assert_called_with(
            station_id='test_st',
            time_series_code='wlo',
            start_date='2025-01-01',
            end_date='2025-01-02',
        )

    @patch('ofs_skill.obs_retrieval.retrieve_chs_station.fetch_chs_station')
    def test_temperature(self, mock_fetch, logger):
        """Temperature should use wt1 code and NOT set Datum."""
        mock_fetch.return_value = _make_chs_api_response([5.0, 5.5, 6.0])

        result = retrieve_chs_station(
            '20250101', '20250102', 'test_st', 'water_temperature', logger)

        assert result is not None
        assert 'Datum' not in result.columns
        assert (result['DEP01'] == 0.0).all()
        mock_fetch.assert_called_with(
            station_id='test_st',
            time_series_code='wt1',
            start_date='2025-01-01',
            end_date='2025-01-02',
        )

    @patch('ofs_skill.obs_retrieval.retrieve_chs_station.fetch_chs_station')
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
        assert calls[0][1]['time_series_code'] == 'wt1'
        assert calls[1][1]['time_series_code'] == 'wt2'

    @patch('ofs_skill.obs_retrieval.retrieve_chs_station.fetch_chs_station')
    def test_salinity(self, mock_fetch, logger):
        """Salinity should use ws1 code and NOT set Datum."""
        mock_fetch.return_value = _make_chs_api_response([23.0, 23.5])

        result = retrieve_chs_station(
            '20250101', '20250102', 'test_st', 'salinity', logger)

        assert result is not None
        assert 'Datum' not in result.columns
        mock_fetch.assert_called_with(
            station_id='test_st',
            time_series_code='ws1',
            start_date='2025-01-01',
            end_date='2025-01-02',
        )

    @patch('ofs_skill.obs_retrieval.retrieve_chs_station.fetch_chs_station')
    def test_no_data_returns_none(self, mock_fetch, logger):
        """Should return None when no data available."""
        empty_df = pd.DataFrame(columns=[
            'eventDate', 'qcFlagCode', 'value', 'timeSeriesId', 'reviewed'])
        mock_fetch.return_value = empty_df

        result = retrieve_chs_station(
            '20250101', '20250102', 'test_st', 'water_temperature', logger)

        assert result is None


class TestRetrieveCurrents:
    """Test current speed/direction retrieval and merge."""

    @patch('ofs_skill.obs_retrieval.retrieve_chs_station.fetch_chs_station')
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

    @patch('ofs_skill.obs_retrieval.retrieve_chs_station.fetch_chs_station')
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

    @patch('ofs_skill.obs_retrieval.retrieve_chs_station.fetch_chs_station')
    def test_currents_no_speed_returns_none(self, mock_fetch, logger):
        """If speed data is missing, should return None."""
        empty_df = pd.DataFrame(columns=[
            'eventDate', 'qcFlagCode', 'value', 'timeSeriesId', 'reviewed'])
        dir_df = _make_chs_api_response([90.0, 135.0])

        # wcs1 empty, wcs2 empty, then wcd1 would succeed but speed is None
        mock_fetch.side_effect = [empty_df, empty_df, dir_df]

        result = retrieve_chs_station(
            '20250101', '20250102', 'test_st', 'currents', logger)

        assert result is None

    @patch('ofs_skill.obs_retrieval.retrieve_chs_station.fetch_chs_station')
    def test_currents_api_codes(self, mock_fetch, logger):
        """Should call wcs1 for speed and wcd1 for direction."""
        speed_df = _make_chs_api_response([1.0])
        dir_df = _make_chs_api_response([90.0])

        mock_fetch.side_effect = [speed_df, dir_df]

        retrieve_chs_station(
            '20250101', '20250102', 'test_st', 'currents', logger)

        calls = mock_fetch.call_args_list
        assert calls[0][1]['time_series_code'] == 'wcs1'
        assert calls[1][1]['time_series_code'] == 'wcd1'


class TestDateChunking:
    """Test 7-day date chunking behavior."""

    @patch('ofs_skill.obs_retrieval.retrieve_chs_station.fetch_chs_station')
    def test_short_range_no_chunking(self, mock_fetch, logger):
        """Ranges <= 7 days should result in a single API call."""
        mock_fetch.return_value = _make_chs_api_response([1.0])

        retrieve_chs_station(
            '20250101', '20250105', 'test_st', 'water_level', logger)

        assert mock_fetch.call_count == 1

    @patch('ofs_skill.obs_retrieval.retrieve_chs_station.fetch_chs_station')
    def test_long_range_chunked(self, mock_fetch, logger):
        """Ranges > 7 days should be split into multiple API calls."""
        mock_fetch.return_value = _make_chs_api_response([1.0])

        retrieve_chs_station(
            '20250101', '20250120', 'test_st', 'water_level', logger)

        assert mock_fetch.call_count > 1


class TestUnsupportedVariable:
    """Test behavior with unsupported variable names."""

    @patch('ofs_skill.obs_retrieval.retrieve_chs_station.fetch_chs_station')
    def test_invalid_variable(self, mock_fetch, logger):
        """Unsupported variable should return None."""
        result = retrieve_chs_station(
            '20250101', '20250102', 'test_st', 'ice_concentration', logger)

        assert result is None
