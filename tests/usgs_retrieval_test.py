"""
Test suite for USGS observation retrieval.

This module tests the USGS data retrieval functionality including:
- Station data retrieval for different variables
- Inventory retrieval with variable availability tracking
- Unit conversions (feet to meters, Fahrenheit to Celsius)
- Error handling for invalid stations
"""

import logging
from unittest.mock import patch

import pandas as pd
import pytest

from ofs_skill.obs_retrieval import (
    inventory_usgs_station,
    retrieve_usgs_station,
)


# Setup test logger
@pytest.fixture
def logger():
    """Create a logger for tests."""
    logging.basicConfig(level=logging.INFO)
    return logging.getLogger('test_usgs')


class MockRetrieveInput:
    """Mock input object for retrieve_usgs_station."""

    def __init__(
        self,
        station: str,
        start_date: str,
        end_date: str,
        variable: str = 'water_level'
    ):
        self.station = station
        self.start_date = start_date
        self.end_date = end_date
        self.variable = variable


class TestUSGSImports:
    """Test that USGS modules can be imported correctly."""

    def test_retrieve_usgs_station_import(self):
        """Test that retrieve_usgs_station can be imported."""
        from ofs_skill.obs_retrieval import retrieve_usgs_station
        assert callable(retrieve_usgs_station)

    def test_inventory_usgs_station_import(self):
        """Test that inventory_usgs_station can be imported."""
        from ofs_skill.obs_retrieval import inventory_usgs_station
        assert callable(inventory_usgs_station)

    def test_usgs_properties_import(self):
        """Test that USGSProperties can be imported."""
        from ofs_skill.obs_retrieval.usgs_properties import USGSProperties
        props = USGSProperties()
        assert hasattr(props, 'base_url')
        assert hasattr(props, 'obs_final')


class TestUSGSStationRetrieval:
    """Test USGS station data retrieval."""

    @pytest.mark.network
    def test_retrieve_water_level_potomac(self, logger):
        """Test water level retrieval from Potomac River station."""
        # USGS station 01646500 - Potomac River at Washington, DC
        retrieve_input = MockRetrieveInput(
            station='01646500',
            start_date='20240101',
            end_date='20240102',
            variable='water_level'
        )

        result = retrieve_usgs_station(retrieve_input, logger)

        # Station may or may not have data for this period
        if result is not None:
            assert isinstance(result, pd.DataFrame)
            assert 'DateTime' in result.columns
            assert 'OBS' in result.columns
            assert 'DEP01' in result.columns
            assert len(result) > 0

    @pytest.mark.network
    def test_retrieve_water_temperature(self, logger):
        """Test water temperature retrieval."""
        # USGS station with temperature data
        retrieve_input = MockRetrieveInput(
            station='01646500',
            start_date='20240701',
            end_date='20240702',
            variable='water_temperature'
        )

        result = retrieve_usgs_station(retrieve_input, logger)

        if result is not None:
            assert isinstance(result, pd.DataFrame)
            assert 'DateTime' in result.columns
            assert 'OBS' in result.columns
            # Temperature should be in Celsius (reasonable range)
            if len(result) > 0:
                assert result['OBS'].min() > -10  # Not frozen
                assert result['OBS'].max() < 50   # Not boiling

    @pytest.mark.network
    def test_retrieve_nonexistent_station(self, logger):
        """Test retrieval from a non-existent station returns None."""
        retrieve_input = MockRetrieveInput(
            station='99999999',  # Non-existent station
            start_date='20240101',
            end_date='20240102',
            variable='water_level'
        )

        result = retrieve_usgs_station(retrieve_input, logger)
        assert result is None

    @pytest.mark.network
    def test_retrieve_invalid_variable(self, logger):
        """Test retrieval with invalid variable returns None."""
        retrieve_input = MockRetrieveInput(
            station='01646500',
            start_date='20240101',
            end_date='20240102',
            variable='invalid_variable'
        )

        result = retrieve_usgs_station(retrieve_input, logger)
        assert result is None


class TestUSGSInventory:
    """Test USGS station inventory retrieval."""

    @pytest.mark.network
    def test_inventory_chesapeake_region(self, logger):
        """Test inventory retrieval for Chesapeake Bay region."""
        # Small bounding box around Washington DC area
        argu_list = [38.8, 39.0, -77.2, -77.0]

        result = inventory_usgs_station(
            argu_list,
            start_date='20240101',
            end_date='20240102',
            logger=logger
        )

        assert isinstance(result, pd.DataFrame)
        # Core columns always present
        for col in ['ID', 'X', 'Y', 'Source', 'Name']:
            assert col in result.columns

        if len(result) > 0:
            assert all(result['Source'] == 'USGS')

    @pytest.mark.network
    def test_inventory_empty_region(self, logger):
        """Test inventory retrieval for region with no stations."""
        # Middle of the ocean - no USGS stations
        argu_list = [30.0, 30.1, -60.0, -59.9]

        result = inventory_usgs_station(
            argu_list,
            start_date='20240101',
            end_date='20240102',
            logger=logger
        )

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_inventory_has_core_columns(self, logger):
        """Test that inventory DataFrame has required core columns."""
        from ofs_skill.obs_retrieval.inventory_usgs_station import inventory_usgs_station

        # Use a tiny region that's unlikely to have stations
        argu_list = [0.0, 0.01, 0.0, 0.01]

        result = inventory_usgs_station(
            argu_list,
            start_date='20240101',
            end_date='20240102',
            logger=logger
        )

        for col in ['ID', 'X', 'Y', 'Source', 'Name']:
            assert col in result.columns, f'Missing column: {col}'


class TestUSGSDataFormat:
    """Test USGS data format and structure."""

    @pytest.mark.network
    def test_datetime_format(self, logger):
        """Test that DateTime column is properly formatted."""
        retrieve_input = MockRetrieveInput(
            station='01646500',
            start_date='20240101',
            end_date='20240102',
            variable='water_level'
        )

        result = retrieve_usgs_station(retrieve_input, logger)

        if result is not None and len(result) > 0:
            assert pd.api.types.is_datetime64_any_dtype(result['DateTime'])

    @pytest.mark.network
    def test_numeric_columns(self, logger):
        """Test that numeric columns are properly typed."""
        retrieve_input = MockRetrieveInput(
            station='01646500',
            start_date='20240101',
            end_date='20240102',
            variable='water_level'
        )

        result = retrieve_usgs_station(retrieve_input, logger)

        if result is not None and len(result) > 0:
            assert pd.api.types.is_numeric_dtype(result['OBS'])
            assert pd.api.types.is_numeric_dtype(result['DEP01'])

    @pytest.mark.network
    def test_water_level_has_datum(self, logger):
        """Test that water level data includes datum information."""
        retrieve_input = MockRetrieveInput(
            station='01646500',
            start_date='20240101',
            end_date='20240102',
            variable='water_level'
        )

        result = retrieve_usgs_station(retrieve_input, logger)

        if result is not None and len(result) > 0:
            assert 'Datum' in result.columns
            # Datum should be one of the known values
            valid_datums = {'NAVD88', 'NGVD', 'IGLD'}
            assert result['Datum'].iloc[0] in valid_datums


class TestUSGSUnitConversions:
    """Test unit conversion logic."""

    def test_feet_to_meters_conversion(self):
        """Test feet to meters conversion factor."""
        feet_value = 10.0
        meters_value = feet_value * 0.3048
        assert abs(meters_value - 3.048) < 0.001

    def test_fahrenheit_to_celsius_conversion(self):
        """Test Fahrenheit to Celsius conversion."""
        fahrenheit = 77.0  # Room temperature
        celsius = (fahrenheit - 32) * (5 / 9)
        assert abs(celsius - 25.0) < 0.001

    def test_feet_per_second_to_meters_per_second(self):
        """Test ft/s to m/s conversion for currents."""
        fps = 3.28084  # Approximately 1 m/s
        mps = fps * 0.3048
        assert abs(mps - 1.0) < 0.001


class TestMockRetrieveInput:
    """Test the MockRetrieveInput helper class."""

    def test_mock_input_attributes(self):
        """Test that MockRetrieveInput has all required attributes."""
        mock = MockRetrieveInput(
            station='12345678',
            start_date='20240101',
            end_date='20240102',
            variable='water_level'
        )

        assert mock.station == '12345678'
        assert mock.start_date == '20240101'
        assert mock.end_date == '20240102'
        assert mock.variable == 'water_level'

    def test_mock_input_default_variable(self):
        """Test that MockRetrieveInput defaults to water_level."""
        mock = MockRetrieveInput(
            station='12345678',
            start_date='20240101',
            end_date='20240102'
        )

        assert mock.variable == 'water_level'


def _make_usgs_multiindex_df(rows):
    """Build a DataFrame mimicking get_usgs_station_data output.

    Each row is a dict with keys: site_no, datetime, code, option, value.
    Returns a DataFrame with a MultiIndex of (site_no, datetime, code, option).
    """
    df = pd.DataFrame(rows)
    df['datetime'] = pd.to_datetime(df['datetime'])
    df = df.set_index(['site_no', 'datetime', 'code', 'option'])
    return df


class TestDeduplication:
    """Test that duplicate time series (multiple options) are collapsed."""

    def test_multiple_options_deduplicated(self, logger):
        """When multiple option variants exist, only the first is kept."""
        mock_data = _make_usgs_multiindex_df([
            {'site_no': '01646500', 'datetime': '2024-01-01 00:00',
             'code': '00065', 'option': '00000', 'value': 1.0},
            {'site_no': '01646500', 'datetime': '2024-01-01 00:15',
             'code': '00065', 'option': '00000', 'value': 1.1},
            {'site_no': '01646500', 'datetime': '2024-01-01 00:00',
             'code': '00065', 'option': '00001', 'value': 2.0},
            {'site_no': '01646500', 'datetime': '2024-01-01 00:15',
             'code': '00065', 'option': '00001', 'value': 2.1},
        ])

        inp = MockRetrieveInput('01646500', '20240101', '20240102', 'water_level')

        with patch(
            'ofs_skill.obs_retrieval.retrieve_usgs_station.get_usgs_station_data',
            return_value=mock_data,
        ):
            result = retrieve_usgs_station(inp, logger)

        assert result is not None
        assert len(result) == 2  # Only option '00000' kept
        assert list(result['OBS']) == pytest.approx([1.0 * 0.3048, 1.1 * 0.3048])

    def test_duplicate_timestamps_dropped(self, logger):
        """Even within a single option, duplicate timestamps are dropped."""
        mock_data = _make_usgs_multiindex_df([
            {'site_no': '01646500', 'datetime': '2024-01-01 00:00',
             'code': '00065', 'option': '00000', 'value': 1.0},
            {'site_no': '01646500', 'datetime': '2024-01-01 00:00',
             'code': '00065', 'option': '00000', 'value': 1.5},
            {'site_no': '01646500', 'datetime': '2024-01-01 00:15',
             'code': '00065', 'option': '00000', 'value': 2.0},
        ])

        inp = MockRetrieveInput('01646500', '20240101', '20240102', 'water_level')

        with patch(
            'ofs_skill.obs_retrieval.retrieve_usgs_station.get_usgs_station_data',
            return_value=mock_data,
        ):
            result = retrieve_usgs_station(inp, logger)

        assert result is not None
        assert len(result) == 2  # Duplicate timestamp collapsed
        assert result['OBS'].iloc[0] == pytest.approx(1.0 * 0.3048)

    def test_no_option_column_still_works(self, logger):
        """If option column is absent after reset_index, no crash."""
        mock_data = _make_usgs_multiindex_df([
            {'site_no': '01646500', 'datetime': '2024-01-01 00:00',
             'code': '00065', 'option': '00000', 'value': 1.0},
        ])
        # Simulate a DataFrame without 'option' after reset
        mock_data.index = mock_data.index.droplevel('option')

        inp = MockRetrieveInput('01646500', '20240101', '20240102', 'water_level')

        with patch(
            'ofs_skill.obs_retrieval.retrieve_usgs_station.get_usgs_station_data',
            return_value=mock_data,
        ):
            result = retrieve_usgs_station(inp, logger)

        assert result is not None
        assert len(result) == 1

    def test_time_series_id_dedup(self, logger):
        """Prefer time_series_id over option for dedup (new Water Data API)."""
        mock_data = _make_usgs_multiindex_df([
            {'site_no': '11162765', 'datetime': '2024-01-01 00:00',
             'code': '00095', 'option': '', 'value': 33800.0},
            {'site_no': '11162765', 'datetime': '2024-01-01 00:15',
             'code': '00095', 'option': '', 'value': 33800.0},
            {'site_no': '11162765', 'datetime': '2024-01-01 00:00',
             'code': '00095', 'option': '', 'value': 33600.0},
            {'site_no': '11162765', 'datetime': '2024-01-01 00:15',
             'code': '00095', 'option': '', 'value': 33600.0},
        ])
        # Add time_series_id column (present in new API, not in index)
        df = mock_data.reset_index()
        df['time_series_id'] = ['ts_A', 'ts_A', 'ts_B', 'ts_B']
        mock_data = df.set_index(['site_no', 'datetime', 'code', 'option'])

        inp = MockRetrieveInput('11162765', '20240101', '20240102', 'salinity')

        with patch(
            'ofs_skill.obs_retrieval.retrieve_usgs_station.get_usgs_station_data',
            return_value=mock_data,
        ):
            result = retrieve_usgs_station(inp, logger)

        assert result is not None
        assert len(result) == 2  # Only ts_A kept
        # All values from ts_A (33800 * 0.00064 = 21.632)
        assert result['OBS'].iloc[0] == pytest.approx(33800.0 * 0.00064)
        assert result['OBS'].iloc[1] == pytest.approx(33800.0 * 0.00064)


class TestSpecificConductanceConversion:
    """Test specific conductance (code 00095) to PSU conversion."""

    def test_conductance_converted_to_psu(self, logger):
        """Code 00095 values are multiplied by 0.00064."""
        mock_data = _make_usgs_multiindex_df([
            {'site_no': '294643095035200', 'datetime': '2024-01-01 00:00',
             'code': '00095', 'option': '00000', 'value': 1000.0},
            {'site_no': '294643095035200', 'datetime': '2024-01-01 00:15',
             'code': '00095', 'option': '00000', 'value': 1500.0},
        ])

        inp = MockRetrieveInput(
            '294643095035200', '20240101', '20240102', 'salinity'
        )

        with patch(
            'ofs_skill.obs_retrieval.retrieve_usgs_station.get_usgs_station_data',
            return_value=mock_data,
        ):
            result = retrieve_usgs_station(inp, logger)

        assert result is not None
        assert result['OBS'].iloc[0] == pytest.approx(1000.0 * 0.00064)
        assert result['OBS'].iloc[1] == pytest.approx(1500.0 * 0.00064)

    def test_actual_salinity_code_no_conversion(self, logger):
        """Code 00480 (actual salinity PSU) is not converted."""
        mock_data = _make_usgs_multiindex_df([
            {'site_no': '01646500', 'datetime': '2024-01-01 00:00',
             'code': '00480', 'option': '00000', 'value': 5.0},
        ])

        inp = MockRetrieveInput('01646500', '20240101', '20240102', 'salinity')

        with patch(
            'ofs_skill.obs_retrieval.retrieve_usgs_station.get_usgs_station_data',
            return_value=mock_data,
        ):
            result = retrieve_usgs_station(inp, logger)

        assert result is not None
        assert result['OBS'].iloc[0] == pytest.approx(5.0)


class TestSalinityCodePreference:
    """Test that actual salinity codes are preferred over conductance."""

    def test_preferred_code_chosen_over_conductance(self, logger):
        """When both 00480 and 00095 exist, 00480 is used."""
        mock_data = _make_usgs_multiindex_df([
            # Conductance data listed first
            {'site_no': '01646500', 'datetime': '2024-01-01 00:00',
             'code': '00095', 'option': '00000', 'value': 1000.0},
            {'site_no': '01646500', 'datetime': '2024-01-01 00:15',
             'code': '00095', 'option': '00000', 'value': 1500.0},
            # Actual salinity data listed second
            {'site_no': '01646500', 'datetime': '2024-01-01 00:00',
             'code': '00480', 'option': '00000', 'value': 5.0},
            {'site_no': '01646500', 'datetime': '2024-01-01 00:15',
             'code': '00480', 'option': '00000', 'value': 6.0},
        ])

        inp = MockRetrieveInput('01646500', '20240101', '20240102', 'salinity')

        with patch(
            'ofs_skill.obs_retrieval.retrieve_usgs_station.get_usgs_station_data',
            return_value=mock_data,
        ):
            result = retrieve_usgs_station(inp, logger)

        assert result is not None
        # Should get the raw PSU values from 00480, not the conductance
        assert result['OBS'].iloc[0] == pytest.approx(5.0)
        assert result['OBS'].iloc[1] == pytest.approx(6.0)

    def test_falls_back_to_conductance_if_no_preferred(self, logger):
        """When only 00095 is available, it is used with conversion."""
        mock_data = _make_usgs_multiindex_df([
            {'site_no': '01646500', 'datetime': '2024-01-01 00:00',
             'code': '00095', 'option': '00000', 'value': 1000.0},
        ])

        inp = MockRetrieveInput('01646500', '20240101', '20240102', 'salinity')

        with patch(
            'ofs_skill.obs_retrieval.retrieve_usgs_station.get_usgs_station_data',
            return_value=mock_data,
        ):
            result = retrieve_usgs_station(inp, logger)

        assert result is not None
        assert result['OBS'].iloc[0] == pytest.approx(0.64)


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-m', 'not network'])
