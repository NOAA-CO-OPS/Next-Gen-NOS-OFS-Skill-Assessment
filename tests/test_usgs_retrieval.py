"""
Test suite for USGS observation retrieval.

This module tests the USGS data retrieval functionality including:
- Station data retrieval for different variables
- Inventory retrieval with variable availability tracking
- Unit conversions (feet to meters, Fahrenheit to Celsius)
- Error handling for invalid stations
"""

import logging

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
        assert 'ID' in result.columns
        assert 'X' in result.columns
        assert 'Y' in result.columns
        assert 'Source' in result.columns
        assert 'Name' in result.columns

        # Check for variable availability columns
        assert 'has_wl' in result.columns
        assert 'has_temp' in result.columns
        assert 'has_salt' in result.columns
        assert 'has_cu' in result.columns

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

    def test_inventory_has_variable_columns(self, logger):
        """Test that inventory DataFrame has variable availability columns."""
        # Create empty result to check column structure
        from ofs_skill.obs_retrieval.inventory_usgs_station import inventory_usgs_station

        # Use a tiny region that's unlikely to have stations
        argu_list = [0.0, 0.01, 0.0, 0.01]

        result = inventory_usgs_station(
            argu_list,
            start_date='20240101',
            end_date='20240102',
            logger=logger
        )

        expected_columns = [
            'ID', 'X', 'Y', 'Source', 'Name',
            'has_wl', 'has_temp', 'has_salt', 'has_cu'
        ]
        for col in expected_columns:
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


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-m', 'not network'])
