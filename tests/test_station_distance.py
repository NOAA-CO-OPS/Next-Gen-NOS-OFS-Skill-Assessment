"""
Tests for station_distance module.

Tests the calculate_station_distance function which computes great-circle
distances between geographic coordinates, this includes test cases for
identical coordinates, one degree of longitude at the equator, and distance
is non-negative.
"""

import math

from ofs_skill.model_processing.station_distance import calculate_station_distance


class TestCalculateStationDistance:
    """Test cases for calculate_station_distance function."""

    def test_identical_coordinates_returns_zero(self):
        """Distance between identical coordinates should be zero."""
        result = calculate_station_distance(37.0, -76.0, 37.0, -76.0)

        assert isinstance(result, float)
        assert result == 0.0

    def test_one_degree_longitude_at_equator(self):
        """One degree of longitude at the equator is approximately 111.19 km."""
        result = calculate_station_distance(0.0, 0.0, 0.0, 1.0)

        assert isinstance(result, float)
        assert result >= 0.0
        assert math.isclose(result, 111.19, rel_tol=0.01)

    def test_distance_is_non_negative(self):
        """Distance should always be non-negative."""
        result = calculate_station_distance(40.0, -74.0, 51.5, -0.1)

        assert isinstance(result, float)
        assert result >= 0.0
