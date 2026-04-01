"""
Pytest unit tests for calculate_station_distance.

Tests geographic distance calculation (Haversine) in
ofs_skill.model_processing.station_distance.
"""

import pytest

from ofs_skill.model_processing.station_distance import (
    calculate_station_distance,
    station_distance,
)


class TestCalculateStationDistance:
    """Tests for calculate_station_distance."""

    def test_identical_coordinates_returns_zero(self):
        """Identical coordinates must yield distance 0.0 km."""
        dist = calculate_station_distance(37.0, -76.0, 37.0, -76.0)
        assert dist == 0.0

    def test_one_degree_longitude_at_equator_approx_111_km(self):
        """1 degree longitude at equator (0,0) to (0,1) is ~111.19 km."""
        dist = calculate_station_distance(0.0, 0.0, 0.0, 1.0)
        assert dist == pytest.approx(111.19, rel=0.01)

    def test_distance_is_non_negative(self):
        """Distance must always be non-negative."""
        pairs = [
            (0.0, 0.0, 1.0, 1.0),
            (-90.0, 0.0, 90.0, 180.0),
            (36.9, -76.3, 39.3, -76.6),
        ]
        for lat1, lon1, lat2, lon2 in pairs:
            dist = calculate_station_distance(lat1, lon1, lat2, lon2)
            assert dist >= 0.0, f'distance({lat1},{lon1},{lat2},{lon2}) = {dist}'

    def test_symmetry_distance_a_b_equals_distance_b_a(self):
        """distance(A, B) must equal distance(B, A)."""
        a = (36.9, -76.3)
        b = (39.3, -76.6)
        d_ab = calculate_station_distance(a[0], a[1], b[0], b[1])
        d_ba = calculate_station_distance(b[0], b[1], a[0], a[1])
        assert d_ab == d_ba

    def test_short_distance_grid_node_matching(self):
        """Short distance (~1 km) typical of station-to-grid-node matching."""
        dist = calculate_station_distance(37.0, -76.0, 37.01, -76.01)
        assert dist == pytest.approx(1.423, rel=0.01)

    def test_antipodal_distance_pole_to_pole(self):
        """North Pole to South Pole is half Earth circumference (~20015 km)."""
        dist = calculate_station_distance(90.0, 0.0, -90.0, 0.0)
        assert dist == pytest.approx(20015.09, rel=0.01)

    def test_antimeridian_crossing(self):
        """Crossing the antimeridian (179.5 to -179.5) at 60N is ~55.6 km."""
        dist = calculate_station_distance(60.0, 179.5, 60.0, -179.5)
        assert dist == pytest.approx(55.60, rel=0.01)

    def test_lat_lon_ordering_matters(self):
        """Swapping lat2/lon2 values must produce a different distance."""
        d1 = calculate_station_distance(10.0, 20.0, 40.0, 60.0)
        d2 = calculate_station_distance(10.0, 20.0, 60.0, 40.0)
        assert d1 != d2


class TestLegacyAlias:
    """Tests for deprecated station_distance alias."""

    def test_legacy_alias_matches_primary(self):
        """station_distance() must return same result as calculate_station_distance()."""
        args = (36.9, -76.3, 39.3, -76.6)
        assert station_distance(*args) == calculate_station_distance(*args)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
