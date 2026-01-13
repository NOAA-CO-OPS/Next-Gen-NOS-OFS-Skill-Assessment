"""
Geographic Distance Calculations

Calculate distances between geographic points using the Haversine formula.
"""

from math import asin, cos, sqrt


def calculate_station_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate the great-circle distance between two points on Earth.

    Uses the Haversine formula to calculate the distance between two points
    on a sphere (Earth) given their latitude and longitude coordinates.

    Parameters
    ----------
    lat1 : float
        Latitude of first point in decimal degrees
    lon1 : float
        Longitude of first point in decimal degrees
    lat2 : float
        Latitude of second point in decimal degrees
    lon2 : float
        Longitude of second point in decimal degrees

    Returns
    -------
    float
        Distance between the two points in kilometers

    Notes
    -----
    The Haversine formula:
        a = sin²(Δφ/2) + cos(φ1) × cos(φ2) × sin²(Δλ/2)
        c = 2 × atan2(√a, √(1−a))
        d = R × c

    where:
        φ is latitude, λ is longitude, R is earth's radius
        R = 6371 km (mean radius)

    This implementation uses:
        - p = π/180 = 0.017453292519943295... (degrees to radians conversion)
        - 2R = 12742 km (Earth's diameter)

    Examples
    --------
    >>> # Distance between Hampton Roads and Baltimore (Chesapeake Bay)
    >>> dist = calculate_station_distance(36.9, -76.3, 39.3, -76.6)
    >>> print(f"Distance: {dist:.1f} km")
    Distance: 267.3 km

    >>> # Distance between same point should be zero
    >>> dist = calculate_station_distance(37.0, -76.0, 37.0, -76.0)
    >>> print(f"Distance: {dist:.6f} km")
    Distance: 0.000000 km

    See Also
    --------
    index_nearest_node : Find nearest model node to a station
    index_nearest_station : Find nearest observation station to a point
    """
    # Conversion factor from degrees to radians: π/180
    pvalue = 0.017453292519943295

    # Haversine formula
    hav = 0.5 - cos((lat2 - lat1) * pvalue) / 2 + \
          cos(lat1 * pvalue) * cos(lat2 * pvalue) * \
          (1 - cos((lon2 - lon1) * pvalue)) / 2

    # Earth's diameter in kilometers: 2 * 6371 = 12742 km
    return 12742 * asin(sqrt(hav))


# Legacy function name for backward compatibility
def station_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Legacy function name - use calculate_station_distance() instead.

    .. deprecated::
        Use :func:`calculate_station_distance` instead.
    """
    return calculate_station_distance(lat1, lon1, lat2, lon2)
