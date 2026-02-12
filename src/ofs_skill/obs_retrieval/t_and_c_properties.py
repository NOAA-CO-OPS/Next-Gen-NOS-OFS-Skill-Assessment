"""
Properties for CO-OPS Tides and Currents observation retrieval.

This module defines the properties class used by retrieve_t_and_c_station.py
for managing CO-OPS API interactions and data retrieval.
"""

from datetime import datetime, timedelta
from typing import Optional


class TidesandCurrentsProperties:
    """
    Properties for CO-OPS Tides and Currents API interactions.

    This class stores configuration and state information for retrieving
    tidal and current observations from NOAA CO-OPS API.

    Attributes:
        mdapi_url: CO-OPS metadata API base URL
        api_url: CO-OPS data API base URL
        start_dt_0: Original start datetime (for masking)
        end_dt_0: Original end datetime (for masking)
        start_dt: Current start datetime (for chunked retrieval)
        end_dt: Current end datetime (for chunked retrieval)
        total_date: Accumulated datetime values
        total_var: Accumulated variable values
        total_dir: Accumulated direction values (for currents/wind)
        station_url: Primary API URL for station data
        station_url_2: Backup API URL for station data
        depth: Observation depth in meters
        depth_url: URL for depth metadata
        delta: Time delta for chunked retrieval (typically 30 days)
        date: Temporary datetime list for current chunk
        var: Temporary variable list for current chunk
        drt: Temporary direction list for current chunk
    """

    def __init__(self):
        self.mdapi_url: str = ''
        self.api_url: str = ''
        self.start_dt_0: Optional[datetime] = None
        self.end_dt_0: Optional[datetime] = None
        self.start_dt: Optional[datetime] = None
        self.end_dt: Optional[datetime] = None
        self.total_date: list[str] = []
        self.total_var: list[str] = []
        self.total_dir: list[str] = []
        self.station_url: str = ''
        self.station_url_2: str = ''
        self.depth: float = 0.0
        self.depth_url: Optional[dict] = None
        self.delta: Optional[timedelta] = None
        self.date: list[str] = []
        self.var: list[str] = []
        self.drt: list[str] = []
