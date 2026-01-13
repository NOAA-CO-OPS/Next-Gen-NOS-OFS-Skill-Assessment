"""
Properties for USGS observation retrieval.

This module defines the properties class used by retrieve_usgs_station.py
for managing USGS API interactions and data retrieval.
"""

from typing import Optional

import pandas as pd


class USGSProperties:
    """
    Properties for USGS stream gauge API interactions.

    This class stores configuration and state information for retrieving
    stream gauge observations from USGS Water Services API.

    Attributes:
        base_url: USGS API base URL
        url: Complete API URL with parameters
        obs_final: Final DataFrame with observations
        start: Start date string
        end: End date string
        start_year: Start year component
        start_month: Start month component
        start_day: Start day component
        end_year: End year component
        end_month: End month component
        end_day: End day component
        start_str: Formatted start date string
        end_str: Formatted end date string
    """

    def __init__(self):
        self.base_url: str = ''
        self.url: str = ''
        self.obs_final: Optional[pd.DataFrame] = None
        self.start: str = ''
        self.end: str = ''
        self.start_year: str = ''
        self.start_month: str = ''
        self.start_day: str = ''
        self.end_year: str = ''
        self.end_month: str = ''
        self.end_day: str = ''
        self.start_str: str = ''
        self.end_str: str = ''
