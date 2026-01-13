"""
Observation Retrieval Subpackage

Provides functionality for:
- Retrieving observation data from multiple sources
  - NOAA Tides & Currents (CO-OPS)
  - NDBC buoys
  - USGS stream gauges
- Station inventory management
- Data formatting and standardization
- Geometric operations
- Quality control
"""

# Utility functions
# Inventory filtering and geometry
from ofs_skill.obs_retrieval.filter_inventory import filter_inventory

# Data formatting
from ofs_skill.obs_retrieval.format_obs_timeseries import scalar, vector

# Main observation retrieval and control file functions
from ofs_skill.obs_retrieval.get_station_observations import get_station_observations
from ofs_skill.obs_retrieval.inventory_ndbc_station import inventory_ndbc_station

# Inventory functions
from ofs_skill.obs_retrieval.inventory_t_c_station import inventory_t_c_station
from ofs_skill.obs_retrieval.inventory_usgs_station import inventory_usgs_station
from ofs_skill.obs_retrieval.ofs_geometry import ofs_geometry
from ofs_skill.obs_retrieval.ofs_inventory_stations import ofs_inventory_stations
from ofs_skill.obs_retrieval.retrieve_ndbc_station import retrieve_ndbc_station

# Property classes
from ofs_skill.obs_retrieval.retrieve_properties import RetrieveProperties

# Data retrieval functions
from ofs_skill.obs_retrieval.retrieve_t_and_c_station import (
    find_nearest_tidal_station,
    find_nearest_tidal_stations,
    get_HTTP_error,
    retrieve_t_and_c_station,
    retrieve_tidal_predictions,
)
from ofs_skill.obs_retrieval.retrieve_usgs_station import retrieve_usgs_station

# Control file operations
from ofs_skill.obs_retrieval.station_ctl_file_extract import station_ctl_file_extract
from ofs_skill.obs_retrieval.t_and_c_properties import TidesandCurrentsProperties
from ofs_skill.obs_retrieval.usgs_properties import USGSProperties
from ofs_skill.obs_retrieval.utils import Utils, parse_arguments_to_list
from ofs_skill.obs_retrieval.write_obs_ctlfile import write_obs_ctlfile

__all__ = [
    # Utilities
    'Utils',
    'parse_arguments_to_list',
    'station_ctl_file_extract',
    'scalar',
    'vector',
    # Properties
    'RetrieveProperties',
    'TidesandCurrentsProperties',
    'USGSProperties',
    # Data retrieval
    'retrieve_t_and_c_station',
    'get_HTTP_error',
    'retrieve_tidal_predictions',
    'find_nearest_tidal_stations',
    'find_nearest_tidal_station',
    'retrieve_ndbc_station',
    'retrieve_usgs_station',
    'get_station_observations',
    'write_obs_ctlfile',
    # Inventory
    'inventory_t_c_station',
    'inventory_ndbc_station',
    'inventory_usgs_station',
    'ofs_inventory_stations',
    'filter_inventory',
    'ofs_geometry',
]
