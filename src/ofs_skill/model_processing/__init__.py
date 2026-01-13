"""
Model Processing Subpackage

Provides functionality for:
- OFS model data extraction and manipulation
- Node and depth indexing
- Control file management
- Datum conversions
- Forecast cycle handling
- Model file validation
- File listing and discovery
- Model data intake and lazy loading
"""

# Core classes (most commonly used)
# Model file validation
from ofs_skill.model_processing.check_model_files import check_model_files

# Datum conversions
from ofs_skill.model_processing.get_datum_offset import (
    get_datum_offset,
    is_number,
    read_vdatum_from_bucket,
    report_datums,
    roms_nodes,
)

# Forecast cycle management
from ofs_skill.model_processing.get_fcst_cycle import get_fcst_cycle
from ofs_skill.model_processing.get_forecast_hours import get_forecast_hours

# Model node extraction
from ofs_skill.model_processing.get_node_ofs import get_node_ofs

# Spatial indexing
from ofs_skill.model_processing.indexing import (
    index_nearest_depth,
    index_nearest_node,
    index_nearest_station,
)

# Model data intake
from ofs_skill.model_processing.intake_scisa import (
    calc_sigma,
    fix_fvcom,
    fix_roms_uv,
    get_station_dim,
    intake_model,
    remove_extra_stations,
)

# File listing and discovery
from ofs_skill.model_processing.list_of_files import (
    construct_expected_files,
    construct_s3_url,
    dates_range,
    list_of_dir,
    list_of_files,
)
from ofs_skill.model_processing.model_format_properties import ModelFormatProperties
from ofs_skill.model_processing.model_properties import ModelProperties

# Model source detection
from ofs_skill.model_processing.model_source import get_model_source

# Control file operations
from ofs_skill.model_processing.parse_ofs_ctlfile import parse_ofs_ctlfile

# Distance calculations
from ofs_skill.model_processing.station_distance import calculate_station_distance
from ofs_skill.model_processing.write_ofs_ctlfile import (
    user_input_extract,
    write_ofs_ctlfile,
)

__all__ = [
    # Core classes
    'ModelProperties',
    'ModelFormatProperties',
    # Model source
    'get_model_source',
    # Control files
    'parse_ofs_ctlfile',
    'write_ofs_ctlfile',
    'user_input_extract',
    # Forecast cycle
    'get_fcst_cycle',
    'get_forecast_hours',
    # Model node extraction
    'get_node_ofs',
    # Spatial indexing
    'index_nearest_node',
    'index_nearest_depth',
    'index_nearest_station',
    # Distance calculations
    'calculate_station_distance',
    # File listing
    'construct_s3_url',
    'dates_range',
    'construct_expected_files',
    'list_of_dir',
    'list_of_files',
    # File validation
    'check_model_files',
    # Datum conversions
    'get_datum_offset',
    'read_vdatum_from_bucket',
    'report_datums',
    'roms_nodes',
    'is_number',
    # Model intake
    'intake_model',
    'fix_roms_uv',
    'fix_fvcom',
    'calc_sigma',
    'get_station_dim',
    'remove_extra_stations',
]
