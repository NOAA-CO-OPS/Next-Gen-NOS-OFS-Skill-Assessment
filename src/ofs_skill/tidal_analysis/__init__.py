"""
Tidal Analysis Subpackage

Provides functionality for:
- Harmonic analysis of water level and current time series
- Tidal prediction from harmonic constants
- Preprocessing (gap filling, equal-interval resampling)
- NOS standard 37 tidal constituent definitions
- Signal filtering (low-pass, non-tidal residual)
- Extrema extraction (high/low water, max flood/ebb, slack water)
- Current analysis (principal direction, tidal ellipses)
- Persistence forecasting
- Harmonic constant comparison
"""

from ofs_skill.tidal_analysis.constituent_table import (
    build_constituent_table,
    write_constituent_table_csv,
)
from ofs_skill.tidal_analysis.constituents import (
    CONSTITUENT_SPEEDS,
    COOPS_API_NAME_MAP,
    NOS_37_CONSTITUENTS,
    normalize_constituent_name,
)
from ofs_skill.tidal_analysis.current_analysis import (
    compute_principal_direction,
    current_harmonic_analysis,
)
from ofs_skill.tidal_analysis.extremes import (
    extract_current_extrema,
    extract_water_level_extrema,
    find_slack_water,
)
from ofs_skill.tidal_analysis.filtering import (
    butterworth_lowpass,
    compute_nontidal_residual,
    fourier_lowpass_filter,
)
from ofs_skill.tidal_analysis.ha_comparison import compare_harmonic_constants
from ofs_skill.tidal_analysis.harmonic_analysis import harmonic_analysis
from ofs_skill.tidal_analysis.persistence import build_persistence_forecast
from ofs_skill.tidal_analysis.preprocessing import to_equal_interval
from ofs_skill.tidal_analysis.tidal_prediction import (
    predict_from_constants,
    predict_tide,
)

__all__ = [
    # Constituent definitions
    'NOS_37_CONSTITUENTS',
    'CONSTITUENT_SPEEDS',
    'COOPS_API_NAME_MAP',
    'normalize_constituent_name',
    # Preprocessing
    'to_equal_interval',
    # Harmonic analysis
    'harmonic_analysis',
    # Tidal prediction
    'predict_tide',
    'predict_from_constants',
    # Filtering
    'fourier_lowpass_filter',
    'butterworth_lowpass',
    'compute_nontidal_residual',
    # Extrema extraction
    'extract_water_level_extrema',
    'extract_current_extrema',
    'find_slack_water',
    # Current analysis
    'compute_principal_direction',
    'current_harmonic_analysis',
    # Persistence forecast
    'build_persistence_forecast',
    # Harmonic constant comparison
    'compare_harmonic_constants',
    # Constituent statistics table
    'build_constituent_table',
    'write_constituent_table_csv',
]
