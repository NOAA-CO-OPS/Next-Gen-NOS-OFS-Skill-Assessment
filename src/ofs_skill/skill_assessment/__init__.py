"""
Skill assessment module for OFS.

This module provides functions for calculating skill assessment metrics,
creating paired time series, and generating skill assessment maps and reports.

Main Functions
--------------
get_skill : Main skill assessment coordinator
format_paired_one_d : Format paired model-observation data for 1D
metrics_paired_one_d : Calculate 1D skill metrics (RMSE, bias, correlation, etc.)
metrics_two_d : Calculate 2D skill metrics
make_skill_maps : Create skill assessment maps
nos_metrics : NOS Standard Suite metrics (single source of truth)
"""

from ofs_skill.skill_assessment.format_paired_one_d import (
    get_distance_angle,
    paired_scalar,
    paired_vector,
)
from ofs_skill.skill_assessment.get_skill import (
    get_skill,
    name_convent,
    ofs_ctlfile_extract,
    prepare_series,
    skill,
)
from ofs_skill.skill_assessment.make_skill_maps import (
    make_skill_maps,
)
from ofs_skill.skill_assessment.metrics_paired_one_d import (
    skill_scalar,
    skill_vector,
)
from ofs_skill.skill_assessment.metrics_two_d import (
    return_one_d,
    return_two_d,
)
from ofs_skill.skill_assessment.nos_metrics import (
    central_frequency,
    check_nos_criteria,
    get_error_threshold,
    max_duration_negative_outliers,
    max_duration_positive_outliers,
    mean_bias,
    negative_outlier_freq,
    pearson_r,
    positive_outlier_freq,
    rmse,
    standard_deviation,
)

__all__ = [
    # Main skill assessment
    'get_skill',
    'skill',
    'ofs_ctlfile_extract',
    'prepare_series',
    'name_convent',
    # Formatting functions
    'paired_scalar',
    'paired_vector',
    'get_distance_angle',
    # 1D metrics
    'skill_scalar',
    'skill_vector',
    # 2D metrics
    'return_one_d',
    'return_two_d',
    # Mapping
    'make_skill_maps',
    # NOS Standard Suite metrics
    'rmse',
    'pearson_r',
    'mean_bias',
    'standard_deviation',
    'central_frequency',
    'positive_outlier_freq',
    'negative_outlier_freq',
    'max_duration_positive_outliers',
    'max_duration_negative_outliers',
    'check_nos_criteria',
    'get_error_threshold',
]
