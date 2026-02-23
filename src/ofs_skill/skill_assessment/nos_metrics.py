"""
NOS Standard Suite metrics — single source of truth.

Pure functions with no DataFrame coupling, no rounding, no logger, no side effects.
All metric computations used across the skill assessment package are defined here.

Metrics
-------
rmse : Root mean squared error
pearson_r : Pearson correlation coefficient
mean_bias : Mean of error array
standard_deviation : Standard deviation of error array (ddof=0)
central_frequency : Percentage of errors within +/- threshold (<=, NOS convention)
positive_outlier_freq : Percentage of errors >= 2*threshold
negative_outlier_freq : Percentage of errors <= -2*threshold
max_duration_positive_outliers : Longest consecutive run of positive outliers
max_duration_negative_outliers : Longest consecutive run of negative outliers
check_nos_criteria : Evaluate CF/POF/NOF against NOS pass/fail thresholds
get_error_threshold : Read error-range thresholds from CSV or built-in defaults
"""

import csv
import os

import numpy as np
from scipy.stats import pearsonr

# Built-in default thresholds: variable -> (X1, X2)
_DEFAULT_THRESHOLDS = {
    'wl': (0.15, 0.5),
    'salt': (3.5, 0.5),
    'temp': (3.0, 0.5),
    'cu': (0.26, 0.5),
    'ice_conc': (10.0, 0.5),
}


def rmse(predicted, observed):
    """Root mean squared error (NaN-safe).

    Parameters
    ----------
    predicted : array-like
        Model predictions.
    observed : array-like
        Observations.

    Returns
    -------
    float
    """
    return float(np.sqrt(np.nanmean((np.asarray(predicted) - np.asarray(observed))**2)))


def pearson_r(predicted, observed):
    """Pearson correlation coefficient.

    Parameters
    ----------
    predicted : array-like
        Model predictions.
    observed : array-like
        Observations.

    Returns
    -------
    float
        Correlation coefficient, or NaN if undefined.
    """
    r, _ = pearsonr(observed, predicted)
    return float(r)


def mean_bias(errors):
    """Mean of an error array, ignoring NaNs.

    Parameters
    ----------
    errors : array-like

    Returns
    -------
    float
    """
    return float(np.nanmean(errors))


def standard_deviation(errors):
    """Standard deviation of an error array (ddof=0), ignoring NaNs.

    Parameters
    ----------
    errors : array-like

    Returns
    -------
    float
    """
    return float(np.nanstd(errors))


def central_frequency(errors, threshold):
    """Percentage of errors within [-threshold, +threshold] (inclusive, NOS convention).

    Parameters
    ----------
    errors : array-like
    threshold : float

    Returns
    -------
    float
        Percentage (0–100), or NaN if *errors* is empty.
    """
    errors = np.asarray(errors, dtype=float)
    n = np.count_nonzero(~np.isnan(errors))
    if n == 0:
        return float('nan')
    within = np.nansum((-threshold <= errors) & (errors <= threshold))
    return float(within / n * 100)


def positive_outlier_freq(errors, threshold):
    """Percentage of errors >= 2*threshold.

    Parameters
    ----------
    errors : array-like
    threshold : float

    Returns
    -------
    float
        Percentage (0–100), or NaN if *errors* is empty.
    """
    errors = np.asarray(errors, dtype=float)
    n = np.count_nonzero(~np.isnan(errors))
    if n == 0:
        return float('nan')
    count = np.nansum(errors >= 2 * threshold)
    return float(count / n * 100)


def negative_outlier_freq(errors, threshold):
    """Percentage of errors <= -2*threshold.

    Parameters
    ----------
    errors : array-like
    threshold : float

    Returns
    -------
    float
        Percentage (0–100), or NaN if *errors* is empty.
    """
    errors = np.asarray(errors, dtype=float)
    n = np.count_nonzero(~np.isnan(errors))
    if n == 0:
        return float('nan')
    count = np.nansum(errors <= -2 * threshold)
    return float(count / n * 100)


def max_duration_positive_outliers(errors, threshold):
    """Longest consecutive run of positive outliers (errors >= 2*threshold).

    NaN values break the streak.

    Parameters
    ----------
    errors : array-like
    threshold : float

    Returns
    -------
    int
    """
    errors = np.asarray(errors, dtype=float)
    limit = 2 * threshold
    max_run = 0
    current = 0
    for val in errors:
        if np.isnan(val):
            max_run = max(max_run, current)
            current = 0
        elif val >= limit:
            current += 1
        else:
            max_run = max(max_run, current)
            current = 0
    return max(max_run, current)


def max_duration_negative_outliers(errors, threshold):
    """Longest consecutive run of negative outliers (errors <= -2*threshold).

    NaN values break the streak.

    Parameters
    ----------
    errors : array-like
    threshold : float

    Returns
    -------
    int
    """
    errors = np.asarray(errors, dtype=float)
    limit = -2 * threshold
    max_run = 0
    current = 0
    for val in errors:
        if np.isnan(val):
            max_run = max(max_run, current)
            current = 0
        elif val <= limit:
            current += 1
        else:
            max_run = max(max_run, current)
            current = 0
    return max(max_run, current)


def check_nos_criteria(cf, pof, nof):
    """Evaluate CF/POF/NOF against NOS Standard Suite thresholds.

    Parameters
    ----------
    cf : float
        Central frequency (%).
    pof : float
        Positive outlier frequency (%).
    nof : float
        Negative outlier frequency (%).

    Returns
    -------
    dict
        ``{'cf': 'pass'/'fail', 'pof': 'pass'/'fail', 'nof': 'pass'/'fail'}``
    """
    return {
        'cf': 'pass' if cf >= 90 else 'fail',
        'pof': 'pass' if pof <= 1 else 'fail',
        'nof': 'pass' if nof <= 1 else 'fail',
    }


def get_error_threshold(variable_name, config_path=None):
    """Return (X1, X2) error-range thresholds for *variable_name*.

    If *config_path* is given and the file exists, thresholds are read from
    the CSV (expected columns: ``name_var,X1,X2``).  Otherwise the built-in
    defaults are used.

    Parameters
    ----------
    variable_name : str
        One of ``'wl'``, ``'salt'``, ``'temp'``, ``'cu'``, ``'ice_conc'``.
    config_path : str or None
        Path to ``error_ranges.csv``.

    Returns
    -------
    tuple[float, float]
        ``(X1, X2)``

    Raises
    ------
    KeyError
        If *variable_name* is not found in defaults or the CSV.
    """
    if config_path and os.path.isfile(config_path):
        with open(config_path, newline='') as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                if row['name_var'] == variable_name:
                    return float(row['X1']), float(row['X2'])
        # Variable not found in CSV — fall through to defaults
    if variable_name not in _DEFAULT_THRESHOLDS:
        raise KeyError(
            f"Unknown variable '{variable_name}'. "
            f"Known variables: {sorted(_DEFAULT_THRESHOLDS)}"
        )
    return _DEFAULT_THRESHOLDS[variable_name]
