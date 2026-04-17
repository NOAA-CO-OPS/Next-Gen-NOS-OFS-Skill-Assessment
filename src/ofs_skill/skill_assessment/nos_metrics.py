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
    'cu_dir': (22.5, 0.5),
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

def timing_central_frequency(timing_errors, threshold=0.5):
    '''
    Percentage of timing errors within [-threshold, +threshold] (inclusive).

    Parameters -->

    timing_errors : array-like
        Differences between model and observation extrema times (hours).
    threshold : float
        Timing error threshold (default 0.5 hours for NOS).

    Returns --> float percentage

    '''

    errors = np.asarray(timing_errors, dtype=float)
    n = np.count_nonzero(~np.isnan(errors))
    if n == 0:
        return float('nan')
    within = np.nansum((-threshold <= errors) & (errors <= threshold))
    return float(within / n * 100)

def worst_case_outlier_frequency(ofs, obs, tides, threshold):
    """
    Calculates the percentage of worst-case outliers between model and observation data.

    A "worst-case outlier" is defined as a data point where both of the following conditions are met:
    1. The model (`ofs`) and observation (`obs`) fall on opposite sides of the tidal baseline (`tides`).
    2. The absolute difference between the forecast and observation is strictly greater than twice the specified error threshold.

    Args:
        ofs (numpy.ndarray): Array of forecast values (e.g., Ocean Forecast System data).
        obs (numpy.ndarray): Array of observational data values. Must be the same length as `ofs`.
        tides (numpy.ndarray): Array of tidal baseline values. Must be the same length as `ofs` and `obs`.
        threshold (float or int): The baseline error threshold. The calculation checks for differences greater than 2x this value.

    Returns:
        float or None: The percentage (0.0 to 100.0) of valid (non-NaN) data points that are classified as worst-case outliers. Returns `None` if a `ValueError` is raised (e.g., due to array shape mismatches).
    """

    # ofs, obs, tides are numpy arrays of same length and
    # nsum counts where the sign of (ofs-tides) is different from (obs-tides)
    # AND the absolute difference between ofs and obs is greater than 2x error threshold
    n = np.count_nonzero(~np.isnan(ofs-obs))
    if n == 0:
        return None
    mask = np.abs(ofs - obs) > threshold*2
    try:
        crossings = ((ofs > tides) & (obs < tides)) | ((ofs < tides) & (obs > tides))
    except ValueError:
        return None
    # Combine conditions and count occurrences
    nsum = np.sum(mask & crossings)
    # Calculate the percentage and return
    return (100.0 * nsum / n)


def check_nos_criteria(cf, pof, nof, mdpo, mdno, wof, tcf=None):
    """Evaluate skill metrics against NOS Standard Suite pass/fail thresholds.

    This function checks if the calculated metrics meet the official National Ocean Service
    (NOS) criteria for acceptance.

    Parameters
    ----------
    cf : float
        Central frequency (%). Accepted if >= 90%.
    pof : float
        Positive outlier frequency (%). Accepted if <= 1%.
    nof : float
        Negative outlier frequency (%). Accepted if <= 1%.
    mdpo : float
        Maximum duration of positive outliers (hours). Accepted if <= 24.
    mdno : float
        Maximum duration of negative outliers (hours). Accepted if <= 24.
    wof : float or None
        Worst-case outlier frequency (%). Accepted if <= 0.5%.
    tcf : float, optional
        Timing central frequency (%) for water level extrema. Accepted if >= 90%.

    Returns
    -------
    dict
        A dictionary containing 'pass', 'fail', or 'NA' status for each metric:
        ``{'cf', 'pof', 'nof', 'mdpo', 'mdno', 'wof', 'tcf'}``.
    """
    if wof is None:
        wof = 9999

    results = {
        'cf': 'pass' if cf >= 90 else 'fail',
        'pof': 'pass' if pof <= 1 else 'fail',
        'nof': 'pass' if nof <= 1 else 'fail',
        'mdpo': 'pass' if mdpo <= 24 else 'fail',
        'mdno': 'pass' if mdno <= 24 else 'fail',
        'wof': 'pass' if wof <= 0.5 else 'NA' if wof == 9999 else 'fail',
    }

    if tcf is not None:
        results['tcf'] = 'pass' if tcf >= 90 else 'fail'

    return results



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
        # Variable not found in CSV fall through to defaults
    if variable_name not in _DEFAULT_THRESHOLDS:
        raise KeyError(
            f"Unknown variable '{variable_name}'. "
            f'Known variables: {sorted(_DEFAULT_THRESHOLDS)}'
        )
    return _DEFAULT_THRESHOLDS[variable_name]
