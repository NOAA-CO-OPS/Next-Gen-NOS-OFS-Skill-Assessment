"""
Calculate two-dimensional skill metrics.

This module provides functions to calculate skill statistics for 2D arrays,
including RMSE, correlation, standard deviation, and frequency metrics.

Created on Fri Jul 26 10:10:30 2024

@author: PL

Notes
-----
This is set up to be within a for-loop that cycles through time, and calls this
function each loop iteration/time step.

Incoming data arrays should be numpy but this can be relaxed/changed.

Functions
---------
return_one_d : Calculate 1D statistics from 2D arrays
return_two_d : Calculate 2D statistics from 3D arrays
"""

import logging
import sys
from logging import Logger
from typing import Union

import numpy as np
from numpy import isnan

from ofs_skill.skill_assessment import nos_metrics

# Capture warnings
logging.captureWarnings(True)


def return_one_d(
    obs_data: np.ndarray,
    mod_data: np.ndarray,
    logger: Logger,
    errorrange: float = 3,
) -> list[Union[float, np.floating]]:
    """
    Return single statistics from 2D arrays.

    Calculates statistics like RMSE and correlation from 2D spatial arrays
    at a single time step.

    Parameters
    ----------
    obs_data : np.ndarray
        2D array of observed data
    mod_data : np.ndarray
        2D array of modeled data (must match obs_data shape)
    logger : Logger
        Logger instance for logging messages
    errorrange : float, optional
        Error threshold for CF/POF/NOF calculations (default 3)

    Returns
    -------
    List[Union[float, np.floating]]
        List of statistics:
        [obs_mean, obs_std, mod_mean, mod_std, modobs_bias, modobs_bias_std,
         r_value, rmse, cf, pof, nof]
        Where:
        - obs_mean: Mean of observations
        - obs_std: Standard deviation of observations
        - mod_mean: Mean of model output
        - mod_std: Standard deviation of model output
        - modobs_bias: Mean bias (model - observations)
        - modobs_bias_std: Standard deviation of bias
        - r_value: Pearson correlation coefficient
        - rmse: Root mean squared error
        - cf: Central frequency (percentage within +/- errorrange threshold)
        - pof: Positive outlier frequency
        - nof: Negative outlier frequency

    Raises
    ------
    SystemExit
        If array shapes do not match
    """
    # Check array shapes -- must be the same OR ELSE FAIL AHHHHH
    if obs_data.shape != mod_data.shape:
        logger.error('Arrays in 2D stats module are not the same size! Try Again :(')
        sys.exit(-1)

    logger.info('Starting 2D stats calcs for 1D stats time series!')

    # Threshold for RMSE and R -- need enough data to calculate a robust stat.
    # It is kinda arbitrary for now, adjust as necessary
    threshold = 5

    # Mean & standard deviation of observations and model output
    obs_mean = np.nanmean(obs_data)
    obs_std = np.nanstd(obs_data)
    mod_mean = np.nanmean(mod_data)
    mod_std = np.nanstd(mod_data)
    modobs_bias = np.nanmean(mod_data-obs_data)
    modobs_bias_std = np.nanstd(mod_data-obs_data)

    # Pearson's R where model and observations have sufficient number of values
    if np.nansum(~isnan(mod_data)) > threshold and np.nansum(~isnan(obs_data)) >\
        threshold:
        # Flatten arrays
        obs_flat = np.array(obs_data.flatten())
        mod_flat = np.array(mod_data.flatten())
        # Get rid of those pesky nans
        badnans = ~isnan(mod_flat) * ~isnan(obs_flat)
        obs_flat = obs_flat[badnans]
        mod_flat = mod_flat[badnans]
        # Find R
        r_value = nos_metrics.pearson_r(mod_flat, obs_flat)
        r_value = np.around(r_value, decimals=3)
    else:
        r_value = np.nan

    # RMSE all pixels
    if np.nansum(~isnan(mod_data)) > threshold and np.nansum(~isnan(obs_data)) >\
        threshold:
        rmse = np.sqrt(np.nanmean((mod_data-obs_data)**2))
    else:
        rmse = np.nan

    # Central frequency
    diff = np.array(mod_data-obs_data)
    diff_flat = diff.flatten()
    cf = nos_metrics.central_frequency(diff_flat, errorrange)

    # Positive & negative outlier frequency
    pof = nos_metrics.positive_outlier_freq(diff_flat, errorrange)
    nof = nos_metrics.negative_outlier_freq(diff_flat, errorrange)

    # Return all stats, stat!
    statsall = [obs_mean, obs_std, mod_mean, mod_std, modobs_bias,
                modobs_bias_std, r_value, rmse, cf, pof, nof]

    return statsall


def return_two_d(
    obs_data: np.ndarray,
    mod_data: np.ndarray,
    logger: Logger,
    errorrange: float = 3,
) -> list[np.ndarray]:
    """
    Calculate 2D statistics from 3D arrays.

    Takes two 3D arrays of the same shape, calculates stats along time axis,
    and returns 2D arrays filled with statistical values.

    Parameters
    ----------
    obs_data : np.ndarray
        3D array of observed data (time, lat, lon)
    mod_data : np.ndarray
        3D array of modeled data (must match obs_data shape)
    logger : Logger
        Logger instance for logging messages
    errorrange : float, optional
        Error threshold for CF/POF/NOF calculations (default 3)

    Returns
    -------
    List[np.ndarray]
        List of 2D statistical arrays:
        [rmse, diff_mean, diff_max, diff_min, stdev, cf2d, pof2d, nof2d]
        Where:
        - rmse: Root mean squared error at each pixel
        - diff_mean: Mean difference (model - obs) at each pixel
        - diff_max: Maximum difference at each pixel
        - diff_min: Minimum difference at each pixel
        - stdev: Standard deviation of difference at each pixel
        - cf2d: Central frequency at each pixel
        - pof2d: Positive outlier frequency at each pixel
        - nof2d: Negative outlier frequency at each pixel

    Raises
    ------
    SystemExit
        If array shapes do not match
    """
    # Check array shapes -- must be the same OR ELSE FAIL
    if obs_data.shape != mod_data.shape:
        logger.error('Arrays in 2D stats module are not the same size!')
        sys.exit(-1)

    logger.info('Starting 2D stats calcs for maps!')

    # Difference the 3D arrays
    diff = np.array(mod_data - obs_data)
    # Figure out if there's enough values (not nans) to make meaningful
    # calculations
    nan_find = ~np.isnan(diff)
    nan_sum = np.nansum(nan_find, axis=0)
    # Set threshold for number of values needed for calculations!
    nan_threshold = 2

    # Mean 2D diff between observations and model output
    diff_mean = np.array(np.nanmean(diff, axis=0))
    diff_mean = np.where(nan_sum >= nan_threshold, diff_mean, np.nan)
    # Max 2D diff between observations and model output
    diff_max = np.array(np.nanmax(diff, axis=0))
    diff_max = np.where(nan_sum >= nan_threshold, diff_max, np.nan)
    # Min 2D diff between observations and model output
    diff_min = np.array(np.nanmin(diff, axis=0))
    diff_min = np.where(nan_sum >= nan_threshold, diff_min, np.nan)
    # RMSE
    rmse = np.array(np.sqrt(np.nanmean(((diff)**2), axis=0)))
    rmse = np.where(nan_sum >= nan_threshold, rmse, np.nan)
    # Standard deviation
    stdev = np.array(np.nanstd(diff, axis=0))
    stdev = np.where(nan_sum >= nan_threshold, stdev, np.nan)

    # Central frequency, positive & negative outlier freq in 2D. Huzzah
    cf2d = np.zeros([diff.shape[1], diff.shape[2]])
    pof2d = np.zeros([diff.shape[1], diff.shape[2]])
    nof2d = np.zeros([diff.shape[1], diff.shape[2]])
    for i in range(diff.shape[1]):
        for j in range(diff.shape[2]):
            if np.count_nonzero(~np.isnan(diff[:, i, j])) >= nan_threshold:
                pixel_errors = diff[:, i, j]
                cf2d[i, j] = nos_metrics.central_frequency(pixel_errors, errorrange)
                pof2d[i, j] = nos_metrics.positive_outlier_freq(pixel_errors, errorrange)
                nof2d[i, j] = nos_metrics.negative_outlier_freq(pixel_errors, errorrange)
            else:
                cf2d[i, j] = np.nan
                pof2d[i, j] = np.nan
                nof2d[i, j] = np.nan

    return [rmse, diff_mean, diff_max, diff_min, stdev, cf2d, pof2d, nof2d]
