# -*- coding: utf-8 -*-
"""
Created on Fri Jul 26 10:10:30 2024

@author: PL

NOTES:
This is set up to be within a for-loop that cycles through time, and calls this
function each loop iteration/time step.

Incoming data arrays should be numpy but this can be relaxed/changed

Functions:
    return_one_d: takes two 2D arrays (observed and modeled) and returns 1D stats
        like RMSE, R, standard deviation, mean.
    return_two_d: takes two 3D arrays (observed and modeled) and returns
        2D arrays of stats.

"""
import sys
import logging
import numpy as np
from numpy import isnan
from scipy import stats

# Capture warnings
logging.captureWarnings(True)


def return_one_d(obs_data,mod_data,logger):
    ''''Returns single stat values (like RMSE) derived from 2D array'''

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
        # <pirate voice> Find arrrrrrr
        r_value = stats.pearsonr(obs_flat,mod_flat)[0]
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
    errorrange = int(3)
    diff = np.array(mod_data-obs_data)
    cf = ((((-errorrange <= diff) & (diff <= errorrange)).sum())/
          np.count_nonzero(~np.isnan(diff)))*100

    # Positive & negative outlier frequency
    pof = ((((2*errorrange <= diff)).sum())/
           np.count_nonzero(~np.isnan(diff)))*100

    nof = ((((diff <= -2*errorrange)).sum())/
           np.count_nonzero(~np.isnan(diff)))*100

    # Return all stats, stat!
    statsall = [obs_mean, obs_std, mod_mean, mod_std, modobs_bias, \
                modobs_bias_std,r_value, rmse, cf, pof, nof]

    return statsall


def return_two_d(obs_data,mod_data,logger):
    '''Takes two 3D arrays of same shape, calculates stats along time axis,
    returns 2D array filled with staty goodness'''

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
    nan_threshold = 5

    ## Mean 2D diff between observations and model output
    diff_mean = np.array(np.nanmean(diff, axis=0))
    diff_mean = np.where(nan_sum >= nan_threshold, diff_mean, np.nan)
    ## Max 2D diff between observations and model output
    diff_max = np.array(np.nanmax(diff, axis=0))
    diff_max = np.where(nan_sum >= nan_threshold, diff_max, np.nan)
    ## Min 2D diff between observations and model output
    diff_min = np.array(np.nanmin(diff, axis=0))
    diff_min = np.where(nan_sum >= nan_threshold, diff_min, np.nan)
    ## RMSE
    rmse = np.array(np.sqrt(np.nanmean(((diff)**2),axis=0)))
    rmse = np.where(nan_sum >= nan_threshold, rmse, np.nan)
    ## Standard deviation
    stdev = np.array(np.nanstd(diff,axis=0))
    stdev = np.where(nan_sum >= nan_threshold, stdev, np.nan)

    ## Central frequency, positive & negative outlier freq in 2D. Huzzah
    errorrange = int(3)
    cf2d = np.zeros([diff.shape[1],diff.shape[2]])
    pof2d = np.zeros([diff.shape[1],diff.shape[2]])
    nof2d = np.zeros([diff.shape[1],diff.shape[2]])
    for i in range(diff.shape[1]):
        for j in range(diff.shape[2]):
            if np.count_nonzero(~np.isnan(diff[:,i,j])) >= nan_threshold:
                cf2d[i,j] = ((((-errorrange <= diff[:,i,j]) &
                               (diff[:,i,j] <= errorrange)).sum())/
                             np.count_nonzero(~np.isnan(diff[:,i,j])))*100
                pof2d[i,j] = ((((2*errorrange <= diff[:,i,j])).sum())/
                       np.count_nonzero(~np.isnan(diff[:,i,j])))*100
                nof2d[i,j] = ((((diff[:,i,j] <= -2*errorrange)).sum())/
                       np.count_nonzero(~np.isnan(diff[:,i,j])))*100
            else:
                cf2d[i,j] = np.nan
                pof2d[i,j] = np.nan
                nof2d[i,j] = np.nan

    return diff_mean, diff_max, rmse, stdev, cf2d, pof2d, nof2d, diff_min
