"""
Great Lakes ice skill assessment.

This module performs ice concentration skill assessment for Great Lakes OFS
by comparing model output to GLSEA satellite observations.

Directory Location: skill_assessment/

Technical Contact(s): Name: PL

Abstract
--------
During a run, for each day, the ice skill assessment:

1) Downloads ice concentration maps from the Great Lakes Surface Environmental
   Analysis (GLSEA) for the time period of interest, and clips it to an OFS area
2) Fetches available nowcast and/or forecast GLOFS guidance of ice
   concentration for the same time period and OFS area
3) Produces 1D time series of GLSEA and modeled ice concentration with skill
   statistics at specified locations within the OFS
4) Interpolates the model output to the regular GLSEA grid, so they are
   directly comparable
5) Produces basin-wide skill statistics and 2D skill statistics maps

Language: Python 3.11

Estimated Execution Time: depends on date range; typically <20 minutes

NOTE: This file has been migrated with updated imports. Full type hints and
comprehensive docstrings should be added in a future update.

TODO: Add comprehensive type hints and docstrings to all functions
"""

from __future__ import annotations

from datetime import date
from logging import Logger

import numpy as np

# NOTE: The functions below have been preserved from the original file with
# minimal changes. They require comprehensive docstrings and type hints.


def make_2d_mask(
    array_to_mask: np.ndarray,
    conditional_array: np.ndarray | None,
    threshold: float,
) -> np.ndarray:
    """
    Apply conditional masking to a 2D array.

    Parameters
    ----------
    array_to_mask : np.ndarray
        Array to apply mask to
    conditional_array : Optional[np.ndarray]
        Array to use for conditional masking, or None
    threshold : float
        Threshold value for masking

    Returns
    -------
    np.ndarray
        Masked array
    """
    if conditional_array is not None:
        array_to_mask[conditional_array < threshold] = np.nan
    else:
        array_to_mask[array_to_mask < threshold] = np.nan
    return array_to_mask


def iceonoff(
    time_all_dt: list,
    meanicecover: list,
    logger: Logger,
) -> tuple[date | None, date | None]:
    """
    Find ice onset and thaw dates for ice concentration time series.

    Parameters
    ----------
    time_all_dt : list
        List of datetime objects
    meanicecover : list
        List of mean ice cover values
    logger : Logger
        Logger instance

    Returns
    -------
    Tuple[Optional[date], Optional[date]]
        Ice onset and thaw dates, or None if not found
    """
    counter = 0
    iceon = None
    iceoff = None
    # Find ice onset date
    for i in range(len(time_all_dt)):
        if meanicecover[i] >= 10:
            counter = counter + 1
        else:
            counter = 0
        if counter == 5:
            iceon = time_all_dt[i]
            logger.info('Ice onset date found!')
            break
    if iceon is None:
        logger.info('Ice onset date not found!')
    # Find ice thaw date
    try:
        logger.info('Trying ice thaw enumerate...')
        idx = len(meanicecover) - next(
            i for i, val in
            enumerate(reversed(meanicecover), 1)
            if val >= 10
        )
        logger.info('Completed ice thaw enumerate!')
        if (
            len([idx]) > 0
            and (len(meanicecover)-1)-idx >= 5
            and sum(np.isnan(meanicecover[idx:])) <= 2
        ):
            iceoff = time_all_dt[idx+5]
            logger.info('Ice thaw date found!')
        else:
            logger.info('Ice thaw date not found!')
    except StopIteration:
        logger.error('StopIteration exception: ice thaw date not found!')
    logger.info('Completed ice onset/thaw date-finding, return to main...')
    return iceon, iceoff


# Placeholder note for future migration
__doc__ += """

This module requires additional documentation work:
- Add comprehensive NumPy-style docstrings to all functions
- Add type hints to all function parameters and returns
- Update examples in docstrings

The core functionality has been preserved with updated imports.

For complete implementation, see the original file at:
bin/skill_assessment/do_iceskill.py
"""
