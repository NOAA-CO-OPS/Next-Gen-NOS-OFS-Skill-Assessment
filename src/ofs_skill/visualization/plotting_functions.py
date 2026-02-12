"""
Shared plotting utility functions for OFS skill assessment visualizations.

This module contains common utility functions used across multiple visualization
modules. Functions handle color palettes, marker styles, plot titles, error ranges,
and data gap detection.

Key Features:
    - Cubehelix color palettes (colorblind-accessible)
    - Marker symbol management for multiple time series
    - Plot title generation with station metadata
    - Target error range retrieval from configuration
    - Data gap detection for gap handling in plots

Functions:
    make_cubehelix_palette: Generate accessibility-optimized color palette
    get_markerstyles: Get list of distinct marker symbols
    get_title: Generate formatted plot title with metadata
    get_error_range: Retrieve target error ranges for variables
    find_max_data_gap: Find maximum consecutive NaN gap in data

Author: AJK
Created: Extracted from create_1dplot.py for modularity
"""
from __future__ import annotations

import json
import os
import urllib.request
from datetime import datetime
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
import seaborn as sns

if TYPE_CHECKING:
    from logging import Logger


def make_cubehelix_palette(
    ncolors: int,
    start_val: float,
    rot_val: float,
    light_val: float
) -> tuple[list[str], list]:
    """
    Create custom cubehelix color palette for accessible plotting.

    The cubehelix palette linearly varies hue AND intensity so colors can be
    distinguished in greyscale, improving accessibility for colorblind users
    and printed materials.

    Args:
        ncolors: Number of discrete colors in palette (1 to ~1000)
                Should correspond to number of time series in plot
        start_val: Starting hue for color palette (0.0 to 3.0)
        rot_val: Rotations around hue wheel over palette range
                Larger absolute values = more different colors
                Can be positive or negative
        light_val: Intensity of lightest color (0.0=darker to 1.0=lighter)

    Returns:
        Tuple containing:
            - palette_hex: List of color values as HEX strings
            - palette_rgb: List of color values as RGB tuples

    Example:
        >>> palette_hex, palette_rgb = make_cubehelix_palette(5, 2.5, 0.9, 0.65)
        >>> len(palette_hex)
        5

    References:
        https://seaborn.pydata.org/generated/seaborn.cubehelix_palette.html
    """
    palette_rgb = sns.cubehelix_palette(
        n_colors=ncolors, start=start_val, rot=rot_val, gamma=1.0,
        hue=0.8, light=light_val, dark=0.15, reverse=False, as_cmap=False
    )
    # Convert RGB to HEX numbers (easier to handle than RGB)
    palette_hex = palette_rgb.as_hex()
    return palette_hex, palette_rgb


def get_markerstyles() -> list[str]:
    """
    Get list of marker symbols for multi-series plots.

    Returns a predefined list of distinct marker symbols that can be assigned
    to different time/data series in plots. This ensures each series has a
    unique visual marker.

    Returns:
        List of marker symbol names compatible with Plotly

    Example:
        >>> markers = get_markerstyles()
        >>> markers[0]
        'circle'

    Notes:
        - Returns 7 distinct marker types
        - Can be extended if more series types are needed
        - Previously used SymbolValidator but simplified to fixed list
    """
    return ['circle', 'square', 'diamond', 'cross', 'x', 'triangle-up', 'pentagon']


def get_title(
    prop,
    node: str,
    station_id: tuple,
    name_var: str,
    logger: Logger
) -> str:
    """
    Generate formatted HTML plot title with station and run metadata.

    Creates a multi-line title including OFS name, station information,
    node ID, NWS ID (for CO-OPS stations), and date range.

    Args:
        prop: Properties object containing run configuration
              Must have: start_date_full, end_date_full, ofs
        node: Model node identifier
        station_id: Tuple of (station_number, station_name, source)
        name_var: Variable name (used to exclude NWS lookup for currents)
        logger: Logger instance for error messages

    Returns:
        HTML-formatted title string with bold headers and proper spacing

    Example:
        >>> title = get_title(prop, '123', ('8454000', 'Providence', 'CO-OPS'), 'wl', logger)

    Notes:
        - Handles both ISO format (YYYY-MM-DDTHH:MM:SSZ) and legacy format
        - Retrieves NWS SHEF code from NOAA API for CO-OPS stations
        - Uses non-breaking spaces (&nbsp;) for proper spacing in HTML
    """
    # If incoming date format is YYYY-MM-DDTHH:MM:SSZ, remove 'Z' and 'T'
    if 'Z' in prop.start_date_full and 'Z' in prop.end_date_full:
        start_date = prop.start_date_full.replace('Z', '')
        end_date = prop.end_date_full.replace('Z', '')
        start_date = start_date.replace('T', ' ')
        end_date = end_date.replace('T', ' ')
    # If the format is YYYYMMDD-HH:MM:SS, format correctly
    else:
        start_date = datetime.strptime(prop.start_date_full, '%Y%m%d-%H:%M:%S')
        end_date = datetime.strptime(prop.end_date_full, '%Y%m%d-%H:%M:%S')
        start_date = start_date.strftime('%Y-%m-%d %H:%M:%S')
        end_date = end_date.strftime('%Y-%m-%d %H:%M:%S')

    # Get the NWS ID (shefcode) if CO-OPS station
    # All CO-OPS stations have 7-digit ID
    if station_id[2] == 'CO-OPS' and name_var != 'cu':
        metaurl = 'https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations/' +\
            str(station_id[0]) + '.json?units=metric'
        try:
            with urllib.request.urlopen(metaurl) as url:
                metadata = json.load(url)
            nws_id = metadata['stations'][0]['shefcode']
        except Exception as e:
            logger.error(f'Exception in get_title when getting nws id: {e}')
            nws_id = 'NA'
        nwsline = f'NWS ID:&nbsp;{nws_id}'
    else:
        nwsline = ''

    return f'<b>NOAA/NOS OFS Skill Assessment<br>' \
            f'{station_id[2]} station:&nbsp;{station_id[1]} ' \
            f'({station_id[0]})<br>' \
            f'OFS:&nbsp;{prop.ofs.upper()}&nbsp;&nbsp;&nbsp;Node ID:&nbsp;' \
            f'{node}&nbsp;&nbsp;&nbsp;' \
            + nwsline + \
            f'<br>From:&nbsp;{start_date}' \
            f'&nbsp;&nbsp;&nbsp;To:&nbsp;' \
            f'{end_date}<b>'


def get_error_range(
    name_var: str,
    prop,
    logger: Logger
) -> tuple[float, float]:
    """
    Retrieve target error ranges for a given variable.

    Reads error range configuration from CSV file or creates default
    values if file doesn't exist. Returns X1 (primary target) and
    X2 (secondary target) error thresholds.

    Args:
        name_var: Variable name ('salt', 'temp', 'wl', 'cu', 'ice_conc')
        prop: Properties object with path attribute for config location
        logger: Logger instance (unused but kept for consistency)

    Returns:
        Tuple of (X1, X2) where:
            - X1: Primary target error range
            - X2: Secondary target error range

    Default Values:
        - salt: X1=3.5, X2=0.5 (PSU)
        - temp: X1=3.0, X2=0.5 (°C)
        - wl: X1=0.15, X2=0.5 (m)
        - cu: X1=0.26, X2=0.5 (m/s)
        - ice_conc: X1=10, X2=0.5 (%)

    Example:
        >>> X1, X2 = get_error_range('wl', prop, logger)
        >>> X1
        0.15

    Notes:
        - Creates error_ranges.csv in conf/ if missing
        - File location: {prop.path}/conf/error_ranges.csv
    """
    filename = 'error_ranges.csv'
    filepath = os.path.join(prop.path, 'conf', filename)
    try:
        # Dataframe of error range file
        df = pd.read_csv(filepath)
        subs = df[df['name_var'] == name_var]
        # Get error ranges for variable
        X1 = pd.to_numeric(subs['X1']).values[0]
        X2 = pd.to_numeric(subs['X2']).values[0]
    except (KeyError, FileNotFoundError, IndexError):
        # Make file using default values
        errordata = [
            ['salt', 3.5, 0.5],
            ['temp', 3, 0.5],
            ['wl', 0.15, 0.5],
            ['cu', 0.26, 0.5],
            ['ice_conc', 10, 0.5]
        ]
        df = pd.DataFrame(errordata, columns=['name_var', 'X1', 'X2'])
        subs = df[df['name_var'] == name_var]
        df.to_csv(filepath, index=False)
        X1 = pd.to_numeric(subs['X1']).values[0]
        X2 = pd.to_numeric(subs['X2']).values[0]

    return X1, X2


def find_max_data_gap(arr: pd.Series) -> int:
    """
    Find maximum consecutive NaN gap in time series data.

    Identifies the longest sequence of consecutive NaN values in a pandas
    Series. Used to determine whether to connect gaps in line plots.

    Args:
        arr: Pandas Series containing data with potential NaN gaps

    Returns:
        Integer count of maximum consecutive NaNs

    Example:
        >>> import pandas as pd
        >>> data = pd.Series([1.0, 2.0, np.nan, np.nan, np.nan, 3.0])
        >>> find_max_data_gap(data)
        3

    Notes:
        - Returns 0 for empty arrays
        - A difference of 1 between NaN indices indicates consecutive gaps
        - Used to set connectgaps parameter in Plotly plots
    """
    if len(arr) == 0:
        return 0

    # Find indices of nans. Then difference indices to locate consecutive nans
    # A difference of 1 means consecutive nans, and a data gap is present
    gap_check = (np.diff(np.argwhere(arr.isnull()), axis=0))
    max_count = 0
    current_count = 0
    for x in gap_check:
        if x == 1:  # value of 1 indicates data gap
            current_count += 1
        else:
            max_count = max(max_count, current_count)
            current_count = 0
    max_count = max(max_count, current_count)  # Handle case where array ends with 1s
    return max_count
