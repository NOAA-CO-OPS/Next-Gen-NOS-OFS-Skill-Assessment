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
import re
import urllib.request
from datetime import datetime
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
import seaborn as sns

from ofs_skill.obs_retrieval.retrieve_t_and_c_station import _get_station_depth
from ofs_skill.skill_assessment import nos_metrics

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
    node ID, NWS ID (for CO-OPS water-level / temp / salt stations), and
    date range. For currents plots (``name_var == 'cu'``) the title also
    carries an ``Obs depth / Model depth`` annotation produced by
    :func:`_build_depth_line`, with a ``Bin NN`` prefix for CO-OPS
    per-bin ADCP virtual IDs.

    Args:
        prop: Properties object containing run configuration.
            Must have: ``start_date_full``, ``end_date_full``, ``ofs``,
            and (for currents) ``control_files_path`` so the depth line
            can resolve obs/model depths from the station and model
            ctl files.
        node: Model node identifier (integer index as string).
        station_id: Tuple of (station_number, station_name, source).
            For ADCP per-bin plots ``station_number`` is the virtual ID
            ``{parent}_b{NN}``.
        name_var: Variable name. Controls whether the NWS SHEF lookup
            runs (wl/temp/salt) or the currents depth annotation runs
            (cu).
        logger: Logger instance for error messages.

    Returns:
        HTML-formatted title string with bold headers and proper spacing.

    Example:
        >>> title = get_title(prop, '123',
        ...     ('8454000', 'Providence', 'CO-OPS'), 'wl', logger)

    Notes:
        - Handles both ISO format (YYYY-MM-DDTHH:MM:SSZ) and legacy format.
        - Retrieves NWS SHEF code from NOAA API for CO-OPS non-currents
          stations.
        - Uses non-breaking spaces (&nbsp;) for proper spacing in HTML.
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

    # Currents plots get an explicit "Obs depth | Model depth" annotation.
    # For CO-OPS ADCP virtual IDs (``{parent}_b{NN}``) a "Bin NN" prefix
    # is included. Obs depth is resolved from the CO-OPS bins endpoint
    # (with distance fallback for PICS bins); model depth comes from the
    # model_station.ctl last column (list_of_depths).
    depth_line = _build_depth_line(
        prop, station_id, name_var, logger
    )

    return f'<b>NOAA/NOS OFS Skill Assessment<br>' \
            f'{station_id[2]} station:&nbsp;{station_id[1]} ' \
            f'({station_id[0]})<br>' \
            f'OFS:&nbsp;{prop.ofs.upper()}&nbsp;&nbsp;&nbsp;Node ID:&nbsp;' \
            f'{node}&nbsp;&nbsp;&nbsp;' \
            + nwsline + depth_line + \
            f'<br>From:&nbsp;{start_date}' \
            f'&nbsp;&nbsp;&nbsp;To:&nbsp;' \
            f'{end_date}<b>'


_VIRTUAL_BIN_RE = re.compile(r'^(.+)_b(\d+)$')
_COOPS_MDAPI_URL = 'https://api.tidesandcurrents.noaa.gov/mdapi/prod/'

# Cache parsed model_station.ctl lookups keyed by file path so we read
# each file at most once per process.
_MODEL_CTL_CACHE: dict[str, dict[str, float]] = {}

# Cache obs-side station.ctl parses: {ctl_path: {station_id: depth_m}}.
_OBS_CTL_CACHE: dict[str, dict[str, float]] = {}


def _load_obs_station_depths(ctl_path: str) -> dict[str, float]:
    """Return ``{station_id: obs_depth_m}`` parsed from a station ctl file.

    Station control file format (2 lines per station)::

        <id> <source_id> "<name>"
          <lat> <lon> <zdiff> <obs_depth> <shift>

    The 4th space-separated token on the coord line is the observation
    depth in meters. For non-virtual-ID stations (NDBC, USGS, CHS) this
    is the only source of obs depth.
    """
    if ctl_path in _OBS_CTL_CACHE:
        return _OBS_CTL_CACHE[ctl_path]
    result: dict[str, float] = {}
    if not os.path.isfile(ctl_path):
        _OBS_CTL_CACHE[ctl_path] = result
        return result
    try:
        with open(ctl_path, encoding='utf-8') as fh:
            lines = fh.read().splitlines()
        for i in range(0, len(lines) - 1, 2):
            head = lines[i].split()
            coord = lines[i + 1].split()
            if not head or len(coord) < 4:
                continue
            station_id = head[0]
            try:
                result[station_id] = float(coord[3])
            except (TypeError, ValueError):
                continue
    except OSError:
        pass
    _OBS_CTL_CACHE[ctl_path] = result
    return result


def _load_model_station_depths(ctl_path: str) -> dict[str, float]:
    """Return ``{station_id: model_depth_m}`` parsed from a model ctl file.

    The model control file format is::

        <node> <layer> <lat> <lon> <station_id> <model_depth>

    and ``list_of_depths`` (the last column) is the depth of the nearest
    model layer that the paired data was sampled from. Missing files or
    malformed lines yield an empty dict.
    """
    if ctl_path in _MODEL_CTL_CACHE:
        return _MODEL_CTL_CACHE[ctl_path]
    result: dict[str, float] = {}
    if not os.path.isfile(ctl_path):
        _MODEL_CTL_CACHE[ctl_path] = result
        return result
    try:
        with open(ctl_path, encoding='utf-8') as fh:
            for raw in fh:
                parts = raw.split()
                if len(parts) < 6:
                    continue
                try:
                    result[parts[-2]] = float(parts[-1])
                except (TypeError, ValueError):
                    continue
    except OSError:
        pass
    _MODEL_CTL_CACHE[ctl_path] = result
    return result


def _lookup_obs_depth(
    station_id_tuple, prop, name_var, logger,
) -> tuple[int | None, float, str] | None:
    """Return ``(bin_num, obs_depth_m, orientation)`` for a cu station.

    Preferred source for CO-OPS virtual-ID (per-bin) stations is the
    MDAPI bins endpoint's ``depth`` field. For non-virtual IDs (NDBC,
    USGS, CHS) — or when the bins endpoint returns ``depth: None`` (PICS
    / side-looking ADCPs) — falls back to the obs station.ctl file
    (``{parent}_{name_var}_station.ctl``) which stores the depth emitted
    at CTL-write time.

    Returns ``None`` when no depth can be resolved from either source.
    For non-virtual IDs, ``bin_num`` is ``None``.
    """
    parent_id: str | None = None
    bin_num: int | None = None
    orientation = ''

    # Try virtual-ID parse first.
    m = _VIRTUAL_BIN_RE.match(str(station_id_tuple[0]))
    if m:
        parent_id = m.group(1)
        try:
            bin_num = int(m.group(2))
        except (TypeError, ValueError):
            bin_num = None

    # Preferred source for CO-OPS ADCPs: the MDAPI bins endpoint —
    # but only when it has an explicit per-bin ``depth``. Side-looking
    # (PICS) ADCPs leave that null; for those we fall through to the
    # obs station.ctl file, which the model-CTL writer has already
    # back-patched with a water_depth - height_from_bottom resolution.
    if (
        bin_num is not None
        and station_id_tuple[2] == 'CO-OPS'
    ):
        try:
            payload = _get_station_depth(
                parent_id, _COOPS_MDAPI_URL, logger)
        except Exception as exc:
            logger.warning(
                'Bin metadata lookup failed for %s: %s',
                station_id_tuple[0], exc)
            payload = None

        if payload and payload.get('bins'):
            for entry in payload['bins']:
                try:
                    entry_num = entry.get('num', entry.get('bin'))
                    if entry_num is None or int(entry_num) != bin_num:
                        continue
                    depth_val = entry.get('depth')
                    orientation = str(
                        entry.get('orientation', '') or '')
                    if depth_val is not None:
                        return bin_num, float(depth_val), orientation
                    # depth is None: leave MDAPI loop, try the CTL.
                    break
                except (TypeError, ValueError):
                    continue

    # Fallback: obs station.ctl file. Handles NDBC/USGS/CHS and the
    # PICS case where the MDAPI has no depth (CTL carries the resolved
    # value written by write_ofs_ctlfile._resolve_side_looking_depths).
    ctl_dir = getattr(prop, 'control_files_path', None)
    if ctl_dir:
        ctl_path = os.path.join(
            ctl_dir, f'{prop.ofs}_{name_var}_station.ctl')
        table = _load_obs_station_depths(ctl_path)
        depth_val = table.get(str(station_id_tuple[0]))
        if depth_val is not None:
            return bin_num, float(depth_val), orientation

    return None


def _build_depth_line(prop, station_id, name_var, logger):
    """HTML fragment showing obs + model depth (+ bin number) for cu plots.

    Returns an empty string for non-currents plots or when no depth info
    can be resolved. Format::

        <br>Bin NN — Obs depth -X.X m (orientation)  |  Model depth -Y.Y m
    """
    if name_var != 'cu':
        return ''

    obs_info = _lookup_obs_depth(station_id, prop, name_var, logger)
    bin_num = None
    obs_depth: float | None = None
    orientation = ''
    if obs_info is not None:
        bin_num, obs_depth, orientation = obs_info

    # Resolve model depth from the model_station.ctl / model.ctl file.
    model_depth: float | None = None
    ctl_dir = getattr(prop, 'control_files_path', None)
    if ctl_dir:
        for suffix in ('_model_station.ctl', '_model.ctl'):
            ctl_path = os.path.join(
                ctl_dir, f'{prop.ofs}_{name_var}{suffix}')
            table = _load_model_station_depths(ctl_path)
            if str(station_id[0]) in table:
                model_depth = table[str(station_id[0])]
                break

    if obs_depth is None and model_depth is None:
        return ''

    parts = []
    if bin_num is not None:
        parts.append(f'Bin&nbsp;{bin_num:02d}')
    if obs_depth is not None:
        orient_txt = f'&nbsp;({orientation})' if orientation else ''
        parts.append(
            f'Obs&nbsp;depth&nbsp;{-abs(obs_depth):.1f}&nbsp;m{orient_txt}')
    if model_depth is not None:
        parts.append(
            f'Model&nbsp;depth&nbsp;{-abs(model_depth):.1f}&nbsp;m')

    return '<br>' + '&nbsp;—&nbsp;'.join(parts)


def get_error_range(
    name_var: str,
    prop,
    logger: Logger
) -> tuple[float, float]:
    """
    Retrieve target error ranges for a given variable.

    Thin wrapper around ``nos_metrics.get_error_threshold`` that preserves
    the legacy ``(name_var, prop, logger)`` call signature used by all
    plotting modules.  If the CSV file does not exist, a default one is
    written so that downstream callers find it on subsequent runs.

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
    config_path = os.path.join(prop.path, 'conf', 'error_ranges.csv')

    # Delegate to the canonical implementation
    X1, X2 = nos_metrics.get_error_threshold(name_var, config_path)

    # Preserve legacy behaviour: write a default CSV when no file exists
    if not os.path.isfile(config_path):
        errordata = [
            ['salt', 3.5, 0.5],
            ['temp', 3, 0.5],
            ['wl', 0.15, 0.5],
            ['cu', 0.26, 0.5],
            ['ice_conc', 10, 0.5],
        ]
        df = pd.DataFrame(errordata, columns=['name_var', 'X1', 'X2'])
        df.to_csv(config_path, index=False)

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
