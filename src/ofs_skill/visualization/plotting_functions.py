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
import requests
import seaborn as sns

from ofs_skill.obs_retrieval.currents_bins_override import (
    split_virtual_currents_id,
)
from ofs_skill.obs_retrieval.retrieve_t_and_c_station import get_station_depth
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
    # Below the depth line: ADCP orientation / type (Side-Looking,
    # Upward-Looking, …) for CO-OPS currents stations only.
    adcp_type_line = _build_adcp_type_line(
        prop, station_id, name_var, logger
    )

    return f'<b>NOAA/NOS OFS Skill Assessment<br>' \
            f'{station_id[2]} station:&nbsp;{station_id[1]} ' \
            f'({station_id[0]})<br>' \
            f'OFS:&nbsp;{prop.ofs.upper()}&nbsp;&nbsp;&nbsp;Node ID:&nbsp;' \
            f'{node}&nbsp;&nbsp;&nbsp;' \
            + nwsline + depth_line + adcp_type_line + \
            f'<br>From:&nbsp;{start_date}' \
            f'&nbsp;&nbsp;&nbsp;To:&nbsp;' \
            f'{end_date}<b>'


_COOPS_MDAPI_URL = 'https://api.tidesandcurrents.noaa.gov/mdapi/prod/'

# Cache parsed model_station.ctl lookups keyed by file path so we read
# each file at most once per process.
_MODEL_CTL_CACHE: dict[str, dict[str, float]] = {}

# Cache obs-side station.ctl parses:
#   {ctl_path: {station_id: (depth_m, hfb_m, mounting_type)}}.
_OBS_CTL_CACHE: dict[str, dict[str, tuple[float, float, str]]] = {}


def _invalidate_obs_station_depths(ctl_path: str) -> None:
    """Drop a cached parse so the next read re-parses the file on disk.

    ``_resolve_side_looking_depths`` (model-CTL writer) rewrites the
    obs station.ctl mid-pipeline to back-patch resolved depths for
    side-looking ADCPs; without this hook a plotting call that
    happened earlier in the same process would otherwise read the
    pre-backpatch (depth=0) entry from cache and produce a misleading
    plot title.
    """
    _OBS_CTL_CACHE.pop(ctl_path, None)


def _load_obs_station_depths(
    ctl_path: str,
) -> dict[str, tuple[float, float, str]]:
    """Return ``{station_id: (obs_depth_m, hfb_m, mounting_type)}`` from a CTL.

    Station control file format (2 lines per station)::

        <id> <source_id> "<name>"
          <lat> <lon> <zdiff> <obs_depth> <shift> [<hfb> [<mounting_type>]]

    The 4th space-separated token on the coord line is the observation
    depth in meters. The optional 6th token is ``height_from_bottom``
    (non-zero only for CO-OPS side-looking ADCPs); the optional 7th
    token is the canonical mounting symbol
    (``side``/``up``/``down``/``unknown``). Both are missing on legacy
    CTL files written before this change — defaults are 0.0 / ``''``.
    """
    if ctl_path in _OBS_CTL_CACHE:
        return _OBS_CTL_CACHE[ctl_path]
    result: dict[str, tuple[float, float, str]] = {}
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
                depth = float(coord[3])
            except (TypeError, ValueError):
                continue
            hfb = 0.0
            if len(coord) >= 6:
                try:
                    hfb = float(coord[5])
                except (TypeError, ValueError):
                    hfb = 0.0
            mounting = ''
            if len(coord) >= 7:
                # Canonicalised at write time by
                # ``write_obs_ctlfile._emit_coops_currents_entries``.
                token = coord[6].strip().lower()
                if token in ('side', 'up', 'down'):
                    mounting = token
                # 'unknown' / anything else stays empty so the caller
                # gracefully omits the ADCP-type label.
            result[station_id] = (depth, hfb, mounting)
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
    """Return ``(bin_num, obs_depth_m, mounting_type)`` for a cu station.

    Source-of-truth precedence:

    1. **Obs station.ctl** (``{parent}_{name_var}_station.ctl``) — the
       canonical artefact written by ``_emit_coops_currents_entries``.
       The 7th coord-line token carries the mounting symbol
       (``side``/``up``/``down``/``unknown``); the 4th token is the
       depth (back-patched by ``_resolve_side_looking_depths`` for
       side-looking PICS ADCPs whose MDAPI per-bin ``depth`` is null).
    2. **MDAPI bins endpoint** — used only when the CTL did not carry
       the station (e.g. an out-of-band lookup before the CTL was
       written). Per-bin ``depth`` is always populated for up/down
       ADCPs but is null for side-looking; the helper does not invent
       an orientation here, leaving classification for the CTL-driven
       call site.

    Returns ``None`` when no depth can be resolved. ``bin_num`` is
    ``None`` for non-virtual IDs (NDBC/USGS/CHS).
    """
    parent_id_str, bin_num = split_virtual_currents_id(station_id_tuple[0])
    parent_id: str | None = parent_id_str if bin_num is not None else None

    # CTL is checked first — it embeds the canonical mounting symbol
    # and the depth resolution that the model-CTL writer has already
    # finalised (including the side-looking back-patch).
    ctl_dir = getattr(prop, 'control_files_path', None)
    if ctl_dir:
        ctl_path = os.path.join(
            ctl_dir, f'{prop.ofs}_{name_var}_station.ctl')
        table = _load_obs_station_depths(ctl_path)
        entry = table.get(str(station_id_tuple[0]))
        if entry is not None:
            depth_val, _hfb, mounting = entry
            return bin_num, float(depth_val), mounting

    # MDAPI fallback — only reached when the CTL did not contain the
    # station. We DO NOT default mounting to 'up' or invent 'side-looking'
    # here; that was the issue #141 root cause (every downward-looking
    # station was silently mislabelled).
    if bin_num is not None and station_id_tuple[2] == 'CO-OPS':
        try:
            payload = get_station_depth(
                parent_id, _COOPS_MDAPI_URL, logger)
        except (requests.exceptions.RequestException,
                ValueError, KeyError, TypeError) as exc:
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
                    if depth_val is not None:
                        return bin_num, float(depth_val), ''
                except (TypeError, ValueError):
                    continue

    return None


def _build_depth_line(prop, station_id, name_var, logger):
    """HTML fragment showing obs + model depth (+ bin number) for cu plots.

    Returns an empty string for non-currents plots or when no depth info
    can be resolved. Format::

        <br>Bin NN — Obs depth X.X m  —  Model depth Y.Y m
    """
    if name_var != 'cu':
        return ''

    obs_info = _lookup_obs_depth(station_id, prop, name_var, logger)
    bin_num = None
    obs_depth: float | None = None
    if obs_info is not None:
        bin_num, obs_depth, _orientation = obs_info

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
        parts.append(
            f'Obs&nbsp;depth&nbsp;{abs(obs_depth):.1f}&nbsp;m')
    if model_depth is not None:
        parts.append(
            f'Model&nbsp;depth&nbsp;{abs(model_depth):.1f}&nbsp;m')

    return '<br>' + '&nbsp;—&nbsp;'.join(parts)


def _format_adcp_orientation_label(raw: str) -> str:
    """Map a canonical mounting symbol to a human-readable ADCP type.

    Expected canonical inputs (written into the obs CTL 7th token by
    ``_emit_coops_currents_entries``): ``side``, ``up``, ``down``,
    ``unknown``. Legacy free-form MDAPI strings (``horizontal``,
    ``upward``, ...) are still recognised for backward compatibility
    with stale CTL files. Returns ``''`` when the input is empty,
    ``'unknown'``, or unrecognised — better silence than wrong
    attribution (issue #141).
    """
    if not raw:
        return ''
    low = raw.strip().lower()
    if not low or low == 'unknown':
        return ''
    if low == 'side' or low.startswith('horiz') or 'side' in low or \
            low == 'h':
        return 'Side-Looking ADCP'
    if low == 'up' or low.startswith('upward') or low == 'u':
        return 'Upward-Looking ADCP (bottom-mounted)'
    if low == 'down' or low.startswith('downward') or low == 'd':
        return 'Downward-Looking ADCP (top-mounted)'
    if low.startswith('vert') or low == 'v':
        return 'Vertical ADCP'
    return ''


def _build_adcp_type_line(prop, station_id, name_var, logger) -> str:
    """HTML fragment (``<br>Side-Looking ADCP``) for a CO-OPS ADCP station.

    Returns an empty string for non-currents plots, non-CO-OPS sources,
    or when the orientation cannot be resolved. ``_lookup_obs_depth`` is
    responsible for populating the raw orientation string (from MDAPI
    for vertical ADCPs, or from the station.ctl ``hfb`` presence for
    side-looking PICS ADCPs).
    """
    if name_var != 'cu':
        return ''
    if len(station_id) < 3 or station_id[2] != 'CO-OPS':
        return ''

    obs_info = _lookup_obs_depth(station_id, prop, name_var, logger)
    if obs_info is None:
        return ''
    label = _format_adcp_orientation_label(obs_info[2])
    if not label:
        return ''
    return f'<br>{label}'


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
