"""
Constituent statistics table comparing model-derived and reference harmonic constants.

Reproduces the per-station harmonic-constant comparison table from NOS
technical reports (CS 17 / CS 24), matching the legacy Fortran program
``table_Harmonic_C.f`` (STEP 10 of the skill assessment pipeline).

Water levels
    Reference = CO-OPS accepted harmonic constants (from API).
    Model     = HA on model time series.

Currents
    Reference = HA on observation time series.
    Model     = HA on model time series.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .constituents import NOS_37_CONSTITUENTS
from .ha_comparison import compare_harmonic_constants
from .harmonic_analysis import harmonic_analysis

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------
# Threshold constants
# -----------------------------------------------------------------------
DEFAULT_AMP_THRESHOLD_M = 0.05        # 5 cm
DEFAULT_PHASE_THRESHOLD_DEG = 10.0    # 10 degrees
DEFAULT_VECTOR_DIFF_THRESHOLD_M = 0.05  # 5 cm

# Major tidal constituents for summary statistics
MAJOR_CONSTITUENTS = ('M2', 'S2', 'N2', 'K2', 'K1', 'O1', 'P1', 'Q1')


def build_constituent_table(
    model_time: pd.DatetimeIndex,
    model_values: np.ndarray,
    latitude: float,
    data_type: str,
    station_id: str,
    obs_time: pd.DatetimeIndex | None = None,
    obs_values: np.ndarray | None = None,
    accepted_constants: dict[str, dict[str, float]] | None = None,
    constit: list[str] | None = None,
    min_duration_days: float = 15.0,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    """
    Build a constituent statistics table comparing model and reference HA.

    Parameters
    ----------
    model_time : pd.DatetimeIndex
        Timestamps for the model time series (UTC).
    model_values : np.ndarray
        Model values (water level in metres or current speed in m/s).
    latitude : float
        Station latitude (decimal degrees).
    data_type : str
        ``"water_level"`` or ``"currents"``.
    station_id : str
        Station identifier (for logging only).
    obs_time : pd.DatetimeIndex, optional
        Observation timestamps — required for currents.
    obs_values : np.ndarray, optional
        Observation values — required for currents.
    accepted_constants : dict, optional
        CO-OPS accepted harmonic constants — required for water_level.
        Must contain ``"amplitudes"`` and ``"phases"`` sub-dicts mapping
        constituent names to floats (the format returned by
        ``retrieve_harmonic_constants``).
    constit : list of str, optional
        Constituent list.  Defaults to :data:`NOS_37_CONSTITUENTS`.
    min_duration_days : float, optional
        Minimum record length for HA (default 15 days).
    logger : logging.Logger, optional
        Logger instance.

    Returns
    -------
    pd.DataFrame
        Columns: ``N``, ``Constituent``, ``Ref_Amp``, ``Ref_Phase``,
        ``Model_Amp``, ``Model_Phase``, ``Amp_Diff``, ``Phase_Diff``,
        ``Vector_Diff``.

    Raises
    ------
    ValueError
        If *data_type* is invalid, or required inputs are missing.
    """
    _log = logger or logging.getLogger(__name__)

    # ------------------------------------------------------------------
    # Validate inputs
    # ------------------------------------------------------------------
    if data_type not in ('water_level', 'currents'):
        raise ValueError(
            f"data_type must be 'water_level' or 'currents', got '{data_type}'."
        )

    if data_type == 'water_level' and accepted_constants is None:
        raise ValueError(
            'accepted_constants is required for water_level data_type.'
        )

    if data_type == 'currents' and (obs_time is None or obs_values is None):
        raise ValueError(
            'obs_time and obs_values are required for currents data_type.'
        )

    if constit is None:
        constit = list(NOS_37_CONSTITUENTS)

    # ------------------------------------------------------------------
    # Model HA
    # ------------------------------------------------------------------
    _log.info('Running model HA for station %s (%s).', station_id, data_type)
    model_result = harmonic_analysis(
        time=model_time,
        values=model_values,
        latitude=latitude,
        constit=constit,
        min_duration_days=min_duration_days,
        logger=_log,
    )
    model_df = model_result['constituents']
    model_amp_map = dict(zip(model_df['Name'], model_df['Amplitude']))
    model_phase_map = dict(zip(model_df['Name'], model_df['Phase']))

    # ------------------------------------------------------------------
    # Reference constants
    # ------------------------------------------------------------------
    if data_type == 'water_level':
        assert accepted_constants is not None  # narrowed by earlier check
        ref_amp_map = accepted_constants['amplitudes']
        ref_phase_map = accepted_constants['phases']
    else:
        _log.info('Running obs HA for station %s (currents).', station_id)
        assert obs_time is not None and obs_values is not None  # narrowed above
        obs_result = harmonic_analysis(
            time=obs_time,
            values=obs_values,
            latitude=latitude,
            constit=constit,
            min_duration_days=min_duration_days,
            logger=_log,
        )
        obs_df = obs_result['constituents']
        ref_amp_map = dict(zip(obs_df['Name'], obs_df['Amplitude']))
        ref_phase_map = dict(zip(obs_df['Name'], obs_df['Phase']))

    # ------------------------------------------------------------------
    # Build arrays aligned to the canonical constituent list
    # ------------------------------------------------------------------
    ref_amps = np.array([ref_amp_map.get(c, np.nan) for c in constit])
    ref_phases = np.array([ref_phase_map.get(c, np.nan) for c in constit])
    model_amps = np.array([model_amp_map.get(c, np.nan) for c in constit])
    model_phases = np.array([model_phase_map.get(c, np.nan) for c in constit])

    # ------------------------------------------------------------------
    # Compare
    # ------------------------------------------------------------------
    comparison = compare_harmonic_constants(
        model_amp=model_amps,
        model_phase=model_phases,
        accepted_amp=ref_amps,
        accepted_phase=ref_phases,
        constituents=constit,
        logger=_log,
    )

    # ------------------------------------------------------------------
    # Rename and reorder columns to match NOS report format
    # ------------------------------------------------------------------
    comparison = comparison.rename(columns={
        'Accepted_Amp': 'Ref_Amp',
        'Accepted_Phase': 'Ref_Phase',
    })

    comparison.insert(0, 'N', np.arange(1, len(constit) + 1))

    table = comparison[
        ['N', 'Constituent', 'Ref_Amp', 'Ref_Phase',
         'Model_Amp', 'Model_Phase', 'Amp_Diff', 'Phase_Diff', 'Vector_Diff']
    ].copy()

    _log.info(
        'Constituent table built for station %s: %d constituents.',
        station_id, len(table),
    )
    return table


def write_constituent_table_csv(
    table: pd.DataFrame,
    output_path: str,
    station_id: str = '',
    data_type: str = '',
    metadata: dict[str, Any] | None = None,
    summary_stats: dict[str, Any] | None = None,
    logger: logging.Logger | None = None,
) -> None:
    """
    Write a constituent statistics table to CSV with a metadata header.

    Parameters
    ----------
    table : pd.DataFrame
        Table produced by :func:`build_constituent_table`.
    output_path : str
        Destination file path.
    station_id : str, optional
        Station identifier (written in the header).
    data_type : str, optional
        Data type label (written in the header).
    metadata : dict, optional
        Extra key/value pairs to include in the header.
    summary_stats : dict, optional
        Summary statistics from :func:`compute_constituent_summary_stats`.
        Appended as ``#`` comment lines at the bottom of the CSV.
    logger : logging.Logger, optional
        Logger instance.
    """
    _log = logger or logging.getLogger(__name__)

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    header_lines = []
    if station_id:
        header_lines.append(f'# Station: {station_id}')
    if data_type:
        header_lines.append(f'# Data Type: {data_type}')
    header_lines.append(
        f"# Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}"
    )
    if metadata:
        for key, value in metadata.items():
            header_lines.append(f'# {key}: {value}')

    # Format numeric columns to match Fortran convention:
    #   Amplitudes / vector diff: F7.4 (4 decimal places)
    #   Phases: F6.1 (1 decimal place)
    amp_cols = ['Ref_Amp', 'Model_Amp', 'Amp_Diff', 'Vector_Diff']
    phase_cols = ['Ref_Phase', 'Model_Phase', 'Phase_Diff']
    formatted = table.copy()
    for col in amp_cols:
        if col in formatted.columns:
            formatted[col] = formatted[col].map(
                lambda x: f'{x:.4f}' if pd.notna(x) else ''
            )
    for col in phase_cols:
        if col in formatted.columns:
            formatted[col] = formatted[col].map(
                lambda x: f'{x:.1f}' if pd.notna(x) else ''
            )

    with open(path, 'w', newline='', encoding='utf-8') as f:
        for line in header_lines:
            f.write(line + '\n')
        formatted.to_csv(f, index=False)

        # Append summary statistics as footer comments
        if summary_stats:
            f.write('\n')
            for key, value in summary_stats.items():
                if isinstance(value, float):
                    f.write(f'# {key}: {value:.6f}\n')
                else:
                    f.write(f'# {key}: {value}\n')

    _log.info('Constituent table written to %s.', path)


def compute_constituent_summary_stats(
    table: pd.DataFrame,
    major_constituents: tuple[str, ...] | list[str] | None = None,
) -> dict[str, Any]:
    """
    Compute summary statistics from a constituent comparison table.

    Parameters
    ----------
    table : pd.DataFrame
        Table produced by :func:`build_constituent_table`.
    major_constituents : sequence of str, optional
        Constituents considered "major".  Defaults to
        :data:`MAJOR_CONSTITUENTS`.

    Returns
    -------
    dict
        Keys: ``mean_vector_diff``, ``mean_vector_diff_major``,
        ``rmse_amp``, ``rmse_amp_major``, ``rmse_phase``,
        ``rmse_phase_major``, ``n_valid``, ``n_major_valid``.
    """
    if major_constituents is None:
        major_constituents = MAJOR_CONSTITUENTS

    vd = table['Vector_Diff'].values.astype(float)
    ad = table['Amp_Diff'].values.astype(float)
    phase_diffs = table['Phase_Diff'].values.astype(float)

    valid = np.isfinite(vd)
    n_valid = int(np.sum(valid))

    major_mask = table['Constituent'].isin(major_constituents) & valid
    n_major_valid = int(np.sum(major_mask))

    def _rmse(arr):
        finite = arr[np.isfinite(arr)]
        return float(np.sqrt(np.mean(finite ** 2))) if len(finite) > 0 else np.nan

    return {
        'mean_vector_diff': float(np.nanmean(vd)) if n_valid > 0 else np.nan,
        'mean_vector_diff_major': (
            float(np.nanmean(vd[major_mask])) if n_major_valid > 0 else np.nan
        ),
        'rmse_amp': _rmse(ad),
        'rmse_amp_major': _rmse(ad[major_mask]),
        'rmse_phase': _rmse(phase_diffs),
        'rmse_phase_major': _rmse(phase_diffs[major_mask]),
        'n_valid': n_valid,
        'n_major_valid': n_major_valid,
    }


def flag_constituent_exceedances(
    table: pd.DataFrame,
    amp_threshold: float = DEFAULT_AMP_THRESHOLD_M,
    phase_threshold: float = DEFAULT_PHASE_THRESHOLD_DEG,
    vector_diff_threshold: float = DEFAULT_VECTOR_DIFF_THRESHOLD_M,
) -> pd.DataFrame:
    """
    Flag constituents that exceed amplitude, phase, or vector-diff thresholds.

    Parameters
    ----------
    table : pd.DataFrame
        Table produced by :func:`build_constituent_table`.
    amp_threshold : float
        Maximum acceptable absolute amplitude difference (metres).
    phase_threshold : float
        Maximum acceptable absolute phase difference (degrees).
    vector_diff_threshold : float
        Maximum acceptable vector difference (metres).

    Returns
    -------
    pd.DataFrame
        Copy of *table* with an ``Exceeds_Threshold`` column containing
        comma-separated codes (``AMP``, ``PHASE``, ``VD``) or empty string.
    """
    result = table.copy()

    flags = []
    for _, row in result.iterrows():
        codes = []
        amp_diff = row.get('Amp_Diff', np.nan)
        phase_diff = row.get('Phase_Diff', np.nan)
        vd = row.get('Vector_Diff', np.nan)

        if np.isfinite(amp_diff) and abs(amp_diff) > amp_threshold:
            codes.append('AMP')
        if np.isfinite(phase_diff) and abs(phase_diff) > phase_threshold:
            codes.append('PHASE')
        if np.isfinite(vd) and vd > vector_diff_threshold:
            codes.append('VD')

        flags.append(','.join(codes))

    result['Exceeds_Threshold'] = flags
    return result
