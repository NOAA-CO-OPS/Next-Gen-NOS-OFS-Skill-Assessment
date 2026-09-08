"""
Format paired one-dimensional time series data.

This module takes pandas dataframes generated from observation and model
time series and creates paired datasets for skill assessment.
"""

from datetime import datetime, timedelta
from enum import Enum
from logging import Logger

import numpy as np
import pandas as pd


class PairingStatus(str, Enum):
    """Sentinel outcomes returned by the pairing functions.

    Members are ``str`` subclasses so that plain string comparisons and
    string-typed handling elsewhere in the codebase keep working, while
    call sites that want type safety can compare against the enum members
    and let a type checker verify exhaustiveness.
    """

    #: A series was built but obs and model never share a valid timestamp.
    NO_TEMPORAL_OVERLAP = 'NoTemporalOverlap'


# Return type shared by the pairing functions: either the paired result
# tuple, or a PairingStatus sentinel explaining why no pairing was produced.
PairedResult = tuple[list[list], pd.DataFrame] | PairingStatus


def paired_scalar(
    obs_df: pd.DataFrame,
    ofs_df: pd.DataFrame,
    start_date_full: str,
    end_date_full: str,
    logger: Logger,
    lookback_hours: int = 6,
) -> PairedResult | None:
    """
    Create paired time series for scalar variables.

    Creates paired time series for scalar variables (temperature, salinity,
    water level). Previous version interpolated observations to match model
    time series, but this has been revised so that observations are no longer
    interpolated and are kept as their original values.

    Parameters
    ----------
    obs_df : pd.DataFrame
        Observation data with columns [0: julian, 1: year, 2: month, 3: day,
        4: hour, 5: minute, 6: value]
    ofs_df : pd.DataFrame
        Model data with same structure as obs_df
    start_date_full : str
        Start date in format 'YYYY-MM-DDThh:mm:ssZ' or 'YYYYMMDD-HH:MM:SS'
    end_date_full : str
        End date in format 'YYYY-MM-DDThh:mm:ssZ' or 'YYYYMMDD-HH:MM:SS'
    logger : Logger
        Logger instance for logging messages
    lookback_hours : int, optional
        Number of hours before start_date to include in paired data.
        Default is 6 hours. The lookback ensures overlap between
        consecutive casts (e.g., nowcast + forecast_a). Set to 0
        to disable the lookback.

    Returns
    -------
    Optional[PairedResult]
        On success, a tuple containing:
        - formatted_series: List of lists with paired data
        - paired: DataFrame with paired observations and model data
        Returns ``PairingStatus.NO_TEMPORAL_OVERLAP`` when both series
        exist but share no valid timestamp in the requested window, and
        ``None`` for the remaining no-valid-paired-data cases (e.g. the
        obs or model series is entirely NaN).
    """
    try:
        datetime.strptime(start_date_full, '%Y%m%d-%H:%M:%S')
        datetime.strptime(end_date_full, '%Y%m%d-%H:%M:%S')
    except ValueError:
        start_date_full = start_date_full.replace('-', '')
        end_date_full = end_date_full.replace('-', '')
        start_date_full = start_date_full.replace('Z', '')
        end_date_full = end_date_full.replace('Z', '')
        start_date_full = start_date_full.replace('T', '-')
        end_date_full = end_date_full.replace('T', '-')

    # Reading the input dataframes
    obs_df['DateTime'] = pd.to_datetime(
        dict(
            year=obs_df[1],
            month=obs_df[2],
            day=obs_df[3],
            hour=obs_df[4],
            minute=obs_df[5],
        )
    )
    obs_df = obs_df.rename(columns={6: 'OBS'})

    ofs_df['DateTime'] = pd.to_datetime(
        dict(
            year=ofs_df[1],
            month=ofs_df[2],
            day=ofs_df[3],
            hour=ofs_df[4],
            minute=ofs_df[5],
        )
    )
    ofs_df = ofs_df.rename(columns={6: 'OFS'})

    paired_0 = pd.DataFrame()
    paired_0['DateTime'] = ofs_df['DateTime']
    interplimit = 3
    filllimit = 1
    # First we concat the observations to the reference time, remove
    # duplicates, interpolate to the 6 min timestep, fill gaps, reindex
    paired_obs = pd.concat([paired_0, obs_df]).sort_values(
        by='DateTime'
    )
    paired_obs = paired_obs[
        ~paired_obs['DateTime'].duplicated(keep=False)
        | paired_obs[['OBS']].notnull().any(axis=1)
    ]
    paired_obs = (
        paired_obs.sort_values(by='DateTime')
        .set_index('DateTime')
        .astype(float)
        .interpolate(method='linear', limit=interplimit)
        .ffill(limit=filllimit)
        .bfill(limit=filllimit)
        .reset_index()
    )

    # Second we concat the ofs to the reference time, remove duplicates,
    # reindex
    paired_ofs = ofs_df[
        ~ofs_df['DateTime'].duplicated(keep=False)
    ]
    paired_ofs = (
        paired_ofs.sort_values(by='DateTime')
        .set_index('DateTime')
        .astype(float)
        .reset_index()
    )

    # Third we merge the observations onto the ofs series. Merge on
    # DateTime alone (as paired_vector does): the numbered date columns are redundant with
    # DateTime, and using the float julian column [0] as a merge key
    # made pairing silently fail whenever the obs and model files were
    # written with different julian rounding (e.g. a cached series from
    # before the issue #200 precision fix meeting a fresh one).
    paired = pd.merge(
            paired_ofs,
            paired_obs[['DateTime', 'OBS']],
            on=['DateTime'],
            how='left'
    )

    if paired['OBS'].isna().all():
        logger.error('All OBS values are NaN in paired data - returning None')
        return None
    if paired['OFS'].isna().all():
        logger.error('All OFS values are NaN in paired data - returning None')
        return None

    paired['OBS'] = paired['OBS'].fillna(np.nan)

    cols = list(paired.columns)
    obs_index = cols.index('OBS')
    ofs_index = cols.index('OFS')

    cols.insert(ofs_index, cols.pop(obs_index))
    paired = paired[cols]

    if paired.dropna(subset=['OBS', 'OFS']).empty:
        logger.error('No valid paired OBS/OFS data after dropping NaN - returning None')
        logger.error('OBS range: %s to %s', paired['OBS'].min(), paired['OBS'].max())
        logger.error('OFS range: %s to %s', paired['OFS'].min(), paired['OFS'].max())
        # Distinct sentinel so the caller can attribute this drop to the
        # specific "no overlapping valid timestamps" cause (keep + warn per
        # issue #200) rather than lumping it in with missing-file failures.
        return PairingStatus.NO_TEMPORAL_OVERLAP

    paired = paired.reset_index()

    # Then we create the bias and mask for start and end time
    paired['BIAS'] = paired['OFS'] - paired['OBS']

    # Lookback ensures overlap between consecutive casts (e.g., nowcast + forecast_a)
    paired = paired.loc[
        (
            (
                paired['DateTime']
                >= datetime.strptime(start_date_full, '%Y%m%d-%H:%M:%S') - timedelta(hours=lookback_hours)
            )
            & (
                paired['DateTime']
                <= datetime.strptime(end_date_full, '%Y%m%d-%H:%M:%S')
            )
        )
    ]
    # Finally, we write the file and return the results
    paired = paired.drop(columns=['index', 'DateTime'])
    paired = paired.astype({0: float, 1: int, 2: int, 3: int, 4: int, 5: int,
                            'OBS': float, 'OFS': float, 'BIAS': float})
    formatted_series = list(map(list, paired.itertuples(index=False)))

    return formatted_series, paired


def get_distance_angle(ofs_angle: float, obs_angle: float) -> float:
    """
    Calculate the angular difference between two angles.

    This function gives the difference between angles (ofs-obs) and handles
    the 0-360 degrees wraparound problem.

    Parameters
    ----------
    ofs_angle : float
        Model angle in degrees (0-360)
    obs_angle : float
        Observation angle in degrees (0-360)

    Returns
    -------
    float
        Signed angular difference in degrees, where positive means the model
        angle is greater (clockwise) than the observation angle
    """
    phi = abs(obs_angle - ofs_angle) % 360
    sign = 1
    # This is used to calculate the sign
    if not (
        (ofs_angle - obs_angle >= 0 and ofs_angle - obs_angle <= 180)
        or (ofs_angle - obs_angle <= -180 and ofs_angle - obs_angle >= -360)
    ):
        sign = -1
    if phi > 180:
        result = 360 - phi
    else:
        result = phi

    return result * sign


def paired_vector(
    obs_df: pd.DataFrame,
    ofs_df: pd.DataFrame,
    start_date_full: str,
    end_date_full: str,
    logger: Logger,
    lookback_hours: int = 6,
) -> tuple[list[list], pd.DataFrame] | None:
    """
    Create paired time series for vector variables.

    Creates paired time series for vector variables (currents). Previous version
    interpolated observations to match model time series, but this has been
    revised so that observations are no longer interpolated and are kept as
    their original values.

    Parameters
    ----------
    obs_df : pd.DataFrame
        Observation data with columns [0: julian, 1: year, 2: month, 3: day,
        4: hour, 5: minute, 6: speed, 7: direction, 8: u, 9: v]
    ofs_df : pd.DataFrame
        Model data with same structure as obs_df
    start_date_full : str
        Start date in format 'YYYY-MM-DDThh:mm:ssZ' or 'YYYYMMDD-HH:MM:SS'
    end_date_full : str
        End date in format 'YYYY-MM-DDThh:mm:ssZ' or 'YYYYMMDD-HH:MM:SS'
    logger : Logger
        Logger instance for logging messages
    lookback_hours : int, optional
        Number of hours before start_date to include in paired data.
        Default is 6 hours. The lookback ensures overlap between
        consecutive casts (e.g., nowcast + forecast_a). Set to 0
        to disable the lookback.

    Returns
    -------
    Optional[Tuple[List[List], pd.DataFrame]]
        Tuple containing:
        - formatted_series: List of lists with paired data
        - paired: DataFrame with paired observations and model data
        Returns None if no valid paired data is found
    """
    try:
        datetime.strptime(start_date_full, '%Y%m%d-%H:%M:%S')
        datetime.strptime(end_date_full, '%Y%m%d-%H:%M:%S')
    except ValueError:
        start_date_full = start_date_full.replace('-', '')
        end_date_full = end_date_full.replace('-', '')
        start_date_full = start_date_full.replace('Z', '')
        end_date_full = end_date_full.replace('Z', '')
        start_date_full = start_date_full.replace('T', '-')
        end_date_full = end_date_full.replace('T', '-')

    # Reading the input dataframes
    obs_df['DateTime'] = pd.to_datetime(
        dict(
            year=obs_df[1],
            month=obs_df[2],
            day=obs_df[3],
            hour=obs_df[4],
            minute=obs_df[5],
        )
    )
    obs_df = obs_df.rename(columns={6: 'OBS',
                                    7: 'OBS_DIR',
                                    8: 'OBS_U',
                                    9: 'OBS_V'})

    ofs_df['DateTime'] = pd.to_datetime(
        dict(
            year=ofs_df[1],
            month=ofs_df[2],
            day=ofs_df[3],
            hour=ofs_df[4],
            minute=ofs_df[5],
        )
    )
    ofs_df = ofs_df.rename(columns={6: 'OFS',
                                    7: 'OFS_DIR',
                                    8: 'OFS_U',
                                    9: 'OFS_V'})

    paired_end_time = datetime.strptime(end_date_full,
        '%Y%m%d-%H:%M:%S').replace(
            second=0,
            microsecond=0,
            minute=0,
            )
    paired_end_time = paired_end_time + timedelta(hours=1)

    paired_0 = pd.DataFrame()
    paired_0['DateTime'] = ofs_df['DateTime']
    interplimit = 3
    filllimit = 1
    # First we concat the observations to the reference time, remove
    # duplicates, interpolate to the 6 min timestep, fill gaps, reindex
    paired_obs = pd.concat([paired_0, obs_df]).sort_values(
        by='DateTime'
    )
    paired_obs = paired_obs[
        ~paired_obs['DateTime'].duplicated(keep=False)
        | paired_obs[['OBS']].notnull().any(axis=1)
    ]
    paired_obs = (
        paired_obs.sort_values(by='DateTime')
        .set_index('DateTime')
        .astype(float)
        .interpolate(method='linear', limit=interplimit)
        .ffill(limit=filllimit)
        .bfill(limit=filllimit)
        .reset_index()
    )

    # Second we concat the ofs to the reference time, remove duplicates,
    # reindex
    paired_ofs = ofs_df[
        ~ofs_df['DateTime'].duplicated(keep=False)
    ]
    paired_ofs = (
        paired_ofs.sort_values(by='DateTime')
        .set_index('DateTime')
        .astype(float)
        .reset_index()
    )

    # Third we merge the observations onto the ofs series on DateTime
    paired = pd.merge(
        paired_ofs,
        paired_obs[['DateTime', 'OBS', 'OBS_DIR', 'OBS_U', 'OBS_V']],
        on=['DateTime'],
        how='left'
    )

    paired = paired.reset_index()

    # Then we create the speed bias and mask for start and end time
    paired['SPD_BIAS'] = paired['OFS'] - paired['OBS']
    # Lookback ensures overlap between consecutive casts (e.g., nowcast + forecast_a)
    paired = paired.loc[
        (
            (
                paired['DateTime']
                >= datetime.strptime(start_date_full, '%Y%m%d-%H:%M:%S') - timedelta(hours=lookback_hours)
            )
            & (
                paired['DateTime']
                <= datetime.strptime(end_date_full, '%Y%m%d-%H:%M:%S')
            )
        )
    ]
    # Here we create the numpy arrays that will be used in the paired
    # timeseries file

    # This is the direction bias
    ofs_dir = paired['OFS_DIR'].to_numpy()
    obs_dir = paired['OBS_DIR'].to_numpy()
    dir_bias = []
    for j in range(len(paired)):
        dir_bias.append(get_distance_angle(ofs_dir[j], obs_dir[j]))
    paired['DIR_BIAS'] = dir_bias
    # Finally, we write the file and return the results
    paired = paired.drop(columns=['index', 'DateTime', 'OBS_U', 'OBS_V',
                                    'OFS_U', 'OFS_V'])
    paired = paired[[0, 1, 2, 3, 4, 5, 'OBS', 'OFS', 'SPD_BIAS', 'OBS_DIR',
                      'OFS_DIR', 'DIR_BIAS']]
    paired = paired.astype({0: float, 1: int, 2: int, 3: int, 4: int, 5: int,
                            'OBS': float, 'OFS': float, 'SPD_BIAS': float,
                            'OBS_DIR': float, 'OFS_DIR': float,
                            'DIR_BIAS': float})
    formatted_series = list(map(list, paired.itertuples(index=False)))

    return formatted_series, paired
