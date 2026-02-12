"""
Format paired one-dimensional time series data.

This module takes pandas dataframes generated from observation and model
time series and creates paired datasets for skill assessment.
"""

from datetime import datetime, timedelta
from logging import Logger
from typing import Optional

import numpy as np
import pandas as pd


def paired_scalar(
    obs_df: pd.DataFrame,
    ofs_df: pd.DataFrame,
    start_date_full: str,
    end_date_full: str,
    logger: Logger,
) -> Optional[tuple[list[list], pd.DataFrame]]:
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
    # interpolate to the 6 min timestep, fill gaps, reindex
    paired_ofs = pd.concat([paired_0, ofs_df]).sort_values(
        by='DateTime'
    )
    paired_ofs = paired_ofs[
        ~paired_ofs['DateTime'].duplicated(keep=False)
        | paired_ofs[['OFS']].notnull().any(axis=1)
    ]
    paired_ofs = (
        paired_ofs.sort_values(by='DateTime')
        .set_index('DateTime')
        .astype(float)
        .interpolate(method='linear')
        .ffill()
        .bfill()
        .reset_index()
    )

    # Third we concat the observations to the ofs, group so same times
    # are combined, drop nan, reindex
    paired = pd.merge(
            paired_ofs,
            paired_obs[['DateTime', 'OBS', 0, 1, 2, 3, 4, 5]],
            on=['DateTime', 0, 1, 2, 3, 4, 5],
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
        return None

    paired = paired.reset_index()

    # Then we create the speed bias, mask for start and end time and
    # create julian
    paired['BIAS'] = paired['OFS'] - paired['OBS']

    paired = paired.loc[
        (
            (
                paired['DateTime']
                >= datetime.strptime(start_date_full, '%Y%m%d-%H:%M:%S')
            )
            & (
                paired['DateTime']
                <= datetime.strptime(end_date_full, '%Y%m%d-%H:%M:%S')
            )
        )
    ]
    julian = (
        pd.array(paired['DateTime']).to_julian_date()
        - pd.Timestamp(
            datetime.strptime(
                str(datetime.strptime(start_date_full,
                                      '%Y%m%d-%H:%M:%S').year), '%Y'
            )
        ).to_julian_date()
    )

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
) -> Optional[tuple[list[list], pd.DataFrame]]:
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

    # This is the reference time:
    paired_start_time = datetime.strptime(start_date_full,
                                          '%Y%m%d-%H:%M:%S').replace(
        second=0,
        microsecond=0,
        minute=0,
        hour=datetime.strptime(start_date_full,
                               '%Y%m%d-%H:%M:%S').hour,
    )

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
    # interpolate to the 6 min timestep, fill gaps, reindex
    paired_ofs = pd.concat([paired_0, ofs_df]).sort_values(
        by='DateTime'
    )
    paired_ofs = paired_ofs[
        ~paired_ofs['DateTime'].duplicated(keep=False)
        | paired_ofs[['OFS']].notnull().any(axis=1)
    ]
    paired_ofs = (
        paired_ofs.sort_values(by='DateTime')
        .set_index('DateTime')
        .astype(float)
        .interpolate(method='linear')
        .ffill()
        .bfill()
        .reset_index()
    )

    # Third we concat the observations to the ofs, group so same times
    # are combined, drop nan, reindex
    paired = pd.merge(
        paired_ofs,
        paired_obs[['DateTime', 'OBS', 'OBS_DIR', 'OBS_U', 'OBS_V']],
        on=['DateTime'],
        how='left'
    )

    paired = paired.reset_index()

    # Then we create the speed bias, mask for start and end time and
    # create julian
    paired['SPD_BIAS'] = paired['OFS'] - paired['OBS']
    paired = paired.loc[
        (
            (
                paired['DateTime']
                >= datetime.strptime(start_date_full, '%Y%m%d-%H:%M:%S')
            )
            & (
                paired['DateTime']
                <= datetime.strptime(end_date_full, '%Y%m%d-%H:%M:%S')
            )
        )
    ]
    julian = (
        pd.array(paired['DateTime']).to_julian_date()
        - pd.Timestamp(
            datetime.strptime(
                str(datetime.strptime(start_date_full,
                                      '%Y%m%d-%H:%M:%S').year), '%Y'
            )
        ).to_julian_date()
    )

    # Here we create the numpy arrays that will be used in the paired
    # timeseries file

    # This is the direction bias
    dir_bias = []
    for j in range(len(julian)):
        dir_bias.append(
            get_distance_angle(
                paired['OFS_DIR'].to_numpy()[j],
                paired['OBS_DIR'].to_numpy()[j]
            )
        )
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
