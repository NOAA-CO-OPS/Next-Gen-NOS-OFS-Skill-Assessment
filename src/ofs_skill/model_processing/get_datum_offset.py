"""
Vertical Datum Conversion Module

This module handles vertical datum conversions for OFS model water level data.
It provides functions to:
- Retrieve datum conversion fields from S3 buckets
- Calculate datum offsets for model time series
- Generate datum conversion reports

Functions
---------
is_number : Check if a string can be converted to a number
roms_nodes : Convert ROMS node index to i,j coordinates
report_datums : Write a report summarizing datum conversions
read_vdatum_from_bucket : Read vertical datum conversion file from S3
get_datum_offset : Get the datum offset to apply to model time series

Notes
-----
Vertical datum conversions are necessary because:
- Observation data may be in different vertical datums (NAVD88, MSL, etc.)
- Model data is typically in model-specific datums (model-0, LWD, etc.)
- All data must be converted to a common datum for skill assessment

Special handling for:
- GLOFS models (LEOFS, LMHOFS, LOOFS, LSOFS) using LWD
- STOFS models using XGEOID20B
- WCOFS requiring additional MSL to model-0 conversion
- SSCOFS with unique vertical datum setup

Author: PL
Created: Fri Jun 6 09:11:51 2025
"""

import os
from logging import Logger
from typing import Any, Optional, Union

import numpy as np
import pandas as pd
import s3fs
import xarray as xr

from ofs_skill.obs_retrieval.station_ctl_file_extract import station_ctl_file_extract


def is_number(n: Any) -> bool:
    """
    Check if a value can be converted to a float.

    Parameters
    ----------
    n : Any
        Value to check for numeric conversion

    Returns
    -------
    bool
        True if value can be converted to float, False otherwise

    Examples
    --------
    >>> is_number('3.14')
    True
    >>> is_number('hello')
    False
    >>> is_number(42)
    True
    """
    try:
        float(n)   # Type-casting the string to `float`.
                   # If string is not a valid `float`,
                   # it'll raise `ValueError` exception
    except ValueError:
        return False
    return True


def roms_nodes(model: xr.Dataset, node_num: int) -> tuple[int, int]:
    """
    Convert ROMS node index to i,j coordinates.

    ROMS uses a 2D grid with i,j indexing. This function converts
    a flattened node number to the corresponding i,j indices.

    Parameters
    ----------
    model : xr.Dataset
        ROMS model dataset containing 'lon_rho' variable
    node_num : int
        Flattened node index number

    Returns
    -------
    tuple of (int, int)
        i_index, j_index coordinates in ROMS grid

    Examples
    --------
    >>> i, j = roms_nodes(model, 1234)
    >>> print(f"Node 1234 is at i={i}, j={j}")
    """
    i_index, j_index = np.unravel_index(int(node_num), np.shape(model['lon_rho']))
    return i_index, j_index


def report_datums(prop: Any, datum_offsets: list[list[Optional[float]]], logger: Logger) -> None:
    """
    Write a report summarizing datum conversions for all stations.

    Creates a CSV file in the control files directory with datum conversion
    information for each station, including success/failure status and reasons
    for any failures.

    Parameters
    ----------
    prop : ModelProperties
        ModelProperties object containing:
        - control_files_path : str
            Directory for control files
        - ofs : str
            OFS model name
        - user_input_location : bool
            True if using user-specified locations
        - datum : str
            Target vertical datum
    datum_offsets : list of list
        Nested list containing:
        - datum_offsets[0] : list of str
            Station IDs
        - datum_offsets[1] : list of float or None
            Model-to-target datum offsets in meters
    logger : Logger
        Logger instance for logging messages

    Returns
    -------
    None
        Writes report to CSV file: {ofs}_wl_datum_report.csv

    Notes
    -----
    Report includes columns:
    - Station ID
    - Station provider (e.g., 'NOAA CO-OPS')
    - Target datum
    - Model-to-target offset (m)
    - Obs source datum
    - Obs-to-target offset (m)
    - Datum conversion pass/fail
    - Reason for failure (if applicable)

    Failure codes:
    - -9999: Out of geographic range (model)
    - -9990: Error opening model vdatum netcdf
    - -9991: Target datum unavailable for model conversion
    - -9992: Error finding model XY location (field file)
    - -9993: Error finding model XY location (station file)
    - -9994: WCOFS MSL to model-0 conversion file not found

    Examples
    --------
    >>> report_datums(prop, datum_offsets, logger)
    INFO:root:Datums report written successfully!
    """
    logger.info('Starting datums report...')

    try:
        station_datums = []
        station_providers = []
        id_numbers = []
        station_datum_offsets = []
        success = []
        reason = []
        obsctl_filepath = os.path.join(prop.control_files_path,
                                       f'{prop.ofs}_wl_station.ctl')
        if os.path.isfile(obsctl_filepath) and \
            prop.user_input_location is False:
            read_station_ctl_file = station_ctl_file_extract(
                f'{prop.control_files_path}/{prop.ofs}_wl_station.ctl')
        elif os.path.isfile(obsctl_filepath) is False or \
            prop.user_input_location:
            logger.info('No obs control file found to make datum report. '
                        'No report will be written. (This is normal when '
                        'running get_node_ofs to extract model time series '
                        'only).')
            return

        for i in range(len(datum_offsets[0])):
            # First find obs row for corresponding model station
            obs_row = [y[0] for y in read_station_ctl_file[0]].\
                index(datum_offsets[0][i])
            if read_station_ctl_file[0][obs_row][0] != \
                datum_offsets[0][i]:
                raise Exception

            station_providers.append(read_station_ctl_file[0][obs_row][-1])
            id_numbers.append(read_station_ctl_file[0][obs_row][0])
            station_datums.append(read_station_ctl_file[1][obs_row][-1])
            if (is_number(read_station_ctl_file[1][obs_row][-3]) and
                datum_offsets[1][i] is not None):
                station_datum_offsets.append(read_station_ctl_file[1]
                                             [obs_row][-3])
                if datum_offsets[1][i] > -999:
                    success.append('pass')
                    reason.append(' ')
                elif datum_offsets[1][i] <= -999:
                    success.append('fail')
            elif datum_offsets[1][i] is None:
                station_datum_offsets.append(read_station_ctl_file[1]
                                             [obs_row][-3])
                success.append('NA')
                reason.append('No stations model data here, this is expected')
            else:
                success.append('fail')
                if is_number(read_station_ctl_file[1][obs_row][-3]):
                    station_datum_offsets.append(read_station_ctl_file[1]
                                                 [obs_row][-3])
                else:
                    station_datum_offsets.append(0)
            if success[i] == 'fail':
                reason_str = ''
                if read_station_ctl_file[1][obs_row][-3] == 'RANGE':
                    reason_str = reason_str + ' Out of geographic range (obs);'
                if read_station_ctl_file[1][obs_row][-3] == 'UNKNOWN':
                    reason_str = reason_str + ' Target datum is unavailable for obs conversion;'
                if datum_offsets[1][i] == -9999:
                    reason_str = reason_str + ' Out of geographic range (model);'
                if datum_offsets[1][i] == -9990:
                    reason_str = reason_str + ' Error opening model vdatum netcdf on the fly;'
                if datum_offsets[1][i] == -9991:
                    reason_str = reason_str + ' Target datum is unavailable for model conversion;'
                if datum_offsets[1][i] == -9992:
                    reason_str = reason_str + ' Error finding model XY location (field file);'
                if datum_offsets[1][i] == -9993:
                    reason_str = reason_str + ' Error finding model XY location (station file);'
                if datum_offsets[1][i] == -9994:
                    reason_str = reason_str + ' WCOFS MSL to model-0 conversion file not found;'
                reason.append(reason_str.rstrip(';').lstrip(' '))

        # Make datums report dataframe
        df_dr = pd.DataFrame({'Station ID': id_numbers,
                              'Station provider': station_providers,
                              'Target datum': prop.datum,
                              'Model-to-target offset (m)': datum_offsets[1][:],
                              'Obs source datum': station_datums,
                              'Obs-to-target offset (m)': station_datum_offsets,
                              'Datum conversion pass/fail': success,
                              'Reason for failure': reason
                              })

        df_dr['Model-to-target offset (m)'] = \
            df_dr['Model-to-target offset (m)'].round(2)
        filename_new = \
            f'{prop.control_files_path}/{prop.ofs}_wl_datum_report.csv'
        df_dr.to_csv(filename_new, header=True, index=False)

        logger.info('Datums report written successfully!')

    except Exception as e_x:
        logger.error('Cannot append datum info to ctl file! '
                     'Skipping this step. Exception: %s', e_x)


def read_vdatum_from_bucket(prop: Any, logger: Logger) -> Union[xr.Dataset, int]:
    """
    Read vertical datum conversion file from the NODD S3 bucket.

    Retrieves the OFS-specific vertical datum conversion netCDF file
    directly from the cloud without downloading.

    Parameters
    ----------
    prop : ModelProperties
        ModelProperties object containing:
        - ofs : str
            OFS model name
    logger : Logger
        Logger instance for logging messages

    Returns
    -------
    xr.Dataset or int
        Vertical datum conversion dataset, or -9990 if error occurs

    Notes
    -----
    - Uses s3fs with anonymous access to NOAA NOS OFS Public Dataset
    - Bucket: noaa-nos-ofs-pds
    - Key format: OFS_Grid_Datum/{ofs}_vdatums.nc
    - Returns error code -9990 if file cannot be opened

    Examples
    --------
    >>> vdatums = read_vdatum_from_bucket(prop, logger)
    >>> if isinstance(vdatums, int):
    ...     print("Error reading vdatum file")
    ... else:
    ...     print(f"Variables: {list(vdatums.data_vars)}")
    """
    s3 = s3fs.S3FileSystem(anon=True)
    bucket_name = 'noaa-nos-ofs-pds'
    key = f'OFS_Grid_Datum/{prop.ofs}_vdatums.nc'
    url = f's3://{bucket_name}/{key}'
    try:
        vdatums = xr.open_dataset(s3.open(url, 'rb'))
        return vdatums
    except Exception as e_x:
        logger.error('Error opening vdatums on the fly!')
        logger.error(f'Error: {e_x}')
        return -9990


def get_datum_offset(prop: Any, node: int, model: xr.Dataset,
                      id_number: str, logger: Logger) -> float:
    """
    Calculate the datum offset to apply to model water level time series.

    Determines the vertical offset needed to convert model water levels
    from the model's native vertical datum to the user-specified target datum.

    Parameters
    ----------
    prop : ModelProperties
        ModelProperties object containing:
        - datum : str
            Target vertical datum (e.g., 'NAVD88', 'MSL', 'LWD')
        - ofs : str
            OFS model name
        - ofsfiletype : str
            'stations' or 'fields'
        - model_source : str
            'fvcom', 'roms', or 'schism'
        - path : str
            Base path for auxiliary files (WCOFS MSL conversion)
    node : int
        Model node/grid index
    model : xr.Dataset
        Model dataset containing grid coordinates
    id_number : str
        Station ID for logging
    logger : Logger
        Logger instance for logging messages

    Returns
    -------
    float
        Datum offset in meters to add to model water levels
        Negative values returned for error conditions:
        - -9990: Error opening vdatum file from S3
        - -9991: Target datum unavailable in vdatum file
        - -9992: Error extracting offset for fields file
        - -9993: Error extracting offset for stations file
        - -9994: WCOFS MSL conversion file not found
        - -9999: Offset out of reasonable range

    Notes
    -----
    Special cases:
    - GLOFS (LEOFS, LMHOFS, LOOFS, LSOFS):
        * Uses LWD (Low Water Datum)
        * If datum='LWD', returns 0 (no conversion needed)
        * Otherwise converts via '{datum}toLWD' field
        * Sign is inverted except for LEOFS
    - STOFS models:
        * Native datum is XGEOID20B
        * If datum='XGEOID20B', returns 0
    - SSCOFS:
        * Model-0 is 0.23m below XGEOID20B
        * Converts via XGEOID20B as intermediate
    - WCOFS:
        * Requires additional MSL to model-0 conversion
        * Uses wcofs_msl.nc file
    - Other OFS:
        * Uses '{datum}toMSL' conversion fields

    Examples
    --------
    >>> offset = get_datum_offset(prop, 1234, model, '8638610', logger)
    >>> print(f"Datum offset: {offset:.3f} m")
    Datum offset: 0.234 m
    """
    # If doing GLOFS and using the LWD datum, no correction is necessary.
    if prop.datum.lower() == 'lwd':
        return 0
    # If doing STOFS and using the xgeoid20b datum, no correction is necessary.
    if 'stofs' in prop.ofs and prop.datum.lower() == 'xgeoid20b':
        return 0

    # If not STOFS, read the correct vdatum file from NODD S3 on-the-fly
    if 'stofs' not in prop.ofs:
        vdatums = read_vdatum_from_bucket(prop, logger)
        if isinstance(vdatums, int):
            return vdatums
    else:
        logger.info('Doing datum conversion for STOFS!')

    # Set water levels to user-specified datum
    if prop.ofs not in ['leofs', 'lmhofs', 'loofs', 'lsofs']:
        # Deal with SSCOFS separately
        if prop.ofs == 'sscofs':
            # First get from model-0 to xgeoid -- the ofs-wide offset is
            # 0.23 m, where xgeoid is 0.23 cm above model-0.
            # Then convert from xgeoid to other datums.
            try:
                datum_field1 = vdatums['xgeoid20btomsl']
                if prop.datum.lower() == 'msl':
                    datum_field = 0.23 - datum_field1
                else:
                    datum_field2 = vdatums[f'{prop.datum.lower()}tomsl']
                    datum_field = 0.23 - datum_field1 + datum_field2
            except Exception as e_x:
                logger.error(f'Datum conversion error: {e_x}')
                return -9991
        else:  # Not SSCOFS
            try:
                datum_field = vdatums[f'{prop.datum.lower()}tomsl']
                if prop.ofs == 'wcofs':
                    file = os.path.join(prop.path, 'src', 'wcofs_msl.nc')
                    try:
                        ds_wcofs = xr.open_dataset(file)
                    except FileNotFoundError:
                        logger.error('WCOFS MSL2MZ conversion not found!')
                        return -9994
                    datum_field = datum_field + np.array(ds_wcofs['MSL2MZ'])
            except Exception as e_x:
                logger.error('Wrong netcdf datum variable name!')
                logger.error(f'Error: {e_x}')
                return -9991
    else:
        try:
            datum_field = vdatums[f'{prop.datum.lower()}tolwd']
        except Exception as e_x:
            logger.error('Wrong netcdf datum variable name for GLOFS!')
            logger.error(f'Error: {e_x}')
            return -9991

    # Do stations
    if prop.ofsfiletype == 'stations':
        try:
            if prop.model_source == 'roms':
                datum_offset = float(datum_field[
                    int(np.array(model['Jpos'][0, node])),
                    int(np.array(model['Ipos'][0, node]))]
                    )
            elif prop.model_source == 'fvcom':
                # Gotta search with lat/lon here...
                vlonlat = np.around(np.array([vdatums[
                    'longitude'], vdatums['latitude']]), 3)
                target = np.around(
                    np.array([[model['lon'][0, node] - 360],
                              [model['lat'][0, node]]]), 3)
                moddistances = np.linalg.norm(vlonlat - target,
                                              axis=0)
                datum_offset = float(datum_field[int(
                    np.argmin(moddistances))])
        except Exception as e_x:
            logger.error('Error getting datum offset from datum field for '
                         'stations files and %s: %s', prop.model_source, e_x)
            return -9992

    # Do fields -- easy peasy
    elif prop.ofsfiletype == 'fields':
        try:
            if prop.model_source == 'roms':
                i_idx, j_idx = roms_nodes(model, node)
                datum_offset = float(datum_field[i_idx, j_idx])
            elif prop.model_source == 'fvcom':
                datum_offset = float(datum_field[node])
        except Exception as e_x:
            logger.error('Error getting datum offset from datum field for '
                         'fields files and %s: %s', prop.model_source, e_x)
            return -9993

    if datum_offset < -9999 or datum_offset > 9999:
        logger.error('Did not find datum offset for %s. Returning -9999.9',
                     str(id_number))
        datum_offset = -9999
    if prop.ofs in ['lmhofs', 'loofs', 'lsofs'] and datum_offset > -999:
        datum_offset = datum_offset * -1  # Switch sign for GLOFS, except leofs

    return datum_offset
