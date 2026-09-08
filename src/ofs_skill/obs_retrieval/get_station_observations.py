"""
-*- coding: utf-8 -*-

Documentation for Scripts get_station_observations.py

Directory Location:   /path/to/ofs_dps/server/bin/obs_retrieval

Technical Contact(s): Name:  FC

Abstract:

   This is the final station observation data function.
   This function calls the Tides and Currents, NDBC, USGS, and CHS retrieval
   function in loop for all stations found in the
   ofs_inventory_stations(OFS, Start_Date, End_Date, Path) and variables
   ['water_level', 'water_temperature', 'salinity', 'currents'].
   The output is a .obs file for each station with DateTime and OBS
   and a final control file (.ctl)

Language:  Python 3.8

Estimated Execution Time: <10min

Scripts/Programs Called:
1) ofs_inventory_stations(OFS, Start_Date, End_Date, Path)
   This script is only called if inventory_all_{OFS}.csv is not found
   in SCI_SA/Control_Files directory
2) retrieve_t_and_c_station(Station, Start_Date, End_Date, Variable, Datum)
    This script is used to retrieve Tides and Currents station data
3) retrieve_ndbc_year_station(Station, Year, Variable)
    This script is used to retrieve NDBC station data that is stored as
    yearly files
4) retrieve_NDBC_month_station(Station, Year, Variable, Month_Num, Month)
    This script is used to retrieve NDBC station data that is stored as
    monthly files
5) retrieve_NDBC_RT_station(Station, Year, Variable, Month_Num, Month)
    This script is used to retrieve the most recent (up to real time) NDBC
    station data.
6) retrieve_usgs_station(Station, Start_Date, End_Date, Variable, Datum)
    This script is used to retrieve USGS station data
7) write_obs_ctlfile((Start_Date, End_Date, Datum, Path, OFS))
    This script is used in case the station control file is not found
8) station_ctl_file_extract(ctlfile_Path)
    This script is used to read the station control file and extract the
    necessary information
9) scalar() and vector() from format_obs_timeseries module
    These functions are used to format the time series that will be saved

usage: python write_obs_ctlfile.py

 ofs write Station Control File

optional arguments:
  -h, --help            show this help message and exit
  -o OFS, --ofs OFS     Choose from the list on the ofs_Extents folder, you
                        can also create your own shapefile, add it top the
                        ofs_Extents folder and call it here
  -p PATH, --path PATH  Inventary File path
  -s STARTDATE_FULL, --StartDate_full STARTDATE_FULL
                        Start Date_full YYYYMMDD-hh:mm:ss e.g.
                        '20220115-05:05:05'
  -e ENDDATE_FULL, --EndDate_full ENDDATE_FULL
                        End Date_full YYYYMMDD-hh:mm:ss e.g.
                        '20230808-05:05:05'
  -d DATUM, --datum DATUM
                        datum: 'MHHW', 'MHW', 'MTL', 'MSL', 'DTL', 'MLW',
                        'MLLW', 'NAVD', 'IGLD', 'LWD', 'STND'

Output:
1) station_timeseries
    /data/observations/1d_station
    .obs file with DateTime, Depth of observation, Observed variable for
    each station found
2) station_control_file
    /Control_Files
    .ctl file that has the final station information including station name,
    id, lat, lon, datum, depth
3) observation data
    /data/observations/1d_station
    .obs file that has all the observations from start date to end date

Author Name:  FC       Creation Date:  08/04/2023

Revisions:
    Date          Author             Description
    07-20-2023    MK           Modified the scripts to add config,
                                logging, try/except and argparse features
    08-01-2023    FC   Modified this script to be get data
                                       from station control file ONLY
    09-06-2023    MK       Modified the code to match PEP-8 standard.
    08-26-2024    AJK            Fix issues with OS path conventions.

"""
import copy
import logging
import logging.config
import os
import socket
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from ofs_skill.obs_retrieval import (
    retrieve_properties,
    scalar,
    utils,
    vector,
)
from ofs_skill.obs_retrieval.currents_bins_override import (
    split_virtual_currents_id,
)
from ofs_skill.obs_retrieval.retrieve_chs_station import retrieve_chs_station
from ofs_skill.obs_retrieval.retrieve_ndbc_station import retrieve_ndbc_station
from ofs_skill.obs_retrieval.retrieve_t_and_c_station import (
    configure_coops_rate_limit,
    retrieve_t_and_c_station,
)
from ofs_skill.obs_retrieval.retrieve_usgs_station import retrieve_usgs_station
from ofs_skill.obs_retrieval.station_ctl_file_extract import station_ctl_file_extract
from ofs_skill.obs_retrieval.utils import get_parallel_config
from ofs_skill.obs_retrieval.write_obs_ctlfile import write_obs_ctlfile
from ofs_skill.utils import cache_manifest
from ofs_skill.utils.file_headers import series_header

# Import directly from module to avoid circular import

# parse_arguments_to_list is now in utils module

TIMEOUT_SEC = 120 # default API timeout in seconds
socket.setdefaulttimeout(TIMEOUT_SEC)

# Variable -> ctl/obs/prd name-part mapping. Single source of truth so the
# obs, ctl-manifest, and reuse gates agree on the ``wl``/``temp``/``salt``/
# ``cu`` suffixes rather than re-deriving them inline at each site.
_VAR_TO_NAME = {
    'water_level': 'wl',
    'water_temperature': 'temp',
    'salinity': 'salt',
    'currents': 'cu',
}

# Serializes the station ctl file build across variable threads.
# ``write_obs_ctlfile`` creates the ctl files for ALL variables in a
# single pass, so under parallel_variables every variable thread that
# finds its own ctl file missing would otherwise launch the same full
# build concurrently — quadruplicating the CO-OPS currents scan.
_ctl_create_lock = threading.Lock()

def parameter_validation(argu_list, datum_list, logger):
    """ Parameter validation """

    start_date, end_date, path, ofs, ofs_extents_path, datum, var_list = (
        str(argu_list[0]),
        str(argu_list[1]),
        str(argu_list[2]),
        str(argu_list[3]),
        str(argu_list[4]),
        str(argu_list[5]),
        argu_list[6],
        )

    # start_date and end_date validation
    try:
        start_dt = datetime.strptime(start_date,
                                     '%Y-%m-%dT%H:%M:%SZ')
        end_dt = datetime.strptime(end_date,
                                   '%Y-%m-%dT%H:%M:%SZ')
    except ValueError as ex:
        error_message = f"""Error: {str(ex)}. Please check Start Date -
        {start_date}, End Date - '{end_date}'. Abort!"""
        logger.error(error_message)
        print(error_message)
        sys.exit(-1)

    if start_dt > end_dt:
        error_message = f"""End Date {end_date} is before Start Date
        {start_date}. Abort!"""
        logger.error(error_message)
        sys.exit(-1)

    # path validation
    if not os.path.exists(ofs_extents_path):
        error_message = f"""ofs_extents/ folder is not found. Please
        check path - {path}. Abort!"""
        logger.error(error_message)
        sys.exit(-1)

    # ofs validation
    if not os.path.isfile(f'{ofs_extents_path}/{ofs}.shp'):
        error_message = f"""Shapefile {ofs}.shp is not found at the
        folder {ofs_extents_path}. Abort!"""
        logger.error(error_message)
        sys.exit(-1)

    # datum validation
    if datum not in datum_list:
        error_message = f'Datum {datum} is not valid. Abort!'
        logger.error(error_message)
        sys.exit(-1)

    # Handle variable input argument
    correct_var_list = ['water_level','water_temperature',
                        'salinity','currents']
    list_diff = list(set(var_list) - set(correct_var_list))
    if len(list_diff) != 0:
        logger.error('Incorrect inputs to variable selection argument: %s. '
                     'Please use %s. Exiting...', list_diff,
                     correct_var_list)
        sys.exit(-1)

def is_number(n):
    try:
        float(n)   # Type-casting the string to `float`.
                   # If string is not a valid `float`,
                   # it'll raise `ValueError` exception
    except ValueError:
        return False
    return True


def _split_virtual_currents_id(sid: str) -> tuple[str, int | None]:
    """Backwards-compat shim around :func:`split_virtual_currents_id`.

    Existing tests import this private name; the canonical helper now
    lives in ``currents_bins_override`` alongside the virtual-ID grammar.
    """
    return split_virtual_currents_id(sid)

def _apply_datum_shift(
    timeseries, variable, station_id, source, ofs, datum, datum_list,
    datum_shift, retrieve_input, logger, control_files_path, config_file=None,
):
    """Apply datum shift to water_level timeseries, with CO-OPS multi-datum fallback.

    For CO-OPS/TC sources, if the initial retrieval failed (timeseries is not
    a DataFrame), this tries all known datums in sequence until data is found.
    Then, if a numeric datum_shift is specified, it is added to the OBS column.

    Parameters
    ----------
    timeseries : pd.DataFrame or None
        The retrieved observation timeseries.
    variable : str
        Must be 'water_level' (caller should only call for water_level).
    station_id : str
        Station identifier.
    source : str
        Data source label (TC, COOPS, CO-OPS, USGS, NDBC, CHS, etc.).
    ofs : str
        OFS name.
    datum : str
        Primary datum string.
    datum_list : list
        List of accepted datums.
    datum_shift : str or float
        Datum shift value from station metadata.
    retrieve_input : object
        RetrieveProperties instance (used only for CO-OPS fallback).
    logger : logging.Logger
        Logger instance.

    Returns
    -------
    pd.DataFrame or None
        The timeseries with datum shift applied (if applicable).
    """
    # CO-OPS multi-datum fallback: try alternative datums when primary fails
    if source in ('TC', 'TAC', 'COOPS', 'CO-OPS'):
        if isinstance(timeseries, pd.DataFrame) is False:
            all_datums = [
                'NAVD', 'MSL', 'MLLW', 'IGLD', 'LWD',
                'MHHW', 'MHW', 'MTL', 'DTL', 'MLW', 'STND',
            ]
            accepted_datums = datum_list
            length = len(all_datums)
            for dat in range(0, length):
                try:
                    logger.info('Trying different datum '
                                'for CO-OPS retrieval: %s',
                                all_datums[dat])
                    retrieve_input.station = str(station_id)
                    retrieve_input.variable = variable
                    retrieve_input.datum = all_datums[dat]
                    timeseries = retrieve_t_and_c_station(
                        retrieve_input, logger, control_files_path,
                        config_file=config_file,
                    )
                    if (timeseries is not None and
                            all_datums[dat] in accepted_datums):
                        logger.info(
                            'This (%s) is the datum for which '
                            'data was found. '
                            "If that's not what was expected, "
                            'please revise.',
                            all_datums[dat],
                        )
                        break
                except ValueError:
                    logger.info(
                        'Fail # %s when when trying '
                        'multiple datums:'
                        'COOPS %s data for station %s '
                        'and datum %s',
                        dat, variable,
                        station_id, all_datums[dat]
                    )
                    pass

    # Apply numeric datum shift
    if is_number(datum_shift):
        timeseries['OBS'] = timeseries['OBS'] + float(datum_shift)
        logger.info(
            'A datum shift of '
            '%s meters was '
            'applied to the water '
            'level data for station '
            '%s (%s) as '
            'specified in '
            '%s_wl_station.',
            datum_shift,
            str(station_id),
            source,
            ofs
        )
    else:
        logger.info(
            'No datum shift was read '
            'for station %s (%s) '
            'from %s_wl_station).',
            str(station_id),
            source,
            ofs
        )

    return timeseries


def _format_timeseries(timeseries, variable, start_date_full, end_date_full):
    """Dispatch timeseries formatting based on variable type.

    Parameters
    ----------
    timeseries : pd.DataFrame
        The observation timeseries to format.
    variable : str
        Variable name (water_level, water_temperature, salinity, currents).
    start_date_full : str
        Full start date string (YYYYMMDD-HH:MM:SS).
    end_date_full : str
        Full end date string (YYYYMMDD-HH:MM:SS).

    Returns
    -------
    list
        Formatted timeseries lines.
    """
    if variable == 'currents':
        return vector(timeseries, start_date_full, end_date_full)
    else:
        return scalar(timeseries, start_date_full, end_date_full)


def _fetch_and_format_station(
    station_info, station_metadata, variable, name_var, datum, datum_list,
    start_date, end_date, start_date_full, end_date_full, ofs,
    data_observations_1d_station_path, logger, control_files_path, config_file=None,
    obs_signature=None,
):
    """Fetch observation data for a single station, format it, and write .obs file.

    This is the worker function called from ThreadPoolExecutor. Each
    invocation creates its own RetrieveProperties instance to avoid
    race conditions on shared mutable state.

    Parameters
    ----------
    station_info : list
        Row from read_station_ctl_file[0][i] — [id, ..., ..., source, ...]
    station_metadata : list
        Row from read_station_ctl_file[1][i] — [..., ..., datum_shift, ...]
    variable : str
        Variable name (water_level, water_temperature, salinity, currents)
    name_var : str
        Short variable name (wl, temp, salt, cu)
    datum : str
        Datum string
    datum_list : list
        List of accepted datums
    start_date, end_date : str
        Date strings (YYYYMMDD)
    start_date_full, end_date_full : str
        Full date strings (YYYYMMDD-HH:MM:SS)
    ofs : str
        OFS name
    data_observations_1d_station_path : str
        Output directory for .obs files
    logger : logging.Logger
        Logger instance

    Returns
    -------
    str or None
        Station ID on success, None on failure.
    """
    try:
        station_id = station_info[0]
        source = station_info[3]
        # ``datum_list`` arrives as a parameter, read from config once by
        # the caller — no per-station config re-read here.

        # Each worker gets its own RetrieveProperties — critical for
        # thread safety since the object carries mutable request state.
        retrieve_input = retrieve_properties.RetrieveProperties()

        formatted_series = 'NoDataFound'

        if source in ('TC', 'TAC', 'COOPS', 'CO-OPS'):
            try:
                # For CO-OPS currents, the CTL station ID may be a
                # virtual bin ID ``{parent}_b{NN}``. Retrieve against
                # the parent station and select the matching bin.
                parent_id, bin_num = (
                    _split_virtual_currents_id(station_id)
                    if variable == 'currents' else (str(station_id), None)
                )
                # Find the retrieval datum: pick the first station-native
                # datum (from ``station_metadata``) that is also one of the
                # configured CO-OPS-servable datums in ``datum_list``. The
                # observations are pulled at this native datum and a
                # downstream shift re-references them to the requested datum.
                datum_set = set(datum_list)
                # First native datum present in the configured set, else None.
                common_value = next(
                    (x for x in station_metadata if x in datum_set), None)
                if common_value == 'NAVD88':
                    common_value = 'NAVD'
                if common_value == 'IGLD85':
                    common_value = 'IGLD'
                if common_value is None:
                    # No station-native datum matched the configured set;
                    # fall back to the originally-requested datum.
                    logger.info(
                        'No native datum for station %s matched the '
                        'configured datum_list; falling back to requested '
                        'datum %s', station_id, datum)
                    common_value = datum

                retrieve_input.station = parent_id
                retrieve_input.start_date = start_date
                retrieve_input.end_date = end_date
                retrieve_input.variable = variable
                retrieve_input.datum = common_value

                # For currents, restrict retrieval to the one bin this ctl
                # row asked for. Without ``only_bins`` the retrieval walks
                # every bin of the parent ADCP (30-day chunks each) and this
                # caller keeps one — K bins per station meant K× the CO-OPS
                # requests per row, all through the shared rate limiter.
                only_bins = {bin_num} if bin_num is not None else None
                timeseries = retrieve_t_and_c_station(
                    retrieve_input, logger,
                    control_files_path,
                    config_file=config_file,
                    only_bins=only_bins,
                    )

                if (only_bins is not None
                        and isinstance(timeseries, dict)
                        and bin_num not in timeseries):
                    # The pinned bin returned nothing. Preserve the legacy
                    # any-available-bin fallback by retrying unrestricted;
                    # rare path, so the extra fetch only happens when the
                    # requested bin is genuinely absent.
                    logger.warning(
                        'CO-OPS currents bin %s of station %s returned no '
                        'data with a restricted fetch; retrying across all '
                        'bins.', bin_num, station_id)
                    timeseries = retrieve_t_and_c_station(
                        retrieve_input, logger,
                        control_files_path,
                        config_file=config_file,
                        )

                if variable == 'currents' and isinstance(timeseries, dict):
                    # Pick out the requested bin. When no bin suffix was
                    # encoded (legacy inventory) fall back to the first
                    # available bin so the pipeline keeps producing data.
                    if bin_num is not None and bin_num in timeseries:
                        timeseries = timeseries[bin_num]
                        logger.info('Picked currents bin successfully!')
                    elif bin_num is not None and timeseries:
                        # Virtual bin row whose exact bin is still absent
                        # after the unrestricted retry. Substituting another
                        # bin here would publish that bin's data under this
                        # row's virtual ID and depth, silently corrupting
                        # the skill statistics — skip the station instead.
                        logger.error(
                            'CO-OPS currents bin %s not found for station '
                            '%s (available: %s); skipping station rather '
                            'than substituting another bin\'s data.',
                            bin_num, station_id,
                            sorted(timeseries.keys()))
                        return None
                    elif timeseries:
                        fallback_key = sorted(timeseries.keys())[0]
                        logger.warning(
                            'CO-OPS currents bin %s not found for station '
                            '%s (available: %s); using bin %s.',
                            bin_num, station_id,
                            sorted(timeseries.keys()), fallback_key)
                        timeseries = timeseries[fallback_key]
                    else:
                        timeseries = None

                if timeseries is None:
                    logger.info(
                        'Fail first try to extract '
                        'CO-OPS %s data for '
                        'station %s', variable,
                        str(station_id))
                    return None
                else:
                    timeseries = timeseries[timeseries['OBS'].notna()]

                if variable == 'water_level':
                    datum_shift = station_metadata[2]
                    timeseries = _apply_datum_shift(
                        timeseries, variable, station_id, source,
                        ofs, datum, datum_list, datum_shift,
                        retrieve_input, logger, control_files_path, config_file=config_file,
                    )

                formatted_series = _format_timeseries(
                    timeseries, variable, start_date_full, end_date_full
                )
            except Exception as e_x:
                logger.error('Fail when getting COOPS %s data for '
                             'station %s', variable, station_id)
                logger.error('Caught an exception: %s', e_x)

        elif source == 'USGS':
            try:
                retrieve_input.station = str(station_id)
                retrieve_input.start_date = start_date
                retrieve_input.end_date = end_date
                retrieve_input.variable = variable
                retrieve_input.datum = datum
                timeseries = retrieve_usgs_station(
                    retrieve_input, logger
                )
                if timeseries is None:
                    return None
                timeseries = timeseries[timeseries['OBS'].notna()]

                if variable == 'water_level':
                    datum_shift = station_metadata[2]
                    timeseries = _apply_datum_shift(
                        timeseries, variable, station_id, source,
                        ofs, datum, datum_list, datum_shift,
                        retrieve_input, logger, control_files_path, config_file=config_file,
                    )

                formatted_series = _format_timeseries(
                    timeseries, variable, start_date_full, end_date_full
                )
            except Exception as e_x:
                logger.error('Fail when getting USGS '
                             '%s data for station %s',
                             variable, station_id)
                logger.error('Caught an exception: %s', e_x)

        elif source == 'NDBC':
            try:
                data_station = retrieve_ndbc_station(
                    start_date, end_date, str(station_id),
                    variable, logger
                )

                if data_station is None:
                    return None
                timeseries = data_station
                timeseries = timeseries[timeseries['OBS'].notna()]

                if variable == 'water_level':
                    datum_shift = station_metadata[2]
                    timeseries = _apply_datum_shift(
                        timeseries, variable, station_id, source,
                        ofs, datum, datum_list, datum_shift,
                        retrieve_input, logger, control_files_path, config_file=config_file,
                    )

                formatted_series = _format_timeseries(
                    timeseries, variable, start_date_full, end_date_full
                )
            except Exception as e_x:
                logger.error('Fail when getting NDBC %s '
                             'data for station %s',
                             variable, station_id)
                logger.error('Caught an exception: %s', e_x)

        elif source == 'CHS':
            try:
                data_station = retrieve_chs_station(
                    start_date, end_date, str(station_id),
                    variable, logger
                )

                if data_station is None:
                    return None
                timeseries = data_station
                timeseries = timeseries[timeseries['OBS'].notna()]

                if variable == 'water_level':
                    datum_shift = station_metadata[2]
                    timeseries = _apply_datum_shift(
                        timeseries, variable, station_id, source,
                        ofs, datum, datum_list, datum_shift,
                        retrieve_input, logger, control_files_path, config_file=config_file,
                    )

                formatted_series = _format_timeseries(
                    timeseries, variable, start_date_full, end_date_full
                )
            except Exception as e_x:
                logger.error('Fail when getting CHS %s '
                             'data for station %s',
                             variable, station_id)
                logger.error('Caught an exception: %s', e_x)

        # Write the .obs file
        try:
            if formatted_series != 'NoDataFound':
                # FIX: Check for data before stamping the file
                if len(formatted_series) > 0:
                    obs_path = os.path.join(
                        data_observations_1d_station_path,
                        str(station_id + '_' + ofs + '_' +
                            name_var + '_station.obs'))
                    with open(obs_path, 'w', encoding='utf-8') as output:
                        output.write(series_header(name_var))
                        for line in formatted_series:
                            output.write(str(line) + '\n')
                        logger.info(
                            '%s_%s_%s_station.obs created successfully',
                            station_id, ofs, name_var
                        )
                    # Stamp the run signature so a same-parameter rerun
                    # reuses this file and a changed-parameter run treats
                    # it as stale. Only reached once the file is known to
                    # have rows, so an empty series is never stamped.
                    # Without this the gate finds no entry for any .obs,
                    # reads every one as stale, and re-fetches the whole
                    # station set on every run.
                    if obs_signature is not None:
                        cache_manifest.record_artifact(
                            obs_path, obs_signature,
                            data_observations_1d_station_path, logger)
                    return station_id
                else:
                    # Return None so it gets appended to the `failed` list
                    logger.info('Formatted %s time series '
                                'empty for station %s',
                                variable, station_id)
                    return None
            else:
                logger.info('Formatted %s time series '
                            'not found for station %s',
                            variable, station_id)
                return None
        except FileNotFoundError as e_x:
            logger.error(
                'Saving station failed. Please check the'
                ' directory '
                'path: %s -- %s.',
                data_observations_1d_station_path,
                str(e_x),
            )
            return None

    except Exception as e_x:
        logger.error(
            'Unexpected error processing station %s: %s',
            station_info[0] if station_info else 'unknown',
            e_x
        )
        return None


def _create_station_ctl_file(
    ctl_file_path, prop, datum, start_date, end_date, path, ofs,
    stationowner, var_list, logger, config_file=None,
):
    """Build the station ctl files and return the parsed result for
    ``ctl_file_path``.

    Runs under ``_ctl_create_lock`` so only one variable thread executes
    the full ``write_obs_ctlfile`` build; the others queue on the lock,
    re-check existence after acquiring it, and find the file written by
    the winning thread. The lock covers only this create path — threads
    whose ctl file already exists never touch it.
    """
    with _ctl_create_lock:
        read_station_ctl_file = station_ctl_file_extract(ctl_file_path)
        if read_station_ctl_file is not None:
            logger.info(
                'Station ctl file %s was created by a concurrent '
                'variable thread; skipping duplicate creation.',
                ctl_file_path,
            )
            return read_station_ctl_file
        try:
            logger.info(
                'Station ctl file not found. Creating station ctl file!. '
                'This might take a couple of minutes'
            )
            write_obs_ctlfile(
                start_date, end_date, datum, path, ofs,
                stationowner, var_list, logger,
                currents_bins_csv=getattr(
                    prop, 'currents_bins_csv', None),
                config_file=config_file,
            )
            read_station_ctl_file = station_ctl_file_extract(ctl_file_path)
            logger.info('Station ctl file created '
                        'successfully')
            return read_station_ctl_file
        except Exception as ex:
            logger.error(
                'Errors happened when creating station '
                'ctl files -- %s.',
                str(ex)
            )
            raise Exception('Error happened when '
                            'creating station '
                            'ctl files') from ex


def _process_variable_obs(
    variable, prop, datum, datum_list, start_date, end_date,
    start_date_full, end_date_full, path, ofs, stationowner, var_list,
    control_files_path, data_observations_1d_station_path, logger,
    config_file=None,
):
    """Process observation retrieval for a single variable.

    Encapsulates one iteration of the outer variable loop so that it can
    be dispatched either sequentially or via ThreadPoolExecutor.

    Parameters
    ----------
    variable : str
        One of 'water_level', 'water_temperature', 'salinity', 'currents'.
    prop : object
        Properties object (should be a per-thread copy when running in
        parallel to avoid shared-state issues).
    datum : str
        Datum string from the caller.
    datum_list : list
        Accepted datums.
    start_date, end_date : str
        YYYYMMDD date strings (already padded +/- 3 days).
    start_date_full, end_date_full : str
        Full date strings (YYYYMMDD-HH:MM:SS).
    path, ofs : str
        Project path and OFS name.
    stationowner : list
        Station owner list.
    var_list : list
        Full variable list (needed if ctl file creation is triggered).
    control_files_path : str
        Path to the control-files directory.
    data_observations_1d_station_path : str
        Output directory for .obs files.
    logger : logging.Logger
        Logger instance.

    Returns
    -------
    bool
        True if the obs control file was blank (no stations), False otherwise.
    """
    # Use a local copy of datum to avoid mutating the outer variable
    effective_datum = datum
    if variable == 'water_level':
        # Fix datum for CO-OPS API calls
        if effective_datum.lower() == 'igld85':
            effective_datum = 'IGLD'
        if effective_datum.lower() == 'navd88':
            effective_datum = 'NAVD'
        name_var = 'wl'
        logger.info('Making water level station '
                    'ctl file.')

    elif variable == 'water_temperature':
        name_var = 'temp'
        logger.info('Making temp station ctl file.')

    elif variable == 'salinity':
        name_var = 'salt'
        logger.info('Making salinity station ctl file.')

    elif variable == 'currents':
        name_var = 'cu'
        logger.info('Making currents station ctl file.')

    else:
        logger.error('Unknown variable: %s', variable)
        return False

    # This will try to read the station ctl file for the given ofs and for
    # all variables. If not found then it will create it using
    # write_obs_ctlfile.py
    ctl_file_path = (
        r'' + control_files_path + '/' + ofs +
        '_' + name_var + '_station.ctl'
    )
    # A station ctl left from a run with a different window / station-owner
    # selection encodes the wrong station set; its filename does not record
    # those parameters, so delete it when the recorded run signature differs
    # (issue: stale cache reuse across runs). The manifest also covers the
    # inventory + all sibling ctl files written in the same build pass.
    obs_ctl_signature = cache_manifest.run_signature(prop, variable=variable)
    cache_manifest.ensure_fresh(
        ctl_file_path, obs_ctl_signature, control_files_path,
        'obs ctl', logger)
    read_station_ctl_file = station_ctl_file_extract(ctl_file_path)

    if read_station_ctl_file is not None:
        logger.info('Station ctl file (%s_%s_station.ctl) '
                    'found in %s. '
                    'If you instead want to create a new '
                    'ctl file, change the name or '
                    'delete the current file.', ofs, name_var,
                    control_files_path)
    else:
        read_station_ctl_file = _create_station_ctl_file(
            ctl_file_path, prop, datum, start_date, end_date, path, ofs,
            stationowner, var_list, logger, config_file=config_file,
        )
        # Record the signature for every station ctl the build produced, so
        # sibling variables reuse them and a same-parameter rerun stays fast.
        if read_station_ctl_file is not None:
            for other_var in var_list:
                other_name = _VAR_TO_NAME.get(other_var)
                if other_name is None:
                    continue
                other_path = (
                    r'' + control_files_path + '/' + ofs +
                    '_' + other_name + '_station.ctl'
                )
                if os.path.isfile(other_path):
                    cache_manifest.record_artifact(
                        other_path,
                        cache_manifest.run_signature(prop, variable=other_var),
                        control_files_path,
                        logger)

    logger.info('Downloading data found in the station ctl files')

    if read_station_ctl_file is not None:
        # Group stations by data source for parallel dispatch
        source_groups = {}
        for i in range(len(read_station_ctl_file[0])):
            source = read_station_ctl_file[0][i][3]
            if source not in source_groups:
                source_groups[source] = []
            source_groups[source].append(
                (read_station_ctl_file[0][i],
                 read_station_ctl_file[1][i])
            )

        # Read parallel config for worker counts
        parallel_cfg = get_parallel_config(logger, config_file=config_file)

        # Apply any configured CO-OPS rate-limit override before the
        # station fan-out. Defaults (2 concurrent / 0.5 s gap) stay in
        # force unless the [parallelization] section says otherwise.
        configure_coops_rate_limit(
            concurrency=parallel_cfg.get('coops_request_concurrency'),
            gap_sec=parallel_cfg.get('coops_request_gap_sec'),
            logger=logger,
        )

        # Currents retrieval now issues one HTTP call per ADCP bin per
        # station, so it is orders of magnitude more request-dense than
        # scalar variables. Cap CO-OPS currents concurrency hard to stay
        # under the per-IP rate limit.
        coops_workers = (
            min(parallel_cfg['obs_coops_workers'], 2)
            if variable == 'currents'
            else parallel_cfg['obs_coops_workers']
        )

        # Map source names to worker counts
        source_worker_map = {
            'TC': coops_workers,
            'TAC': coops_workers,
            'COOPS': coops_workers,
            'CO-OPS': coops_workers,
            'USGS': parallel_cfg['obs_usgs_workers'],
            'NDBC': parallel_cfg['obs_ndbc_workers'],
            'CHS': parallel_cfg['obs_chs_workers'],
        }

        succeeded = []
        failed = []

        for source, station_pairs in source_groups.items():
            max_workers = source_worker_map.get(source, 1)

            # Check for unsupported source
            if source not in source_worker_map:
                logger.error(
                    'The second item on the first line of '
                    'each station '
                    'in the %s_%s_station.ctl should be '
                    'written as '
                    'ID_variable_ofs_DataSouce('
                    'TC, NDBC,or USGS)',
                    ofs,
                    name_var,
                )
                logger.error(
                    'Data source %s in %s_%s_station.ctl '
                    'not supported',
                    source,
                    ofs,
                    name_var,
                )
                return False

            logger.info(
                'Processing %d %s stations with %d workers',
                len(station_pairs), source, max_workers
            )

            with ThreadPoolExecutor(
                max_workers=max_workers
            ) as executor:
                futures = {}
                for station_info, station_metadata in station_pairs:
                    obs_path = os.path.join(
                        data_observations_1d_station_path,
                        f'{station_info[0]}_{ofs}_{name_var}_station.obs',
                    )
                    # Reuse an existing .obs only if it was written for this
                    # run's parameters; a file from an earlier window/options
                    # is deleted and re-fetched (issue: stale cache reuse).
                    obs_signature = cache_manifest.run_signature(
                        prop, variable=variable)
                    reusable = cache_manifest.ensure_fresh(
                        obs_path, obs_signature,
                        data_observations_1d_station_path, 'obs', logger)
                    if not reusable:
                        future = executor.submit(
                            _fetch_and_format_station,
                            station_info,
                            station_metadata,
                            variable,
                            name_var,
                            effective_datum,
                            datum_list,
                            start_date,
                            end_date,
                            start_date_full,
                            end_date_full,
                            ofs,
                            data_observations_1d_station_path,
                            logger,
                            control_files_path,
                            config_file,
                            obs_signature,
                        )
                        futures[future] = station_info[0]
                    else:
                        logger.info('Reusing existing %s', obs_path)

                for future in as_completed(futures):
                    sid = futures[future]
                    result = future.result()
                    if result is not None:
                        succeeded.append(result)
                    else:
                        failed.append(sid)

        logger.info(
            'Station retrieval complete for %s: '
            '%d succeeded, %d failed/no-data',
            variable, len(succeeded), len(failed)
        )
        return False
    else:
        logger.info('%s obs control file is blank!', name_var)
        return True


def get_station_observations(prop,logger):
    """Download and format 1D station observations for an OFS run window.

    Inventories (or reuses) stations for the OFS extent, then retrieves
    time series from CO-OPS, NDBC, USGS, and/or CHS for each requested
    variable. Writes per-station ``.obs`` files and observation control
    files under the paths in ``prop``.

    Args:
        prop: ``ModelProperties`` (or compatible) with ``ofs``,
            ``start_date_full``, ``end_date_full``, ``datum``, ``path``,
            ``stationowner``, and ``var_list`` set.
        logger: Logger instance, or ``None`` to configure from
            ``conf/logging.conf``.

    Returns:
        None. Side effects: observation and control files on disk.

    Raises:
        SystemExit: If logging config is missing when ``logger`` is ``None``.
    """

    # Hand out vars from the prop Santa
    start_date_full = prop.start_date_full
    end_date_full = prop.end_date_full
    datum = prop.datum
    path = prop.path
    ofs = prop.ofs
    stationowner = prop.stationowner
    var_list = prop.var_list

    # Specify defaults (can be overridden with
    # command line options)

    if logger is None:
        log_config_file = 'conf/logging.conf'
        log_config_file = (Path(__file__).parent.parent.parent.parent / log_config_file).resolve()

        # Check if log file exists
        if not os.path.isfile(log_config_file):
            sys.exit(-1)

        # Creater logger
        logging.config.fileConfig(log_config_file)
        logger = logging.getLogger('root')
        logger.info('Using log config %s', log_config_file)
    logger.info('--- Starting Station Observation Process ---')

    _conf = getattr(prop, 'config_file', None)
    dir_params = utils.Utils(_conf).read_config_section(
        'directories', logger)
    datum_list = (utils.Utils(_conf).read_config_section('datums', logger)\
                       ['datum_list']).split(' ')

    # Parse arguments to lists
    stationowner = utils.parse_arguments_to_list(stationowner, logger)
    var_list = utils.parse_arguments_to_list(var_list, logger)

    # parameter validation
    ofs_extents_path =\
        utils.resolve_asset_path(path, dir_params['ofs_extents_dir'])

    argu_list = (start_date_full,
                 end_date_full,
                 path,
                 ofs,
                 ofs_extents_path,
                 datum,
                 var_list)
    parameter_validation(argu_list, datum_list, logger)

    control_files_path = \
        os.path.join(path, dir_params['control_files_dir'])
    os.makedirs(control_files_path, exist_ok=True)

    data_observations_1d_station_path = os.path.join(
        path,
        dir_params['data_dir'],
        dir_params['observations_dir'],
        dir_params['1d_station_dir'],
    )
    os.makedirs(data_observations_1d_station_path,
                exist_ok=True)

    # This is adding +- 3 days to make sure when the data is sliced
    # it has data from beginning to end
    start_date_full = start_date_full.replace('-', '')
    end_date_full = end_date_full.replace ( '-' , '' )
    start_date_full = start_date_full.replace('Z', '')
    end_date_full = end_date_full.replace ( 'Z' , '' )
    start_date = start_date_full.split('T')[0]
    end_date = end_date_full.split('T')[0]
    start_date_full = start_date_full.replace('T', '-')
    end_date_full = end_date_full.replace ( 'T' , '-' )

    start_dt = datetime.strptime(
        start_date, '%Y%m%d') - timedelta(days=3)
    end_dt = datetime.strptime(
        end_date, '%Y%m%d') + timedelta(days=3)

    start_date = start_dt.strftime('%Y%m%d')
    end_date = end_dt.strftime('%Y%m%d')

    # This outer loop is used to download all data for all variables
    # Inside this loop there is another loop that will go over each line
    # in the station ctl file and will try to download the data from TandC,
    # USGS, and NDBC based on the station data source

    # Read parallel config once to decide variable-level dispatch strategy
    parallel_cfg = get_parallel_config(
        logger,
        config_file=getattr(prop, 'config_file', None),
    )
    use_parallel_variables = parallel_cfg.get('parallel_variables', False)

    if use_parallel_variables:
        logger.info(
            'Variable-level parallel mode ENABLED -- processing %d '
            'variables concurrently.', len(var_list))
        blank_results = []
        with ThreadPoolExecutor(max_workers=len(var_list)) as var_executor:
            var_futures = {}
            for variable in var_list:
                # Each thread gets its own copy of prop to avoid shared
                # mutable state across variables.
                prop_copy = copy.copy(prop)
                future = var_executor.submit(
                    _process_variable_obs,
                    variable,
                    prop_copy,
                    datum,
                    datum_list,
                    start_date,
                    end_date,
                    start_date_full,
                    end_date_full,
                    path,
                    ofs,
                    stationowner,
                    var_list,
                    control_files_path,
                    data_observations_1d_station_path,
                    logger,
                    _conf,
                )
                var_futures[future] = variable

            for future in as_completed(var_futures):
                variable = var_futures[future]
                try:
                    is_blank = future.result()
                    blank_results.append(is_blank)
                except Exception as ex:
                    logger.error(
                        'Error processing variable %s in parallel: %s',
                        variable, ex)
                    blank_results.append(False)

        blank_file = sum(1 for b in blank_results if b)
    else:
        blank_file = 0
        for variable in var_list:
            is_blank = _process_variable_obs(
                variable,
                prop,
                datum,
                datum_list,
                start_date,
                end_date,
                start_date_full,
                end_date_full,
                path,
                ofs,
                stationowner,
                var_list,
                control_files_path,
                data_observations_1d_station_path,
                logger,
                config_file=_conf,
            )
            if is_blank:
                blank_file += 1

    # Check if all obs ctl files are blank. If so, exit program -- nothing
    # else to do.
    if blank_file == len(var_list):
        logger.error('All observation control files are blank! That means '
                     'no observation data was found. Without that, the '
                     'skill assessment cannot proceed.')
        sys.exit()
