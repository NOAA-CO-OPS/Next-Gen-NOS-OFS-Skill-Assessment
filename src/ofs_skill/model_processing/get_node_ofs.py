"""
This is the final model 1d extraction function, it opens the path and looks
 for the model ctl file,
if model ctl file is found, then the script uses it for extracting the model
 timeseries
if model ctl file is not found, all the predefined function for finding the
nearest node and depth are applied and a new model ctl file is created along
with the time series
"""

import copy
import logging
import logging.config
import math
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from ofs_skill.model_processing import do_horizon_skill_utils

# Use new package imports - import directly from modules to avoid circular import
from ofs_skill.model_processing.get_datum_offset import get_datum_offset as get_datum_offset_func
from ofs_skill.model_processing.get_datum_offset import report_datums
from ofs_skill.model_processing.intake_scisa import intake_model
from ofs_skill.model_processing.list_of_files import list_of_dir
from ofs_skill.model_processing.list_of_files import list_of_files as list_of_files_func
from ofs_skill.model_processing.model_format_properties import ModelFormatProperties
from ofs_skill.model_processing.model_source import get_model_source
from ofs_skill.model_processing.write_ofs_ctlfile import write_ofs_ctlfile
from ofs_skill.obs_retrieval import scalar, utils, vector
from ofs_skill.obs_retrieval.utils import get_parallel_config


def parse_arguments_to_list(argument, logger):
    '''
    takes a string from a user-supplied argument and parses it to a list
    of strings.
    '''
    try:
        argument = argument.lower().replace('[', '').replace(']','').\
            replace(' ','').split(',')
    except AttributeError: # If argument is not a string
        logger.info('Input argument (%s) being parsed from str to list is '
                     'already a list. Moving on...', argument)
        return argument
    try:
        argument[0]
        return argument
    except IndexError:
        logger.error('Cannot parse input argument %s! Correct formatting and '
                     'try again.', argument)
        sys.exit(-1)

def name_convent(variable):
    """
    change variable names to correspond to model netcdfs
    """
    if variable == 'water_level':
        name_var = 'wl'
        model_var = 'zeta'

    elif variable == 'water_temperature':
        name_var = 'temp'
        model_var = 'temp'

    elif variable == 'salinity':
        name_var = 'salt'
        model_var = 'salinity'

    elif variable == 'currents':
        name_var = 'cu'
        model_var = 'currents'

    return name_var, model_var

def get_time_step(prop, logger):
    '''
    Gets the model time step depending on OFS and file type (fields or stations)

    Parameters
    ----------
    prop : program input arguments
    logger : logger

    Returns
    -------
    Time step in integer minutes

    '''
    # Define your expected frequency in minutes (e.g. 6 minutes)
    exp_freq = 6
    if prop.ofsfiletype == 'fields':
        if prop.ofs in ['gomofs', 'wcofs', 'ngofs2', 'necofs']:
            exp_freq = 180
        else:
            exp_freq = 60
    return exp_freq

def find_time_gaps(prop, model, logger):
    '''
    Look for missing data/time gaps in the model time series.

    Parameters
    ----------
    prop : main input arguments
    model : lazily loaded concatenated model dataset
    logger : logger

    Returns
    -------
    True if gaps detected, False if no gaps detected.

    '''

    # Get time name depending on model source
    time_name = 'time'
    if prop.model_source == 'roms':
        time_name = 'ocean_time'

    # Calculate time differences between consecutive time steps in minutes
    time_deltas = np.diff(model[time_name].values)/np.timedelta64(1, 'm')

    # Define your expected frequency in minutes (e.g. 6 minutes)
    exp_freq = float(get_time_step(prop, logger))

    # Check for gaps (where the delta is greater than expected)
    gaps = (time_deltas != exp_freq)

    return bool(np.any(gaps))


def ofs_ctlfile_extract(prop, name_var, model, logger):
    """
    The input here is the path, variable name, and logger.
    Extracts data from an OFS control file. If the file does not exist,
    it generates it first.
    """

    if prop.ofsfiletype == 'fields':
        filename = f'{prop.control_files_path}/{prop.ofs}_{name_var}_model.ctl'
        if (os.path.isfile(filename)) is False and prop.ctl_flag == 0:
            write_ofs_ctlfile(prop, model, logger)
            prop.ctl_flag += 1 # Raise flag -- we've gone through ctl file production
    elif prop.ofsfiletype == 'stations':
        filename = f'{prop.control_files_path}/{prop.ofs}_{name_var}_model_station.ctl'
        if (os.path.isfile(filename)) is False and prop.ctl_flag == 0:
            write_ofs_ctlfile(prop, model, logger)
            prop.ctl_flag += 1 # Raise flag -- we've gone through ctl file production

    try:
        with open(
                filename, encoding='utf-8'
        ) as file:
            model_ctlfile = file.read()
            lines = model_ctlfile.split('\n')
            lines = [i.split(' ') for i in lines]
            lines = [list(filter(None, i)) for i in lines]
            nodes = np.array(lines[:-1])[:, 0]
            nodes = [int(i) for i in nodes]
            depths = np.array(lines[:-1])[:, 1]
            depths = [int(i) for i in depths]

            # this is the shift that can be applied to the ofs timeseries,
            # for instance if there is a known bias in the model
            shifts = np.array(lines[:-1])[:, -1]
            shifts = [float(i) for i in shifts]

            # This is the station id, of the nearest station to the mesh node
            ids = np.array(lines[:-1])[:, -2]
            ids = [str(i) for i in ids]

            return lines, nodes, depths, shifts, ids
    except IndexError:
        logger.warning('%s model ctl file is blank -- no '
                     'model nodes/stations found! Moving on...',
                     name_var)
        return None
    except FileNotFoundError:
        logger.warning('%s model ctl file is missing, probably because there '
                       'are no matches between obs and model stations! '
                       'Moving on...', name_var)
        return None
    except Exception as ex:
        logger.error('Unexpected error when processing %s model ctl '
                     'file! Error: %s',
                     name_var, ex)
        return None

def roms_nodes(model, node_num):
    """
    This function converts the node from the ofs control file
    into i and j for ROMS
    """
    i_index,j_index = np.unravel_index(int(node_num),np.shape(model['lon_rho']))
    return i_index,j_index


def _batch_extract(model, var_name, idx_list, dep_list, idx_first=False):
    """Extract all stations for a variable via batched dask.compute().

    Parameters
    ----------
    model : xarray.Dataset
        The model dataset
    var_name : str
        Model variable name
    idx_list : list of int
        Station/node indices (2nd or 3rd dim depending on model)
    dep_list : list of int or None
        Depth/layer indices. None for 2D variables.
    idx_first : bool
        If True, indexing is [:, idx, dep] (ROMS order).
        If False, indexing is [:, dep, idx] (FVCOM/SCHISM order).
    """
    import dask

    n = len(idx_list)
    if dep_list is None:
        lazy = [model[var_name][:, idx_list[i]] for i in range(n)]
    elif idx_first:
        lazy = [model[var_name][:, idx_list[i], dep_list[i]]
                for i in range(n)]
    else:
        lazy = [model[var_name][:, dep_list[i], idx_list[i]]
                for i in range(n)]
    # Batch compute: Dask fuses shared chunks across all selections
    if hasattr(lazy[0].data, 'dask'):
        computed = dask.compute(*[s.data for s in lazy])
    else:
        computed = [np.array(s) for s in lazy]
    return np.stack(computed, axis=1)


def _precompute_current_data(prop, model, ofs_ctlfile, logger):
    """Batch-extract current (u/v) station data in a single Dask compute call.

    Returns a dict with 'u_data' and 'v_data' numpy arrays.
    """
    n_stations = len(ofs_ctlfile[1])
    indices = [int(ofs_ctlfile[1][i]) for i in range(n_stations)]
    depths = [int(ofs_ctlfile[2][i]) for i in range(n_stations)]

    if prop.model_source == 'fvcom':
        u_data = _batch_extract(model, 'u', indices, depths, idx_first=False)
        v_data = _batch_extract(model, 'v', indices, depths, idx_first=False)
    elif prop.model_source == 'roms':
        u_data = _batch_extract(model, 'u_east', indices, depths, idx_first=True)
        v_data = _batch_extract(model, 'v_north', indices, depths, idx_first=True)
    elif prop.model_source == 'schism':
        if 'stofs' not in prop.ofs:
            u_data = _batch_extract(model, 'u', indices, depths, idx_first=False)
            v_data = _batch_extract(model, 'v', indices, depths, idx_first=False)
        else:
            u_data = _batch_extract(model, 'u', indices, None)
            v_data = _batch_extract(model, 'v', indices, None)

    return {'u_data': u_data, 'v_data': v_data}


def _precompute_scalar_data(prop, model, ofs_ctlfile, model_var, logger):
    """Batch-extract scalar (temp/salt/water level) station data.

    Returns a dict with 'scalar_data' numpy array.
    """
    n_stations = len(ofs_ctlfile[1])
    indices = [int(ofs_ctlfile[1][i]) for i in range(n_stations)]
    depths = [int(ofs_ctlfile[2][i]) for i in range(n_stations)]

    actual_var = model_var
    if prop.model_source == 'roms' and model_var == 'salinity':
        actual_var = 'salt'
    if prop.model_source == 'schism' and model_var == 'temp':
        actual_var = 'temperature'
    if prop.model_source == 'schism' and model_var == 'zeta':
        actual_var = 'elevation' if prop.ofsfiletype == 'fields' else model_var

    if prop.model_source == 'fvcom':
        scalar_data = _batch_extract(model, actual_var, indices, depths,
                                     idx_first=False)
    elif prop.model_source == 'roms':
        scalar_data = _batch_extract(model, actual_var, indices, depths,
                                     idx_first=True)
    elif prop.model_source == 'schism':
        if 'stofs' in prop.ofs and model_var in ('temp', 'temperature'):
            scalar_data = _batch_extract(model, 'temperature', indices, None)
        else:
            try:
                scalar_data = _batch_extract(model, actual_var, indices, depths,
                                             idx_first=False)
            except IndexError:
                # 2D variable (e.g. water level) — no depth dimension
                scalar_data = _batch_extract(model, actual_var, indices, None)

    return {'scalar_data': scalar_data}


def _precompute_stations_data(prop, model, ofs_ctlfile, model_var, logger):
    """Batch-extract all station data in a single Dask compute call.

    Returns a dict with pre-computed numpy arrays keyed by data type.
    Only used for stations files to avoid N separate .compute() calls.

    Uses dask.compute() to batch all station selections into one
    optimized graph execution, so Dask reads each time chunk once
    for all stations instead of once per station. This is critical
    for monthly/yearly runs where many time chunks exist.
    """
    n_stations = len(ofs_ctlfile[1])

    time_var = 'ocean_time' if prop.model_source == 'roms' else 'time'
    model_time = np.array(model[time_var])

    result = {'model_time': model_time}

    if model_var in ('u', 'u_east', 'horizontalVelX', 'currents'):
        # Current variables — need u and v
        current_result = _precompute_current_data(
            prop, model, ofs_ctlfile, logger)
        result.update(current_result)
    else:
        # Scalar variables (temp, salt, water level)
        scalar_result = _precompute_scalar_data(
            prop, model, ofs_ctlfile, model_var, logger)
        result.update(scalar_result)

    logger.info('Pre-computed batch extraction for %d stations, var=%s',
                n_stations, model_var)
    return result


def format_temp_salt(prop, model, ofs_ctlfile, model_var, i, precomputed=None):
    """
    extract temperature and salinity time series from concatenated model data
    """

    if precomputed is not None and prop.ofsfiletype == 'stations':
        model_time = precomputed['model_time']
        model_obs = precomputed['scalar_data'][:, i].copy()
        if prop.model_source == 'schism':
            invalid_mask = (model_obs <= -999) | (model_obs >= 999)
            model_obs[invalid_mask] = np.nan
    elif prop.model_source=='fvcom':
        if prop.ofsfiletype == 'fields':
            model_time = np.array(model['time'])
            model_obs = np.array(
                model[model_var][:, int(ofs_ctlfile[2][i]),
                                 int(ofs_ctlfile[1][i])]
            )
            model_obs = model_obs #+ ofs_ctlfile[3][i]
        elif prop.ofsfiletype == 'stations':
            # Dimensions: time x siglay x station
            model_time = np.array(model['time'])
            #if int(ofs_ctlfile[1][i]) > -999:
            model_obs = np.array(
                model[model_var][:, int(ofs_ctlfile[2][i]),
                                 int(ofs_ctlfile[1][i])]
            )
            model_obs = model_obs #+ ofs_ctlfile[3][i]
            #else:
            #    model_obs = None

    elif prop.model_source=='roms':
        if model_var=='salinity':
            model_var='salt'
        if prop.ofsfiletype == 'fields':
            i_index,j_index = roms_nodes(model, int(ofs_ctlfile[1][i]))
            model_time = np.array(model['ocean_time'])
            model_obs = np.array(model[model_var][:, int(ofs_ctlfile[2][i]),
                                                  i_index,j_index])
            model_obs = model_obs #+ ofs_ctlfile[3][i]
        elif prop.ofsfiletype == 'stations':
            # Dimensions: time x station x s_rho
            model_time = np.array(model['ocean_time'])
            #if int(ofs_ctlfile[1][i]) > -999:
            model_obs = np.array(model[model_var]
                                 [:, int(ofs_ctlfile[1][i]),
                                  int(ofs_ctlfile[2][i])])
            model_obs = model_obs #+ ofs_ctlfile[3][i]
    elif prop.model_source=='schism':
        if prop.ofsfiletype == 'fields':
            if model_var=='temp':
               model_var='temperature'
            model_time = np.array(model['time'])
            model_obs = np.array(model[model_var][:, int(ofs_ctlfile[1][i]),
                                                  int(ofs_ctlfile[2][i])])
            model_obs = model_obs
        elif prop.ofsfiletype == 'stations':
            model_time = np.array(model['time'])
            if 'stofs' in prop.ofs:
                model_var = 'temperature'
                model_obs = np.array(model[model_var][:, int(ofs_ctlfile[1][i])])
            else:
                model_obs = np.array(model[model_var][:, int(ofs_ctlfile[1][i]),
                                                      int(ofs_ctlfile[2][i])])
            invalid_mask = (model_obs <= -999) | (model_obs >= 999)
            model_obs[invalid_mask] = np.nan
    elif prop.model_source == 'adcirc':
        if prop.ofs == 'stofs_2d_glo':
            # We raise en exception here for STOFS-2D-Global because it does
            # not have temp/sal data, and logic elsewhere should steer users
            # away from calling this function with STOFS-2D-Global.
            raise ValueError('Temperature and salinity data are not available for STOFS-2D-Global.')

    data_model = pd.DataFrame(
        {'DateTime': model_time,
         'OBS': model_obs}, columns=['DateTime', 'OBS']
    )

    start_date = (
        str(
            (
                datetime.strptime(prop.start_date_full.split('T')[0].replace('-', ''), '%Y%m%d')
                - timedelta(days=2)
            ).strftime('%Y%m%d')
        )
        + '-01:01:01'
    )
    end_date = (
        str(
            (
                datetime.strptime(prop.end_date_full.split('T')[0].replace('-', ''), '%Y%m%d')
                + timedelta(days=2)
            ).strftime('%Y%m%d')
        )
        + '-01:01:01'
    )

    formatted_series = \
        scalar(data_model, start_date, end_date)
    return formatted_series


def format_currents(prop, model, ofs_ctlfile, i, precomputed=None):
    """
    extract current velocity time series from concatenated model data
    """

    if precomputed is not None and prop.ofsfiletype == 'stations':
        mfp = ModelFormatProperties()
        mfp.model_time = precomputed['model_time']
        u_i = precomputed['u_data'][:, i]
        v_i = precomputed['v_data'][:, i]
        mfp.model_obs = np.array(u_i**2 + v_i**2) ** 0.5
        mfp.model_ang = np.array(
            [math.atan2(u_i[t], v_i[t]) / math.pi * 180 % 360.0
             for t in range(len(mfp.model_time))])
        if prop.model_source == 'schism':
            invalid_mask = (mfp.model_obs <= -999) | (mfp.model_obs >= 999)
            mfp.model_obs[invalid_mask] = np.nan
            mfp.model_ang[invalid_mask] = np.nan
    elif prop.model_source=='fvcom':
        mfp = ModelFormatProperties()
        mfp.model_time = np.array(model['time'])
        if prop.ofsfiletype == 'fields':
            u_i = np.array(
                model['u'][:, int(ofs_ctlfile[2][i]), int(ofs_ctlfile[1][i])]
            )
            v_i = np.array(
                model['v'][:, int(ofs_ctlfile[2][i]), int(ofs_ctlfile[1][i])]
            )

            mfp.model_obs = np.array(u_i**2 + v_i**2) ** 0.5

            mfp.model_ang = np.array(
            [math.atan2(u_i[t], v_i[t]) / math.pi * 180 % 360.0 for t in range(
            len(np.array(mfp.model_time)))])

            mfp.model_obs = mfp.model_obs #+ ofs_ctlfile[3][i]

        elif prop.ofsfiletype == 'stations':
            #if int(ofs_ctlfile[1][i]) > -999:
            mfp = ModelFormatProperties()
            mfp.model_time = np.array(model['time'])

            u_i = np.array(
                model['u'][:, int(ofs_ctlfile[2][i]),
                           int(ofs_ctlfile[1][i])]
            )
            v_i = np.array(
                model['v'][:, int(ofs_ctlfile[2][i]),
                           int(ofs_ctlfile[1][i])]
            )

            mfp.model_obs = np.array(u_i**2 + v_i**2) ** 0.5

            mfp.model_ang = np.array(
                [math.atan2(u_i[t], v_i[t]) / math.pi * \
                 180 % 360.0 for t in range(
                    len(np.array(mfp.model_time)))])

            mfp.model_obs = mfp.model_obs #+ ofs_ctlfile[3][i]
            #else:
            #    mfp.model_obs = None
            #    mfp.model_ang = None

    elif prop.model_source=='roms':
        mfp = ModelFormatProperties()
        mfp.model_time = np.array(model['ocean_time'])
        if prop.ofsfiletype == 'fields':
            i_index,j_index = roms_nodes(model, int(ofs_ctlfile[1][i]))
            u_i = np.array(model['u_east'][:, int(ofs_ctlfile[2][i]),
                                           i_index,j_index])
            v_i = np.array(model['v_north'][:, int(ofs_ctlfile[2][i]),
                                            i_index,j_index])

            mfp.model_obs = np.array(u_i**2 + v_i**2) ** 0.5
            mfp.model_ang = np.array(
                [math.atan2(u_i[t], v_i[t]) / math.pi * \
                 180 % 360.0 for t in range(
                    len(np.array(mfp.model_time)))])

            mfp.model_obs = mfp.model_obs #+ ofs_ctlfile[3][i]
        elif prop.ofsfiletype == 'stations':
            # Dimensions: time x station x s_rho
            #if int(ofs_ctlfile[1][i]) > -999:
            u_i = np.array(model['u_east'][:, int(ofs_ctlfile[1][i]),
                                           int(ofs_ctlfile[2][i])])
            v_i = np.array(model['v_north'][:, int(ofs_ctlfile[1][i]),
                                            int(ofs_ctlfile[2][i])])

            mfp.model_obs = np.array(u_i**2 + v_i**2) ** 0.5
            mfp.model_ang = np.array(
                [math.atan2(u_i[t], v_i[t]) / math.pi * \
                 180 % 360.0 for t in range(
                    len(np.array(mfp.model_time)))])

            mfp.model_obs = mfp.model_obs #+ ofs_ctlfile[3][i]
            #else:
            #    mfp.model_obs = None
            #    mfp.model_ang = None
    elif prop.model_source=='schism':
        mfp = ModelFormatProperties()
        mfp.model_time = np.array(model['time'])
        if prop.ofsfiletype == 'fields':
            u_i = np.array(
                model['horizontalVelX'][:, int(ofs_ctlfile[1][i]), int(ofs_ctlfile[2][i])]
            )
            v_i = np.array(
                model['horizontalVelY'][:, int(ofs_ctlfile[1][i]), int(ofs_ctlfile[2][i])]
            )
            mfp.model_obs = np.array(u_i**2 + v_i**2) ** 0.5
            mfp.model_ang = np.array(
            [math.atan2(u_i[t], v_i[t]) / math.pi * 180 % 360.0 for t in range(
            len(np.array(mfp.model_time)))])
            mfp.model_obs = mfp.model_obs #+ ofs_ctlfile[3][i]
        elif prop.ofsfiletype == 'stations':
            if 'stofs' not in prop.ofs:
                u_i = np.array(
                    model['u'][:, int(ofs_ctlfile[2][i]),
                               int(ofs_ctlfile[1][i])]
                )
                v_i = np.array(
                    model['v'][:, int(ofs_ctlfile[2][i]),
                               int(ofs_ctlfile[1][i])]
                )
            else:
                u_i = np.array(
                    model['u'][:, int(ofs_ctlfile[1][i])]
                )
                v_i = np.array(
                    model['v'][:, int(ofs_ctlfile[1][i])]
                )

            mfp.model_obs = np.array(u_i**2 + v_i**2) ** 0.5
            mfp.model_ang = np.array(
                [math.atan2(u_i[t], v_i[t]) / math.pi * \
                 180 % 360.0 for t in range(
                    len(np.array(mfp.model_time)))])
            mfp.model_obs = mfp.model_obs
            invalid_mask = (mfp.model_obs <= -999) | (mfp.model_obs >= 999)
            mfp.model_obs[invalid_mask] = np.nan
            mfp.model_ang[invalid_mask] = np.nan
    elif prop.model_source == 'adcirc':
        if prop.ofs == 'stofs_2d_glo':
            # We raise en exception here for STOFS-2D-Global because it does
            # not have current data, and logic elsewhere should steer users
            # away from calling this function with STOFS-2D-Global.
            raise ValueError('Current data are not available for STOFS-2D-Global.')

    mfp.data_model = pd.DataFrame(
        {'DateTime': mfp.model_time,
         'DIR': mfp.model_ang,
         'OBS': mfp.model_obs},
        columns=['DateTime', 'DIR', 'OBS'],
    )

    start_date = (
        str(
            (
                datetime.strptime(prop.start_date_full.split('T')[0].replace('-', ''), '%Y%m%d')
                - timedelta(days=2)
            ).strftime('%Y%m%d')
        )
        + '-01:01:01'
    )
    end_date = (
        str(
            (
                datetime.strptime(prop.end_date_full.split('T')[0].replace('-', ''), '%Y%m%d')
                + timedelta(days=2)
            ).strftime('%Y%m%d')
        )
        + '-01:01:01'
    )
    formatted_series = \
        vector(mfp.data_model, start_date, end_date)

    return formatted_series


def format_waterlevel(prop, model, ofs_ctlfile, model_var,
                      i, logger, precomputed=None):
    """
    extract water level time series from concatenated model data
    """


    id_number = ofs_ctlfile[4][i]
    datum_offset = get_datum_offset_func(
        prop, int(ofs_ctlfile[1][i]), model, id_number, logger)
    logger.info(f'Datum offset for station {id_number} (node {ofs_ctlfile[1][i]}): {datum_offset}')

    if precomputed is not None and prop.ofsfiletype == 'stations':
        model_time = precomputed['model_time']
        model_obs = precomputed['scalar_data'][:, i].copy()
        if prop.model_source == 'schism':
            if datum_offset > -999 and datum_offset < 999:
                sign = 1 if 'stofs' in prop.ofs else -1
                model_obs = model_obs + sign * datum_offset
        else:
            if datum_offset > -999:
                model_obs = model_obs - datum_offset
    elif prop.model_source=='fvcom':
        if prop.ofsfiletype == 'fields':
            model_time = np.array(model['time'])
            model_obs = np.array(model[model_var][:, int(ofs_ctlfile[1][i])])
            if datum_offset > -999:
                model_obs = model_obs - datum_offset
        elif prop.ofsfiletype == 'stations':
            model_time = np.array(model['time'])
            #if int(ofs_ctlfile[1][i]) > -999:
            model_obs = np.array(model[model_var][:,
                                                  int(ofs_ctlfile[1][i])])
            if datum_offset > -999:
                model_obs = model_obs - datum_offset
            #else:
            #    model_obs = None
    elif prop.model_source=='roms':
        if prop.ofsfiletype == 'fields':
            i_index,j_index = roms_nodes(model, int(ofs_ctlfile[1][i]))
            model_time = np.array(model['ocean_time'])
            model_obs = np.array(model[model_var][:, i_index,j_index])
            if datum_offset > -999:
                model_obs = model_obs - datum_offset
        elif prop.ofsfiletype == 'stations':
            # Dimensions: time x stations
            #i_index = roms_station_nodes(model, int(ofs_ctlfile[1][i]))
            model_time = np.array(model['ocean_time'])
            #if int(ofs_ctlfile[1][i]) > -999:
            model_obs = np.array(model[model_var][:,
                                                  int(ofs_ctlfile[1][i])])
            if datum_offset > -999:
                model_obs = model_obs - datum_offset
            #else:
            #    model_obs = None
    elif prop.model_source=='schism':
        if prop.ofsfiletype == 'fields':
            if model_var=='zeta':
               model_var='elevation' # Using out2d files
            model_time = np.array(model['time'])
            model_obs = np.array(model[model_var][:, int(ofs_ctlfile[1][i])])
            model_obs = model_obs + ofs_ctlfile[3][i]
            if datum_offset > -999 and datum_offset < 999:
                sign = 1 if 'stofs' in prop.ofs else -1
                model_obs = model_obs + sign * datum_offset
        elif prop.ofsfiletype == 'stations':
            model_time = np.array(model['time'])
            model_obs = np.array(model[model_var][:, int(ofs_ctlfile[1][i])])
            if datum_offset > -999 and datum_offset < 999:
                sign = 1 if 'stofs' in prop.ofs else -1
                model_obs = model_obs + sign * datum_offset
    elif prop.model_source =='adcirc':
        model_time = np.array(model['time'])
        model_obs = np.array(model[model_var][:, int(ofs_ctlfile[1][i])])
        if datum_offset > -999 and datum_offset < 999:
            model_obs = model_obs - datum_offset

    data_model = pd.DataFrame(
        {'DateTime': model_time,
         'OBS': model_obs}, columns=['DateTime', 'OBS']
    )

    start_date = (
        str(
            (
                datetime.strptime(prop.start_date_full.split('T')[0].replace('-', ''), '%Y%m%d')
                - timedelta(days=2)
            ).strftime('%Y%m%d')
        )
        + '-01:01:01'
    )
    end_date = (
        str(
            (
                datetime.strptime(prop.end_date_full.split('T')[0].replace('-', ''), '%Y%m%d')
                + timedelta(days=2)
            ).strftime('%Y%m%d')
        )
        + '-01:01:01'
    )

    formatted_series = \
        scalar(data_model, start_date, end_date)

    return formatted_series, datum_offset


def parameter_validation(prop, dir_params, logger):
    """Parameter validation"""
    # Start Date and End Date validation

    try:
        start_dt = datetime.strptime(
            prop.start_date_full, '%Y-%m-%dT%H:%M:%SZ')
        end_dt = datetime.strptime(
            prop.end_date_full, '%Y-%m-%dT%H:%M:%SZ')
    except ValueError:
        error_message = f'Please check Start Date - ' \
                        f"'{prop.start_date_full}', End Date -" \
                        f" '{prop.end_date_full}'. Abort!"
        logger.error(error_message)
        print(error_message)
        sys.exit(-1)

    if start_dt > end_dt:
        error_message = f'End Date {prop.end_date_full} ' \
                        f'is before Start Date ' \
                        f'{prop.start_date_full}. Abort!'
        logger.error(error_message)
        sys.exit(-1)

    if prop.path is None:
        prop.path = dir_params['home']

    # Path validation
    ofs_extents_path = os.path.join(prop.path, dir_params['ofs_extents_dir'])
    if not os.path.exists(ofs_extents_path):
        error_message = f'ofs_extents/ folder is not found. ' \
                        f'Please check Path - ' \
                        f"'{prop.path}'. Abort!"
        logger.error(error_message)
        sys.exit(-1)

    # OFS validation
    shapefile = f'{ofs_extents_path}/{prop.ofs}.shp'
    if not os.path.isfile(shapefile):
        error_message = f"Shapefile '{prop.ofs}' " \
                        f'is not found at the folder' \
                        f' {ofs_extents_path}. Abort!'
        logger.error(error_message)
        sys.exit(-1)

    # Whichcast validation
    if (prop.whichcast is not None) and (
        prop.whichcast not in ['nowcast', 'forecast_a', 'forecast_b', 'hindcast']
    ):
        error_message = f'Please check Whichcast - ' \
                        f"'{prop.whichcast}'. Abort!"
        logger.error(error_message)
        sys.exit(-1)

    if prop.whichcast == 'forecast_a' and prop.forecast_hr is None:
        error_message = 'Forecast_Hr is required if ' \
                        'Whichcast is forecast_a. Abort!'
        logger.error(error_message)
        sys.exit(-1)

    # datum validation
    if prop.datum not in prop.datum_list:
        error_message = f'Datum {prop.datum} is not valid. Abort!'
        logger.error(error_message)
        sys.exit(-1)
    # GLOFS datum validation
    if (prop.datum.lower() not in ('igld85', 'lwd') and prop.ofs in
        ['leofs','loofs','lmhofs','lsofs']):
        error_message = f'Use only LWD or IGLD85 datums for {prop.ofs}!'
        logger.error(error_message)
        sys.exit()
    # Non-GLOFS datum validation
    if (prop.datum.lower() in ('igld85', 'lwd') and 'l' not in prop.ofs[0]):
        error_message = f'Do not use LWD or IGLD85 datums for {prop.ofs}!'
        logger.error(error_message)
        sys.exit()
    # File type validation
    if prop.ofsfiletype not in ['stations','fields']:
        logger.error('Uh-oh, please select a valid model output file type! '
                     'You chose %s. The options are "stations" or "fields".',
                     prop.ofsfiletype)
    if (prop.ofs == 'stofs_2d_glo') and (prop.ofsfiletype == 'fields'):
        logger.error('STOFS-2D-Global is only available as station output files. Please select "stations" for file type. Exiting...')
        raise NotImplementedError('STOFS-2D-Global fields file processing is not currently implemented.')
    # Warn if using custom lat/lon inputs and stations files
    if prop.ofsfiletype == 'stations' and prop.user_input_location:
        logger.warning('You are using custom lat/lon coordinates for model time '
                       'series extraction from stations files. All lats/lons '
                       'may not have a matching station output location. '
                       'To extract model time series for all lats/lons, try '
                       'using field files! Continuing...')
    # Check for user input file if using custom lat/lon inputs
    _conf = getattr(prop, 'config_file', None)
    filepath = (utils.Utils(_conf).read_config_section('user_xy_inputs', logger)
                ['user_xy_path'])
    if os.path.isfile(filepath) is False and prop.user_input_location:
        logger.error('No user lat & lon inputs found! Please make sure '
                     'the path to your input is correct in ofs_dps.conf, or '
                     'create a text file with the following columns separated '
                     ' by a space: '
                     '{location_name} '
                     '{latitude (decimal deg)} '
                     '{longitude (decimal deg)} '
                     '{water depth (m)}')
        sys.exit()
    # Handle variable input argument
    correct_var_list = ['water_level','water_temperature',
                        'salinity','currents']
    list_diff = list(set(prop.var_list) - set(correct_var_list))
    if len(list_diff) != 0:
        logger.error('Incorrect inputs to variable selection argument: %s. '
                     'Please use %s. Exiting...', list_diff,
                     correct_var_list)
        sys.exit()
    if prop.ofs == 'stofs_2d_glo':
        if set(prop.var_list) != {'water_level'}:
            logger.warning('"water_level" is the only available variable for STOFS-2D-Global: '
                           'Removing other variables from list.')
            prop.var_list = ['water_level']
            # I think we can alter its state here, but maybe there's a reason not to?


def get_node_ofs(prop, logger, model_dataset=None):
    """
    This is the final model 1d extraction function, it opens the path and looks
     for the model ctl file,
    if model ctl file is found, then the script uses it for extracting the model
     timeseries
    if model ctl file is not found, all the predefined function for finding the
    nearest node and depth are applied and a new model ctl file is created along
    with the time series.

    Parameters
    ----------
    model_dataset : xarray.Dataset or None
        Pre-loaded model dataset. When provided, skips the expensive
        intake_model() call and reuses this dataset instead.

    Returns
    -------
    xarray.Dataset
        The loaded (and gap-filled) model dataset, so callers can cache it.
    """
    prop.model_source = get_model_source(prop.ofs)
    if logger is None:
        log_config_file = 'conf/logging.conf'
        log_config_file = (Path(__file__).parent.parent.parent / log_config_file).resolve()

        # Check if log file exists
        if not os.path.isfile(log_config_file):
            sys.exit(-1)

        # Creater logger
        logging.config.fileConfig(log_config_file)
        logger = logging.getLogger('root')
        logger.info('Using log config %s', log_config_file)

    logger.info('--- Starting OFS Model process ---')

    _conf = getattr(prop, 'config_file', None)
    dir_params = utils.Utils(_conf).read_config_section('directories', logger)
    prop.datum_list = (utils.Utils(_conf).read_config_section('datums', logger)\
                       ['datum_list']).split(' ')
    # Parse variable selection input to list
    prop.var_list = parse_arguments_to_list(prop.var_list, logger)
    # Parameter validation
    parameter_validation(prop, dir_params, logger)

    prop.model_path = os.path.join(
        dir_params['model_historical_dir'], prop.ofs, dir_params['netcdf_dir']
    )
    prop.model_path = Path(prop.model_path).as_posix()

    prop.control_files_path = os.path.join(
        prop.path, dir_params['control_files_dir']
    )
    os.makedirs(prop.control_files_path, exist_ok=True)

    prop.data_model_1d_node_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['model_dir'],
        dir_params['1d_node_dir'])
    prop.data_model_1d_node_path = Path(prop.data_model_1d_node_path).as_posix()
    os.makedirs(prop.data_model_1d_node_path, exist_ok = True)

    # Reformat start & end dates
    # Convert ISO format to internal format for processing
    # Use local variables to avoid modifying prop permanently
    start_date_internal = prop.start_date_full.replace('-', '').replace('Z', '').replace('T', '-')
    end_date_internal = prop.end_date_full.replace('-', '').replace('Z', '').replace('T', '-')

    try:
        prop.startdate = (datetime.strptime(
            start_date_internal.split('-')[0], '%Y%m%d')).strftime(
            '%Y%m%d') + '00'
        prop.enddate = (datetime.strptime(
            end_date_internal.split('-')[0], '%Y%m%d')).strftime(
            '%Y%m%d') + '23'
    except Exception as e:
        logger.error(f'Problem with date format in get_node_ofs: {e}')
        raise SystemExit(1)

    # Lazy load the model data (or reuse pre-loaded dataset)
    if model_dataset is not None:
        model = model_dataset
        logger.info('Using pre-loaded model dataset (skipping intake_model)')
    else:
        dir_list = list_of_dir(prop, logger)
        list_files = list_of_files_func(prop, dir_list, logger)
        logging.info('About to start intake_scisa from get_node ...')
        model = intake_model(list_files, prop, logger)
        logging.info('Lazily loaded dataset complete for %s!', prop.whichcast)

    if not model:
        logger.error('No model files or URLs to load in intake! Exiting...')
        raise SystemExit(1)

    # Write filenames to CSV
    try:
        time_name = 'time'
        if prop.model_source == 'roms':
            time_name = 'ocean_time'
        # Format time step
        time_step = str(get_time_step(prop, logger)) + 'min'
        serieskey = model[[time_name, 'filename']].to_dataframe()
        full_date_range = pd.date_range(start=serieskey.index.min(),
                                        end=serieskey.index.max(), freq=time_step)
        serieskey = serieskey.reindex(full_date_range)
        # Write 'key' that lists all model files used to construct
        # time series
        logger.info('Writing model time series filename key!')
        filename = f'{prop.ofs}_{prop.whichcast}_filename_key.csv'
        filepath = Path(os.path.join(prop.data_model_1d_node_path,
                                     filename)).as_posix()
        serieskey.to_csv(filepath, index_label='DateTime')
    except KeyError:
        logger.error('No filename variable found in the lazy loaded model '
                     'dataset! Cannot write filename time series key. '
                     'Moving on...')
    except Exception as ex:
        logger.error('Error writing model time series filename '
                     'key: %s', ex)

    # Check for time gaps caused by missing model output files.
    # If there is a gap, resample data to the correct time step and add nans
    # to fill gap
    isgap = find_time_gaps(prop, model, logger)
    if isgap: # Resample if time gap
        logger.info('Preserving model time series gaps as nans...')
        # Now resample
        if prop.model_source == 'roms':
            model = model.resample(
                ocean_time=time_step).asfreq()
        elif prop.model_source in ('fvcom', 'schism'):
            model = model.resample(
                time=time_step).asfreq()

    prop.ctl_flag = 0 #Need flag to track control file production if
                 #user_input_location == True

    def _extract_variable(variable, prop_local):
        """Process a single variable — extractable for parallel dispatch."""
        try:
            name_conventions = name_convent(variable)
            if not prop_local.user_input_location:
                control_file = f'{prop_local.control_files_path}/{prop_local.ofs}_' \
                               f'{name_conventions[0]}_station.ctl'
                if os.path.isfile(control_file) is False:
                    logger.info('%s is not found. If not providing a custom XY '
                                'input file, then an observation control file '
                                'must be present! Exiting...', control_file)
                    sys.exit()
                if os.path.getsize(control_file): # Gets size of obs ctl file!
                    ofs_ctlfile = ofs_ctlfile_extract(
                        prop_local, name_conventions[0], model, logger)
                    if ofs_ctlfile is None:
                        return
                else:
                    logger.info('%s obs ctl file is blank!', variable)
                    logger.info('For GLOFS, salt and cu ctl files may be blank. '
                                'If running with a single station provider/owner, '
                                'ctl files may also be blank.')
                    return
            else:
                ofs_ctlfile = ofs_ctlfile_extract(prop_local, name_conventions[0], model, logger)

            # Batch-extract all station data if using stations files
            precomputed = None
            if prop_local.ofsfiletype == 'stations':
                try:
                    precomputed = _precompute_stations_data(
                        prop_local, model, ofs_ctlfile,
                        name_conventions[-1], logger)
                except Exception as ex:
                    logger.warning(
                        'Batch precomputation failed, falling back to '
                        'per-station extraction: %s', ex)
                    precomputed = None

            def _process_single_station(i, ofs_ctlfile, prop_local, model,
                                        name_conventions, precomputed,
                                        variable, logger):
                """Process a single station: format data and write .prd file.

                Returns (datum_offset, model_station) for water_level,
                or (None, None) for other variables.
                """
                datum_offset = None
                model_station = None

                if variable in ('salinity', 'water_temperature'):
                    formatted_series = format_temp_salt(
                        prop_local,
                        model,
                        ofs_ctlfile,
                        name_conventions[-1],
                        i,
                        precomputed=precomputed,
                    )
                elif variable == 'currents':
                    formatted_series = format_currents(prop_local, model,
                                                       ofs_ctlfile,
                                                       i,
                                                       precomputed=precomputed)
                else:
                    formatted_series, datum_offset = format_waterlevel(
                        prop_local,
                        model,
                        ofs_ctlfile,
                        name_conventions[-1],
                        i, logger,
                        precomputed=precomputed,
                    )
                    model_station = ofs_ctlfile[4][i]

                if (prop_local.whichcast == 'forecast_a' and
                    not prop_local.horizonskill):
                    with open(
                        r''
                        + f'{prop_local.data_model_1d_node_path}'
                          f'/{ofs_ctlfile[4][i]}_'
                          f'{prop_local.ofs}_{name_conventions[0]}_'
                          f'{ofs_ctlfile[1][i]}_'
                          f'{prop_local.whichcast}_{prop_local.forecast_hr}_'
                          f'{prop_local.ofsfiletype}_model.prd',
                        'w',
                        encoding='utf-8',
                    ) as output:
                        for line in formatted_series:
                            output.write(str(line) + '\n')
                        logger.info(
                            '%s/%s_%s_%s_%s_%s_%s_%s_model.prd created '
                            'successfully',
                            prop_local.data_model_1d_node_path,
                            ofs_ctlfile[4][i],
                            prop_local.ofs,
                            name_conventions[0],
                            ofs_ctlfile[1][i],
                            prop_local.whichcast,
                            prop_local.forecast_hr,
                            prop_local.ofsfiletype
                        )
                elif (prop_local.horizonskill and os.path.isfile(
                        f'{prop_local.data_model_1d_node_path}/'
                        f'{ofs_ctlfile[-1][i]}_{prop_local.ofs}_{name_conventions[0]}_'
                        f'{ofs_ctlfile[1][i]}_forecast_b_{prop_local.ofsfiletype}_'
                        f'model.prd'
                    ) is True):
                    datecycle = prop_local.start_date_full.split('T')[0].replace('-', '') + \
                        '-' + prop_local.forecast_hr + '-' + 'forecast'
                    try:
                        df = do_horizon_skill_utils.pandas_processing(
                            name_conventions[0],datecycle,formatted_series)
                    except Exception as e_x:
                        logger.error('Could not merge datecycle %s! Skipping.'
                                     'Error: %s', e_x)
                        return (datum_offset, model_station)
                    filename = (f'{prop_local.ofs}_{ofs_ctlfile[4][i]}_'
                    f'{name_conventions[0]}_fcst_horizons.csv')
                    filepath = os.path.join(prop_local.data_horizon_1d_node_path,
                                 filename)
                    if os.path.isfile(filepath):
                        try:
                            df = do_horizon_skill_utils.pandas_merge(filepath, df,
                                                            datecycle,prop_local)
                        except Exception as e_x:
                            logger.error('Could not concat forecast horizon '
                                         'series in pandas for %s at station '
                                         '%s! Error: %s', name_conventions[0],
                                         ofs_ctlfile[4][i], e_x)
                            logger.error('No forecast horizons available!')
                            return (datum_offset, model_station)
                    # Save pandas dataframe with horizon time series
                    try:
                        df.to_csv(filepath, index=False)
                    except Exception as e_x:
                        logger.error("Couldn't save forecast horizons to csv!"
                                     'Error: %s', e_x)
                        return (datum_offset, model_station)
                else:
                    with open(
                        r''
                        +f'{prop_local.data_model_1d_node_path}/{ofs_ctlfile[4][i]}_'
                          f'{prop_local.ofs}_'
                          f'{name_conventions[0]}_{ofs_ctlfile[1][i]}'
                          f'_{prop_local.whichcast}_{prop_local.ofsfiletype}_model.prd',
                        'w',
                        encoding='utf-8',
                    ) as output:
                        for line in formatted_series:
                            output.write(str(line) + '\n')
                        logger.info(
                            '%s/%s_%s_%s_%s_%s_%s_model.prd created successfully',
                            prop_local.data_model_1d_node_path,
                            ofs_ctlfile[4][i],
                            prop_local.ofs,
                            name_conventions[0],
                            ofs_ctlfile[1][i],
                            prop_local.whichcast,
                            prop_local.ofsfiletype
                        )

                return (datum_offset, model_station)

            # Dispatch station processing — parallel or sequential
            parallel_cfg = get_parallel_config(logger)
            n_stations = len(ofs_ctlfile[1])
            datum_offsets = []
            model_stations = []

            if (parallel_cfg.get('parallel_stations')
                    and n_stations > 1 and precomputed is not None):
                logger.info('Processing %d stations in parallel for %s',
                            n_stations, variable)
                with ThreadPoolExecutor(
                        max_workers=min(n_stations, 8)) as executor:
                    futures = []
                    for i in range(n_stations):
                        prop_copy = copy.copy(prop_local)
                        futures.append(executor.submit(
                            _process_single_station, i, ofs_ctlfile,
                            prop_copy, model, name_conventions,
                            precomputed, variable, logger))
                    for f in futures:
                        try:
                            datum_offset, model_station = f.result()
                            if datum_offset is not None:
                                datum_offsets.append(datum_offset)
                            if model_station is not None:
                                model_stations.append(model_station)
                        except Exception as ex:
                            logger.error(
                                'Station processing failed for %s: %s',
                                variable, ex)
            else:
                for i in range(n_stations):
                    datum_offset, model_station = _process_single_station(
                        i, ofs_ctlfile, prop_local, model,
                        name_conventions, precomputed, variable, logger)
                    if datum_offset is not None:
                        datum_offsets.append(datum_offset)
                    if model_station is not None:
                        model_stations.append(model_station)

            # Generate datum report
            if not prop_local.user_input_location:
                datum_filename = (f'{prop_local.ofs}_wl_datum_report.csv')
                filepath = os.path.join(prop_local.control_files_path,
                                        datum_filename)
                # Check datum report age. Overwrite only if it's > 1 hour old
                try:
                    st=os.stat(filepath)
                    timediffhour = (datetime.now() - (datetime.fromtimestamp(
                        st.st_mtime))).total_seconds()/60/60
                except FileNotFoundError:
                    timediffhour = 99
                if (variable == 'water_level' and timediffhour > 1):
                    logger.error('No datum report found, writing new one.')
                    datum_offsets = [model_stations, datum_offsets]
                    report_datums(prop_local, datum_offsets, logger)

        except FileNotFoundError:
            logger.warning('No control file for %s was written because '
                         'no model stations matched observation stations.',
                         variable)
        except Exception as ex:
            import traceback
            logger.error('Error happened when process %s - %s',
                         variable,
                         str(ex))
            logger.error('Full traceback:\n%s', traceback.format_exc())

    # Dispatch variable processing — parallel or sequential
    parallel_cfg = get_parallel_config(logger)
    if parallel_cfg['parallel_variables'] and len(prop.var_list) > 1:
        logger.info('Processing %d variables in parallel', len(prop.var_list))
        with ThreadPoolExecutor(max_workers=min(len(prop.var_list), 4)) as executor:
            futures = []
            for variable in prop.var_list:
                prop_local = copy.deepcopy(prop)
                prop_local.var_list = [variable]
                futures.append(executor.submit(_extract_variable, variable, prop_local))
            for f in futures:
                f.result()  # Raise any exceptions
    else:
        for variable in prop.var_list:
            _extract_variable(variable, prop)

    logger.info('Finished with model data processing!')
    return model
