"""
OFS Control File Writer Module

This module creates OFS control files that map observation stations to model nodes
and depths. Control files are essential for extracting model time series data at
specific locations for skill assessment.

If a model control file is not found, this module applies predefined functions to:
- Find the nearest model node to each observation station
- Find the nearest model depth layer to each observation depth
- Write these mappings to a control file

Functions
---------
user_input_extract : Extract user-provided XY locations from configuration file
write_ofs_ctlfile : Main function to create or verify model control files

Notes
-----
This module is called by get_node_ofs.py when the OFS control file is not found.
It handles multiple model sources (FVCOM, ROMS, SCHISM) and file types (fields, stations).

Control file format (space-delimited):
node_index layer_index latitude longitude station_id depth

Example:
1234 5 36.850 -76.012 8638610 5.0
"""

import os
from logging import Logger
from pathlib import Path
from typing import Any

import numpy as np

import ofs_skill.model_processing.indexing as indexing
from ofs_skill.obs_retrieval import utils
from ofs_skill.obs_retrieval.station_ctl_file_extract import station_ctl_file_extract


def user_input_extract(prop: Any, logger: Logger) -> list[list[list[Any]]]:
    """
    Extract user-provided XY locations from configuration file.

    Reads a user-specified file containing custom location information
    (name, latitude, longitude, depth) and formats it to match the structure
    of observation control files.

    Parameters
    ----------
    prop : ModelProperties
        ModelProperties object containing:
        - datum : str
            Vertical datum for the locations
    logger : Logger
        Logger instance for logging messages

    Returns
    -------
    list of list of list
        3D nested list structure matching observation control file format:
        - Outer dimension: 2 (station info categories)
        - Middle dimension: number of stations
        - Inner dimension: 5 (name, lat, lon, depth, datum)

    Notes
    -----
    - Reads from path specified in config file under 'user_xy_inputs'
    - Each line should have 4 space-separated values: name lat lon depth
    - Blank lines and lines with incorrect format are skipped
    - All location names are repeated 5 times for consistency with obs control file format

    Examples
    --------
    >>> station_info = user_input_extract(prop, logger)
    >>> print(station_info[0][0])  # First station name fields
    ['Station1', 'Station1', 'Station1', 'Station1', 'Station1']
    >>> print(station_info[1][0])  # First station data fields
    ['36.85', '-76.01', '5.0', 0, 'NAVD88']
    """
    xy_path = (utils.Utils().read_config_section('user_xy_inputs', logger)
               ['user_xy_path'])
    lines = []
    try:
        with open(xy_path) as file:
            for line in file:
                # Remove leading/trailing whitespace (including newline characters)
                cleaned_line = line.strip()
                # Split the cleaned line by the specified delimiter
                inner_list = cleaned_line.split(' ')
                if len(inner_list) > 0:
                    lines.append(inner_list)
    except FileNotFoundError:
        logger.error(f"Error: The file '{xy_path}' was not found.")
        return lines
    except Exception as e:
        logger.error(f'An error occurred: {e}')
        return []

    # First remove blank lines
    lines = [sublist for sublist in lines if '' not in sublist]
    # Now check formatting -- toss any entries that do not have 4 inputs
    lines = [sublist for sublist in lines if len(sublist) == 4]
    # Now format input to match the obs ctl file.
    rows = 2
    cols = len(lines)
    depth = 5
    station_info = [[[[] for _ in range(depth)] for _ in range(cols)]
                    for _ in range(rows)]
    for i, line in enumerate(lines):
        station_info[0][i][0] = line[0]
        station_info[0][i][1] = line[0]
        station_info[0][i][2] = line[0]
        station_info[0][i][3] = line[0]
        station_info[0][i][4] = line[0]
        # Next column
        station_info[1][i][0] = line[1]
        station_info[1][i][1] = line[2]
        station_info[1][i][2] = line[3]
        station_info[1][i][3] = 0
        station_info[1][i][4] = prop.datum

    return station_info


def write_ofs_ctlfile(prop: Any, model: Any, logger: Logger) -> Any:
    """
    Create or verify OFS control files for model data extraction.

    This function checks if model control files exist for each variable in prop.var_list.
    If a control file is missing or if user_input_location is True, it creates a new
    control file by:
    1. Finding the nearest model node to each observation station
    2. Finding the nearest model depth layer to each observation depth
    3. Writing the node/layer/location mappings to a control file

    Parameters
    ----------
    prop : ModelProperties
        ModelProperties object containing:
        - control_files_path : str
            Directory path for control files
        - ofs : str
            OFS model name (e.g., 'cbofs', 'ngofs2')
        - var_list : list of str
            List of variables ('water_level', 'water_temperature', 'salinity', 'currents')
        - ofsfiletype : str
            Type of file ('stations' or 'fields')
        - user_input_location : bool
            True to use user-specified locations instead of observation stations
        - model_source : str
            Model type ('fvcom', 'roms', 'schism')
    model : xarray.Dataset
        Model dataset containing grid information
    logger : Logger
        Logger instance for logging messages

    Returns
    -------
    xarray.Dataset
        The input model dataset (unchanged)

    Notes
    -----
    - Control files are named: {ofs}_{var}_model.ctl (fields) or {ofs}_{var}_model_station.ctl (stations)
    - Variable name mappings: water_level->wl, water_temperature->temp, salinity->salt, currents->cu
    - If observation control file is blank, creates blank model control file
    - Different coordinate extraction methods for FVCOM, ROMS, and SCHISM models
    - Skips stations where no matching model node can be found

    Examples
    --------
    >>> model = write_ofs_ctlfile(prop, model, logger)
    INFO:root:Model Control File for water_level created successfully
    """
    dir_params = utils.Utils().read_config_section('directories', logger)

    prop.model_path = os.path.join(
        dir_params['model_historical_dir'], prop.ofs, dir_params['netcdf_dir']
    )
    prop.model_path = Path(prop.model_path).as_posix()

    name_var = []

    for variable in prop.var_list:

        if variable == 'water_level':
            name_var = 'wl'

        elif variable == 'water_temperature':
            name_var = 'temp'

        elif variable == 'salinity':
            name_var = 'salt'

        elif variable == 'currents':
            name_var = 'cu'

        if (
            (os.path.isfile(
                f'{prop.control_files_path}/{prop.ofs}_'
                f'{name_var}_model.ctl') is False
                and prop.ofsfiletype == 'fields') or (os.path.isfile(
                    f'{prop.control_files_path}/{prop.ofs}_'
                    f'{name_var}_model_station.ctl') is False
                    and prop.ofsfiletype == 'stations') or
                    prop.user_input_location
                    ):

            logger.info(
                f'Model Control File ({prop.ofs}_{name_var}_model.ctl) not '
                f'found')

            logger.info(
                'Creating Model Control File for %s. This might take a couple '
                'of minutes', variable, )

            logger.info(
                'Searching for the nearest nodes and their respective '
                'depths in relation to the stations found '
                'in the station_ctl_file.ctl'
            )
            control_file = f'{prop.control_files_path}/{prop.ofs}_' \
                               f'{name_var}_station.ctl'
            if prop.user_input_location is False:
                extract = station_ctl_file_extract(
                    control_file)
                try:
                    station_id = [item[0] for item in extract[0]]
                except TypeError:
                    logger.warning('Observation control file is blank, '
                                   'so the model control file for %s '
                                   'cannot be created. Moving to next '
                                   'variable...', variable)
                    continue
            else:
                extract = user_input_extract(prop, logger)
                if len(extract) == 0:
                    logger.error('No location information found in the user '
                                 'input file! Check the file and try again.')
                    raise SystemExit()
                station_id = [item[0] for item in extract[0]]

            if extract is not None:
                if prop.ofsfiletype == 'fields':
                    list_of_nearest_node = \
                        indexing.index_nearest_node(
                        extract[-1],
                        model,
                        prop.model_source,
                        name_var,
                        logger,
                    )
                    list_of_nearest_layer, list_of_depths = \
                        indexing.index_nearest_depth(
                        prop,
                        list_of_nearest_node,
                        model,
                        extract[-1],
                        prop.model_source,
                        name_var,
                        logger,
                    )
                elif prop.ofsfiletype == 'stations':
                    list_of_nearest_node = \
                        indexing.index_nearest_station(
                        extract[-1],
                        model,
                        prop.model_source,
                        name_var,
                        logger,
                        extract[0]
                    )
                    list_of_nearest_layer, list_of_depths = \
                        indexing.index_nearest_depth(
                        prop,
                        list_of_nearest_node,
                        model,
                        extract[-1],
                        prop.model_source,
                        name_var,
                        logger,
                    )

                logger.info('Extracting data found in the Model Control File')

                # This loop is used to write every line of every model ctl file
                model_ctl_file = []

                # Check for valid nodes
                # For stations type, list_of_nearest_layer will be empty (valid)
                # For fields type, check if all values are NaN (invalid)
                if len(list_of_nearest_layer) > 0 and np.isnan(list_of_nearest_layer).all():
                    logger.warning('No user input locations or model nodes '
                                 'found for %s! Moving to next variable',
                                 name_var)
                    continue

                # For stations, use nearest_node length; for fields, use nearest_layer length
                if prop.ofsfiletype == 'stations':
                    length = len(list_of_nearest_node)
                else:
                    length = len(list_of_nearest_layer)
                if prop.model_source == 'fvcom':
                    for i in range(0, length):
                        if np.isnan(list_of_nearest_node[i]) == False:
                            if prop.ofsfiletype == 'fields':
                                if name_var == 'cu':
                                    model_ctl_file.append(
                                        f'{list_of_nearest_node[i]} '
                                        f'{list_of_nearest_layer[i]} '
                                        f"{model['latc'][list_of_nearest_node[i]].data.compute():.3f}  "
                                        f"{model['lonc'][list_of_nearest_node[i]].data.compute() - 360:.3f}  "
                                        f'{station_id[i]}  {list_of_depths[i]:.1f}\n'
                                        )
                                else:
                                    model_ctl_file.append(
                                        f'{list_of_nearest_node[i]} '
                                        f'{list_of_nearest_layer[i]} '
                                        f"{model['lat'][list_of_nearest_node[i]].data.compute():.3f}  "
                                        f"{model['lon'][list_of_nearest_node[i]].data.compute() - 360:.3f}  "
                                        f'{station_id[i]}  {list_of_depths[i]:.1f}\n'
                                        )
                            else:
                                # For stations type, depth layer is not used (set to 0)
                                layer = list_of_nearest_layer[i] if len(list_of_nearest_layer) > 0 else 0
                                depth = list_of_depths[i] if len(list_of_depths) > 0 else 0.0
                                model_ctl_file.append(
                                    f'{list_of_nearest_node[i]} '
                                    f'{layer} '
                                    f"{model['lat'][0,list_of_nearest_node[i]].data.compute():.3f}  "
                                    f"{model['lon'][0,list_of_nearest_node[i]].data.compute() - 360:.3f}  "
                                    f'{station_id[i]}  {depth:.1f}\n'
                                )
                        else:
                            logger.info('No matching model station found for '
                                        'obs station %s.', station_id[i])

                elif prop.model_source == 'roms':
                    for i in range(length):
                        if np.isnan(list_of_nearest_node[i]) == False:
                            # For stations type, depth layer is not used (set to 0)
                            layer = list_of_nearest_layer[i] if len(list_of_nearest_layer) > 0 else 0
                            depth = list_of_depths[i] if len(list_of_depths) > 0 else 0.0
                            model_ctl_file.append(
                            f'{list_of_nearest_node[i]} '
                            f'{layer} '
                            f"{float(model['lat_rho'][np.unravel_index(list_of_nearest_node[i],np.shape(model['lon_rho']))]):.3f}  "
                            f"{float(model['lon_rho'][np.unravel_index(list_of_nearest_node[i],np.shape(model['lon_rho']))]):.3f}  "
                            f'{station_id[i]}  {depth:.1f}\n'
                            )
                        else:
                            logger.info('No matching model station found for '
                                        'obs station %s.', station_id[i])

                elif prop.model_source == 'schism':
                    for i in range(length):
                        if np.isnan(list_of_nearest_node[i]) == False:
                            if prop.ofsfiletype == 'fields':

                                if name_var == 'wl':
                                    x_var = model['SCHISM_hgrid_node_x']
                                    y_var = model['SCHISM_hgrid_node_y']

                                    if 'time' in x_var.dims:
                                        for t in range(x_var.sizes['time']):
                                            test_slice = x_var.isel(time=t)
                                            if not test_slice.isnull().all():
                                                break
                                        x_val = x_var.isel(time=t, nSCHISM_hgrid_node=list_of_nearest_node[i]).values.item()
                                        y_val = y_var.isel(time=t, nSCHISM_hgrid_node=list_of_nearest_node[i]).values.item()
                                    else:
                                        x_val = x_var.isel(nSCHISM_hgrid_node=list_of_nearest_node[i]).values.item()
                                        y_val = y_var.isel(nSCHISM_hgrid_node=list_of_nearest_node[i]).values.item()

                                    # For stations type, depth layer is not used (set to 0)
                                    layer = list_of_nearest_layer[i] if len(list_of_nearest_layer) > 0 else 0
                                    model_ctl_file.append(
                                        f'{list_of_nearest_node[i]} '
                                        f'{layer} '
                                        f'{y_val:.3f}  '
                                        f'{x_val:.3f}  '
                                        f'{station_id[i]}  0.0\n'
                                    )

                                else:
                                    if not np.isnan(list_of_nearest_node[i]):
                                        x_var = model['SCHISM_hgrid_node_x']
                                        y_var = model['SCHISM_hgrid_node_y']

                                        if 'time' in x_var.dims:
                                            for t in range(x_var.sizes['time']):
                                                test_slice = x_var.isel(time=t)
                                                if not test_slice.isnull().all():
                                                    break
                                            x_val = x_var.isel(time=t, nSCHISM_hgrid_node=list_of_nearest_node[i]).values.item()
                                            y_val = y_var.isel(time=t, nSCHISM_hgrid_node=list_of_nearest_node[i]).values.item()
                                        else:
                                            x_val = x_var.isel(nSCHISM_hgrid_node=list_of_nearest_node[i]).values.item()
                                            y_val = y_var.isel(nSCHISM_hgrid_node=list_of_nearest_node[i]).values.item()
                                        # For stations type, depth layer is not used (set to 0)
                                        layer = list_of_nearest_layer[i] if len(list_of_nearest_layer) > 0 else 0
                                        depth = list_of_depths[i] if len(list_of_depths) > 0 else 0.0
                                        model_ctl_file.append(
                                            f'{list_of_nearest_node[i]} '
                                            f'{layer} '
                                            f'{y_val:.3f}  '
                                            f'{x_val:.3f}  '
                                            f'{station_id[i]}  {depth:.1f}\n'
                                        )

                            else:
                                # For stations type, depth layer is not used (set to 0)
                                layer = list_of_nearest_layer[i] if len(list_of_nearest_layer) > 0 else 0
                                depth = list_of_depths[i] if len(list_of_depths) > 0 else 0.0
                                # A mistake in post-processing of stofs-3d-atl
                                if prop.ofs == 'stofs_3d_atl' and model['x'][0,list_of_nearest_node[i]].data.compute()>0 :
                                    model_ctl_file.append(
                                        f'{list_of_nearest_node[i]} '
                                        f'{layer} '
                                        f"{model['x'][0,list_of_nearest_node[i]].data.compute():.3f}  "
                                        f"{model['y'][0,list_of_nearest_node[i]].data.compute():.3f}  "
                                        f'{station_id[i]}  {depth:.1f}\n'
                                    )

                                elif prop.ofs == 'stofs_3d_atl' and model['x'][0,list_of_nearest_node[i]].data.compute()<0 :

                                    model_ctl_file.append(
                                        f'{list_of_nearest_node[i]} '
                                        f'{layer} '
                                        f"{model['y'][0,list_of_nearest_node[i]].data.compute():.3f}  "
                                        f"{model['x'][0,list_of_nearest_node[i]].data.compute():.3f}  "
                                        f'{station_id[i]}  {depth:.1f}\n'
                                    )

                                elif prop.ofs == 'stofs_3d_pac':

                                    model_ctl_file.append(
                                        f'{list_of_nearest_node[i]} '
                                        f'{layer} '
                                        f"{model['y'][0,list_of_nearest_node[i]].data.compute():.3f}  "
                                        f"{model['x'][0,list_of_nearest_node[i]].data.compute() - 360:.3f}  "
                                        f'{station_id[i]}  {depth:.1f}\n'
                                    )

                        else:
                            logger.info('No matching model station found for '
                                        'obs station %s.', station_id[i])

                if prop.ofsfiletype == 'fields':
                    with open(
                        f'{prop.control_files_path}/'
                        f'{prop.ofs}_{name_var}_model.ctl',
                        'w',
                        encoding='utf-8',
                    ) as output:
                        for i in model_ctl_file:
                            output.write(str(i))
                elif prop.ofsfiletype == 'stations':
                    with open(
                        f'{prop.control_files_path}/'
                        f'{prop.ofs}_{name_var}_model_station.ctl',
                        'w',
                        encoding='utf-8',
                    ) as output:
                        for i in model_ctl_file:
                            output.write(str(i))

                logger.info(
                    'Model Control File for %s created successfully',
                    variable,
                )
            else:
                logger.info('Observation ctl file is blank for %s. '
                            'Model ctl file will also be blank', name_var)
                if prop.ofsfiletype == 'fields':
                    with open(
                        f'{prop.control_files_path}/'
                        f'{prop.ofs}_{name_var}_model.ctl',
                        'w',
                        encoding='utf-8',
                    ) as output:
                        pass
                elif prop.ofsfiletype == 'stations':
                    with open(
                        f'{prop.control_files_path}/'
                        f'{prop.ofs}_{name_var}_model_station.ctl',
                        'w',
                        encoding='utf-8',
                    ) as output:
                        pass
        else:
            logger.info(
                'Model Control File (%s_%s_model.ctl) found in %s. If you '
                'instead want to create a new Model Control File, '
                'please change the name/delete the current '
                '%s_%s_model.ctl', prop.ofs, name_var,
                prop.control_files_path, prop.ofs, name_var)

    return model
