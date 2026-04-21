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
It handles multiple model sources (FVCOM, ROMS, SCHISM, ADCIRC) and file types (fields, stations).

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


def _model_bathymetry_at_node(
    node_idx: Any, model: Any, model_source: str,
    logger: Logger,
) -> float:
    """Return local water depth (positive, meters) at a given model node.

    Uses model-source-specific conventions; safely falls back to 0.0
    when the bathymetry field is unavailable or the node is invalid.
    """
    if node_idx is None:
        logger.debug(
            'Bathymetry lookup: node_idx is None; returning 0.0')
        return 0.0
    try:
        if isinstance(node_idx, float) and np.isnan(node_idx):
            logger.debug(
                'Bathymetry lookup: node_idx is NaN; returning 0.0')
            return 0.0
    except TypeError:
        pass
    try:
        if model_source == 'roms':
            h = np.asarray(model['h'])
            h = np.squeeze(h)
            if h.ndim == 3:
                h = h[0]
            flat = h.ravel()
            return float(abs(flat[int(node_idx)]))
        if model_source == 'fvcom':
            # Station-file FVCOM: ``z`` has shape (siglay, node, time) or
            # similar. Take the deepest layer as the bathy proxy.
            if 'h' in model:
                h = np.asarray(model['h'])
                h = np.squeeze(h)
                if h.ndim >= 2:
                    return float(abs(h.flat[int(node_idx)]))
                return float(abs(h[int(node_idx)]))
            z = np.asarray(model['z'])
            if z.ndim == 3:
                return float(abs(z[-1, int(node_idx), 0]))
            if z.ndim == 2:
                return float(abs(z[-1, int(node_idx)]))
            return float(abs(z[int(node_idx)]))
        if model_source == 'schism':
            # Some SCHISM outputs expose 'depth' as a per-level
            # coordinate-axis array rather than nodal bathymetry, so
            # validate rank before accepting 'depth' or 'h'.
            for key in ('depth', 'h', 'zcoords'):
                if key not in model:
                    continue
                arr = np.asarray(model[key])
                if key == 'zcoords':
                    return float(abs(arr[0, int(node_idx), -1]))
                if arr.ndim != 1:
                    logger.debug(
                        "Bathymetry lookup: SCHISM key %r is %d-D "
                        "(expected 1-D nodal array); skipping",
                        key, arr.ndim)
                    continue
                return float(abs(arr[int(node_idx)]))
            logger.debug(
                'Bathymetry lookup: SCHISM exhausted candidate keys '
                '(depth, h, zcoords) without a valid nodal array; '
                'returning 0.0')
            return 0.0
    except (KeyError, IndexError, ValueError, TypeError) as ex:
        logger.debug(
            'Bathymetry lookup for %s raised %s (%s); returning 0.0',
            model_source, type(ex).__name__, ex)
        return 0.0
    logger.debug(
        'Bathymetry lookup: no handler matched model_source=%r; '
        'returning 0.0', model_source)
    return 0.0


def _resolve_side_looking_depths(
    prop: Any,
    extract: Any,
    list_of_nearest_node: list,
    model: Any,
    control_file_path: str,
    logger: Logger,
) -> None:
    """Patch obs depths for side-looking ADCP bins in ``extract[-1]``.

    The obs station ctl file carries a 6th field on each coord line
    containing ``height_from_bottom`` (hfb, meters above channel
    bottom) for CO-OPS ADCPs whose per-bin MDAPI ``depth`` is null.
    When the 4th field (obs depth) is written as 0.0 for such a bin,
    resolve it here as ``water_depth - hfb`` using model bathymetry
    at the nearest node. Mutates ``extract[-1]`` in place and rewrites
    the obs station.ctl file so downstream readers (index_nearest_depth,
    the plot title code) see the corrected depth.
    """
    coord_rows = extract[-1]
    info_rows = extract[0]
    updated = 0
    for idx, coords in enumerate(coord_rows):
        if len(coords) < 6:
            continue
        try:
            depth = float(coords[3])
            hfb = float(coords[5])
        except (TypeError, ValueError, IndexError):
            continue
        if hfb <= 0 or depth > 0.01:
            continue
        if idx >= len(list_of_nearest_node):
            continue
        node = list_of_nearest_node[idx]
        water_depth = _model_bathymetry_at_node(
            node, model, prop.model_source, logger)
        if water_depth <= 0:
            continue
        resolved = max(0.0, water_depth - hfb)
        # Mutate in place as a string to keep downstream parsers happy.
        coords[3] = f'{resolved:.2f}'
        updated += 1
        try:
            station_id = info_rows[idx][0]
        except (IndexError, TypeError):
            station_id = f'idx={idx}'
        if hfb > water_depth:
            logger.warning(
                'Station %s height_from_bottom %.2f m exceeds model '
                'bathymetry %.2f m at nearest node %s — obs depth '
                'clamped to 0.0 m; skill metrics for this station '
                'may be meaningless.',
                station_id, hfb, water_depth, node)
        else:
            logger.info(
                'Side-looking ADCP %s: water_depth=%.2f m, hfb=%.2f m '
                '-> obs_depth=%.2f m',
                station_id, water_depth, hfb, resolved)

    if updated and control_file_path:
        try:
            with open(control_file_path, 'w', encoding='utf-8') as fh:
                for info, coords in zip(info_rows, coord_rows):
                    fh.write(
                        f'{info[0]} {info[1]} "{info[2]}"\n'
                        f'  {" ".join(str(x) for x in coords)}\n'
                    )
            logger.info(
                'Updated %d side-looking ADCP obs depths in %s',
                updated, control_file_path)
        except OSError as ex:
            logger.warning(
                'Could not rewrite obs ctl file with resolved depths: %s',
                ex)


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
    _conf = getattr(prop, 'config_file', None)
    xy_path = (utils.Utils(_conf).read_config_section('user_xy_inputs', logger)
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
            Model type ('fvcom', 'roms', 'schism', 'adcirc')
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
    - Different coordinate extraction methods for FVCOM, ROMS, SCHISM, and ADCIRC models
    - Skips stations where no matching model node can be found

    Examples
    --------
    >>> model = write_ofs_ctlfile(prop, model, logger)
    INFO:root:Model Control File for water_level created successfully
    """
    _conf = getattr(prop, 'config_file', None)
    dir_params = utils.Utils(_conf).read_config_section('directories', logger)

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
                elif prop.ofsfiletype == 'stations':
                    list_of_nearest_node = \
                        indexing.index_nearest_station(
                        prop,
                        extract[-1],
                        model,
                        prop.model_source,
                        name_var,
                        logger,
                        extract[0]
                    )

                # For side-looking ADCPs (``height_from_bottom`` recorded
                # in the 6th field of the station coord line, depth
                # written as 0.0), resolve an accurate obs depth from
                # the model bathymetry at the nearest node before the
                # vertical-layer search runs.  Mutates ``extract[-1]``
                # in place and back-writes the updated obs ctl file so
                # plotting reads the same depth.
                if name_var == 'cu':
                    _resolve_side_looking_depths(
                        prop, extract, list_of_nearest_node, model,
                        control_file, logger,
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
                if len(list_of_nearest_node) > 0 and np.isnan(list_of_nearest_node).all():
                    logger.warning('No user input locations or model nodes '
                                 'found for %s! Moving to next variable',
                                 name_var)
                    continue
                length = len(list_of_nearest_node)
                if prop.model_source=='fvcom':
                    for i in range(0, length):
                        if not np.isnan(list_of_nearest_node[i]):
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
                                model_ctl_file.append(
                                    f'{list_of_nearest_node[i]} '
                                    f'{list_of_nearest_layer[i]} '
                                    f"{model['lat'][0,list_of_nearest_node[i]].data.compute():.3f}  "
                                    f"{model['lon'][0,list_of_nearest_node[i]].data.compute() - 360:.3f}  "
                                    f'{station_id[i]}  {list_of_depths[i]:.1f}\n'
                                )
                        else:
                            logger.info('No matching model station found for '
                                        'obs station %s.', station_id[i])

                elif prop.model_source=='roms':
                    for i in range(length):
                        if not np.isnan(list_of_nearest_node[i]):
                            model_ctl_file.append(
                            f'{list_of_nearest_node[i]} '
                            f'{list_of_nearest_layer[i]} '
                            f"{float(model['lat_rho'][np.unravel_index(list_of_nearest_node[i],np.shape(model['lon_rho']))]):.3f}  "
                            f"{float(model['lon_rho'][np.unravel_index(list_of_nearest_node[i],np.shape(model['lon_rho']))]):.3f}  "
                            f'{station_id[i]}  {list_of_depths[i]:.1f}\n'
                            )
                        else:
                            logger.info('No matching model station found for '
                                        'obs station %s.', station_id[i])

                elif prop.model_source=='schism':
                    for i in range(length):
                        if not np.isnan(list_of_nearest_node[i]):
                            if prop.ofsfiletype == 'fields':
                                if name_var == 'wl':
                                    x_var = model['SCHISM_hgrid_node_x']
                                    y_var = model['SCHISM_hgrid_node_y']

                                    if 'time' in x_var.dims:
                                        for t in range(x_var.sizes['time']):
                                            test_slice = x_var.isel(time=t)
                                            if not test_slice.isnull().all():
                                                break
                                        x_val = x_var.isel(time=t,
                                                           nSCHISM_hgrid_node=list_of_nearest_node[i]).values.item()
                                        y_val = y_var.isel(time=t,
                                                           nSCHISM_hgrid_node=list_of_nearest_node[i]).values.item()
                                    else:
                                        x_val = x_var.isel(
                                            nSCHISM_hgrid_node=list_of_nearest_node[i]).values.item()
                                        y_val = y_var.isel(
                                            nSCHISM_hgrid_node=list_of_nearest_node[i]).values.item()

                                    model_ctl_file.append(
                                        f'{list_of_nearest_node[i]} '
                                        f'{list_of_nearest_layer[i]} '
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
                                            x_val = x_var.isel(time=t,
                                                               nSCHISM_hgrid_node=list_of_nearest_node[i]).values.item()
                                            y_val = y_var.isel(time=t,
                                                               nSCHISM_hgrid_node=list_of_nearest_node[i]).values.item()
                                        else:
                                            x_val = x_var.isel(
                                                nSCHISM_hgrid_node=list_of_nearest_node[i]).values.item()
                                            y_val = y_var.isel(
                                                nSCHISM_hgrid_node=list_of_nearest_node[i]).values.item()
                                        model_ctl_file.append(
                                            f'{list_of_nearest_node[i]} '
                                            f'{list_of_nearest_layer[i]} '
                                            f'{y_val:.3f}  '
                                            f'{x_val:.3f}  '
                                            f'{station_id[i]}  {list_of_depths[i]:.1f}\n'
                                        )
                            elif prop.ofsfiletype == 'stations':
                                if 'stofs' not in prop.ofs:
                                    model_ctl_file.append(
                                        f'{list_of_nearest_node[i]} '
                                        f'{list_of_nearest_layer[i]} '
                                        f"{model['lat'][0,list_of_nearest_node[i]].data.compute():.3f}  "
                                        f"{model['lon'][0,list_of_nearest_node[i]].data.compute():.3f}  "
                                        f'{station_id[i]}  {list_of_depths[i]:.1f}\n'
                                    )
                                # A mistake in post-processing of stofs-3d-atl
                                elif prop.ofs == 'stofs_3d_atl' and \
                                    model['x'][0,list_of_nearest_node[i]].data.compute()>0 :
                                    # For stations type, depth layer is not used (set to 0)
                                    layer = list_of_nearest_layer[i] if len(list_of_nearest_layer) > 0 else 0
                                    depth = list_of_depths[i] if len(list_of_depths) > 0 else 0.0
                                    model_ctl_file.append(
                                        f'{list_of_nearest_node[i]} '
                                        f'{layer} '
                                        f"{model['x'][0,list_of_nearest_node[i]].data.compute():.3f}  "
                                        f"{model['y'][0,list_of_nearest_node[i]].data.compute():.3f}  "
                                        f'{station_id[i]}  {depth:.1f}\n'
                                    )

                                elif prop.ofs == 'stofs_3d_atl' and \
                                    model['x'][0,list_of_nearest_node[i]].data.compute()<0 :
                                    # For stations type, depth layer is not used (set to 0)
                                    layer = list_of_nearest_layer[i] if len(list_of_nearest_layer) > 0 else 0
                                    depth = list_of_depths[i] if len(list_of_depths) > 0 else 0.0
                                    model_ctl_file.append(
                                        f'{list_of_nearest_node[i]} '
                                        f'{layer} '
                                        f"{model['y'][0,list_of_nearest_node[i]].data.compute():.3f}  "
                                        f"{model['x'][0,list_of_nearest_node[i]].data.compute():.3f}  "
                                        f'{station_id[i]}  {depth:.1f}\n'
                                    )
                                elif prop.ofs == 'stofs_3d_pac':
                                    # For stations type, depth layer is not used (set to 0)
                                    layer = list_of_nearest_layer[i] if len(list_of_nearest_layer) > 0 else 0
                                    depth = list_of_depths[i] if len(list_of_depths) > 0 else 0.0
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

                elif prop.model_source == 'adcirc':
                    if prop.ofs == 'stofs_2d_glo':
                        for i in range(length):
                            if ~np.isnan(list_of_nearest_node[i]):
                                # Note the 0 and 0.0 values because STOFS-2D has no layers/depths.
                                model_ctl_file.append(
                                    f'{list_of_nearest_node[i]} '
                                    f'0 '
                                    f"{model['y'][0, list_of_nearest_node[i]].data.compute():.3f}  "
                                    f"{model['x'][0, list_of_nearest_node[i]].data.compute():.3f}  "
                                    f'{station_id[i]}  0.0\n'
                                )
                            else:
                                logger.info('No matching model station found for '
                                            'obs station %s.', station_id[i])
                    else:
                        # STOFS-2D-Global is the only ADCIRC implemented, so it's
                        # not clear how someone would even get here, but we raise
                        # an exception just in case.
                        raise NotImplementedError('ADCIRC control file writing not yet implemented for models other than STOFS-2D-Global.')

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
