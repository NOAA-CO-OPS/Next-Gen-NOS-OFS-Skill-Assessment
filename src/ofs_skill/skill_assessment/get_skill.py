"""
 This is the final skill assessment script.
 This function reads the obs and ofs control files, search for the
 respective data and creates the paired (.int) datasets and skill table.
"""


import copy
import logging
import logging.config
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from ofs_skill.model_processing import do_horizon_skill
from ofs_skill.model_processing.get_fcst_cycle import get_fcst_dates
from ofs_skill.model_processing.get_node_ofs import get_node_ofs
from ofs_skill.obs_retrieval import parse_arguments_to_list, utils
from ofs_skill.obs_retrieval.get_station_observations import get_station_observations
from ofs_skill.obs_retrieval.station_ctl_file_extract import station_ctl_file_extract
from ofs_skill.obs_retrieval.utils import get_parallel_config
from ofs_skill.skill_assessment import format_paired_one_d, metrics_paired_one_d
from ofs_skill.skill_assessment.make_skill_maps import make_skill_maps
from ofs_skill.tidal_analysis.extremes import extract_water_level_extrema


def _cache_key(prop):
    """Fields that uniquely determine the model dataset loaded by get_node_ofs.

    When any of these change on `prop`, the cached dataset is stale and must
    not be reused (it was built from a different set of source files).
    """
    return (
        getattr(prop, 'ofs', None),
        getattr(prop, 'whichcast', None),
        getattr(prop, 'forecast_hr', None),
        getattr(prop, 'start_date_full', None),
        getattr(prop, 'end_date_full', None),
        getattr(prop, 'ofsfiletype', None),
    )


def _get_valid_cached_model(prop):
    """Return the cached model only if it was loaded under the current key."""
    cached = getattr(prop, '_cached_model', None)
    if cached is None:
        return None
    if getattr(prop, '_cached_model_key', None) != _cache_key(prop):
        return None
    return cached


def _set_cached_model(prop, dataset):
    """Stamp the dataset with the current key when caching."""
    if dataset is None:
        return
    prop._cached_model = dataset
    prop._cached_model_key = _cache_key(prop)


def ofs_ctlfile_extract(prop, name_var, logger, model_dataset=None):
    """
    Extract info from model control files. If control file does not exist,
    create it.

    Parameters
    ----------
    model_dataset : xarray.Dataset or None
        Pre-loaded model dataset to avoid redundant intake_model() calls.
    """

    if prop.ofsfiletype == 'fields':
        ctl_path = os.path.join(prop.control_files_path,
                                str(prop.ofs+'_'+name_var+'_model.ctl'))
        if not os.path.isfile(ctl_path):
            result = get_node_ofs(prop, logger,
                                  model_dataset=model_dataset)
            _set_cached_model(prop, result)
    elif prop.ofsfiletype == 'stations':
        ctl_path = os.path.join(prop.control_files_path,
                            str(prop.ofs+'_'+name_var+'_model_station.ctl'))
        if not os.path.isfile(ctl_path):
            result = get_node_ofs(prop, logger,
                                  model_dataset=model_dataset)
            _set_cached_model(prop, result)

    try:
        if os.path.getsize(ctl_path) > 0:
            with open(ctl_path, encoding='utf-8') as file:
                read_ofs_ctl_file = file.read()

                lines = read_ofs_ctl_file.split('\n')
                lines = [x for x in lines if x != '']
                lines = [i.split(' ') for i in lines]
                lines = [list(filter(None, i)) for i in lines]

                nodes = np.array(lines)[:, 0]
                nodes = [int(i) for i in nodes]

                depths = np.array(lines)[:,1]
                # this is the index of the nearest siglay to the
                # observations station
                depths = [int(i) for i in depths]

                shifts = np.array(lines)[:, -1]
                # this is the shift that can be applied to the ofs timeseries,
                # for instance if there is a known bias in the model
                shifts = [float(i) for i in shifts]

                ids = np.array(lines)[:, -2]
                ids = [str(i) for i in ids]

                return lines, nodes, depths, shifts, ids
    except FileNotFoundError:
        logger.warning(
            '%s model control file is missing.', name_var)
    return None


def prepare_series(read_station_ctl_file, read_ofs_ctl_file, prop,
                   name_var, i, obs_row, logger):
    """
    This is creating the paired (model and obs) timeseries used in the skill
    assessment using the .prd and .obs text files.
    """
    formatted_series = 'NoDataFound'
    obs_df = None
    ofs_df = None

    if read_station_ctl_file[0][obs_row][0] == read_ofs_ctl_file[-1][i]:
        obs_path = os.path.join(prop.data_observations_1d_station_path,
                str(read_station_ctl_file[0][obs_row][0]+'_'+prop.ofs+'_'+name_var+\
                    '_station.obs'))

        if os.path.isfile(obs_path):
            if os.path.getsize(obs_path) > 0:
                obs_df = pd.read_csv(obs_path,
                    sep=r'\s+',
                    header=None,
                )
            else:
                logger.error(
                    '%s/%s_%s_%s_station.obs is empty',
                    prop.data_observations_1d_station_path,
                    read_station_ctl_file[0][obs_row][0],
                    prop.ofs,
                    name_var,
                )
                return formatted_series
        else:
            logger.error(
                '%s/%s_%s_%s_station.obs is missing',
                prop.data_observations_1d_station_path,
                read_station_ctl_file[0][obs_row][0],
                prop.ofs,
                name_var,
            )
        if prop.whichcast == 'forecast_a':
            prdfile = str(read_ofs_ctl_file[-1][i]) +\
                '_'+prop.ofs+'_'+name_var+'_'+str(read_ofs_ctl_file[1][i]) +\
                '_'+prop.whichcast+'_'+str(prop.forecast_hr)+\
                '_'+str(prop.ofsfiletype)+'_model.prd'

            prd_path = os.path.join(prop.data_model_1d_node_path,prdfile)
            if os.path.isfile(prd_path):
                ofs_df = pd.read_csv(prd_path,
                    sep=r'\s+',
                    header=None,
                )
            else:
                logger.error(
                    '%s/%s_%s_%s_%s_%s_%s_%s_model.prd is missing',
                    prop.data_model_1d_node_path,
                    read_ofs_ctl_file[-1][i],
                    prop.ofs,
                    name_var,
                    read_ofs_ctl_file[1][i],
                    prop.whichcast,
                    prop.forecast_hr,
                    prop.ofsfiletype
                )
        else:
            prd_path = os.path.join(prop.data_model_1d_node_path,
                    str(read_ofs_ctl_file[-1][i]+'_'+prop.ofs+'_'+name_var
                    +'_'+str(read_ofs_ctl_file[1][i])+'_'+prop.whichcast+\
                    '_'+prop.ofsfiletype+'_model.prd'))
            if os.path.isfile(prd_path) is False :
                logger.info(
                    '%s/%s_%s_%s_%s_%s_%s_model.prd is missing',
                    prop.data_model_1d_node_path,
                    read_ofs_ctl_file[-1][i],
                    prop.ofs,
                    name_var,
                    read_ofs_ctl_file[1][i],
                    prop.whichcast,
                    prop.ofsfiletype
                )
                logger.info(
                    'Calling OFS module for %s',
                    prop.whichcast,
                )
                cached = _get_valid_cached_model(prop)
                result = get_node_ofs(prop, logger,
                                      model_dataset=cached)
                _set_cached_model(prop, result)

            ofs_df = pd.read_csv(prd_path,
                sep=r'\s+',
                header=None,
                )

        if (
            ofs_df is not None
            and obs_df is not None
            and len(obs_df) > 0
            and len(ofs_df) > 0
        ):
            if name_var == 'cu':
                formatted_series = format_paired_one_d.paired_vector(
                    obs_df, ofs_df, prop.start_date_full, prop.end_date_full,
                    logger
                )
            else:
                formatted_series = format_paired_one_d.paired_scalar(
                    obs_df, ofs_df, prop.start_date_full, prop.end_date_full,
                    logger
                )

    return formatted_series


def _station_metadata(read_station_ctl_file, read_ofs_ctl_file, obs_idx, ofs_idx):
    """Build the per-station metadata dict shared by primary + direction + extrema outputs."""
    return {
        'station_id': read_station_ctl_file[0][obs_idx][0],
        'node': read_ofs_ctl_file[1][ofs_idx],
        'obs_depth': read_station_ctl_file[1][obs_idx][-2],
        'mod_depth': read_ofs_ctl_file[-2][ofs_idx],
        'X': str(float(read_station_ctl_file[1][obs_idx][1])),
        'Y': read_station_ctl_file[1][obs_idx][0],
    }


def _process_station_pair(i, read_station_ctl_file, read_ofs_ctl_file,
                          station_id_to_idx, prop, name_var, logger):
    """
    Process a single station pair: match IDs, compute pairing + metrics,
    and write the .int file.

    Returns a dict with a 'primary' key (always) plus optional 'direction',
    'hw', 'lw' sub-dicts. Returns None if this station cannot be processed.
    """
    try:
        ofs_station_id = read_ofs_ctl_file[-1][i]
        if ofs_station_id not in station_id_to_idx:
            logger.error(
                f'Could not match station ID {ofs_station_id} between '
                f'control file in get_node_ofs!'
            )
            return None

        obs_row = station_id_to_idx[ofs_station_id]
        station_id = read_station_ctl_file[0][obs_row][0]

        formatted_series = prepare_series(
            read_station_ctl_file, read_ofs_ctl_file, prop,
            name_var, i, obs_row, logger
        )

        if not (
            formatted_series
            and formatted_series != 'NoDataFound'
            and len(formatted_series[0]) > 1
        ):
            logger.error(
                f'{prop.ofs}_{name_var}_{station_id}_{read_ofs_ctl_file[1][i]}_'
                f'{prop.whichcast}_{prop.ofsfiletype}_pair.int is not created successfully'
            )
            return None

        series_df = formatted_series[-1]
        series_df['DateTime'] = pd.to_datetime({
            'year': series_df[1], 'month': series_df[2], 'day': series_df[3],
            'hour': series_df[4], 'minute': series_df[5]
        })

        out = {}
        primary = _station_metadata(read_station_ctl_file, read_ofs_ctl_file, obs_row, i)

        if name_var == 'cu':
            logger.info(f'Start cu metrics for {station_id}')
            primary['skill'] = metrics_paired_one_d.skill_vector(
                series_df, name_var, prop, logger
            )
            out['primary'] = primary

            logger.info(f'Start cu dir metrics for {station_id}')
            direction = _station_metadata(read_station_ctl_file, read_ofs_ctl_file, obs_row, i)
            direction['skill'] = metrics_paired_one_d.skill_vector_dir(
                series_df, name_var, prop, logger
            )
            out['direction'] = direction
        else:
            logger.info(f'Start {name_var} metrics for {station_id}')
            primary['skill'] = metrics_paired_one_d.skill_scalar(
                series_df, name_var, station_id, prop, logger
            )
            out['primary'] = primary

        # Write the paired time series file (.int)
        filename = (
            f'{prop.ofs}_{name_var}_{station_id}_{read_ofs_ctl_file[1][i]}_'
            f'{prop.whichcast}_{prop.ofsfiletype}_pair.int'
        )
        int_path = os.path.join(prop.data_skill_1d_pair_path, filename)
        with open(int_path, 'w', encoding='utf-8') as output_2:
            if name_var == 'cu':
                output_2.write(
                    'DNUM_JAN1 YEAR MONTH DAY HOUR MINUTE SPEED_OB '
                    'SPEED_MODEL BIAS_SPEED DIR_OB DIR_MODEL BIAS_DIR \n'
                )
            else:
                output_2.write(
                    'DNUM_JAN1 YEAR MONTH DAY HOUR MINUTE VAL_OB '
                    'VAL_MODEL BIAS \n'
                )
            for p_value in formatted_series[0]:
                cleaned_val = str(p_value).replace(',', ' ').replace('[', '').replace(']', '')
                output_2.write(f'{cleaned_val}\n')
        logger.info(f'{filename} is created successfully')

        # Water-level extrema (HW/LW) independent detection + ±3h pairing
        if name_var == 'wl':
            mod_extrema = extract_water_level_extrema(
                np.asarray(series_df['DateTime']),
                np.asarray(series_df['OFS']), 4, logger
            )
            obs_extrema = extract_water_level_extrema(
                np.asarray(series_df['DateTime']),
                np.asarray(series_df['OBS']), 4, logger
            )

            for extrema_type, out_key, log_label in (
                ('high_water', 'hw', 'high water extrema'),
                ('low_water', 'lw', 'low water extrema'),
            ):
                m_times = mod_extrema[f'{extrema_type}_times']
                m_amps = mod_extrema[f'{extrema_type}_amplitudes']
                o_times = obs_extrema[f'{extrema_type}_times']
                o_amps = obs_extrema[f'{extrema_type}_amplitudes']

                paired_data = []
                matched_obs_idx = set()
                window = np.timedelta64(3, 'h')  # Standard NOS pairing window
                for mt, ma in zip(m_times, m_amps):
                    # Candidate obs extrema: within window AND not yet claimed
                    # by another model extremum. This prevents two nearby model
                    # peaks from both pairing to the same observed peak.
                    candidates = [
                        k for k in range(len(o_times))
                        if k not in matched_obs_idx
                        and (mt - window) <= o_times[k] <= (mt + window)
                    ]
                    if not candidates:
                        continue
                    best = candidates[
                        int(np.argmin([abs(o_times[k] - mt) for k in candidates]))
                    ]
                    matched_obs_idx.add(best)
                    ot = o_times[best]
                    oa = o_amps[best]
                    paired_data.append({
                        'DateTime': mt,
                        'OFS': ma,
                        'OBS': oa,
                        'BIAS': ma - oa,
                        'TIMING_ERR': (mt - ot) / np.timedelta64(1, 'h'),
                    })

                # Surface detection-rate asymmetry: extrema counted on one
                # series but not the other. Silent under the prior impl.
                unmatched_mod = len(m_times) - len(paired_data)
                unmatched_obs = len(o_times) - len(matched_obs_idx)
                if unmatched_mod or unmatched_obs:
                    logger.info(
                        '%s %s: %d mod / %d obs extrema unmatched within '
                        '%s window',
                        station_id, log_label, unmatched_mod, unmatched_obs,
                        window,
                    )

                if paired_data:
                    df_extrema = pd.DataFrame(paired_data)
                    logger.info(
                        f'Start {name_var} metrics for {station_id} {log_label}'
                    )
                    extrema_entry = _station_metadata(
                        read_station_ctl_file, read_ofs_ctl_file, obs_row, i
                    )
                    extrema_entry['skill'] = metrics_paired_one_d.skill_extrema(
                        df_extrema, name_var, prop
                    )
                    # Detection-rate visibility: count of matched pairs vs
                    # each series' total extrema, for downstream CSV column.
                    extrema_entry['n_mod_extrema'] = len(m_times)
                    extrema_entry['n_obs_extrema'] = len(o_times)
                    extrema_entry['n_matched_pairs'] = len(paired_data)
                    out[out_key] = extrema_entry

        return out
    except (KeyboardInterrupt, SystemExit):
        # Never swallow these — they must propagate out of worker threads
        raise
    except Exception:
        # Worker-local isolation: one bad station must not kill the whole
        # ThreadPoolExecutor. Log with traceback and return None so the
        # aggregator skips this station.
        logger.exception('Unexpected error processing station index %d', i)
        return None


def skill(read_station_ctl_file, read_ofs_ctl_file, prop, name_var, logger):
    """
    This function 1) writes the paired observation and model time series to
    file (.int), and 2) sends the paired time series to
    metrics_paired_one_d to calculate skill stats, which are returned from the
    function.

    Returns a list: [output] for scalars, [output, output_dir] for currents,
    [output, output_hw, output_lw] for water level (when extrema were paired).
    """

    def _create_output_dict(extrema=False):
        out = {
            'station_id': [], 'X': [], 'Y': [],
            'obs_depth': [], 'mod_depth': [], 'node': [], 'skill': [],
        }
        if extrema:
            # Extra columns for HW/LW variants so the CSV surfaces detection
            # asymmetry between model and observation extrema series.
            out.update({
                'n_mod_extrema': [], 'n_obs_extrema': [], 'n_matched_pairs': [],
            })
        return out

    output = _create_output_dict()
    output_dir = _create_output_dict()
    output_hw = _create_output_dict(extrema=True)
    output_lw = _create_output_dict(extrema=True)

    data_length = min(len(read_station_ctl_file[0]), len(read_ofs_ctl_file[-1]))

    # O(1) station ID -> obs-row lookup, computed once and shared with workers
    station_id_to_idx = {
        row[0]: idx for idx, row in enumerate(read_station_ctl_file[0])
    }

    parallel_config = get_parallel_config(logger)
    max_workers = parallel_config['skill_workers']

    def _append_entry(target_dict, entry):
        for key in target_dict:
            target_dict[key].append(entry[key])

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                _process_station_pair, i, read_station_ctl_file,
                read_ofs_ctl_file, station_id_to_idx, prop, name_var, logger
            )
            for i in range(data_length)
        ]
        # Iterate in submission order to preserve consistent CSV output
        for future in futures:
            result = future.result()
            if result is None:
                continue
            _append_entry(output, result['primary'])
            if 'direction' in result:
                _append_entry(output_dir, result['direction'])
            if 'hw' in result:
                _append_entry(output_hw, result['hw'])
            if 'lw' in result:
                _append_entry(output_lw, result['lw'])

    # Construct final output payload
    if name_var == 'cu' and len(output_dir['station_id']) > 0:
        return [output, output_dir]
    if name_var == 'wl' and len(output_hw['station_id']) > 0:
        return [output, output_hw, output_lw]
    return [output]

def name_convent(variable):
    """
    Set variable names so they correspond to names used in model output data
    """
    name_var = []
    if variable == 'water_level':
        name_var = 'wl'

    elif variable == 'water_temperature':
        name_var = 'temp'

    elif variable == 'salinity':
        name_var = 'salt'

    elif variable == 'currents':
        name_var = 'cu'

    return name_var


def get_skill(prop, logger):
    """
 This is the final skill assessment script.
 This function reads the obs and ofs control files, search for the
 respective data and creates the paired (.int) datasets and skill table.
    """

    if logger is None:
        config_file = utils.Utils().get_config_file()
        log_config_file = 'conf/logging.conf'
        log_config_file = os.path.join(Path(prop.path), log_config_file)

        # Check if log file exists
        if not os.path.isfile(log_config_file):
            sys.exit(-1)
        # Check if config file exists
        if not os.path.isfile(config_file):
            sys.exit(-1)

        # Creater logger
        logging.config.fileConfig(log_config_file)
        logger = logging.getLogger('root')
        logger.info('Using config %s', config_file)
        logger.info('Using log config %s', log_config_file)

    logger.info('--- Starting skill assessment process ---')

    _conf = getattr(prop, 'config_file', None)
    dir_params = utils.Utils(_conf).read_config_section('directories', logger)
    prop.datum_list = (utils.Utils(_conf).read_config_section('datums', logger)\
                       ['datum_list']).split(' ')

    # Do forecast_a start and end date reshuffle

    if 'forecast_a' in prop.whichcast:
        if prop.forecast_hr is not None:
            prop.start_date_full, prop.end_date_full =\
            get_fcst_dates(prop, logger)
            prop.forecast_hr = prop.start_date_full.split('T')[1][0:2] + 'z'
            logger.info(f'Forecast_a: start date reassigned to '
                             f'{prop.start_date_full}')
            logger.info(f'Forecast_a: end date reassigned to '
                             f'{prop.end_date_full}')
        else:
            raise SystemExit(1)

    try:
        start_date = datetime.strptime(prop.start_date_full,'%Y%m%d-%H:%M:%S')
        end_date = datetime.strptime(prop.end_date_full,'%Y%m%d-%H:%M:%S')
        prop.start_date_full = start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        prop.end_date_full = end_date.strftime('%Y-%m-%dT%H:%M:%SZ')

    except ValueError:
        pass

    # Parse incoming arguments stored in prop from string to a list
    prop.stationowner = parse_arguments_to_list(prop.stationowner, logger)
    prop.var_list = parse_arguments_to_list(prop.var_list, logger)

    # Start Date and End Date validation
    try:
        datetime.strptime(prop.start_date_full, '%Y-%m-%dT%H:%M:%SZ')
        datetime.strptime(prop.end_date_full, '%Y-%m-%dT%H:%M:%SZ')
    except ValueError:
        error_message = (
            f'Please check Start Date - '
            f'{prop.start_date_full}, End Date - '
            f'{prop.end_date_full}. Abort!'
        )
        logger.error(error_message)
        print(error_message)
        sys.exit(-1)

    if datetime.strptime(
        prop.start_date_full, '%Y-%m-%dT%H:%M:%SZ'
    ) > datetime.strptime(prop.end_date_full, '%Y-%m-%dT%H:%M:%SZ'):
        error_message = (
            f'End Date {prop.end_date_full} '
            f'is before Start Date {prop.start_date_full}. Abort!'
        )
        logger.error(error_message)
        sys.exit(-1)

    if prop.path is None:
        prop.path = Path(dir_params['home'])

    # prop.path validation
    ofs_extents_path = os.path.join(prop.path, dir_params['ofs_extents_dir'])
    if not os.path.exists(ofs_extents_path):
        error_message = (
            f'ofs_extents/ folder is not found. '
            f'Please check prop.path - {prop.path}. Abort!'
        )
        logger.error(error_message)
        sys.exit(-1)

    # prop.ofs validation
    shape_file = f'{ofs_extents_path}/{prop.ofs}.shp'
    if not os.path.isfile(shape_file):
        error_message = (
            f'Shapefile {prop.ofs} is not found at '
            f'the folder {ofs_extents_path}. Abort!'
        )
        logger.error(error_message)
        sys.exit(-1)

    # prop.whichcast validation
    if (prop.whichcast is not None) and (
        prop.whichcast not in ['nowcast', 'forecast_a', 'forecast_b', 'hindcast']
    ):
        error_message = f'Please check prop.whichcast - ' \
                        f'{prop.whichcast}. Abort!'

        logger.error(error_message)
        sys.exit(-1)

    if prop.whichcast == 'forecast_a' and prop.forecast_hr is None:
        error_message = (
            'prop.forecast_hr is required if prop.whichcast is '
            'forecast_a. Abort!'
        )
        logger.error(error_message)
        sys.exit(-1)

    prop.control_files_path = os.path.join(
        prop.path, dir_params['control_files_dir']
    )
    os.makedirs(prop.control_files_path, exist_ok=True)

    prop.data_observations_1d_station_path = os.path.join(
        prop.path,
        dir_params['data_dir'],
        dir_params['observations_dir'],
        dir_params['1d_station_dir'],
    )
    os.makedirs(prop.data_observations_1d_station_path, exist_ok=True)

    prop.data_model_1d_node_path = os.path.join(
        prop.path,
        dir_params['data_dir'],
        dir_params['model_dir'],
        dir_params['1d_node_dir'],
    )
    os.makedirs(prop.data_model_1d_node_path, exist_ok=True)

    prop.data_skill_1d_pair_path = os.path.join(
        prop.path,
        dir_params['data_dir'],
        dir_params['skill_dir'],
        dir_params['1d_pair_dir'],
    )
    os.makedirs(prop.data_skill_1d_pair_path, exist_ok=True)

    prop.data_skill_1d_table_path = os.path.join(
        prop.path,
        dir_params['data_dir'],
        dir_params['skill_dir'],
        dir_params['stats_dir'],
    )
    os.makedirs(prop.data_skill_1d_table_path, exist_ok=True)

    prop.visuals_1d_station_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['visual_dir'], )
    os.makedirs(prop.visuals_1d_station_path, exist_ok=True)

    # This outer loop is used to download all data for all variables
    # Inside this loop there is another loop that will go over each line
    # in the station ctl file and will try to download the data from TandC,
    # USGS, and NDBC based on the station data source

    def _ensure_obs_files(read_station_ctl_file, p, name_var, logger_):
        """Check for missing .obs files, download if needed."""
        for i in range(0, len(read_station_ctl_file[0])):
            obs_path = os.path.join(p.data_observations_1d_station_path,
                    str(read_station_ctl_file[0][i][0]+'_'+p.ofs+'_'+\
                        name_var+'_station.obs'))
            if os.path.isfile(obs_path):
                if os.path.getsize(obs_path) > 0:
                    logger_.info(
                        '%s/%s_%s_%s_station.obs found',
                        p.data_observations_1d_station_path,
                        read_station_ctl_file[0][i][0],
                        p.ofs,
                        name_var,
                    )
                else:
                    logger_.error(
                        '%s/%s_%s_%s_station.obs is empty',
                        p.data_observations_1d_station_path,
                        read_station_ctl_file[0][i][0],
                        p.ofs,
                        name_var,
                    )
            else:
                logger_.error(
                    '%s/%s_%s_%s_station.obs is missing, calling Obs Module',
                    p.data_observations_1d_station_path,
                    read_station_ctl_file[0][i][0],
                    p.ofs,
                    name_var,
                )
                get_station_observations(p, logger_)
                break

    def _ensure_prd_files(read_ofs_ctl_file, p, name_var, logger_,
                          cached_model=None):
        """Check for missing .prd files, extract if needed.
        Returns the (possibly updated) cached model dataset."""
        for i in range(0, len(read_ofs_ctl_file[-1])):
            if p.whichcast == 'forecast_a':
                if os.path.isfile(
                    f'{p.data_model_1d_node_path}/'
                    f'{read_ofs_ctl_file[-1][i]}_{p.ofs}_{name_var}_'
                    f'{read_ofs_ctl_file[1][i]}_{p.whichcast}_'
                    f'{p.forecast_hr}_{p.ofsfiletype}_model.prd'
                ) is False:
                    logger_.error(
                        '%s/%s_%s_%s_%s_%s_%s_%s_model.prd is missing',
                        p.data_model_1d_node_path,
                        read_ofs_ctl_file[-1][i],
                        p.ofs,
                        name_var,
                        read_ofs_ctl_file[1][i],
                        p.whichcast,
                        p.forecast_hr,
                        p.ofsfiletype
                    )
                    logger_.info(
                        'Calling OFS module for %s',
                        p.whichcast,
                    )
                    result = get_node_ofs(p, logger_,
                                          model_dataset=cached_model)
                    if result is not None:
                        cached_model = result
                    break
            else:
                if os.path.isfile(
                    f'{p.data_model_1d_node_path}/'
                    f'{read_ofs_ctl_file[-1][i]}_{p.ofs}_{name_var}_'
                    f'{read_ofs_ctl_file[1][i]}_{p.whichcast}_'
                    f'{p.ofsfiletype}_model.prd'
                ) is False:
                    logger_.info(
                        '%s/%s_%s_%s_%s_%s_%s_model.prd is missing',
                        p.data_model_1d_node_path,
                        read_ofs_ctl_file[-1][i],
                        p.ofs,
                        name_var,
                        read_ofs_ctl_file[1][i],
                        p.whichcast,
                        p.ofsfiletype
                    )
                    logger_.info(
                        'Calling OFS module for %s',
                        p.whichcast,
                    )
                    result = get_node_ofs(p, logger_,
                                          model_dataset=cached_model)
                    if result is not None:
                        cached_model = result
                    break
        return cached_model

    parallel_cfg = get_parallel_config(logger)

    def _skill_for_variable(variable, p):
        """Process skill assessment for a single variable."""
        name_var = name_convent(variable)

        # =================================================================
        # This will try to read the station ctl file for the given ofs and
        # for all
        # variables. If not found then it will create it using
        # get_station_observations.py
        # =================================================================
        logger.info('Searching for the %s %s station ctl files',
                    p.ofs, variable)
        ctl_path = os.path.join(p.control_files_path,str(p.ofs+'_'+\
                                name_var+'_station.ctl'))
        if os.path.isfile(ctl_path) is False:
            logger.info(
                'Station ctl file not found. Creating station '
                'ctl file!. This might take a couple of minutes'
            )
            get_station_observations(p, logger)
        read_station_ctl_file = \
            station_ctl_file_extract(ctl_path)
        if read_station_ctl_file is not None:
            logger.info(
                'Station ctl file (%s_%s_station.ctl) found in "%s/". '
                'If you instead want to create a new Inventory file, '
                'please change the name/delete the current %s_%s_station.ctl',
                p.ofs,
                name_var,
                p.control_files_path,
                p.ofs,
                name_var,
            )
        else:
            logger.info('Observation ctl file for %s and %s is empty.',
            p.ofs,
            name_var)
            return

        # Ensure model ctl file exists (depends on station ctl file)
        logger.info('Searching for the %s %s model control files',
                    p.ofs, variable)
        cached_model = _get_valid_cached_model(p)
        read_ofs_ctl_file = ofs_ctlfile_extract(
            p, name_var, logger, model_dataset=cached_model
        )  # lines, nodes, depths, shifts, ids
        refreshed = _get_valid_cached_model(p)
        if refreshed is not None:
            cached_model = refreshed

        if read_ofs_ctl_file is None:
            logger.info('Model ctl file for %s and %s is empty.',
            p.ofs,
            name_var)
        elif (parallel_cfg.get('parallel_workflow')
              and read_station_ctl_file is not None):
            # Parallel: check obs files + prd files concurrently
            logger.info('Running obs and model checks in parallel for %s',
                        variable)
            with ThreadPoolExecutor(max_workers=2) as executor:
                obs_future = executor.submit(
                    _ensure_obs_files, read_station_ctl_file,
                    copy.copy(p), name_var, logger)
                prd_future = executor.submit(
                    _ensure_prd_files, read_ofs_ctl_file,
                    copy.copy(p), name_var, logger, cached_model)
                obs_future.result()
                cached_model = prd_future.result()
                _set_cached_model(p, cached_model)
        else:
            # Sequential: check obs files, then prd files
            _ensure_obs_files(read_station_ctl_file, p, name_var, logger)
            cached_model = _ensure_prd_files(
                read_ofs_ctl_file, p, name_var, logger, cached_model)
            _set_cached_model(p, cached_model)

        if read_ofs_ctl_file is not None:
            skill_results = skill(
                read_station_ctl_file, read_ofs_ctl_file, p,
                name_var, logger
            )
            if name_var == 'cu':
                skill_names = ['currents','currents_dir']
            elif name_var == 'wl' and len(skill_results) > 1:
                skill_names = ['water_level','water_level_hw','water_level_lw']
            else:
                skill_names = [variable]

            for i,skill_result in enumerate(skill_results):
                variable = skill_names[i]
                if (
                    len(skill_result.get('station_id')) != 0
                    and len(skill_result.get('node')) != 0
                    and len(skill_result.get('X')) != 0
                    and len(skill_result.get('Y')) != 0
                    and len(skill_result.get('skill')) != 0
                ):

                    #Make overview maps and save them
                    make_skill_maps(skill_result,
                                    prop, name_var,
                                    logger)
                    tabledatum = prop.datum if name_var == 'wl' else None

                    # Materialize columns once; zip(*rows) is O(n*m) per call.
                    skill_cols = list(zip(*skill_result['skill']))

                    # Extrema tables re-use slots 15-17 for timing metrics
                    # (skill_extrema emits timing_rmse / tcf_pass_fail / tcf
                    # there). Rename those three columns so the CSV is honest.
                    is_extrema = variable.endswith(('_hw', '_lw'))
                    slot_15_name = (
                        'timing_rmse_hours' if is_extrema
                        else 'worst_case_outlier_freq'
                    )
                    slot_16_name = (
                        'timing_central_freq_pass_fail' if is_extrema
                        else 'worst_case_outlier_freq_pass_fail'
                    )
                    slot_17_name = (
                        'timing_central_freq' if is_extrema
                        else 'bias_standard_dev'
                    )

                    csv_data = {
                        'ID': skill_result['station_id'],
                        'NODE': skill_result['node'],
                        'obs_water_depth': skill_result['obs_depth'],
                        'mod_water_depth': skill_result['mod_depth'],
                        'rmse': skill_cols[0],
                        'r': skill_cols[1],
                        'bias': skill_cols[2],
                        'bias_perc': skill_cols[3],
                        'bias_dir': skill_cols[4],
                        'central_freq': skill_cols[5],
                        'central_freq_pass_fail': skill_cols[6],
                        'pos_outlier_freq': skill_cols[7],
                        'pos_outlier_freq_pass_fail': skill_cols[8],
                        'neg_outlier_freq': skill_cols[9],
                        'neg_outlier_freq_pass_fail': skill_cols[10],
                        'max_duration_pos_outlier': skill_cols[11],
                        'max_duration_pos_outlier_pass_fail': skill_cols[12],
                        'max_duration_neg_outlier': skill_cols[13],
                        'max_duration_neg_outlier_pass_fail': skill_cols[14],
                        slot_15_name: skill_cols[15],
                        slot_16_name: skill_cols[16],
                        slot_17_name: skill_cols[17],
                        'target_error_range': skill_cols[18],
                        'datum': tabledatum,
                        'Y': skill_result['Y'],
                        'X': skill_result['X'],
                        'start_date': prop.start_date_full,
                        'end_date': prop.end_date_full,
                    }
                    # Extrema variants carry detection-rate counts.
                    if is_extrema and 'n_mod_extrema' in skill_result:
                        csv_data['n_mod_extrema'] = skill_result['n_mod_extrema']
                        csv_data['n_obs_extrema'] = skill_result['n_obs_extrema']
                        csv_data['n_matched_pairs'] = skill_result['n_matched_pairs']
                    pd.DataFrame(csv_data).to_csv(
                        r'' + f'{prop.data_skill_1d_table_path}/'
                              f'skill_{prop.ofs}_'
                        f'{variable}_{prop.whichcast}_{prop.ofsfiletype}.csv'
                    )

                    logger.info(
                        'Summary skill table for prop.ofs %s and variable %s '
                        'is created successfully',
                        prop.ofs,
                        variable,
                    )

                else:
                    logger.error(
                        'Fail to create summary skill table for OFS: %s and '
                        'variable: %s',
                        prop.ofs,
                        variable,
                    )
        else:
            logger.error(
                'Fail to create summary skill table for OFS: %s and '
                'variable: %s',
                p.ofs,
                variable,
            )

    # Variable processing runs sequentially here. Variable parallelism
    # is handled inside get_node_ofs (which loads the model once and
    # dispatches variable extraction in parallel).
    for variable in prop.var_list:
        _skill_for_variable(variable, prop)

    # Now collect forecast horizon time series, if ya want!
    if (prop.horizonskill and
        prop.whichcast == 'forecast_b'):
        # Get all model time series, and put them in a big 'ol CSV file
        # Check to see if this has already been done
        logger.info('Starting forecast horizon skill! This is going to '
                    'take a while...\n')
        try:
            do_horizon_skill.make_horizon_series(prop, logger)
            # Now get obs time series and put that in the CSV file, too.
            do_horizon_skill.merge_obs_series_scalar(prop, logger)
        except Exception as e_x:
            logger.error('Exception caught in do_horizon_skill after '
                         'calling it from get_skill. Error: %s', e_x)
        prop.horizonskill = False
        logger.info('Completed forecast horizon skill!')
