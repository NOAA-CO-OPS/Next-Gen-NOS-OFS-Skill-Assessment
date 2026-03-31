"""
-*- coding: utf-8 -*-

Script Name: run_harmonic_analysis.py

Technical Contact(s): Name: AJK

Abstract:

   Driver script for running harmonic analysis on paired model+obs datasets.
   Follows the established create_1dplot.py pattern: accepts OFS/path/dates/
   variables as inputs, loads paired model+obs data, runs harmonic analysis,
   validates minimum record length, and writes constituent comparison tables.

   Record-length guidance (NOS CO-OPS Tech Memo 0021; NOAA "About Harmonic
   Constituents" page):

     15-28 days   Equivalent to harm15.f.  Only ~9 of the NOS standard 37
                  constituents can be computed directly; the rest must be
                  inferred or will be dropped.

     29-179 days  Equivalent to harm29d.f.  ~10 constituents computed
                  directly; 14+ inferred.  This is the minimum recommended
                  for routine skill assessment.

     >= 180 days  Equivalent to lsqha.f (~6 months).  Progressively more
     (~6 months)  of the full 37 constituents can be resolved directly.

     >= 365 days  A full year is needed to directly observe all 37 NOS
     (1 year)     standard constituents (NOAA CO-OPS recommendation).

   Reference sources for the constituent comparison table:

     Water levels  Reference = CO-OPS accepted harmonic constants from the
                   Tides & Currents API (product=harcon).  These are derived
                   from years of observations at permanent tide stations.

     Currents      Reference = harmonic analysis of the observation time
                   series from the same run period.  CO-OPS does not
                   maintain long-term accepted constants for current
                   stations (deployments are typically temporary), so both
                   model and obs are analyzed over the same period.

Language:  Python 3.9+

Scripts/Programs Called:
 get_skill(prop, logger)
 --- Called if paired datasets are not found

Usage: python run_harmonic_analysis.py -o cbofs -p /path -s 2024-01-01T00:00:00Z -e 2024-02-01T00:00:00Z

Arguments:
 -h, --help            show this help message and exit
 -o ofs, --OFS         OFS name (e.g. cbofs)
 -p Path, --Path       Working directory path
 -s StartDate_full     Start date YYYY-MM-DDThh:mm:ssZ
 -e EndDate_full       End date YYYY-MM-DDThh:mm:ssZ
 -d Datum              Vertical datum (default MLLW)
 -ws Whichcasts        Comma-separated whichcasts (default nowcast)
 -t FileType           stations or fields (default stations)
 -so Station_Owner     Station provider filter (default co-ops)
 -vs Var_Selection     water_level, currents, or both (default water_level)
 --min-duration        Minimum record length in days for HA (default 15.0)
 --predictions         Also produce tidal prediction + non-tidal residual CSVs

Output:
Name                          Description
ha_constituents.csv           Constituent comparison table (model vs reference)
tidal_prediction.csv          Tidal prediction time series (optional)
nontidal_residual.csv         Non-tidal residual time series (optional)

Author Name: AJK       Creation Date: 02/26/2026
"""

import argparse
import logging
import logging.config
import os
import sys
import warnings
from datetime import datetime
from pathlib import Path

import pandas as pd

from ofs_skill.model_processing import (
    model_properties,
    parse_ofs_ctlfile,
)
from ofs_skill.obs_retrieval import parse_arguments_to_list, utils
from ofs_skill.obs_retrieval.retrieve_t_and_c_station import retrieve_harmonic_constants
from ofs_skill.obs_retrieval.station_ctl_file_extract import station_ctl_file_extract
from ofs_skill.skill_assessment.get_skill import get_skill
from ofs_skill.tidal_analysis import (
    build_constituent_table,
    compute_nontidal_residual,
    harmonic_analysis,
    predict_tide,
    to_equal_interval,
    write_constituent_table_csv,
)

warnings.filterwarnings('ignore', module='utide')


def ofs_ctlfile_read(prop, name_var, logger):
    """
    Read the OFS control file for a given OFS and variable.
    If not found, call get_skill to create it.
    """
    logger.info(
        'Trying to extract %s control file for %s from %s',
        prop.ofs, name_var, prop.control_files_path
    )

    filename = None
    if prop.ofsfiletype == 'fields':
        filename = f'{prop.control_files_path}/{prop.ofs}_{name_var}_model.ctl'
    elif prop.ofsfiletype == 'stations':
        filename = f'{prop.control_files_path}/{prop.ofs}_{name_var}_model_station.ctl'
    else:
        logger.error('Invalid OFS file type.')
        return None

    if not os.path.isfile(filename):
        for i in prop.whichcasts:
            prop.whichcast = i.lower()
            logger.info('Running get_skill for whichcast = %s', i)

            if prop.start_date_full.find('T') == -1:
                prop.start_date_full = prop.start_date_full_before
                prop.end_date_full = prop.end_date_full_before

            get_skill(prop, logger)

    if os.path.isfile(filename):
        if os.path.getsize(filename):
            return parse_ofs_ctlfile(filename)
        else:
            logger.info('%s model ctl file is blank!', name_var)
    logger.info(
        'Not able to extract/create %s control file for %s from %s',
        prop.ofs, name_var, prop.control_files_path
    )
    return None


def run_harmonic_analysis_station_loop(
    read_ofs_ctl_file, prop, var_info, min_duration_days, do_predictions, logger
):
    """
    Inner loop over stations: load paired data, run HA, write outputs.

    Parameters
    ----------
    read_ofs_ctl_file : tuple
        Output of parse_ofs_ctlfile (lines, nodes, depths, shifts, ids).
    prop : ModelProperties
        Configuration object with paths and settings.
    var_info : list
        [variable_long_name, variable_short_name, column_headings].
    min_duration_days : float
        Minimum record length (days) for harmonic analysis.
    do_predictions : bool
        Whether to also write tidal prediction and residual CSVs.
    logger : logging.Logger
        Logger instance.
    """
    variable, name_var, list_of_headings = var_info

    logger.info(
        'Starting harmonic analysis station loop for %s, variable %s',
        prop.ofs, variable
    )

    # Read obs station ctl file
    read_station_ctl_file = station_ctl_file_extract(
        r'' + prop.control_files_path + '/' + prop.ofs + '_'
        + name_var + '_station.ctl'
    )
    if read_station_ctl_file is None:
        logger.error('Station ctl file not found for %s. Skipping variable.', name_var)
        return
    logger.info(
        'Station ctl file (%s_%s_station.ctl) found.',
        prop.ofs, name_var
    )

    stations_processed = 0
    stations_skipped = 0
    skip_reasons = []
    get_skill_attempted = False

    for i in range(len(read_ofs_ctl_file[1])):
        station_id = read_ofs_ctl_file[-1][i]
        node_id = read_ofs_ctl_file[1][i]

        # Match station ID between model and obs ctl files
        try:
            obs_row = [y[0] for y in read_station_ctl_file[0]].index(station_id)
            if read_station_ctl_file[0][obs_row][0] != station_id:
                raise ValueError
        except (ValueError, IndexError):
            logger.error(
                'Could not match station ID %s between control files!',
                station_id
            )
            stations_skipped += 1
            skip_reasons.append(f'{station_id}: ctl file mismatch')
            continue

        # Extract latitude from station ctl file
        latitude = float(read_station_ctl_file[1][obs_row][0])

        for cast in prop.whichcasts:
            prop.whichcast = cast.lower()

            # Build paired data file path
            pair_filename = (
                f'{prop.ofs}_{name_var}_{station_id}_{node_id}'
                f'_{prop.whichcast}_{prop.ofsfiletype}_pair.int'
            )
            pair_filepath = os.path.join(prop.data_skill_1d_pair_path, pair_filename)

            # If paired data doesn't exist, try to create it (once per variable)
            if not os.path.isfile(pair_filepath) and not get_skill_attempted:
                logger.warning(
                    'Paired dataset %s not found. Calling get_skill to '
                    'generate all paired data for %s...', pair_filename, variable
                )
                if prop.ofsfiletype == 'fields' or node_id >= 0:
                    get_skill(prop, logger)
                get_skill_attempted = True

            # Check again after attempting to create
            if not os.path.isfile(pair_filepath):
                logger.warning(
                    'Paired dataset %s not found. '
                    'Skipping station %s.', pair_filename, station_id
                )
                stations_skipped += 1
                skip_reasons.append(f'{station_id}: paired data not found')
                continue

            # Load paired data
            paired_data = pd.read_csv(
                pair_filepath,
                sep=r'\s+',
                names=list_of_headings,
                header=0,
            )
            paired_data['DateTime'] = pd.to_datetime(
                paired_data[['year', 'month', 'day', 'hour', 'minute']]
            )

            # Per-station duration validation
            duration = (
                paired_data['DateTime'].iloc[-1] - paired_data['DateTime'].iloc[0]
            ).total_seconds() / 86400.0
            if duration < min_duration_days:
                logger.warning(
                    'Station %s: record length %.1f days is less than '
                    'minimum %.1f days. Skipping.',
                    station_id, duration, min_duration_days
                )
                stations_skipped += 1
                skip_reasons.append(
                    f'{station_id}: duration {duration:.1f}d < {min_duration_days}d'
                )
                continue

            # Run harmonic analysis for this station
            try:
                _run_ha_for_station(
                    paired_data, prop, station_id, node_id, latitude,
                    variable, name_var, min_duration_days, do_predictions,
                    cast, logger
                )
                stations_processed += 1
            except Exception as ex:
                logger.error(
                    'HA failed for station %s (%s): %s. Skipping.',
                    station_id, cast, ex
                )
                stations_skipped += 1
                skip_reasons.append(f'{station_id}: HA error - {ex}')
                continue

    # Summary
    logger.info('--- Harmonic Analysis Summary for %s ---', variable)
    logger.info('Stations processed: %d', stations_processed)
    logger.info('Stations skipped:   %d', stations_skipped)
    if skip_reasons:
        for reason in skip_reasons:
            logger.info('  Skipped: %s', reason)
    logger.info('Output directory: %s', prop.tidal_analysis_path)


def _run_ha_for_station(
    paired_data, prop, station_id, node_id, latitude,
    variable, name_var, min_duration_days, do_predictions, cast, logger
):
    """
    Run harmonic analysis for a single station and write outputs.

    Parameters
    ----------
    paired_data : pd.DataFrame
        Loaded paired data with DateTime column.
    prop : ModelProperties
        Configuration object.
    station_id : str
        Station identifier.
    node_id : int
        Model node index.
    latitude : float
        Station latitude.
    variable : str
        Long variable name (water_level or currents).
    name_var : str
        Short variable name (wl or cu).
    min_duration_days : float
        Minimum record length for HA.
    do_predictions : bool
        Whether to write prediction and residual CSVs.
    cast : str
        Whichcast name.
    logger : logging.Logger
        Logger instance.
    """
    time = pd.DatetimeIndex(paired_data['DateTime'])

    # Metadata for CSV headers
    metadata = {
        'OFS': prop.ofs,
        'Whichcast': cast,
        'Start_Date': prop.start_date_full,
        'End_Date': prop.end_date_full,
        'Datum': prop.datum,
        'Node': str(node_id),
    }

    # Output file prefix
    out_prefix = (
        f'{prop.ofs}_{name_var}_{station_id}_{node_id}_{prop.whichcast}'
    )

    if variable == 'water_level':
        obs_values = paired_data['OBS'].values
        model_values = paired_data['OFS'].values

        # Preprocess to equal interval
        model_time, model_eq = to_equal_interval(time, model_values, logger=logger)
        obs_time, obs_eq = to_equal_interval(time, obs_values, logger=logger)

        # Try to get CO-OPS accepted harmonic constants as reference
        harcon = retrieve_harmonic_constants(station_id, logger,
                                                config_file=_conf)

        if harcon is not None:
            logger.info(
                'Using CO-OPS accepted constants as reference for station %s.',
                station_id
            )
            table = build_constituent_table(
                model_time=model_time,
                model_values=model_eq,
                latitude=latitude,
                data_type='water_level',
                station_id=station_id,
                accepted_constants=harcon,
                min_duration_days=min_duration_days,
                logger=logger,
            )
        else:
            # Fall back: run HA on observations as reference
            logger.warning(
                'No CO-OPS harcon for station %s. Falling back to '
                'obs-derived HA as reference.', station_id
            )
            # Workaround: use data_type='currents' to force obs-vs-model HA
            # comparison path, since accepted_constants is unavailable for
            # this station.  The output CSV correctly labels this as
            # water_level via write_constituent_table_csv below.
            table = build_constituent_table(
                model_time=model_time,
                model_values=model_eq,
                latitude=latitude,
                data_type='currents',
                station_id=station_id,
                obs_time=obs_time,
                obs_values=obs_eq,
                min_duration_days=min_duration_days,
                logger=logger,
            )

        # Write constituent table
        out_csv = os.path.join(
            prop.tidal_analysis_path,
            f'{out_prefix}_ha_constituents.csv'
        )
        write_constituent_table_csv(
            table, out_csv, station_id, 'water_level',
            metadata=metadata, logger=logger,
        )

        # Optional: predictions and residuals
        if do_predictions:
            # NOTE: build_constituent_table runs HA internally but does not
            # expose the coefficients.  This second HA call is needed for
            # tidal prediction output.  A future refactor could eliminate
            # this redundancy.
            ha_result = harmonic_analysis(
                time=model_time, values=model_eq,
                latitude=latitude,
                min_duration_days=min_duration_days,
                logger=logger,
            )
            # Use only constituents with SNR >= 2 for physically meaningful
            # predictions.  Poorly separated pairs (e.g. S2/K2 with short
            # records) produce large, compensating amplitudes and low SNR.
            snr = ha_result['constituents']['SNR']
            good_constits = ha_result['constituents'].loc[
                snr >= 2.0, 'Name'
            ].tolist()
            if good_constits:
                logger.info(
                    'Predicting with %d of %d constituents (SNR >= 2).',
                    len(good_constits), len(ha_result['constituents']),
                )
                prediction = predict_tide(
                    model_time, ha_result['coef'],
                    constit=good_constits, logger=logger,
                )
                residual = compute_nontidal_residual(
                    model_eq, prediction, logger=logger,
                )
                _write_timeseries_csv(
                    model_time, prediction,
                    os.path.join(
                        prop.tidal_analysis_path,
                        f'{out_prefix}_tidal_prediction.csv'
                    ),
                    'Tidal_Prediction', metadata, logger,
                )
                _write_timeseries_csv(
                    model_time, residual,
                    os.path.join(
                        prop.tidal_analysis_path,
                        f'{out_prefix}_nontidal_residual.csv'
                    ),
                    'Nontidal_Residual', metadata, logger,
                )
            else:
                logger.warning(
                    'Station %s: no constituents with SNR >= 2. '
                    'Skipping prediction/residual output.',
                    station_id,
                )

    elif variable == 'currents':
        obs_spd = paired_data['OBS_SPD'].values
        model_spd = paired_data['OFS_SPD'].values

        # Preprocess to equal interval
        model_time, model_eq = to_equal_interval(time, model_spd, logger=logger)
        obs_time, obs_eq = to_equal_interval(time, obs_spd, logger=logger)

        # For currents, obs HA is the reference (no CO-OPS harcon for currents)
        table = build_constituent_table(
            model_time=model_time,
            model_values=model_eq,
            latitude=latitude,
            data_type='currents',
            station_id=station_id,
            obs_time=obs_time,
            obs_values=obs_eq,
            min_duration_days=min_duration_days,
            logger=logger,
        )

        out_csv = os.path.join(
            prop.tidal_analysis_path,
            f'{out_prefix}_ha_constituents.csv'
        )
        write_constituent_table_csv(
            table, out_csv, station_id, 'currents',
            metadata=metadata, logger=logger,
        )

        # Optional: predictions and residuals for current speed
        if do_predictions:
            # NOTE: build_constituent_table runs HA internally but does not
            # expose the coefficients.  This second HA call is needed for
            # tidal prediction output.  A future refactor could eliminate
            # this redundancy.
            ha_result = harmonic_analysis(
                time=model_time, values=model_eq,
                latitude=latitude,
                min_duration_days=min_duration_days,
                logger=logger,
            )
            snr = ha_result['constituents']['SNR']
            good_constits = ha_result['constituents'].loc[
                snr >= 2.0, 'Name'
            ].tolist()
            if good_constits:
                logger.info(
                    'Predicting with %d of %d constituents (SNR >= 2).',
                    len(good_constits), len(ha_result['constituents']),
                )
                prediction = predict_tide(
                    model_time, ha_result['coef'],
                    constit=good_constits, logger=logger,
                )
                residual = compute_nontidal_residual(
                    model_eq, prediction, logger=logger,
                )
                _write_timeseries_csv(
                    model_time, prediction,
                    os.path.join(
                        prop.tidal_analysis_path,
                        f'{out_prefix}_tidal_prediction.csv'
                    ),
                    'Tidal_Prediction', metadata, logger,
                )
                _write_timeseries_csv(
                    model_time, residual,
                    os.path.join(
                        prop.tidal_analysis_path,
                        f'{out_prefix}_nontidal_residual.csv'
                    ),
                    'Nontidal_Residual', metadata, logger,
                )
            else:
                logger.warning(
                    'Station %s: no constituents with SNR >= 2. '
                    'Skipping prediction/residual output.',
                    station_id,
                )


def _write_timeseries_csv(time, values, filepath, column_name, metadata, logger):
    """Write a time series (prediction or residual) to CSV."""
    df = pd.DataFrame({
        'DateTime': time,
        column_name: values,
    })
    path = Path(filepath)
    path.parent.mkdir(parents=True, exist_ok=True)

    header_lines = []
    if metadata:
        for key, value in metadata.items():
            header_lines.append(f'# {key}: {value}')

    with open(path, 'w', newline='', encoding='utf-8') as f:
        for line in header_lines:
            f.write(line + '\n')
        df.to_csv(f, index=False)

    logger.info('Time series written to %s.', path)


def run_harmonic_analysis(prop, logger):
    """
    Main function for running harmonic analysis on paired datasets.

    Parameters
    ----------
    prop : ModelProperties
        Configuration object populated with CLI arguments.
    logger : logging.Logger or None
        Logger instance. If None, one is created from conf/logging.conf.

    Returns
    -------
    logging.Logger
        The logger used throughout the run.
    """
    # ------------------------------------------------------------------
    # 1. Logger setup
    # ------------------------------------------------------------------
    _conf = getattr(prop, 'config_file', None)
    if logger is None:
        config_file = utils.Utils(_conf).get_config_file()
        log_config_file = os.path.join(Path(prop.path), 'conf/logging.conf')

        if not os.path.isfile(log_config_file):
            print(f'Log config file not found: {log_config_file}')
            sys.exit(-1)
        if not os.path.isfile(config_file):
            print(f'Config file not found: {config_file}')
            sys.exit(-1)

        logging.config.fileConfig(log_config_file)
        logger = logging.getLogger('root')
        logger.info('Using config %s', config_file)
        logger.info('Using log config %s', log_config_file)

    logger.info('--- Starting Harmonic Analysis Process ---')

    # ------------------------------------------------------------------
    # 2. Read config
    # ------------------------------------------------------------------
    dir_params = utils.Utils(_conf).read_config_section('directories', logger)
    prop.datum_list = (
        utils.Utils(_conf).read_config_section('datums', logger)['datum_list']
    ).split(' ')

    # ------------------------------------------------------------------
    # 3. Parse list arguments
    # ------------------------------------------------------------------
    prop.whichcasts = parse_arguments_to_list(prop.whichcasts, logger)
    prop.stationowner = parse_arguments_to_list(prop.stationowner, logger)
    prop.var_list = parse_arguments_to_list(prop.var_list, logger)

    # ------------------------------------------------------------------
    # 4. Normalize inputs
    # ------------------------------------------------------------------
    prop.ofs = prop.ofs.lower()
    prop.datum = prop.datum.upper()
    prop.ofsfiletype = prop.ofsfiletype.lower()

    logger.info('Starting parameter validation...')

    # ------------------------------------------------------------------
    # 5. Validate parameters
    # ------------------------------------------------------------------
    # Date validation
    if prop.end_date_full is None:
        logger.error('End date is required. Abort!')
        sys.exit(-1)
    try:
        prop.start_date_full_before = prop.start_date_full
        prop.end_date_full_before = prop.end_date_full
        start_dt = datetime.strptime(prop.start_date_full, '%Y-%m-%dT%H:%M:%SZ')
        end_dt = datetime.strptime(prop.end_date_full, '%Y-%m-%dT%H:%M:%SZ')
    except ValueError:
        logger.error(
            'Please check Start Date - %s, End Date - %s. Abort!',
            prop.start_date_full, prop.end_date_full
        )
        sys.exit(-1)

    if start_dt > end_dt:
        logger.error(
            'End Date %s is before Start Date %s. Abort!',
            prop.end_date_full, prop.start_date_full
        )
        sys.exit(-1)

    # Path validation
    if prop.path is None:
        prop.path = dir_params['home']

    ofs_extents_path = os.path.join(prop.path, dir_params['ofs_extents_dir'])
    if not os.path.exists(ofs_extents_path):
        logger.error(
            'ofs_extents/ folder not found. Please check path - %s. Abort!',
            prop.path
        )
        sys.exit(-1)

    # OFS shapefile validation
    shape_file = f'{ofs_extents_path}/{prop.ofs}.shp'
    if not os.path.isfile(shape_file):
        logger.error(
            'Shapefile %s not found at %s. Abort!',
            prop.ofs, ofs_extents_path
        )
        sys.exit(-1)

    # Datum validation
    if prop.datum not in prop.datum_list:
        logger.error('Datum %s is not valid! Switching to MLLW...', prop.datum)
        prop.datum = 'MLLW'

    # Variable validation — only water_level and currents are valid for HA
    valid_ha_vars = ['water_level', 'currents']
    invalid_vars = [v for v in prop.var_list if v not in valid_ha_vars]
    if invalid_vars:
        logger.error(
            'Invalid variables for harmonic analysis: %s. '
            'Only %s are supported. Removing invalid variables.',
            invalid_vars, valid_ha_vars
        )
        prop.var_list = [v for v in prop.var_list if v in valid_ha_vars]
        if not prop.var_list:
            logger.error('No valid variables remain. Abort!')
            sys.exit(-1)

    # Up-front duration check (warning only, don't abort)
    total_duration = (end_dt - start_dt).total_seconds() / 86400.0
    if total_duration < prop.min_duration_days:
        logger.warning(
            'Total date range (%.1f days) is less than minimum HA duration '
            '(%.1f days). Individual stations may be skipped.',
            total_duration, prop.min_duration_days
        )

    logger.info('Parameter validation complete!')

    # ------------------------------------------------------------------
    # 6. Build directory tree
    # ------------------------------------------------------------------
    logger.info('Making directory tree...')

    prop.control_files_path = os.path.join(
        prop.path, dir_params['control_files_dir'])
    os.makedirs(prop.control_files_path, exist_ok=True)

    prop.data_observations_1d_station_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['observations_dir'],
        dir_params['1d_station_dir'])
    os.makedirs(prop.data_observations_1d_station_path, exist_ok=True)

    prop.data_model_1d_node_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['model_dir'],
        dir_params['1d_node_dir'])
    os.makedirs(prop.data_model_1d_node_path, exist_ok=True)

    prop.data_skill_1d_pair_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['skill_dir'],
        dir_params['1d_pair_dir'])
    os.makedirs(prop.data_skill_1d_pair_path, exist_ok=True)

    prop.data_skill_stats_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['skill_dir'],
        dir_params['stats_dir'])
    os.makedirs(prop.data_skill_stats_path, exist_ok=True)

    prop.visuals_1d_station_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['visual_dir'])
    os.makedirs(prop.visuals_1d_station_path, exist_ok=True)

    # Tidal analysis output directory
    tidal_analysis_dir = dir_params.get('tidal_analysis_dir', 'tidal_analysis')
    prop.tidal_analysis_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['skill_dir'],
        tidal_analysis_dir)
    os.makedirs(prop.tidal_analysis_path, exist_ok=True)

    logger.info('Directory tree built!')

    # ------------------------------------------------------------------
    # 7. Variable loop
    # ------------------------------------------------------------------
    for variable in prop.var_list:
        if variable == 'water_level':
            name_var = 'wl'
            list_of_headings = [
                'Julian', 'year', 'month', 'day', 'hour',
                'minute', 'OBS', 'OFS', 'BIAS'
            ]
            logger.info('Running harmonic analysis for Water Level.')
        elif variable == 'currents':
            name_var = 'cu'
            list_of_headings = [
                'Julian', 'year', 'month', 'day', 'hour',
                'minute', 'OBS_SPD', 'OFS_SPD', 'BIAS_SPD',
                'OBS_DIR', 'OFS_DIR', 'BIAS_DIR'
            ]
            logger.info('Running harmonic analysis for Currents.')
        else:
            logger.error(
                'Variable %s is not valid for harmonic analysis. Skipping.',
                variable
            )
            continue

        var_info = [variable, name_var, list_of_headings]

        # Read OFS model ctl files
        read_ofs_ctl_file = ofs_ctlfile_read(prop, name_var, logger)

        if read_ofs_ctl_file is not None:
            run_harmonic_analysis_station_loop(
                read_ofs_ctl_file, prop, var_info,
                prop.min_duration_days, prop.do_predictions, logger
            )
        else:
            logger.error(
                'Could not read/create control file for %s. '
                'Skipping variable.', variable
            )

    logger.info('--- Harmonic Analysis Process Complete ---')
    return logger


def main():
    """Entry point for the run-harmonic-analysis CLI command."""
    parser = argparse.ArgumentParser(
        prog='run_harmonic_analysis.py',
        usage='%(prog)s',
        description='Run harmonic analysis on paired model+obs datasets',
    )
    parser.add_argument(
        '-o', '--OFS',
        required=True,
        help='OFS name (e.g. cbofs, dbofs, gomofs)',
    )
    parser.add_argument(
        '-p', '--Path',
        required=True,
        help='Working directory path where ofs_extents/ folder is located',
    )
    parser.add_argument(
        '-s', '--StartDate_full',
        required=True,
        help='Assessment start date: YYYY-MM-DDThh:mm:ssZ '
        "(e.g. '2024-01-01T00:00:00Z')",
    )
    parser.add_argument(
        '-e', '--EndDate_full',
        required=True,
        help='Assessment end date: YYYY-MM-DDThh:mm:ssZ '
        "(e.g. '2024-02-01T00:00:00Z')",
    )
    parser.add_argument(
        '-d', '--Datum',
        required=False,
        default='MLLW',
        help='Vertical datum (default MLLW). Options: '
        "'MHW', 'MHHW', 'MLW', 'MLLW', 'NAVD88', 'IGLD85', 'LWD'",
    )
    parser.add_argument(
        '-ws', '--Whichcasts',
        required=False,
        default='nowcast',
        help="Whichcasts: 'nowcast', 'forecast_b' (default nowcast)",
    )
    parser.add_argument(
        '-t', '--FileType',
        required=False,
        default='stations',
        help="OFS file type: 'fields' or 'stations' (default stations)",
    )
    parser.add_argument(
        '-so', '--Station_Owner',
        required=False,
        default='co-ops',
        help="Station provider filter: 'co-ops', 'ndbc', 'usgs' (default co-ops)",
    )
    parser.add_argument(
        '-vs', '--Var_Selection',
        required=False,
        default='water_level',
        help="Variables: 'water_level', 'currents', or 'water_level,currents' "
        '(default water_level)',
    )
    parser.add_argument(
        '--min-duration',
        type=float,
        default=15.0,
        help='Minimum record length in days for HA (default 15.0). '
             'NOS CO-OPS recommends: 29+ days for routine assessment '
             '(~10 constituents resolved), 180+ days (~6 months) for '
             'more complete resolution, 365+ days (1 year) to directly '
             'observe all 37 NOS standard constituents.',
    )
    parser.add_argument(
        '--predictions',
        action='store_true',
        help='Also produce tidal prediction and non-tidal residual CSVs',
    )
    parser.add_argument(
        '-c', '--config',
        help='Path to configuration file (default: conf/ofs_dps.conf)')

    args = parser.parse_args()

    prop = model_properties.ModelProperties()
    prop.ofs = args.OFS
    prop.path = args.Path
    prop.start_date_full = args.StartDate_full
    prop.end_date_full = args.EndDate_full
    prop.datum = args.Datum
    prop.whichcasts = args.Whichcasts
    prop.ofsfiletype = args.FileType
    prop.stationowner = args.Station_Owner
    prop.var_list = args.Var_Selection
    prop.min_duration_days = args.min_duration
    prop.do_predictions = args.predictions
    prop.config_file = args.config
    prop.user_input_location = False
    prop.horizonskill = False
    prop.forecast_hr = None

    logger = run_harmonic_analysis(prop, None)
    logger.info('Finished run_harmonic_analysis!')


if __name__ == '__main__':
    main()
