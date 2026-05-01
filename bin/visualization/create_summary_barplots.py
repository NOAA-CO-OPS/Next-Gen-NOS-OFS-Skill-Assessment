"""Regenerate station-by-station summary bar plots from existing skill CSVs.

Reads ``data/skill/stats/skill_{ofs}_{variable}_{whichcast}_{ofsfiletype}.csv``
files left behind by a prior 1D run and writes:

* ``data/visual/{ofs}_summary_barplot_{variable}_{whichcasts}_{type}.html``
* ``data/visual/1d/om/{ofs}_summary_barplot_..._{type}.png`` (if static_plots=True)

Does **not** invoke ``get_skill`` -- it only consumes existing CSVs, so it is
fast (~1s per OFS) and safe to rerun.

Usage:
    python create_summary_barplots.py -o cbofs -p ./ \\
        -s 2026-03-28T00:00:00Z -e 2026-03-29T00:00:00Z \\
        -ws nowcast -t stations -vs water_level
"""
from __future__ import annotations

import argparse
import logging
import logging.config
import os
import sys
from pathlib import Path

from ofs_skill.model_processing import model_properties
from ofs_skill.obs_retrieval import parse_arguments_to_list, utils
from ofs_skill.visualization import summary_barplots

_VAR_TO_NAME = {
    'water_level': 'wl',
    'water_temperature': 'temp',
    'salinity': 'salt',
    'currents': 'cu',
}


def _bootstrap(prop):
    """Resolve config + logger and populate the directory attributes
    summary_barplots needs.  Mirrors the start of create_1dplot.create_1dplot."""
    _conf = getattr(prop, 'config_file', None)
    config_file = utils.Utils(_conf).get_config_file()
    log_config_file = os.path.join(Path(prop.path or '.'), 'conf', 'logging.conf')
    if not os.path.isfile(log_config_file):
        sys.exit(f'logging config not found: {log_config_file}')
    if not os.path.isfile(config_file):
        sys.exit(f'main config not found: {config_file}')
    logging.config.fileConfig(log_config_file)
    logger = logging.getLogger('root')
    logger.info('Using config %s', config_file)

    dir_params = utils.Utils(_conf).read_config_section('directories', logger)
    conf_settings = utils.Utils(_conf).read_config_section('settings', logger)

    prop.whichcasts = parse_arguments_to_list(prop.whichcasts, logger)
    prop.var_list = parse_arguments_to_list(prop.var_list, logger)
    prop.ofs = prop.ofs.lower()
    prop.ofsfiletype = prop.ofsfiletype.lower()
    if prop.path is None:
        prop.path = dir_params['home']

    truthy = {'true', 'yes', '1'}
    static_setting = conf_settings.get('static_plots', 'false')
    prop.static_plots = str(static_setting).lower() in truthy

    prop.data_skill_stats_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['skill_dir'],
        dir_params['stats_dir'])
    prop.visuals_1d_station_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['visual_dir'])
    prop.om_files = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['visual_dir'],
        dir_params['om_dir'])
    os.makedirs(prop.visuals_1d_station_path, exist_ok=True)
    os.makedirs(prop.om_files, exist_ok=True)

    return logger


def main(prop):
    logger = _bootstrap(prop)
    logger.info('--- create_summary_barplots: %s ---', prop.ofs)
    for variable in prop.var_list:
        name_var = _VAR_TO_NAME.get(variable)
        if name_var is None:
            logger.warning('Unknown variable %s -- skipping', variable)
            continue
        var_info = [variable, name_var, []]
        for whichcast in prop.whichcasts:
            prop.whichcast = whichcast
            try:
                summary_barplots.make_summary_bars(prop, var_info, logger)
            except Exception:
                logger.exception(
                    'Summary bar plot failed for %s/%s/%s',
                    prop.ofs, variable, whichcast)
    logger.info('Finished create_summary_barplots')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='create_summary_barplots.py',
        description='Regenerate station summary bar plots from existing '
                    'skill stats CSVs.')
    parser.add_argument('-o', '--OFS', required=True,
                        help='OFS name (lowercase)')
    parser.add_argument('-p', '--Path', required=False,
                        help='Working directory (defaults to config home)')
    parser.add_argument('-s', '--StartDate_full', required=True,
                        help='Start date YYYY-MM-DDThh:mm:ssZ')
    parser.add_argument('-e', '--EndDate_full', required=True,
                        help='End date YYYY-MM-DDThh:mm:ssZ')
    parser.add_argument('-ws', '--Whichcasts', required=False,
                        default='nowcast,forecast_b',
                        help='Comma list: nowcast,forecast_a,forecast_b,hindcast')
    parser.add_argument('-t', '--FileType', required=False, default='stations',
                        help="'stations' or 'fields'")
    parser.add_argument('-vs', '--Var_Selection', required=False,
                        default='water_level,water_temperature,salinity,currents',
                        help='Variables to summarize (comma list)')
    parser.add_argument('-f', '--Forecast_Hr', required=False, default=None,
                        help='Forecast cycle (used only for forecast_a labeling)')
    parser.add_argument('-c', '--config', required=False, default=None,
                        help='Path to alternate ofs_dps.conf')
    args = parser.parse_args()

    prop1 = model_properties.ModelProperties()
    prop1.ofs = args.OFS
    prop1.path = args.Path
    prop1.start_date_full = args.StartDate_full
    prop1.end_date_full = args.EndDate_full
    prop1.whichcasts = args.Whichcasts
    prop1.ofsfiletype = args.FileType
    prop1.var_list = args.Var_Selection
    prop1.forecast_hr = args.Forecast_Hr
    prop1.config_file = args.config

    main(prop1)
