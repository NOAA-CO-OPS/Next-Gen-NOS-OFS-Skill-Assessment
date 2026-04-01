"""
Created on Tue Aug 19 13:53:41 2025

@author: PWL
"""

import argparse
import logging
import logging.config
import os
import sys

from ofs_skill.model_processing import model_properties, model_source
from ofs_skill.open_boundary.obc_plotting import plot_fvcom_obc, plot_roms_obc
from ofs_skill.open_boundary.obc_processing import load_obc_file, parameter_validation


def make_open_boundary_transects(prop,logger):
    '''coming soon'''

    if logger is None:
        log_config_file = 'conf/logging.conf'
        log_config_file = os.path.join(os.getcwd(), log_config_file)

        # Check if log file exists
        if not os.path.isfile(log_config_file):
            sys.exit(-1)

        # Creater logger
        logging.config.fileConfig(log_config_file)
        logger = logging.getLogger('root')
        logger.info('Using log config %s', log_config_file)

    logger.info('--- Starting open boundary transect ---')

    # Do parameter validation
    parameter_validation(prop,logger)

    # Load file(s)
    ds = load_obc_file(prop,logger)

    # Plot OBC netcdf file
    if model_source.model_source(prop.ofs) == 'roms':
        plot_roms_obc(prop,ds,logger)
    elif model_source.model_source(prop.ofs) == 'fvcom':
        plot_fvcom_obc(prop,ds,logger)
    else:
        logger.error('Model source %s not found!',
                     model_source.model_source(prop.ofs))
        sys.exit(-1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='python make_open_boundary_transect.py', usage='%(prog)s',
        description='Run open boundary condition transect maker'
    )
    parser.add_argument(
        '-o',
        '--OFS',
        required=True,
        help='Choose from the list on the prop.ofs_Extents folder, '
        'you can also create your own shapefile, add it to the '
        'prop.ofs_Extents folder and call it here',
    )
    parser.add_argument(
        '-p',
        '--Path',
        required=False,
        help='Use /home as the default. User can specify path',
    )
    parser.add_argument(
        '-s',
        '--StartDate_full',
        required=True,
        help="Start Date YYYY-MM-DDThh:mm:ssZ e.g. '2023-01-01T12:34:00Z'",
    )
    parser.add_argument(
        '-hr',
        '--Cycle_Hr',
        required=False,
        help="Model cycle to assess, e.g. '02', '06', '12', '24' ... ",
    )
    args = parser.parse_args()
    prop1 = model_properties.ModelProperties()
    prop1.ofs = args.OFS
    prop1.path = args.Path
    prop1.start_date_full = args.StartDate_full
    prop1.model_cycle = args.Cycle_Hr

    make_open_boundary_transects(prop1, None)
