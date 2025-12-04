"""
-*- coding: utf-8 -*-

Documentation for Scripts write_obs_ctlfile.py

Script Name: write_obs_ctlfile.py

Technical Contact(s):
Name:  FC

Language:  Python 3.8

Estimated Execution Time: >5min, <10min

Author Name:  FC       Creation Date:  06/29/2023

Revisions:
Date          Author             Description
07-20-2023    MK           Modified the scripts to add config,
logging,
                                 try/except and argparse features
08-01-2023    FC   Modified this script to be write control
                                 file ONLY
08-16-2023    MK           Modified the code to match PEP-8 standard.

"""

import math
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from coastalmodeling_vdatum import vdatum
from obs_retrieval import (
    ofs_inventory_stations,
    retrieve_ndbc_station,
    retrieve_properties,
    retrieve_t_and_c_station,
    retrieve_usgs_station,
    utils,
)


def write_obs_ctlfile(start_date , end_date , datum , path , ofs, stationowner,
                      logger):
    """
    This function calls the Tid_numberes and Currents, NDBC, and USGS
    retrieval
    function in loop for all stations found for the
    ofs_inventory(ofs, start_date, end_date, path) and variables
    ['water_level', 'water_temperature', 'salinity', 'currents'].
    The output is a .ctl file for each variable with all stations that
    have data
    """

    start_dt = datetime.strptime( start_date , '%Y%m%d' )
    end_dt = datetime.strptime( end_date , '%Y%m%d' )

    dir_params = utils.Utils().read_config_section( 'directories' , logger )
    datum_list = (utils.Utils().read_config_section('datums', logger)\
                       ['datum_list']).split(' ')

    control_files_path = os.path.join(
        path , dir_params ['control_files_dir']
        )
    os.makedirs( control_files_path , exist_ok=True )

    data_observations_1d_station_path = os.path.join(
        path , dir_params ['data_dir'] , dir_params ['observations_dir'] ,
        dir_params ['1d_station_dir'] , )
    os.makedirs( data_observations_1d_station_path , exist_ok=True )

    # This part of the script will load the inventory file, if the
    # inventory
    # file is not found it will then create a new one by running the
    # ofs_inventory function
    try:
        dtypes = {
            'ID': 'object',
            'X': 'float64',
            'Y': 'float64',
            'Source': 'object',
            'Name': 'object'
        }
        inventory = pd.read_csv(
            r'' +\
            f'{control_files_path}/inventory_all_{ofs}.csv',
            dtype=dtypes
            )
        logger.info('Inventory (inventory_all_%s.csv) '
                    'found in {control_files_path}. '
                    'If you instead want to create a new '
                    'inventory file, change the name or '
                    'delete the current file.', ofs)
    except FileNotFoundError:
        try:
            logger.info(
                'Inventory file not found. '
                'Creating Inventory file!. '
                'This might take a couple of minutes'
                )
            ofs_inventory_stations.ofs_inventory_stations(
                ofs , start_date , end_date , path, stationowner, logger
                )
            dtypes = {
                'ID': 'object',
                'X': 'float64',
                'Y': 'float64',
                'Source': 'object',
                'Name': 'object'
            }
            inventory = pd.read_csv(
                r'' + f'{control_files_path}/inventory_all_{ofs}.csv',
                dtype=dtypes)
            logger.info( 'Inventory file created successfully' )
        except Exception as ex:
            logger.error(
                f'Error when creating inventory files: {ex}'
                )
            raise Exception(
                'Error when creating inventory files'
                ) from ex

    logger.info('Downloading data from the Inventory file!')

    # This outer loop is used to download all data for all variables
    # Insid_numbere this loop there is another loop that will go over each
    # line in the inventory file and will try to download the data
    # from TandC, USGS, and NDBC based on the station ID

    retrieve_input = retrieve_properties.RetrieveProperties()


    if datum.lower() == 'igld85':
        datum = 'IGLD'

    for variable in ['water_level' , 'water_temperature' , 'salinity' ,
                     'currents' , ]:
        if variable == 'water_level':
            name_var = 'wl'
            logger.info('Making water level station ctl file.')

        elif variable == 'water_temperature':
            name_var = 'temp'
            logger.info(
                'Making water temp station ctl file.'
                )
        elif variable == 'salinity':
            name_var = 'salt'
            logger.info('Making Salinity station ctl file.')

        elif variable == 'currents':
            name_var = 'cu'
            logger.info('Making Currents station ctl file.')

        ctl_file = []
        for id_number in inventory.loc [
            inventory ['Source'] == 'CO-OPS' , 'ID']:
            name = inventory.loc [
                inventory ['ID'] ==
                str(id_number), 'Name'].values[0]
            x_value = inventory.loc [
                inventory ['ID'] ==
                str(id_number), 'X'].values[0]
            y_value = inventory.loc [
                inventory ['ID'] ==
                str(id_number), 'Y'].values[0]
            try:
                retrieve_input.station = str( id_number )
                retrieve_input.start_date = start_date
                retrieve_input.end_date = end_date
                retrieve_input.variable = variable
                retrieve_input.datum = datum
                timeseries = \
                    retrieve_t_and_c_station.\
                        retrieve_t_and_c_station(
                        retrieve_input,logger)
                if variable == 'water_level':
                    if (isinstance(timeseries, pd.DataFrame)
                        is False):
                        all_datums = ['NAVD','MSL','MLLW',
                                      'IGLD','LWD','MHHW',
                                      'MHW','MTL','DTL',
                                      'MLW', 'STND']
                        accepted_datums = datum_list
                        for data in range(0, len(all_datums)):
                            logger.info(
                                'Water level data not '
                                'found for station '
                                '%s for %s. '
                                'Trying %s...',
                                str(id_number), datum, all_datums [data]
                                )
                            try:
                                retrieve_input.station = \
                                    str(id_number)
                                retrieve_input.start_date =\
                                    start_date
                                retrieve_input.end_date =\
                                    end_date
                                retrieve_input.variable =\
                                    variable
                                retrieve_input.datum =\
                                    all_datums [data]
                                timeseries = \
                                    retrieve_t_and_c_station.\
                                        retrieve_t_and_c_station(
                                        retrieve_input, logger)
                                if ((isinstance(timeseries, pd.DataFrame) is \
                                    True) and
                                    (all_datums[data] in accepted_datums)):
                                    datum_found = \
                                        all_datums [data]
                                    if str(datum_found) == 'NAVD':
                                        datum_found = 'NAVD88'
                                    # if str(datum_found) == 'IGLD':
                                    #     datum_found = 'IGLD85'
                                    logger.info(
                                        'Water level data '
                                        'found for datum '
                                        '%s and '
                                        'station '
                                        '%s',  all_datums [data],
                                        str(id_number)
                                        )
                                    break
                            except Exception as ex:
                                logger.info(
                                    'After trying multiple '
                                    'datums, no water '
                                    'level data found for '
                                    'station %s.',
                                    str(id_number)
                                    )
                                raise Exception(
                                    'Error at water level '
                                    'data!'
                                    ) from ex
                    else:
                        datum_found = datum
                    if ofs not in [
                            'leofs',
                            'lmhofs',
                            'loofs',
                            'lsofs'
                            ]:
                        if (str(datum_found).upper() == datum):
                            zdiff = 0
                        elif (str(datum_found).upper() != datum and
                              str(datum_found).upper() in
                              datum_list):
                            ldatum = datum.lower()
                            dummyval = 10
                            _,_,z = vdatum.convert(
                                str(datum_found).lower(),
                                ldatum,
                                y_value,
                                x_value,
                                dummyval, #use dummy value
                                online=True,
                                epoch=None)
                            if math.isinf(z):
                                zdiff = 'RANGE'
                            else:
                                zdiff = round(z-dummyval,2) # datum offset
                        else:
                            zdiff = 'UNKNOWN'
                    else:
                        if datum == 'LWD' and str(datum_found).upper() ==\
                            'IGLD':
                            if ofs == 'leofs':
                                zdiff = -173.5
                            elif ofs == 'lmhofs':
                                zdiff = -176.0
                            elif ofs == 'lsofs':
                                zdiff = -183.2
                            elif ofs == 'loofs':
                                zdiff = -74.2
                        elif datum == 'IGLD' and str(datum_found).upper() ==\
                            'LWD':
                            if ofs == 'leofs':
                                zdiff = 173.5
                            elif ofs == 'lmhofs':
                                zdiff = 176.0
                            elif ofs == 'lsofs':
                                zdiff = 183.2
                            elif ofs == 'loofs':
                                zdiff = 74.2
                        elif datum == str(datum_found).upper():
                            zdiff = 0 # No correction needed
                        else:
                            zdiff = 'UNKNOWN'

                if (variable == 'water_level' and isinstance(
                        timeseries, pd.DataFrame
                        ) is True):
                    logger.info(
                        'CO-OPS %s data found '
                        'for station %s.', variable,
                        str(id_number)
                        )
                    ctl_file.append(
                        f'{str( id_number )} {str( id_number )}_'
                        f'{name_var}_{ofs}_CO-OPS "{name}"\n  {y_value:.3f} '
                        f'{x_value:.3f} {zdiff}  0.0  {datum_found}\n'
                        )
                elif (variable in {'water_temperature',
                                  'salinity'} and isinstance(
                        timeseries, pd.DataFrame) is True
                    ):
                    logger.info(
                        'CO-OPS %s data found for '
                        'station %s.', variable,
                        str(id_number)
                        )
                    ctl_file.append(
                        f'{str( id_number )} {str( id_number )}_'
                        f'{name_var}_{ofs}_CO-OPS "{name}"\n  {y_value:.3f} '
                        f'{x_value:.3f} 0.0  '
                        f'{timeseries ["DEP01"] [1]:.2f}  0.0\n'
                        )
                elif (variable == 'currents' and isinstance(
                        timeseries, pd.DataFrame) is True
                    ):
                    logger.info(
                        'CO-OPS %s data found for '
                        'station %s.', variable,
                        str(id_number)
                        )
                    ctl_file.append(
                        f'{str( id_number )} {str( id_number )}_'
                        f'{name_var}_{ofs}_CO-OPS "{name}"\n  {y_value:.3f} '
                        f'{x_value:.3f} 0.0  '
                        f'{timeseries ["DEP01"] [1]:.2f}  0.0\n'
                        )
            except:
                logger.info(
                    'CO-OPS %s data not found for '
                    'station %s.', variable,
                    str(id_number)
                    )

        for id_number in inventory.loc [
            inventory ['Source'] == 'USGS' , 'ID']:
            # This section tries to download data from USGS
            name = inventory.loc [
                inventory['ID'] ==
                str(id_number),'Name'].values[0]
            x_value = inventory.loc [
                inventory['ID'] ==
                str(id_number),'X'].values[0]
            y_value = inventory.loc [
                inventory['ID'] ==
                str(id_number),'Y'].values[0]
            try:
                retrieve_input.station = str(id_number)
                retrieve_input.start_date = start_date
                retrieve_input.end_date = end_date
                retrieve_input.variable = variable
                timeseries = retrieve_usgs_station.\
                    retrieve_usgs_station(
                    retrieve_input, logger
                    )
                if isinstance(timeseries, pd.DataFrame) \
                    is False:
                    logger.info(
                        'USGS %s data not found for '
                        'station %s.', variable,
                        str(id_number)
                        )
                else:
                    logger.info(
                        'USGS %s data found for '
                        'station %s.', variable,
                        str(id_number)
                        )

                    if variable == 'water_level':
                        if ofs not in [
                                'leofs',
                                'lmhofs',
                                'loofs',
                                'lsofs'
                                ]:
                            if (str(
                                    timeseries['Datum'][1]
                                    ).upper() == datum):
                                zdiff = 0
                            elif (str(
                                    timeseries ['Datum'][1]
                                    ) == 'NAVD88' and
                                    datum != 'NAVD88'):
                                ldatum = datum.lower()
                                dummyval = 10
                                _,_,z = vdatum.convert(
                                    timeseries['Datum'][1].lower(),
                                    ldatum,
                                    y_value,
                                    x_value,
                                    dummyval, #use dummy value
                                    online=True,
                                    epoch=None)
                                if math.isinf(z):
                                    zdiff = 'RANGE'
                                else:
                                    zdiff = round(z-dummyval,2) # datum offset
                            elif (str(
                                    timeseries['Datum'][1]
                                    ) != 'NAVD88'):
                                zdiff = 'UNKNOWN'
                        else:
                            if datum == 'LWD':
                                if ofs == 'leofs':
                                    zdiff = -173.5
                                elif ofs == 'lmhofs':
                                    zdiff = -176.0
                                elif ofs == 'lsofs':
                                    zdiff = -183.2
                                elif ofs == 'loofs':
                                    zdiff = -74.2
                            elif datum == 'IGLD':
                                zdiff = 0 # No correction needed
                            else:
                                zdiff = 'UNKNOWN'
                        ctl_file.append(
                            f'{str( id_number )} '
                            f'{str( id_number )}_{name_var}_'
                            f'{ofs}_USGS "{name}"\n  {y_value:.3f} '
                            f'{x_value:.3f} '
                            f'{zdiff}  0.0  {str(timeseries["Datum"][1])}\n'
                            )
                        logger.info(
                            'There is a datum mismatch between this '
                            'water Level USGS station (%s) and the '
                            'user-specified datum (%s), '
                            'please check control file',timeseries['Datum'][1],
                            datum
                            )

                    elif variable in ['water_temperature' , 'salinity']:
                        ctl_file.append(
                            f'{str( id_number )} {str( id_number )}_'
                            f'{name_var}_{ofs}_USGS "{name}"\n  '
                            f'{y_value:.3f} {x_value:.3f} 0.0  '
                            f'{timeseries ["DEP01"] [1]:.2f}  0.0\n'
                            )
                    elif variable == 'currents':
                        ctl_file.append(
                            f'{str( id_number )} {str( id_number )}_'
                            f'{name_var}_{ofs}_USGS "{name}"\n  '
                            f'{y_value:.3f} {x_value:.3f} 0.0  '
                            f'{timeseries ["DEP01"] [1]:.2f}  0.0\n'
                            )
            except:
                pass


        for id_number in inventory.loc[inventory['Source'] \
                                       == 'NDBC','ID']:
            # This section tries to download data from NDBC
            name = inventory.loc[inventory['ID'] ==
                          str(id_number), 'Name'].values[0]
            x_value = inventory.loc[
                inventory['ID'] ==
                str(id_number),'X'].values[0]
            y_value = inventory.loc[
                inventory['ID'] ==
                str(id_number),'Y'].values[0]
            try:
                data_station = retrieve_ndbc_station.retrieve_ndbc_station(
                    start_date,
                    end_date,
                    id_number,
                    variable,
                    logger
                    )

                if data_station is None:
                    continue

                logger.info(
                    'NDBC %s data found for '
                    'station %s.', variable, str(id_number)
                    )
                if variable == 'water_level':
                    if (str(
                            timeseries['Datum'][1]
                            ).upper() == datum):
                        zdiff = 0
                    elif (str(
                            timeseries['Datum'][1]
                            ) == 'MLLW' and
                            datum != 'MLLW'):
                        ldatum = datum.lower()
                        dummyval = 10
                        _,_,z = vdatum.convert(
                            timeseries['Datum'][1].lower(),
                            ldatum,
                            y_value,
                            x_value,
                            dummyval, #use dummy value
                            online=True,
                            epoch=None)
                        if math.isinf(z):
                            zdiff = 'RANGE'
                        else:
                            zdiff = round(z-dummyval,2) # datum offset
                    elif (str(
                            timeseries['Datum'][1]
                            ) != 'MLLW'):
                        zdiff = 'UNKNOWN'

                    ctl_file.append(
                        f'{str( id_number )} '
                        f'{str( id_number )}_{name_var}_'
                        f'{ofs}_NDBC "{name}"\n  {y_value:.3f} '
                        f'{x_value:.3f} '
                        f'{zdiff}  0.0  {timeseries["Datum"][1]}\n'
                        )
                    logger.info(
                        'There is a datum mismatch between this '
                        'water Level NDBC station (%s) and the '
                        'user-specified datum (%s), '
                        'please check control file',timeseries['Datum'][1],
                        datum
                        )

                elif variable in {'water_temperature','salinity'}:
                    data_station ['DEP01'] = data_station [
                        'DEP01'].astype( float )
                    ctl_file.append(
                        f'{str( id_number )} {str( id_number )}_{name_var}_'
                        f'{ofs}_NDBC "{name}"\n  {y_value:.3f} '
                        f'{x_value:.3f} 0.0  '
                        f'{data_station ["DEP01"].mean():.2f}  '
                        f'0.0\n'
                        )
                elif variable == 'currents':
                    data_station ['DEP01'] = data_station[
                        'DEP01'].astype(float)
                    ctl_file.append(
                        f'{str( id_number )} {str( id_number )}_{name_var}_'
                        f'{ofs}_NDBC "{name}"\n  {y_value:.3f} '
                        f'{x_value:.3f} 0.0  '
                        f'{data_station ["DEP01"].mean():.2f}  '
                        f'0.0\n'
                        )
            except:
                pass

        # This section writes the station.ctl file with all
        # the data found and formatted in the ctl_file list
        try:
            with open(
                    r'' + f'{control_files_path}/{ofs}_'
                          f'{name_var}_station.ctl' ,
                    'w' , encoding='utf-8'
                    ) as output:
                for i in ctl_file:
                    output.write(str(i))
                logger.info(
                    '%s_%s_station.ctl created '
                    'successfully!', ofs,name_var)
        except Exception as ex:
            logger.error(
                'Saving station failed: {ex}. '
                'Please check the directory path: '
                '%s.', control_files_path
                )
            raise Exception('Saving station failed.') from ex
