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

from datetime import datetime , timedelta
import sys
import logging
import logging.config
import os
import pandas as pd

from pathlib import Path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from obs_retrieval import ofs_inventory_stations
from obs_retrieval import retrieve_t_and_c_station
from obs_retrieval import retrieve_ndbc_year_station
from obs_retrieval import retrieve_ndbc_month_station
from obs_retrieval import retrieve_ndbc_rt_station
from obs_retrieval import retrieve_usgs_station
from obs_retrieval import utils
from obs_retrieval import retrieve_properties


def write_obs_ctlfile(start_date , end_date , datum , path , ofs, logger):
    """
    This function calls the Tid_numberes and Currents, NDBC, and USGS
    retrieval
    function in loop for all stations found for the
    ofs_inventory(ofs, start_date, end_date, path) and variables
    ['water_level', 'water_temperature', 'salinity', 'currents'].
    The output is a .ctl file for each variable with all stations that
    have data
    """

    start_dt = datetime.strptime( start_date , "%Y%m%d" )
    end_dt = datetime.strptime( end_date , "%Y%m%d" )

    dir_params = utils.Utils().read_config_section( "directories" , logger )

    control_files_path = os.path.join(
        path , dir_params ["control_files_dir"]
        )
    os.makedirs( control_files_path , exist_ok=True )

    data_observations_1d_station_path = os.path.join(
        path , dir_params ["data_dir"] , dir_params ["observations_dir"] ,
        dir_params ["1d_station_dir"] , )
    os.makedirs( data_observations_1d_station_path , exist_ok=True )

    # This part of the script will load the inventory file, if the
    # inventory
    # file is not found it will then create a new one by running the
    # ofs_inventory function
    try:
        inventory = pd.read_csv(
            r"" + f"{control_files_path}/inventory_all_{ofs}.csv"
            )
        logger.info(
            'Inventory file (inventory_all_%s.csv) found in "%s/". If you '
            'instead want to create a new Inventory file, please change '
            'the name/delete the current inventory_all_%s.csv' , ofs ,
            control_files_path , ofs
            )
    except FileNotFoundError:
        try:
            logger.info(
                "Inventory file not found. Creating Inventory file!. "
                "This might take a couple of minutes"
                )
            ofs_inventory_stations.ofs_inventory_stations(
                ofs , start_date , end_date , path, logger
                )
            inventory = pd.read_csv(
                r"" + f"{control_files_path}/inventory_all_{ofs}.csv"
                )
            logger.info( "Inventory file created successfully" )
        except Exception as ex:
            logger.error(
                "Errors happened when creating inventory files -- %s." ,
                str( ex ) , )
            raise Exception(
                "Errors happened when creating inventory files"
                ) from ex

    logger.info( "Downloading data found in the Inventory file" )

    # This outer loop is used to download all data for all variables
    # Insid_numbere this loop there is another loop that will go over each
    # line in the inventory file and will try to download the data
    # from TandC, USGS, and NDBC based on the station ID

    retrieve_input = retrieve_properties.RetrieveProperties()

    for variable in ["water_level" , "water_temperature" , "salinity" ,
                     "currents" , ]:
        if variable == "water_level":
            name_var = "wl"
            logger.info( "Creating Water Level station control file." )

        elif variable == "water_temperature":
            name_var = "temp"
            logger.info(
                "Creating Water Temperature station control file."
                )

        elif variable == "salinity":
            name_var = "salt"
            logger.info( "Creating Salinity station control file." )

        elif variable == "currents":
            name_var = "cu"
            logger.info( "Creating Currents station control file." )

        ctl_file = []
        for id_number in inventory.loc [
            inventory ["Source"] == "CO-OPS" , "ID"]:
            name = inventory.loc [
                inventory ["ID"] == str( id_number ) , "Name"].values [0]
            x_value = inventory.loc [
                inventory ["ID"] == str( id_number ) , "X"].values [0]
            y_value = inventory.loc [
                inventory ["ID"] == str( id_number ) , "Y"].values [0]
            try:
                retrieve_input.station = str( id_number )
                retrieve_input.start_date = start_date
                retrieve_input.end_date = end_date
                retrieve_input.variable = variable
                retrieve_input.datum = datum
                timeseries = \
                    retrieve_t_and_c_station.retrieve_t_and_c_station(
                        retrieve_input, logger )
                if variable == "water_level":
                    if isinstance( timeseries , pd.DataFrame ) is False:
                        all_datums = ["NAVD", "MSL", "MLLW", "IGLD",
                                      "LWD", "MHHW", "MHW", "MTL",
                                      "DTL", "MLW", "STND"]
                        data_len = len( all_datums )
                        for data in range( 0 , data_len ):
                            logger.info(
                                "Water level data not found for station "
                                "%s for the datum (%s) specified. Trying "
                                "datum %s..." , str( id_number ) , datum , data
                                )
                            try:
                                retrieve_input.station = str(id_number)
                                retrieve_input.start_date = start_date
                                retrieve_input.end_date = end_date
                                retrieve_input.variable = variable
                                retrieve_input.datum = all_datums [data]
                                timeseries = \
                                    retrieve_t_and_c_station.retrieve_t_and_c_station(
                                    retrieve_input , logger )
                                if (isinstance(
                                        timeseries , pd.DataFrame
                                        ) is True):
                                    datum_found = all_datums [data]
                                    logger.info(
                                        "Water level data found for datum "
                                        "%s and station %s" , datum ,
                                        str( id_number )
                                        )
                                    break
                            except Exception as ex:
                                logger.info(
                                    "After trying multiple datums (%s) no "
                                    "water level data not found for "
                                    "station %s" ,
                                    all_datums , str( id_number )
                                    )
                                raise Exception(
                                    "Error happened at water level data"
                                    ) from ex
                    else:
                        datum_found = 0.0

                if (variable == "water_level" and isinstance(
                        timeseries , pd.DataFrame
                        ) is True):
                    logger.info(
                        "COOPS %s data found for station %s" , variable ,
                        str( id_number )
                        )
                    ctl_file.append(
                        f'{str( id_number )} {str( id_number )}_'
                        f'{name_var}_{ofs}_CO-OPS "{name}"\n  {y_value:.3f} '
                        f'{x_value:.3f} {datum_found}  0.0  0.0\n'
                        )
                elif variable in {"water_temperature" , "salinity"}:
                    logger.info(
                        "COOPS %s data found for station %s" , variable ,
                        str( id_number )
                        )
                    ctl_file.append(
                        f'{str( id_number )} {str( id_number )}_'
                        f'{name_var}_{ofs}_CO-OPS "{name}"\n  {y_value:.3f} '
                        f'{x_value:.3f} 0.0  '
                        f'{timeseries ["DEP01"] [1]:.2f}  0.0\n'
                        )
                elif variable == "currents":
                    logger.info(
                        "COOPS %s data found for station %s" , variable ,
                        str( id_number )
                        )
                    ctl_file.append(
                        f'{str( id_number )} {str( id_number )}_'
                        f'{name_var}_{ofs}_CO-OPS "{name}"\n  {y_value:.3f} '
                        f'{x_value:.3f} 0.0  '
                        f'{timeseries ["DEP01"] [1]:.2f}  {0.0}\n'
                        )
            except:
                logger.info(
                    "COOPS %s data not found for station %s" , variable ,
                    str( id_number )
                    )

        for id_number in inventory.loc [
            inventory ["Source"] == "USGS" , "ID"]:
            # This section tries to download the data from USGS
            name = inventory.loc [
                inventory ["ID"] == str( id_number ) , "Name"].values [0]
            x_value = inventory.loc [
                inventory ["ID"] == str( id_number ) , "X"].values [0]
            y_value = inventory.loc [
                inventory ["ID"] == str( id_number ) , "Y"].values [0]
            try:
                retrieve_input.station = str(id_number)
                retrieve_input.start_date = start_date
                retrieve_input.end_date = end_date
                retrieve_input.variable = variable
                timeseries = retrieve_usgs_station.retrieve_usgs_station(
                    retrieve_input, logger
                    )
                if isinstance( timeseries , pd.DataFrame ) is False:
                    logger.info(
                        "USGS %s not data found for station %s" ,
                        variable , str( id_number )
                        )
                else:
                    logger.info(
                        "USGS %s data found for station %s" , variable ,
                        str( id_number )
                        )

                    if variable == "water_level":
                        if (str(
                                timeseries ["Datum"] [1]
                                ) == "NAVD88" and datum == "NAVD"):
                            ctl_file.append(
                                f'{str( id_number )} '
                                f'{str( id_number )}_{name_var}_'
                                f'{ofs}_USGS "{name}"\n  {y_value:.3f} '
                                f'{x_value:.3f} {datum_found} 0.0  0.0 '
                                f'0.0\n'
                                )
                        else:
                            ctl_file.append(
                                f'{str( id_number )} '
                                f'{str( id_number )}_{name_var}_'
                                f'{ofs}_USGS "{name}"\n  {y_value:.3f} '
                                f'{x_value:.3f} '
                                f'{timeseries ["Datum"] [1]}  0.0  0.0\n'
                                )
                            logger.info(
                                "These is a datum mismatch between this "
                                "Water Level USGS station (%s) and the "
                                "above Tid_numbers and Currents stations, "
                                "please check" , str( id_number )
                                )

                            logger.info(
                                "The datum (%s) for this USGS station ("
                                "%s) "
                                "will be highlighed in the water level "
                                "station control file" ,
                                timeseries ["Datum"] [1] , str( id_number )
                                )

                    elif variable in ["water_temperature" , "salinity"]:
                        ctl_file.append(
                            f'{str( id_number )} {str( id_number )}_'
                            f'{name_var}_{ofs}_USGS "{name}"\n  '
                            f'{y_value:.3f} {x_value:.3f} 0.0  '
                            f'{timeseries ["DEP01"] [1]:.2f}  0.0\n'
                            )
                    elif variable == "currents":
                        ctl_file.append(
                            f'{str( id_number )} {str( id_number )}_'
                            f'{name_var}_{ofs}_USGS "{name}"\n  '
                            f'{y_value:.3f} {x_value:.3f} 0.0  '
                            f'{timeseries ["DEP01"] [1]:.2f}  0.0\n'
                            )
            except:
                pass

        for id_number in inventory.loc [inventory ['Source'] == 'NDBC' , 'ID']:
            # This section tries to donwload the data from NDBC
            name = \
            inventory.loc [inventory ['ID'] == str( id_number ) , 'Name'].values [
                0]
            x_value = inventory.loc [
                inventory ['ID'] == str( id_number ) , 'X'].values [0]
            y_value = inventory.loc [
                inventory ['ID'] == str( id_number ) , 'Y'].values [0]
            try:
                data_station = []
                for yr_index in range( start_dt.year , end_dt.year + 1 ):
                    if yr_index != datetime.now().year:
                        retrieve_input.station = str(id_number)
                        retrieve_input.year = yr_index
                        retrieve_input.variable = variable
                        try:
                            timeseries = \
                                retrieve_ndbc_year_station.retrieve_ndbc_year_station(
                                retrieve_input , logger
                                )
                            data_station.append( timeseries )
                        except:
                            date_this_yr = datetime( yr_index, start_dt.month , 1 )
                            while date_this_yr.month <= end_dt.month and date_this_yr.year <= end_dt.year and date_this_yr.year >= datetime.now().year-1 :
                                retrieve_input.station = str( id_number )
                                retrieve_input.year = yr_index
                                retrieve_input.variable = variable
                                retrieve_input.month_num = date_this_yr.month
                                retrieve_input.month = date_this_yr.strftime( "%b" )
                                try:
                                    timeseries = \
                                        retrieve_ndbc_month_station.retrieve_ndbc_month_station(
                                         retrieve_input , logger
                                        )
                                    if timeseries is not None:
                                        data_station.append(timeseries)
                                except:
                                    try:
                                        retrieve_input.month = datetime.strptime(
                                            str(date_this_yr.month),"%m"
                                            ).strftime("%b")
                                        timeseries = \
                                            retrieve_ndbc_rt_station.retrieve_ndbc_rt_station(
                                                retrieve_input , logger
                                            )
                                        if timeseries is not None:
                                            data_station.append(timeseries)
                                    except:
                                        pass

                                date_this_yr += timedelta( days=32 )



                    elif yr_index == datetime.now().year:
                        date_this_yr = datetime( end_dt.year , 1 , 1 )
                        while date_this_yr.month <= end_dt.month and date_this_yr.year <= end_dt.year:
                            retrieve_input.station = str( id_number )
                            retrieve_input.year = yr_index
                            retrieve_input.variable = variable
                            retrieve_input.month_num = date_this_yr.month
                            retrieve_input.month = date_this_yr.strftime( "%b" )
                            try:
                                timeseries = \
                                    retrieve_ndbc_month_station.retrieve_ndbc_month_station(
                                     retrieve_input , logger
                                    )
                                if timeseries is not None:
                                    data_station.append(timeseries)
                            except:
                                try:
                                    retrieve_input.month = datetime.strptime(
                                        str(date_this_yr.month),"%m"
                                        ).strftime("%b")
                                    timeseries = \
                                        retrieve_ndbc_rt_station.retrieve_ndbc_rt_station(
                                            retrieve_input , logger
                                        )
                                    if timeseries is not None:
                                        data_station.append(timeseries)
                                except:
                                    pass

                            date_this_yr += timedelta( days=32 )

                if data_station [0] is None:
                    continue

                # at this point the station data (year+month+RT has been
                # downloaded and saved as data_station)
                # the else statement below is used to organize these
                # broken timeseries into something temporaly adequate
                # and create the ctlfile
                data_station = pd.concat( data_station )
                mask = (data_station ['DateTime'] >= start_dt) & (
                        data_station ['DateTime'] <= end_dt)
                data_station = data_station.loc [mask]

                if data_station.empty:
                    continue
                    
                logger.info(
                    'NDBC %s data found for station %s',
                        variable , str( id_number )
                    )
                if variable == 'water_level':
                    if datum == 'MLLW':
                        ctl_file.append(
                            f'{str( id_number )} {str( id_number )}_'
                            f'{name_var}_{ofs}_NDBC "{name}"\n  '
                            f'{y_value:.3f} {x_value:.3f} 0.0   '
                            f'0.0   0.0\n'
                            )
                    else:
                        ctl_file.append(
                            f'{str( id_number )} {str( id_number )}_'
                            f'{name_var}_{ofs}_NDBC "{name}"\n  '
                            f'{y_value:.3f} {x_value:.3f} '
                            f'{data_station ["Datum"] [1]}  0.0  '
                            f'0.0\n'
                            )
                        logger.info(
                            'These is a datum mismatch between '
                            'this Water Level NDBC station (%s) '
                            'and the above Tides and Currents '
                            'stations, please check',
                                str( id_number )
                            )

                        logger.info(
                            'The datum (MLLW) for this NDBC '
                            'station (%s) will be highlighed in '
                            'the water level station control '
                            'file',
                                str( id_number )
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
                    data_station ['DEP01'] = data_station [
                        'DEP01'].astype( float )
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
                    r"" + f"{control_files_path}/{ofs}_"
                          f"{name_var}_station.ctl" ,
                    "w" , encoding="utf-8"
                    ) as output:
                for i in ctl_file:
                    output.write( str( i ) )
                logger.info(
                    "%s_%s_station.ctl created successfully" , ofs ,
                    name_var
                    )
        except Exception as ex:
            logger.error(
                "Saving station failed. Please check the directory path: "
                "%s -- %s." , control_files_path , str( ex )
                )
            raise Exception( "Saving station failed." ) from ex
