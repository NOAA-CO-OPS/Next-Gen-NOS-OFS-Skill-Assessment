# -*- coding: utf-8 -*-
"""
Created on Fri Jun  6 09:11:51 2025

@author: PL
"""

import sys
from pathlib import Path
import numpy as np
import pandas as pd
import s3fs
import xarray as xr

# Add parent directory to sys.path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from obs_retrieval import station_ctl_file_extract

def is_number(n):
    try:
        float(n)   # Type-casting the string to `float`.
                   # If string is not a valid `float`,
                   # it'll raise `ValueError` exception
    except ValueError:
        return False
    return True

def roms_nodes(model, node_num):
    """
    This function converts the node from the ofs control file
    into i and j for ROMS
    """
    i_index,j_index = np.unravel_index(int(node_num),np.shape(model['lon_rho']))
    return i_index,j_index

def report_datums(prop, datum_offsets, logger):
    '''
    Writes a report to the control files directory summarizing datum
    conversions. Report includes listing all obs & model offsets for each station,
    a 'pass' or 'fail' indicating if the conversion was successful, and if
    it failed, a reason why.
    '''

    logger.info("Starting writing datums report.")

    try:
        station_datums = []
        station_providers = []
        id_numbers = []
        station_datum_offsets = []
        success = []
        reason = []
        read_station_ctl_file = station_ctl_file_extract.\
            station_ctl_file_extract(
            f"{prop.control_files_path}/{prop.ofs}_wl_station.ctl")

        for i in range(len(datum_offsets[0])):
            # First find obs row for corresponding model station
            obs_row = [y[0] for y in read_station_ctl_file[0]].\
                index(datum_offsets[0][i])
            if read_station_ctl_file[0][obs_row][0] != \
                datum_offsets[0][i]:
                raise Exception

            station_providers.append(read_station_ctl_file[0][obs_row][-1])
            id_numbers.append(read_station_ctl_file[0][obs_row][0])
            station_datums.append(read_station_ctl_file[1][obs_row][-1])
            if (is_number(read_station_ctl_file[1][obs_row][-3]) and
                datum_offsets[1][i] is not None):
                station_datum_offsets.append(read_station_ctl_file[1]\
                                             [obs_row][-3])
                if datum_offsets[1][i] > -999:
                    success.append('pass')
                    reason.append(' ')
                elif datum_offsets[1][i] < -999:
                    success.append('fail')
            elif datum_offsets[1][i] is None:
                station_datum_offsets.append(read_station_ctl_file[1]\
                                             [obs_row][-3])
                success.append('NA')
                reason.append('No stations model data here, this is expected')
            else:
                success.append('fail')
                if is_number(read_station_ctl_file[1][obs_row][-3]):
                    station_datum_offsets.append(read_station_ctl_file[1]\
                                                 [obs_row][-3])
                else:
                    station_datum_offsets.append(0)
            if success[i] == 'fail':
                if read_station_ctl_file[1][obs_row][-3] == 'RANGE':
                    reason.append('Out of geographic range (obs)')
                elif read_station_ctl_file[1][obs_row][-3] == 'UNKNOWN':
                    reason.append('Conversion unavailable for this datum')
                elif datum_offsets[1][i] < -999:
                    reason.append('Out of geographic range (model)')
                else:
                    reason.append('Unknown failure')

        # Make datums report dataframe
        df_dr = pd.DataFrame({'ID': id_numbers,
                              'Provider': station_providers,
                              'User_input_datum': prop.datum,
                              'Model_datums': prop.datum,
                              'Model_offset': datum_offsets[1][:],
                              'Obs_datums': station_datums,
                              'Obs_offset': station_datum_offsets,
                              'Datum_conversion_success': success,
                              'Reason_for_fail': reason
                              })

        df_dr['Model_offset'] = df_dr['Model_offset'].round(2)
        filename_new = \
            f"{prop.control_files_path}/{prop.ofs}_wl_datum_report.csv"
        df_dr.to_csv(filename_new, header=True, index=False)

        logger.info("Datums report written successfully!")

    except Exception as e_x:
        logger.error("Cannot append datum info to ctl file! "
                     "Skipping this step. Exception: %s", e_x)

def get_datum_offset(prop, node, model, id_number, logger):
    '''
    Returns datum offset (float) to apply to model time series
    '''

    # If doing GLOFS and using the LWD datum, no correction is necessary.
    if prop.datum.lower() == 'lwd':
        return 0

    # Read the correct vdatum file from NODD S3 on-the-fly
    s3 = s3fs.S3FileSystem(anon=True)
    bucket_name = 'noaa-nos-ofs-pds'
    key = f"OFS_Grid_Datum/{prop.ofs}_vdatums.nc"
    url = f"s3://{bucket_name}/{key}"
    try:
        vdatums = xr.open_dataset(s3.open(url, 'rb'))
    except Exception as e_x:
        logger.error("Error opening vdatums on the fly!")
        logger.error(f"Error: {e_x}")
        return 0

    # Set water levels to user-specified datum
    if prop.ofs not in ['leofs','lmhofs','loofs','lsofs']:
        # Deal with SSCOFS separately
        if prop.ofs == 'sscofs':
            # First get from model-0 to xgeoid -- the ofs-wide offset is
            # 0.23 m, where xgeoid is 0.23 cm above model-0.
            # Then convert from xgeoid to other datums.
            try:
                datum_field1 = vdatums['xgeoid20btomsl']
                if prop.datum.lower() == 'msl':
                    datum_field = 0.23 - datum_field1
                else:
                    datum_field2 = vdatums[f"{prop.datum.lower()}tomsl"]
                    datum_field = 0.23 - datum_field1 + datum_field2
            except Exception as e_x:
                logger.error("Wrong netcdf datum variable name!")
                logger.error(f"Error: {e_x}")
                return 0
        else: # Not SSCOFS
            try:
                datum_field = vdatums[f"{prop.datum.lower()}tomsl"]
            except Exception as e_x:
                logger.error("Wrong netcdf datum variable name!")
                logger.error(f"Error: {e_x}")
                return 0
    else:
        try:
            datum_field = vdatums[f"{prop.datum.lower()}tolwd"]
        except Exception as e_x:
            logger.error("Wrong netcdf datum variable name for GLOFS!")
            logger.error(f"Error: {e_x}")
            return 0

    # Do stations
    if prop.ofsfiletype == 'stations':
        try:
            if prop.model_source == 'roms':
                datum_offset = float(datum_field[
                    int(np.array(model['Jpos'][0,node])),
                    int(np.array(model['Ipos'][0,node]))]
                    )
            elif prop.model_source == 'fvcom':
                # Gotta search with lat/lon here...
                vlonlat = np.around(np.array([vdatums[
                    'longitude'], vdatums['latitude']]),3)
                target = np.around(
                    np.array([[model['lon'][0,node]-360],
                              [model['lat'][0,node]]]), 3)
                moddistances = np.linalg.norm(vlonlat-target,
                                              axis=0)
                datum_offset = float(datum_field[int(
                    np.argmin(moddistances))])
        except Exception as e_x:
            logger.error("Error getting datum offset from datum field for "
                         "stations files and %s: %s", prop.model_source, e_x)
            return 0

    # Do fields -- easy peasy
    elif prop.ofsfiletype == 'fields':
        try:
            if prop.model_source == 'roms':
                i_idx, j_idx = roms_nodes(model, node)
                datum_offset = float(datum_field[i_idx, j_idx])
            elif prop.model_source == 'fvcom':
                datum_offset = float(datum_field[node])
        except Exception as e_x:
            logger.error("Error getting datum offset from datum field for "
                         "fields files and %s: %s", prop.model_source, e_x)
            return 0

    if datum_offset < -999:
        logger.error("Did not find datum offset for %s. Returning -9999.9",
                     str(id_number))
    if prop.ofs in ['lmhofs','loofs','lsofs']:
        datum_offset = datum_offset * -1 # Switch sign for GLOFS, except leofs

    return datum_offset
