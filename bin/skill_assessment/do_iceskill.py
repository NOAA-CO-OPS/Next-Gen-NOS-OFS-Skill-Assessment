'''
Introduction and notes

Documentation for do_iceskill.py

Directory Location:  /bin/skill_assessment/

Technical Contact(s): Name:  PL

Abstract:

During a run, for each day, the ice skill assessment:

1) Downloads ice concentration maps from the Great Lakes Surface Environmental
    Analysis (GLSEA) for the time period of interest, and clips it to an OFS area;
2) Fetches available nowcast and/or forecast GLOFS guidance of ice
    concentration for the same time period and OFS area;
3) Produces 1D time series of GLSEA and modeled ice concentration with skill
    statistics at specified locations within the OFS;
4) Interpolates the model output to the regular GLSEA grid, so they are
    directly comparable;
5) Produces basin-wide skill statistics and 2D skill statistics maps.

Language:  Python 3.11

Estimated Execution Time: depends on date range; typically <20 minutes

Scripts/Programs Called:
1) get_icecover_observations.py -- retrieves GLSEA netcdfs
2) get_icecover_fvcom/schism.py -- retrieves model netcdfs, and concatenates them
3) find_ofs_ice_stations.py -- gets inventory of observation stations in an
    OFS, then finds model nodes & GLSEA cells that correspond to them, and
    finally extracts time series of model & GLSEA ice concentration.
4) create_1dplot_ice.py -- main plotting function for ice concentration time
    series and statistics time series.
5) make_ice_map.py -- main map-making function. Makes static .png maps and json
    maps.


Input arguments:
        "-o", "--ofs", required=True, help="""Choose from the list on the
        ofs_extents/ folder, you can also create your own shapefile,
        add it at the ofs_extents/ folder and call it here""", )

        "-p", "--path", required=True,
        help="Inventory File path where ofs_extents/ folder is located", )

        "-s", "--StartDate_full", required=True,
        help="Start Date_full YYYY-MM-DDThh:mm:ssZ e.g.'2023-01-01T12:34:00Z'")

        "-e", "--EndDate_full", required=True,
        help="End Date_full YYYY-MM-DDThh:mm:ssZ e.g. '2023-01-01T12:34:00Z'")

        "-ws", "--Whichcasts", required=True,
        help="whichcasts: 'Nowcast', 'Forecast_A', 'Forecast_B'", )

        "-da", "--DailyAverage", required=False,
        help="Use a daily average model output instead of single hour;
        True/False", )

Output: See the README for all output types, locations, and filenames:
    https://github.com/NOAA-CO-OPS/" \
        "NOS_Shared_Cyberinfrastructure_and_Skill_Assessment/blob/main/" \
            "README.md#4-great-lakes-ice-skill-assessment"

Author Name:  PL       Creation Date:  07/2024


'''
from __future__ import annotations

import argparse
import csv
import logging.config
import os
import sys
import warnings
from datetime import date, datetime, timedelta
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import scipy.interpolate as interp
from mpl_toolkits.basemap import Basemap
from numpy import isnan
from sklearn.metrics import confusion_matrix

from bin.model_processing import get_icecover_fvcom, get_icecover_schism
from bin.obs_retrieval import get_icecover_observations
from bin.visualization import create_1dplot_ice
from ofs_skill.model_processing import (
    model_properties,
    model_source,
)
from ofs_skill.obs_retrieval import find_ofs_ice_stations, utils
from ofs_skill.visualization import make_ice_boxplots, make_ice_map


def make_2d_mask(array_to_mask, conditional_array, threshold):
    '''
    '''
    if conditional_array is not None:
        array_to_mask[conditional_array < threshold] = np.nan
    else:
        array_to_mask[array_to_mask < threshold] = np.nan
    return array_to_mask


def iceonoff(time_all_dt, meanicecover, logger):
    '''
    Finds and returns ice onset and thaw dates for ice conc
    time series. Returns 'None' if dates cannot be found.
    '''
    counter = 0
    iceon = None
    iceoff = None
    # Find ice onset date
    for i in range(len(time_all_dt)):
        if meanicecover[i] >= 10:
            counter = counter + 1
        else:
            counter = 0
        if counter == 5:
            iceon = time_all_dt[i]
            logger.info('Ice onset date found!')
            break
    if iceon is None:
        logger.info('Ice onset date not found!')
    # Find ice thaw date
    try:
        logger.info('Trying ice thaw enumerate...')
        idx = len(meanicecover) - next(
            i for i, val in
            enumerate(reversed(meanicecover), 1)
            if val >= 10
        )
        logger.info('Completed ice thaw enumerate!')
        if (
            len([idx]) > 0
            and (len(meanicecover)-1)-idx >= 5
            and sum(np.isnan(meanicecover[idx:])) <= 2
        ):
            iceoff = time_all_dt[idx+5]
            logger.info('Ice thaw date found!')
        else:
            logger.info('Ice thaw date not found!')
    except StopIteration:
        logger.error('StopIteration exception: ice thaw date not found!')
    logger.info('Completed ice onset/thaw date-finding, return to main...')
    return iceon, iceoff


def ice_climatology(prop, time_all_dt, ice_clim):
    '''
    Handles loading and parsing 1D and 2D Great Lakes ice
    cover climatology.
    '''
    filename = os.path.join(prop.path, 'conf', 'gl_1d_clim.csv')
    df = pd.read_csv(filename, header=0)
    df['DateTime'] = pd.to_datetime(
        df[['Year', 'Month', 'Day']],
    )

    # Select dates
    dateindex = []
    for i in range(0, len(time_all_dt)):
        tempindex = df.index[(
            df['DateTime'].dt.month ==
            time_all_dt[i].month
        )
            & (
            df['DateTime'].dt.day ==
            time_all_dt[i].day
        )]
        dateindex.append(tempindex[0])

    dfsubset = df.iloc[dateindex]
    icecover_hist = dfsubset[prop.ofs].to_numpy()

    # Now do 2D
    filename = os.path.join(
        prop.path, 'conf',
        'unique_dates.csv',
    )
    clim_dates = pd.read_csv(filename)
    uniq = clim_dates['unique_dates'].tolist()

    # Get climatology days that correspond to time_all_dt
    slices = []
    for i in range(0, len(time_all_dt)):
        datesstr = str(time_all_dt[i].month) + '-' +\
            str(time_all_dt[i].day)
        for j in range(0, len(uniq)):
            if datesstr == uniq[j]:
                slices.append(np.array(ice_clim[j, :, :]))
                # print(datesstr[i])
    if slices is not None:
        if len(time_all_dt) == len(slices):
            icecover_hist_2d = np.stack(slices)
        elif len(time_all_dt) != len(slices):
            icecover_hist_2d = []
    else:
        icecover_hist_2d = []

    return icecover_hist, icecover_hist_2d


def pair_ice(
    time_m, icecover_m, time_o, icecover_o, prop, logger,
    ice_clim,
):
    '''
    Pairs the observed and modeled ice conc time series, and
    makes sure time is correct between time, obs, and mod
    '''
    time_all = []
    time_all_dt = []
    icecover_o_pair = []
    icecover_m_pair = []
    for j in range(0, len(time_o)):
        my_obs_date = pd.to_datetime(time_o[j])

        if prop.ice_dt == 'daily':
            time_all.append(my_obs_date)
            time_all_dt.append(
                datetime.strptime(
                    str(my_obs_date), '%Y-%m-%d %H:%M:%S',
                ).date(),
            )
            icecover_o_pair.append(
                np.array(icecover_o[j][:][:]),
            )
            icecover_m_pair.append(
                np.array(icecover_m[j][:]),
            )
        if prop.ice_dt == 'hourly':
            for i in range(0, len(time_m)):
                my_mod_date = pd.to_datetime(time_m[i])
                if (
                    my_mod_date.day == my_obs_date.day
                    and my_mod_date.year == my_obs_date.year
                    and my_mod_date.month == my_obs_date.month
                ):
                    time_all.append(my_mod_date)
                    time_all_dt.append(
                        datetime.strptime(
                            str(my_mod_date),
                            '%Y-%m-%d %H:%M:%S',
                        ).date(),
                    )
                    icecover_o_pair.append(
                        np.array(
                            icecover_o[j][:][:],
                        ),
                    )
                    icecover_m_pair.append(
                        np.array(
                            icecover_m[i][:],
                        ),
                    )

    icecover_o_pair = np.stack(icecover_o_pair)
    icecover_m_pair = np.stack(icecover_m_pair)
    # Load climatology
    icecover_hist = None
    icecover_hist_2d = None
    if prop.ice_dt == 'daily':
        icecover_hist, icecover_hist_2d =\
            ice_climatology(prop, time_all_dt, ice_clim)

    return icecover_o_pair, icecover_m_pair, time_all, \
        time_all_dt, icecover_hist, icecover_hist_2d


def do_iceskill(prop, logger):
    '''Main ice skill function! Let's go'''

    if logger is None:
        config_file = utils.Utils().get_config_file()
        log_config_file = 'conf/logging.conf'
        log_config_file = (
            Path(__file__).
            parent.parent.parent / log_config_file
        ).resolve()

        # Check if log file exists
        if not os.path.isfile(log_config_file):
            sys.exit(-1)
        # Check if config file exists
        if not os.path.isfile(config_file):
            sys.exit(-1)

        # Create logger
        logging.config.fileConfig(log_config_file)
        logger = logging.getLogger('root')
        logger.info('Using config %s', config_file)
        logger.info('Using log config %s', log_config_file)

    logger.info('--- Starting ice skill assessment, put on a coat ---')

    dir_params = utils.Utils().read_config_section('directories', logger)

    # Do forecast_a start and end date reshuffle
    if 'forecast_a' in prop.whichcasts:
        if prop.forecast_hr is None:
            error_message = (
                'prop.forecast_hr is required if prop.whichcast is '
                'forecast_a. Abort!'
            )
            logger.error(error_message)
            sys.exit(-1)
        elif prop.forecast_hr is not None:
            try:
                int(prop.forecast_hr[:-2])
            except ValueError:
                error_message = (
                    f'Please check Forecast Hr format - '
                    f'{prop.forecast_hr}. Abort!'
                )
                logger.error(error_message)
                sys.exit(-1)
            if prop.forecast_hr[-2:] == 'hr':
                print('what are you doing here?')
                # prop.start_date_full, prop.end_date_full =\
                #    get_fcst_cycle.get_fcst_cycle(prop,logger)
            else:
                error_message = (
                    f'Please check Forecast Hr format - '
                    f'{prop.forecast_hr}. Abort!'
                )
                logger.error(error_message)
                sys.exit(-1)
    # Set filetype to fields (this is not currently used but might be later)
    prop.ofsfiletype = 'fields'

    # Start Date and End Date validation
    try:
        prop.start_date_full_before = prop.start_date_full
        prop.end_date_full_before = prop.end_date_full
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
            prop.start_date_full, '%Y-%m-%dT%H:%M:%SZ',
    ) > datetime.strptime(
        prop.end_date_full, '%Y-%m-%dT%H:%M:%SZ',
    ):
        error_message = (
            f'End Date {prop.end_date_full} '
            f'is before Start Date {prop.end_date_full}. Abort!'
        )
        logger.error(error_message)
        sys.exit(-1)
    # Make sure home path is set
    if prop.path is None:
        prop.path = dir_params['home']

    # Make sure start and end months are in ice season
    monthlist = ['11', '12', '01', '02', '03', '04', '05']
    if (
        prop.start_date_full.split('-')[1] not in monthlist
        or
        prop.end_date_full.split('-')[1] not in monthlist
    ):
        logger.error(
            'Start and/or end months are not in ice season months %s',
            monthlist,
        )
        sys.exit(-1)

    # Check OFS -- if not Great Lakes, then quit
    ofscheck = ['leofs', 'loofs', 'lmhofs', 'lsofs']
    if prop.ofs not in ofscheck:
        logger.error(
            "Ice skill can't be run for %s. Input a Great Lakes OFS.",
            prop.ofs,
        )
        sys.exit(-1)

    # prop.path validation
    prop.ofs_extents_path = os.path.join(
        prop.path, dir_params['ofs_extents_dir'],
    )
    if not os.path.exists(prop.ofs_extents_path):
        error_message = (
            f'ofs_extents/ folder is not found. '
            f'Please check prop.path - {prop.path}. Abort!'
        )
        logger.error(error_message)
        sys.exit(-1)

    # prop.ofs validation
    shape_file = f'{prop.ofs_extents_path}/{prop.ofs}.shp'
    if not os.path.isfile(shape_file):
        error_message = (
            f'Shapefile {prop.ofs} is not found at '
            f'the folder {prop.ofs_extents_path}. Abort!'
        )
        logger.error(error_message)
        sys.exit(-1)

    # Daily model average argument verification
    if prop.dailyavg is None:
        prop.dailyavg = False
    elif prop.dailyavg is not None:
        # Use a dictionary for lookup
        truthy_strings = {'true': True, 'yes': True, '1': True, 'True': True}
        falsy_strings = {'false': False,
                         'no': False, '0': False, 'False': False}
        if prop.dailyavg in truthy_strings:
            prop.dailyavg = truthy_strings[prop.dailyavg]
        elif prop.dailyavg in falsy_strings:
            prop.dailyavg = falsy_strings[prop.dailyavg]
        else:
            prop.dailyavg = False

    # Directory tree set-up
    # stats csv files
    prop.data_skill_stats_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['skill_dir'],
        dir_params['stats_dir'], dir_params['stats_ice_dir'],
    )
    os.makedirs(prop.data_skill_stats_path, exist_ok=True)
    # 1D paired csv files
    prop.data_skill_ice1dpair_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['skill_dir'],
        dir_params['1d_ice_pair_dir'],
    )
    os.makedirs(prop.data_skill_ice1dpair_path, exist_ok=True)
    # visuals -- static maps
    prop.visuals_maps_ice_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['visual_dir'],
        dir_params['visual_ice_dir'], dir_params['visual_ice_static_maps'],
    )
    os.makedirs(prop.visuals_maps_ice_path, exist_ok=True)
    # visuals -- JSON maps
    prop.visuals_json_maps_ice_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['visual_dir'],
        dir_params['visual_ice_dir'], dir_params['visual_ice_json_maps'],
    )
    os.makedirs(prop.visuals_json_maps_ice_path, exist_ok=True)
    # visuals -- 1D time series
    prop.visuals_1d_ice_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['visual_dir'],
        dir_params['visual_ice_dir'], dir_params['visual_ice_time_series'],
    )
    os.makedirs(prop.visuals_1d_ice_path, exist_ok=True)
    # visuals -- stats
    prop.visuals_stats_ice_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['visual_dir'],
        dir_params['visual_ice_dir'], dir_params['visual_ice_stats'],
    )
    os.makedirs(prop.visuals_stats_ice_path, exist_ok=True)
    # GLSEA analysis
    prop.data_observations_2d_satellite_path = os.path.join(
        prop.path,
        dir_params['data_dir'],
        dir_params['observations_dir'],
        dir_params['2d_satellite_ice_dir'],
    )
    os.makedirs(prop.data_observations_2d_satellite_path, exist_ok=True)
    # concated model save
    prop.data_model_ice_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['model_dir'],
        dir_params['model_icesave_dir'],
    )
    os.makedirs(prop.data_model_ice_path, exist_ok=True)
    # Example (local) FVCOM ice data
    prop.model_path = os.path.join(
        dir_params['model_historical_dir'], prop.ofs, dir_params['netcdf_dir'],
    )
    # Example (local) SCHISM ice data
    prop.model_path_schism = os.path.join(
        dir_params['model_historical_dir'], prop.ofs,
    )

    # Parse whichcasts argument
    prop.whichcasts = prop.whichcasts.replace('[', '')
    prop.whichcasts = prop.whichcasts.replace(']', '')
    prop.whichcasts = prop.whichcasts.split(',')

    #########################################################

    # Constants:
    #cellarea = 1.33807829*1.40637584    # GLSEA cell area in km^2.
    # GLSEA Dx=0.01617deg(~1.338km),
    # DY=0.01263deg(~1.406km)
    brdr = 0.2                          # margin for plotting
    shouldimakemaps = True  # Should i make maps?
    shouldimakeplots = True  # Should i make plots?
    dailyplotdays = 10  # Number of days before end date to
    # start making daily plots. If you want
    # plots every day, set it to a big 'ol
    # number like 999
    seasonrun = 'yes'  # Run for the current ice season? This
    # option makes sure forecast fields files
    # are available; if not, exits.

    # Ice SA related settings and thresholds:
    threshold_exte = 10    # threshold ice concentration in % at for ice extent
    stathresh = 1           # threshold for stats & calcs

    # CHECK ice_dt and daily average settings.
    if prop.ice_dt != 'daily' and prop.dailyavg is True:
        prop.dailyavg = False

    ########################################################
    # Loop through whichcasts --> GO!
    for cast in prop.whichcasts:
        prop.whichcast = cast.lower()
        # Adjust start date if using forecast and doing a current season run.
        # Set the start date for 60 days before the end date, as that is how
        # long fields forecast files are retained.
        if prop.whichcast == 'forecast_b' and seasonrun == 'yes':
            prop.oldstartdate = prop.start_date_full
            if (
                (
                    datetime.strptime(
                        prop.end_date_full,
                        '%Y-%m-%dT%H:%M:%SZ',
                    ) -
                    datetime.strptime(
                        prop.start_date_full,
                        '%Y-%m-%dT%H:%M:%SZ',
                    )
                ).days > 60
            ):
                logger.info('Adjusting start date for forecast_b...')
                start_date_forecast = date.today() - timedelta(days=60)
                prop.start_date_full = datetime.strftime(
                    start_date_forecast,
                    '%Y-%m-%dT%H:%M:%SZ',
                )
                logger.info(
                    'Forecast_b start date changed to %s from %s',
                    prop.start_date_full, prop.oldstartdate,
                )
                # Make sure start and end months are in ice season
                if (
                    prop.start_date_full.split('-')[1] not in monthlist
                    or
                    prop.end_date_full.split('-')[1] not in monthlist
                ):
                    logger.error(
                        'Start and/or end months not in ice season %s',
                        monthlist,
                    )
                    sys.exit(-1)
    # -------------------------------------------------------------------------
        # Download, concatenate, and mask/clip satellite and 2D climatology
        obsice, ice_clim = get_icecover_observations.get_icecover_observations(
            prop, logger,
        )
        logger.info('Grabbed ice cover observations')
        # Concatenate existing model output
        if prop.model_source == 'fvcom':
            icecover_m, lon_m, lat_m, time_m = get_icecover_fvcom.\
            get_icecover_fvcom(prop, logger)
        elif prop.model_source == 'schism':
            icecover_m, lon_m, lat_m, time_m = get_icecover_schism.\
                get_icecover_schism(prop, logger)
        logger.info('Grabbed ice cover model output')
    # -------------------------------------------------------------------------

        # -- Read lat, lon and ice cover from GLSEA netCDF file (observations)
        lon_o = np.asarray(obsice.variables['lon'][:])
        lon_o = lon_o + 360
        lat_o = np.asarray(obsice.variables['lat'][:])
        icecover_o = np.asarray(obsice.variables['ice_concentration'][:, :, :])
        time_o = np.asarray(obsice.variables['time'][:])
        # Tile lat & lon into arrays
        latsize = np.size(lat_o)
        lonsize = np.size(lon_o)
        lon_o = np.tile(lon_o, (latsize, 1))
        lat_o = np.tile(lat_o, (lonsize, 1))
        lat_o = np.transpose(lat_o)
        logger.info('GLSEA parsing complete')

        # Pair model output to observations, get master time arrays
        icecover_o, icecover_m, time_all, time_all_dt, icecover_hist, \
            icecover_hist_2d = pair_ice(
                time_m,
                icecover_m,
                time_o,
                icecover_o,
                prop,
                logger,
                ice_clim,
            )
        logger.info('Done with data pairing')

        # Get OFS station inventory, find model & obs lat & lon nearest to
        # stations, extract 1D time series, and save to file for each whichcast
        inventory = find_ofs_ice_stations.find_ofs_ice_stations(
            prop,lon_m, lat_m, lon_o, lat_o, time_all_dt, time_all, icecover_o,
            icecover_m, logger,
        )
        logger.info('Completed inventory and .int files')

    # -------------------------------------------------------------------------
        # ---> Let's do some statistics now, ok? ok.

        # -- 1D obs & model statistics through time
        ice_1d_stats = {
            'mod_meanicecover': [],
            'mod_stdmic': [],
            'mod_extent': [],
            'obs_meanicecover': [],
            'obs_stdmic': [],
            'obs_extent': [],
            'extent_error': [],
            'r_all': [],
            'rmse_either': [],
            'rmse_all': [],
            'skill_score': [],
            'csi_all': [],
            'csi_misses': [],
            'csi_falsealarms': [],
        }

        # -- 2D statistics through time
        ice_2d_stats = {
            'obsmoddiff_all': [],
            'icecover_m_interp_all': [],
            'obs_all': [],
            'mod_all': [],
            'obs_extent_map_all': [],
            'mod_extent_map_all': [],
            'overlap_map_all': [],
            'obs_icedays_all': [],
            'mod_icedays_all': [],
            'falarm_map_all': [],
            'miss_map_all': [],
            'total_extent': [],
        }

        # --- 2D masks, collect 'em all
        ice_2d_masks = {
            'noiceobs_all': [],
            'noicemod_all': [],
            'noiceobs_ext_all': [],
            'noicemod_ext_all': [],
            'openwater_all': [],
            'openwater_ext_all': [],
        }

        # Loop through each day and compare GLSEA and model output
        dayrange = ((time_all_dt[-1] - time_all_dt[0]).days)+1
        for i in range(0, len(time_all)):
            # print(i)
            # percentcomplete = ((i+1)/dayrange)*100
            logger.info(
                '%s percent complete: %s',
                prop.whichcast,
                np.round((i/dayrange)*100, decimals=0),
            )
            # Extract ice concentration info from GLSEA data
            icecover_o_mask = np.array(icecover_o[i][:][:])

            # ---------INTERPOLATION-----------------
            # Create a map
            map = Basemap(
                projection='merc',
                resolution='i', area_thresh=1.0,
                llcrnrlon=lon_o.min()-brdr,
                llcrnrlat=lat_o.min()-brdr,
                urcrnrlon=lon_o.max()+brdr,
                urcrnrlat=lat_o.max()+brdr,
            )

            # Project GLSEA lon&lat onto xo&yo
            xo, yo = map(lon_o, lat_o)
            # Project model lon&lat onto xm&ym
            xm, ym = map(lon_m, lat_m)
            # Interpolate model data to GLSEA grid
            icecover_m_interp = interp.griddata(
                (xm, ym), np.array(icecover_m[i, :]*100), (xo, yo),
                method='nearest',
            )
            ice_2d_stats['icecover_m_interp_all'].append(icecover_m_interp)
            # Apply land mask to interpolated model grid
            # (this is a sneaky way to do it!)
            icecover_m_mask = icecover_m_interp - (icecover_o_mask*0)
            # -----------------------------------------

            # Statistics

            #Pre-processing
            logger.info('Stats pre-processing -- make masks for open water...')
            # Mask where there is open water (both model AND
            # observation have no ice!!)
            # First do openwater mask for conc
            icecover_add = np.array(icecover_o_mask + icecover_m_mask)
            ice_2d_masks['openwater_all'].append(
                make_2d_mask(
                    np.array(icecover_o_mask)*0, icecover_add, stathresh,
                ),
            )
            # Now do openwater mask for extent
            ice_2d_masks['openwater_ext_all'].append(
                make_2d_mask(
                    np.array(icecover_o_mask)*0, icecover_add, threshold_exte,
                ),
            )
            # Now remove ice conc below stathresh
            icecover_o_mask2 = make_2d_mask(
                np.array(icecover_o_mask),
                icecover_add, stathresh,
            )
            icecover_m_mask2 = make_2d_mask(
                np.array(icecover_m_mask),
                icecover_add, stathresh,
            )
            # Mask where open water for observation only, ice conc
            ice_2d_masks['noiceobs_all'].append(
                make_2d_mask(
                    np.array(icecover_o_mask), None, stathresh,
                ),
            )
            # Mask where open water for model only, ice conc
            ice_2d_masks['noicemod_all'].append(
                make_2d_mask(
                    np.array(icecover_m_mask), None, stathresh,
                ),
            )
            # Mask where open water for observation only, ice extent
            ice_2d_masks['noiceobs_ext_all'].append(
                make_2d_mask(
                    np.array(icecover_o_mask), None, threshold_exte,
                ),
            )
            # Mask where open water for model only, ice extent
            ice_2d_masks['noicemod_ext_all'].append(
                make_2d_mask(
                    np.array(icecover_m_mask), None, threshold_exte,
                ),
            )

            # Flatten arrays to calculate corr coefficient amd remove nans
            obs_flat = icecover_o_mask2.flatten()
            mod_flat = icecover_m_mask2.flatten()
            badnans = ~np.logical_or(np.isnan(obs_flat), np.isnan(mod_flat))
            obs_flat = np.array(np.compress(badnans, obs_flat))
            mod_flat = np.array(np.compress(badnans, mod_flat))

            # Calculate stats
            logger.info('Calculating stats!')
            ###############################################
            # Mean ice cover & standard deviation
            ice_1d_stats['mod_meanicecover'].append(
                np.nanmean(icecover_m_mask))
            ice_1d_stats['mod_stdmic'].append(np.nanstd(icecover_m_mask))
            ice_1d_stats['obs_meanicecover'].append(
                np.nanmean(icecover_o_mask))
            ice_1d_stats['obs_stdmic'].append(np.nanstd(icecover_o_mask))

            # Percent ice cover (extent)
            # MODEL
            if (((icecover_m_mask >= 0).sum())*100) > 0:
                ice_1d_stats['mod_extent'].append(
                    ((
                        icecover_m_mask >= threshold_exte
                    ).sum() /
                        (icecover_m_mask >= 0).sum())*100,
                )
            else:
                ice_1d_stats['mod_extent'].append(0)
            # OBS
            if (((icecover_o_mask >= 0).sum())*100) > 0:
                ice_1d_stats['obs_extent'].append(
                    ((
                        icecover_o_mask >= threshold_exte
                    ).sum() /
                        (icecover_o_mask >= 0).sum())*100,
                )
            else:
                ice_1d_stats['obs_extent'].append(0)

            # Pearson's R where either model or observations have ice
            # if np.nansum(~isnan(icecover_m_mask2)) > 5 and np.nansum(
            #         ~isnan(icecover_o_mask2)) > 5:
            #     r_value1 = stats.pearsonr(obs_flat,mod_flat)[0]

            #     r_all.append(np.around(r_value1, decimals=3))
            # else:
            ice_1d_stats['r_all'].append(np.nan)

            # RMSE all pixels
            if np.nansum(~isnan(icecover_m_mask)) >= 2 and np.nansum(
                    ~isnan(icecover_o_mask),
            ) >= 2:
                ice_1d_stats['rmse_all'].append(
                    np.sqrt(
                        np.nanmean(
                            (
                                icecover_m_mask-icecover_o_mask
                            )**2,
                        ),
                    ),
                )
            else:
                ice_1d_stats['rmse_all'].append(np.nan)

            # RMSE ice where either model or observations
            if np.nansum(~isnan(icecover_m_mask2)) >= 2 and np.nansum(
                    ~isnan(icecover_o_mask2),
            ) >= 2:
                ice_1d_stats['rmse_either'].append(
                    np.sqrt(
                        np.nanmean(
                            (
                                icecover_m_mask2-icecover_o_mask2
                            )**2,
                        ),
                    ),
                )
            else:
                ice_1d_stats['rmse_either'].append(np.nan)

            # Skill score from Hebert et al. (2015)
            # DOI: 10.1002/2015JC011283
            # Do it in 2D
            if np.nansum(~isnan(icecover_m_mask)) >= 2 and np.nansum(
                    ~isnan(icecover_o_mask),
            ) >= 2:
                mse2_fO = ice_1d_stats['rmse_all'][i]**2
                mse2_fC = np.nanmean(
                    (
                        icecover_m_mask-icecover_hist_2d[i, :, :]
                    )**2,
                )
                if mse2_fC > 0:
                    skillscore = 1 - (mse2_fO/mse2_fC)
                else:
                    skillscore = np.nan
                ice_1d_stats['skill_score'].append(skillscore)
            else:
                ice_1d_stats['skill_score'].append(np.nan)

            # Daily ice extent & total ice days
            # Do observations
            obs_extent_map = np.array(icecover_o_mask)
            obs_extent_map[obs_extent_map < threshold_exte] = 0
            obs_extent_map[obs_extent_map >= threshold_exte] = 1
            ice_2d_stats['obs_extent_map_all'].append(obs_extent_map)
            # Do model
            mod_extent_map = np.array(icecover_m_mask)
            mod_extent_map[mod_extent_map < threshold_exte] = 0
            mod_extent_map[mod_extent_map >= threshold_exte] = 1
            ice_2d_stats['mod_extent_map_all'].append(mod_extent_map)
            # Collect obs OR model extent to get total ice days for either obs
            # or model
            total_extent = np.array(mod_extent_map + obs_extent_map)
            total_extent[total_extent > 1] = 1
            ice_2d_stats['total_extent'].append(total_extent)
            # Do extent overlap (hits), misses, and false alarms
            overlap_map = np.array(mod_extent_map + obs_extent_map)
            overlap_map[overlap_map <= 1] = 0
            overlap_map[overlap_map == 2] = 1
            ice_2d_stats['overlap_map_all'].append(overlap_map)
            csi_map = np.array(mod_extent_map - obs_extent_map)
            falarm_map = np.array(csi_map)
            falarm_map[falarm_map != 1] = 0
            ice_2d_stats['falarm_map_all'].append(falarm_map)
            miss_map = np.array(csi_map)
            miss_map[miss_map != -1] = 0
            miss_map = miss_map * -1
            ice_2d_stats['miss_map_all'].append(miss_map)
            # Do CSI
            # hits: cm[1][1]
            # false alarms: cm[0][1]
            # misses: cm[1][0]
            try:
                cm = confusion_matrix(
                    obs_extent_map[~isnan(obs_extent_map)].flatten(),
                    mod_extent_map[~isnan(mod_extent_map)].flatten(),
                    labels=[0, 1],
                )
                if (cm[1][1] + cm[0][1] + cm[1][0]) > 0:
                    csi = cm[1][1]/(cm[1][1] + cm[0][1] + cm[1][0])
                    misses = cm[0][1]/(cm[1][1] + cm[0][1] + cm[1][0])
                    falarms = cm[1][0]/(cm[1][1] + cm[0][1] + cm[1][0])
                else:
                    csi = 0
                    misses = 0
                    falarms = 0
                ice_1d_stats['csi_all'].append(csi)
                ice_1d_stats['csi_misses'].append(falarms)
                ice_1d_stats['csi_falsealarms'].append(misses)
                ice_1d_stats['extent_error'].append(falarms+misses)
            except ValueError:
                ice_1d_stats['csi_all'].append(np.nan)
                ice_1d_stats['csi_misses'].append(np.nan)
                ice_1d_stats['csi_falsealarms'].append(np.nan)
                ice_1d_stats['extent_error'].append(np.nan)

            # 2D -- diff between obs and mod
            ice_2d_stats['obsmoddiff_all'].append(
                icecover_m_mask2 -
                icecover_o_mask2,
            )
            # Also keep model interpolated and observation arrays
            ice_2d_stats['obs_all'].append(icecover_o_mask)
            ice_2d_stats['mod_all'].append(icecover_m_mask)

            # Make a map once each day, and save it
            if shouldimakemaps:
                if ((
                    prop.ice_dt == 'hourly' and
                    time_all[i].hour == 12
                )
                    or
                    (
                        prop.ice_dt == 'daily' and
                        (time_all_dt[-1]-time_all_dt[i]).days <= dailyplotdays
                )):
                    # Static maps
                    mapdata = np.stack(
                        (
                            icecover_o_mask2, icecover_m_mask2,
                            ice_2d_stats['obsmoddiff_all'][i],
                        ),
                    )
                    make_ice_map.make_ice_map(
                        prop, lon_o, lat_o, xo, yo, mapdata,
                        time_all[i],
                        'daily', logger,
                    )

            # If last time step, do 2D stats and maps and plots etc.
            #    over time period
            ###
            if i == len(time_all)-1 and dayrange >= 5:
                # Keep these separate as numpy arrays
                obsmoddiff_all = np.stack(ice_2d_stats['obsmoddiff_all'])
                obs_all = np.stack(ice_2d_stats['obs_all'])
                mod_all = np.stack(ice_2d_stats['mod_all'])

                # Average 3D arrays through time to make 2D arrays
                # First make new masks to mask out open water
                # for obs and/or model averaged arrays.
                # First do ice concentration masks
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore', category=RuntimeWarning)
                    ice_2d_masks['noiceobs_mask'] = np.array(
                        np.nanmean(
                            np.stack(ice_2d_masks['noiceobs_all']), axis=0,
                        ),
                    )*0
                    ice_2d_masks['noicemod_mask'] = np.array(
                        np.nanmean(
                            np.stack(ice_2d_masks['noicemod_all']), axis=0,
                        ),
                    )*0
                    ice_2d_masks['openwater_mask'] = np.array(
                        np.nanmean(
                            np.stack(ice_2d_masks['openwater_all']), axis=0,
                        ),
                    )  # Already multiplied by zero earlier
                    # Now do ice extent masks
                    ice_2d_masks['noiceobs_ext_mask'] = np.array(
                        np.nanmean(
                            np.stack(ice_2d_masks['noiceobs_ext_all']), axis=0,
                        ),
                    )*0
                    ice_2d_masks['noicemod_ext_mask'] = np.array(
                        np.nanmean(
                            np.stack(ice_2d_masks['noicemod_ext_all']), axis=0,
                        ),
                    )*0
                    ice_2d_masks['openwater_ext_mask'] = np.array(
                        np.nanmean(
                            np.stack(ice_2d_masks['openwater_ext_all']),axis=0,
                        ),
                    )  # Already multiplied by zero earlier

                    # Now proceed and do mean, min, and max diffs & means for
                    # ice cover
                    ice_2d_stats['obsmoddiff_allmean'] = np.array(
                        np.nanmean(obsmoddiff_all, axis=0),
                    )
                    ice_2d_stats['obsmoddiff_allmax'] = np.array(
                        np.nanmax(obsmoddiff_all, axis=0),
                    )
                    ice_2d_stats['obsmoddiff_allmin'] = np.array(
                        np.nanmin(obsmoddiff_all, axis=0),
                    )
                    ice_2d_stats['obs_allmean'] = np.array(
                        np.nanmean(obs_all, axis=0),
                    )+ice_2d_masks['openwater_mask']
                    ice_2d_stats['mod_allmean'] = np.array(
                        np.nanmean(mod_all, axis=0),
                    )+ice_2d_masks['openwater_mask']
                    # Do RMSE
                    ice_2d_stats['rmse_2d'] = np.array(
                        np.sqrt(
                            np.nanmean(
                                ((obsmoddiff_all)**2), axis=0,
                            ),
                        ),
                    )
                    ice_2d_stats['rmse_2d'] = ice_2d_stats['rmse_2d'] + \
                        ice_2d_masks['openwater_mask']

                    # Make ice extents & days of ice cover
                    # NOTE! numpy nansum returns zeros when
                    # summing across nans! Yargh! So we gotta re-apply
                    # masks.
                    # Do obs --
                    obs_extent_map_allsum = np.array(
                        np.nansum(ice_2d_stats['obs_extent_map_all'], axis=0),
                    )
                    ice_2d_stats['obs_icedays_all'] = np.array(
                        obs_extent_map_allsum +
                        (ice_2d_masks['noiceobs_ext_mask']),
                    )
                    obs_extent_map_allsum[obs_extent_map_allsum > 0] = 1
                    ice_2d_stats['obs_extent_map_allsum'] = np.array(
                        obs_extent_map_allsum +
                        (ice_2d_masks['noiceobs_ext_mask']),
                    )
                    # Do model --
                    mod_extent_map_allsum = np.array(
                        np.nansum(ice_2d_stats['mod_extent_map_all'], axis=0),
                    )
                    ice_2d_stats['mod_icedays_all'] = np.array(
                        mod_extent_map_allsum +
                        (ice_2d_masks['noicemod_ext_mask']),
                    )
                    mod_extent_map_allsum[mod_extent_map_allsum > 0] = 1
                    ice_2d_stats['mod_extent_map_allsum'] = np.array(
                        mod_extent_map_allsum +
                        (ice_2d_masks['noicemod_ext_mask']),
                    )

                    # Do Critical Success Index mapping -->
                    # First, map hits
                    csi_norm = np.array(
                        np.nansum(
                            ice_2d_stats['total_extent'],
                            axis=0,
                        ),
                    )
                    csi_norm = csi_norm + ice_2d_masks['openwater_ext_mask']
                    ice_2d_stats['hit_map_allsum'] = np.array(
                        np.nansum(ice_2d_stats['overlap_map_all'], axis=0) /
                        csi_norm,
                    )*100 + ice_2d_masks['openwater_ext_mask']
                    ice_2d_stats['miss_map_allsum'] = np.array(
                        np.nansum(ice_2d_stats['miss_map_all'], axis=0) /
                        csi_norm,
                    )*100 + ice_2d_masks['openwater_ext_mask']
                    ice_2d_stats['falarm_map_allsum'] = np.array(
                        np.nansum(
                            ice_2d_stats['falarm_map_all'], axis=0,
                        )/csi_norm,
                    )*100 +\
                        ice_2d_masks['openwater_ext_mask']

                # Find ice-on and ice-off dates, if doing a season-long run
                if time_all_dt[0].month == 11 or time_all_dt[0].month == 12:
                    logger.info(
                        'Starting ice onset/thaw date-finding routine...')
                    obs_iceon, obs_iceoff = iceonoff(
                        time_all_dt,
                        ice_1d_stats[
                            'obs_meanicecover'
                        ],
                        logger,
                    )
                    mod_iceon, mod_iceoff = iceonoff(
                        time_all_dt,
                        ice_1d_stats[
                            'mod_meanicecover'
                        ],
                        logger,
                    )
                    clim_iceon, clim_iceoff = iceonoff(
                        time_all_dt, icecover_hist,
                        logger,
                    )
                    logger.info('Completed ice onset/thaw! Back in main.')
                    if mod_iceon is not None and obs_iceon is not None:
                        iceondiff = (mod_iceon-obs_iceon).days
                    else:
                        iceondiff = None
                    if mod_iceoff is not None and obs_iceoff is not None:
                        iceoffdiff = (mod_iceoff-obs_iceoff).days
                    else:
                        iceoffdiff = None
                    logger.info('Calculated ice onset/thaw error!')
                    # Combine ice on/off dates and diff to format for pandas
                    # table
                    logger.info('Writing ice onset/thaw table...')
                    iceonoffall = [
                        [' ', 'Ice onset', 'Ice thaw'],
                        ['Observed', str(obs_iceon), str(obs_iceoff)],
                        ['Modeled', str(mod_iceon), str(mod_iceoff)],
                        ['Climatology', str(clim_iceon), str(clim_iceoff)],
                        [
                            'Model-obs difference (days)', str(iceondiff),
                            str(iceoffdiff),
                        ],
                    ]

                    # Write to csv
                    title = r'' +\
                        f'{prop.data_skill_stats_path}/' +\
                        f'skill_{prop.ofs}_iceonoff_{prop.whichcast}.csv'
                    with open(title, 'w', newline='') as csvfile:
                        writer = csv.writer(csvfile)
                        # Write the header row (column labels)
                        writer.writerow(iceonoffall[0])
                        # Write the data rows
                        for row in iceonoffall[1:]:
                            writer.writerow(row)
                    logger.info('Ice on/off table saved!')

                # Make 2D stats maps
                if shouldimakemaps:
                    logger.info('Starting all stats maps...')
                    # Map conc: mean model, mean obs, and rmse
                    mapdata = np.stack(
                        (
                            ice_2d_stats['obs_allmean'],
                            ice_2d_stats['mod_allmean'],
                            ice_2d_stats['rmse_2d'],
                        ),
                    )
                    make_ice_map.make_ice_map(
                        prop, lon_o, lat_o, xo, yo, mapdata,
                        time_all[i], 'rmse means', logger,
                    )
                    # Map extent: total ice days model,
                    # ice days obs, ice distance
                    mapdata = np.stack(
                        (
                            ice_2d_stats['obs_icedays_all'],
                            ice_2d_stats['mod_icedays_all'],
                        ),
                    )
                    make_ice_map.make_ice_map(
                        prop, lon_o, lat_o, xo, yo, mapdata, time_all[i],
                        'extents', logger,
                    )
                    # Map diff conc: mean diff, max diff,
                    # min diff
                    mapdata = np.stack(
                        (
                            ice_2d_stats['obsmoddiff_allmean'],
                            ice_2d_stats['obsmoddiff_allmax'],
                            ice_2d_stats['obsmoddiff_allmin'],
                        ),
                    )
                    make_ice_map.make_ice_map(
                        prop, lon_o, lat_o, xo, yo, mapdata,
                        time_all[i], 'diff', logger,
                    )
                    # Map CSI metrics
                    mapdata = np.stack(
                        (
                            ice_2d_stats['hit_map_allsum'],
                            ice_2d_stats['falarm_map_allsum'],
                            ice_2d_stats['miss_map_allsum'],
                        ),
                    )
                    make_ice_map.make_ice_map(
                        prop, lon_o, lat_o, xo, yo, mapdata,
                        time_all[i], 'csi', logger,
                    )
                    logger.info('All stats maps complete!')
                if shouldimakeplots:
                    logger.info('Starting histograms...')
                    # ---HISTOGRAMS/PDFs-----------------------------------
                    # Make distributions of errors
                    # Do all RMSEs
                    make_ice_boxplots.make_ice_boxplots(
                        obs_all, mod_all,
                        time_all_dt, prop, logger,
                    )
                    plt.close('all')
                    logger.info('Box plots complete!')
            elif i == len(time_all)-1 and dayrange < 5:
                logger.info(
                    'Day range is < 5, so no maps or cumulative stats!')

        # Before plotting, make pandas dataframe with stats time series
        logger.info('Compiling time series stats for table output...')
        pd.DataFrame(
            {
                'time_all_dt': time_all_dt,
                'obs_meanicecover': ice_1d_stats['obs_meanicecover'],
                'mod_meanicecover': ice_1d_stats['mod_meanicecover'],
                'obs_stdmic': ice_1d_stats['obs_stdmic'],
                'mod_stdmic': ice_1d_stats['mod_stdmic'],
                'icecover_hist': icecover_hist,
                'SS': ice_1d_stats['skill_score'],
                'rmse_all': ice_1d_stats['rmse_all'],
                'rmse_either': ice_1d_stats['rmse_either'],
                'obs_extent': ice_1d_stats['obs_extent'],
                'mod_extent': ice_1d_stats['mod_extent'],
                'r_all': ice_1d_stats['r_all'],
                'csi_all': ice_1d_stats['csi_all'],
                'csi_falsealarms': ice_1d_stats['csi_falsealarms'],
                'csi_misses': ice_1d_stats['csi_misses'],
                'extent_error': ice_1d_stats['extent_error'],
            },
        ).to_csv(
            r'' + f'{prop.data_skill_stats_path}/'
                  f'skill_{prop.ofs}_'
            f'icestatstseries_{prop.whichcast}.csv',
        )

        logger.info(
            'Time series of OFS-wide skill stats is created successfully.',
        )

        # Switch date back to user-inputted start date if doing a season run
        if seasonrun == 'yes' and prop.whichcast == 'forecast_b':
            prop.start_date_full = prop.oldstartdate

    if shouldimakeplots:
        if dayrange >= 5:
            # Send 1D stats, model, and obs time series to plotting module
            create_1dplot_ice.create_1dplot_icestats(
                prop,
                time_all,
                logger,
            )
            create_1dplot_ice.create_1dplot_ice(
                prop, inventory,
                time_all,
                logger,
            )
        else:
            logger.info(
                'Need >=5 days for stats & 1D time series plots! '
                'No 1D plots made.',
            )

    logger.info('Program complete! Go get coffee and bagel')


# Execution:
if __name__ == '__main__':
    # Arguments:
    # Parse (optional and required) command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-o', '--ofs', required=True, help="""Choose from the list on the
        ofs_extents/ folder, you can also create your own shapefile,
        add it at the ofs_extents/ folder and call it here""", )
    parser.add_argument(
        '-p', '--path', required=True,
        help='Inventory File path where ofs_extents/ folder is located', )
    parser.add_argument(
        '-s', '--StartDate_full', required=True,
        help="Start Date_full YYYY-MM-DDThh:mm:ssZ e.g.'2023-01-01T12:34:00Z'",
    )
    parser.add_argument(
        '-e', '--EndDate_full', required=True,
        help="End Date_full YYYY-MM-DDThh:mm:ssZ e.g. '2023-01-01T12:34:00Z'",
    )
    parser.add_argument(
        '-ws', '--Whichcasts', required=True,
        help="whichcasts: 'Nowcast', 'Forecast_A', 'Forecast_B'", )
    parser.add_argument(
        '-da', '--DailyAverage', required=False,
        help='Use a daily average model output instead of single hour; True '
        'or False (False is default)', )
    parser.add_argument(
        '-ts', '--TimeStep', required=False,
        help='Set assessment time step: hourly or daily (daily is default)', )
    args = parser.parse_args()

    prop1 = model_properties.ModelProperties()
    prop1.ofs = args.ofs.lower()
    prop1.path = args.path
    prop1.start_date_full = args.StartDate_full
    prop1.end_date_full = args.EndDate_full
    prop1.whichcasts = args.Whichcasts
    prop1.model_source = model_source.model_source(prop1.ofs)

    # Set time step
    if args.TimeStep is None:
        #print('No time step input -- defaulting to daily')
        prop1.ice_dt = 'daily'
    else:
        prop1.ice_dt = args.TimeStep

    # Set daily average argument
    if args.DailyAverage is None:
        #print('No daily average input -- defaulting to False')
        prop1.dailyavg = False
    else:
        prop1.dailyavg = args.DailyAverage

    # Do forecast_a to assess a single forecast cycle
    if 'forecast_a' in prop1.whichcasts:
        if args.Forecast_Hr is None:
            print('No forecast cycle input -- defaulting to 00Z')
            prop1.forecast_hr = '00hr'
        elif args.FileType is not None:
            prop1.forecast_hr = args.Forecast_Hr

    do_iceskill(prop1, None)
