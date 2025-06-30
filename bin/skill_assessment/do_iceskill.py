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
2) get_icecover_model.py -- retrieves model netcdfs, and concatenates them
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

        "-pt", "--PlotType", required=False,
        help="Plot type: 'static' or 'interactive'", )

Output: See the README for all output types, locations, and filenames:
    https://github.com/NOAA-CO-OPS/" \
        "NOS_Shared_Cyberinfrastructure_and_Skill_Assessment/blob/main/" \
            "README.md#4-great-lakes-ice-skill-assessment"

Author Name:  PL       Creation Date:  07/2024


'''

import os
import sys
import argparse
import logging
import logging.config
import csv
from datetime import datetime, timedelta, date
import numpy as np
from numpy import isnan
from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
import scipy.interpolate as interp
from sklearn.metrics import confusion_matrix
import pandas as pd
#import math
from scipy import stats

# Add parent directory to sys.path
from pathlib import Path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from obs_retrieval import utils
from obs_retrieval import get_icecover_observations
from obs_retrieval import find_ofs_ice_stations
from model_processing import get_icecover_model
from model_processing import model_properties
from visualization import create_1dplot_ice
from visualization import make_ice_map
from visualization import make_ice_boxplots


def iceonoff(time_all_dt,meanicecover,logger):
    '''
    Finds and returns ice onset and thaw dates for ice conc
    time series. Returns 'None' if dates cannot be found.
    '''
    counter = 0
    iceon = None
    iceoff = None
    #Find ice onset date
    for i in range(len(time_all_dt)):
        if meanicecover[i] >= 10:
            counter = counter + 1
        else:
            counter = 0
        if counter == 5:
            iceon = time_all_dt[i]
            logger.info("Ice onset date found!")
            break
    if iceon is None:
        logger.info("Ice onset date not found!")
    # Find ice thaw date
    try:
        logger.info("Trying ice thaw enumerate...")
        idx = len(meanicecover) - next(
            i for i, val in
            enumerate(reversed(meanicecover), 1)
            if val >= 10)
        logger.info("Completed ice thaw enumerate!")
        if (len([idx]) > 0
            and (len(meanicecover)-1)-idx >= 5
            and sum(np.isnan(meanicecover[idx:])) <= 2):
            iceoff = time_all_dt[idx+5]
            logger.info("Ice thaw date found!")
        else:
            logger.info("Ice thaw date not found!")
    except StopIteration:
        logger.error("StopIteration exception: ice thaw date not found!")
    logger.info("Completed ice onset/thaw date-finding, return to main...")
    return iceon,iceoff

def ice_climatology(prop,time_all_dt,ice_clim):
    '''
    Handles loading and parsing 1D and 2D Great Lakes ice
    cover climatology.
    '''
    filename=os.path.join(prop.path,'conf','gl_1d_clim.csv')
    df = pd.read_csv(filename, header=0)
    df['DateTime'] = pd.to_datetime(
                df[['Year', 'Month', 'Day']])

    #Select dates
    dateindex = []
    for i in range(0,len(time_all_dt)):
        tempindex = df.index[(df['DateTime'].dt.month ==
                              time_all_dt[i].month)
                 & (df['DateTime'].dt.day ==
                    time_all_dt[i].day)]
        dateindex.append(tempindex[0])

    dfsubset = df.iloc[dateindex]
    icecover_hist = dfsubset[prop.ofs].to_numpy()

    #Now do 2D
    filename = os.path.join(prop.path,'conf',
                            'unique_dates.csv')
    clim_dates = pd.read_csv(filename)
    uniq = clim_dates['unique_dates'].tolist()

    # Get climatology days that correspond to time_all_dt
    slices = []
    for i in range(0,len(time_all_dt)):
        datesstr = str(time_all_dt[i].month) + '-' +\
            str(time_all_dt[i].day)
        for j in range(0,len(uniq)):
            if datesstr == uniq[j]:
                slices.append(np.array(ice_clim[j,:,:]))
                #print(datesstr[i])
    if slices is not None:
        if len(time_all_dt) == len(slices):
            icecover_hist_2d = np.stack(slices)
        elif len(time_all_dt) != len(slices):
            icecover_hist_2d = []
    else:
        icecover_hist_2d = []

    return icecover_hist, icecover_hist_2d


def pair_ice(time_m,icecover_m,time_o,icecover_o,prop,logger,
             ice_clim):
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
            time_all_dt.append(datetime.strptime(
                str(my_obs_date),'%Y-%m-%d %H:%M:%S').date())
            icecover_o_pair.append(
                np.array(icecover_o[j][:][:]))
            icecover_m_pair.append(
                np.array(icecover_m[j][:]))
        if prop.ice_dt == 'hourly':
            for i in range(0, len(time_m)):
                my_mod_date = pd.to_datetime(time_m[i])

                if (my_mod_date.day == my_obs_date.day
                    and my_mod_date.year == my_obs_date.year
                    and my_mod_date.month ==
                    my_obs_date.month):
                    time_all.append(my_mod_date)
                    time_all_dt.append(datetime.strptime(
                        str(my_mod_date),
                        '%Y-%m-%d %H:%M:%S.%f').date())
                    icecover_o_pair.append(np.array(
                        icecover_o[j][:][:]))
                    icecover_m_pair.append(np.array(
                        icecover_m[i][:]))

    icecover_o_pair = np.stack(icecover_o_pair)
    icecover_m_pair = np.stack(icecover_m_pair)

    icecover_hist,icecover_hist_2d =\
        ice_climatology(prop,time_all_dt,ice_clim)

    return icecover_o_pair, icecover_m_pair, time_all,\
        time_all_dt, icecover_hist, icecover_hist_2d

def do_iceskill(prop, logger):
    '''Main ice skill function! Let's go'''

    if logger is None:
        config_file = utils.Utils().get_config_file()
        log_config_file = "conf/logging.conf"
        log_config_file = os.path.join(os.getcwd(), log_config_file)

        # Check if log file exists
        if not os.path.isfile(log_config_file):
            sys.exit(-1)
        # Check if config file exists
        if not os.path.isfile(config_file):
            sys.exit(-1)

        # Create logger
        logging.config.fileConfig(log_config_file)
        logger = logging.getLogger("root")
        logger.info("Using config %s", config_file)
        logger.info("Using log config %s", log_config_file)

    logger.info("--- Starting ice skill assessment, put on a coat ---")

    dir_params = utils.Utils().read_config_section("directories", logger)

    # Do forecast_a start and end date reshuffle
    if 'forecast_a' in prop.whichcasts:
        if prop.forecast_hr is None:
            error_message = (
                "prop.forecast_hr is required if prop.whichcast is "
                "forecast_a. Abort!")
            logger.error(error_message)
            sys.exit(-1)
        elif prop.forecast_hr is not None:
            try:
                int(prop.forecast_hr[:-2])
            except ValueError:
                error_message = (f"Please check Forecast Hr format - "
                                 f"{prop.forecast_hr}. Abort!")
                logger.error(error_message)
                sys.exit(-1)
            if prop.forecast_hr[-2:] == 'hr':
                print('what are you doing here?')
                #prop.start_date_full, prop.end_date_full =\
                #    get_fcst_cycle.get_fcst_cycle(prop,logger)
            else:
                error_message = (f"Please check Forecast Hr format - "
                                 f"{prop.forecast_hr}. Abort!")
                logger.error(error_message)
                sys.exit(-1)
    # Set filetype to fields (this is not currently used but might be later)
    prop.ofsfiletype = 'fields'

    # Start Date and End Date validation
    try:
        prop.start_date_full_before = prop.start_date_full
        prop.end_date_full_before = prop.end_date_full
        datetime.strptime(prop.start_date_full, "%Y-%m-%dT%H:%M:%SZ")
        datetime.strptime(prop.end_date_full, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        error_message = (f"Please check Start Date - "
                         f"{prop.start_date_full}, End Date - "
                         f"{prop.end_date_full}. Abort!")
        logger.error(error_message)
        print(error_message)
        sys.exit(-1)

    if datetime.strptime(
            prop.start_date_full, "%Y-%m-%dT%H:%M:%SZ") > datetime.strptime(
        prop.end_date_full, "%Y-%m-%dT%H:%M:%SZ"):
        error_message = (f"End Date {prop.end_date_full} "
                         f"is before Start Date {prop.end_date_full}. Abort!")
        logger.error(error_message)
        sys.exit(-1)

    if prop.path is None:
        prop.path = dir_params["home"]

    # Make sure start and end months are in ice season
    monthlist = ['11','12','01','02','03','04','05']
    if (prop.start_date_full.split('-')[1] not in monthlist
        or
        prop.end_date_full.split('-')[1] not in monthlist):
        logger.error("Start and/or end months are not in ice season months %s",
                     monthlist)
        sys.exit(-1)

    # Check ofs -- if not Great Lakes, then quit
    ofscheck = ['leofs','loofs','lmhofs','lsofs']
    if prop.ofs not in ofscheck:
        logger.error("Ice skill can't be run for %s. Input a Great Lakes OFS.",
                    prop.ofs)
        sys.exit(-1)

    # prop.path validation
    prop.ofs_extents_path = os.path.join(
        prop.path, dir_params["ofs_extents_dir"])
    if not os.path.exists(prop.ofs_extents_path):
        error_message = (f"ofs_extents/ folder is not found. "
                         f"Please check prop.path - {prop.path}. Abort!")
        logger.error(error_message)
        sys.exit(-1)

    # prop.ofs validation
    shape_file = f"{prop.ofs_extents_path}/{prop.ofs}.shp"
    if not os.path.isfile(shape_file):
        error_message = (f"Shapefile {prop.ofs} is not found at "
                         f"the folder {prop.ofs_extents_path}. Abort!")
        logger.error(error_message)
        sys.exit(-1)
    ##### Directory tree set-up
    # stats csv files
    prop.data_skill_stats_path = os.path.join(
        prop.path, dir_params["data_dir"], dir_params["skill_dir"],
        dir_params["stats_dir"], dir_params["stats_ice_dir"])
    os.makedirs(prop.data_skill_stats_path, exist_ok=True)
    # 1D paired csv files
    prop.data_skill_ice1dpair_path = os.path.join(
        prop.path, dir_params["data_dir"], dir_params["skill_dir"],
        dir_params["1d_ice_pair_dir"])
    os.makedirs(prop.data_skill_ice1dpair_path, exist_ok=True)
    # visuals -- static maps
    prop.visuals_maps_ice_path = os.path.join(
        prop.path, dir_params["data_dir"], dir_params["visual_dir"],
        dir_params["visual_ice_dir"],dir_params["visual_ice_static_maps"])
    os.makedirs(prop.visuals_maps_ice_path, exist_ok=True)
    # visuals -- JSON maps
    prop.visuals_json_maps_ice_path = os.path.join(
        prop.path, dir_params["data_dir"], dir_params["visual_dir"],
        dir_params["visual_ice_dir"],dir_params["visual_ice_json_maps"])
    os.makedirs(prop.visuals_json_maps_ice_path, exist_ok=True)
    # visuals -- 1D time series
    prop.visuals_1d_ice_path = os.path.join(
        prop.path, dir_params["data_dir"],dir_params["visual_dir"],
        dir_params["visual_ice_dir"],dir_params["visual_ice_time_series"])
    os.makedirs(prop.visuals_1d_ice_path, exist_ok=True)
    # visuals -- stats
    prop.visuals_stats_ice_path = os.path.join(
        prop.path, dir_params["data_dir"], dir_params["visual_dir"],
        dir_params["visual_ice_dir"],dir_params["visual_ice_stats"])
    os.makedirs(prop.visuals_stats_ice_path, exist_ok=True)
    # GLSEA analysis
    prop.data_observations_2d_satellite_path = os.path.join(
        prop.path,
        dir_params["data_dir"],
        dir_params["observations_dir"],
        dir_params["2d_satellite_ice_dir"],
        )
    os.makedirs (prop.data_observations_2d_satellite_path,exist_ok = True)
    # concated model save
    prop.data_model_ice_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['model_dir'],
        dir_params['model_icesave_dir'])
    os.makedirs(prop.data_model_ice_path, exist_ok = True)
    # Example (local) ice data
    prop.model_path = os.path.join(
        dir_params["model_historical_dir"], prop.ofs, dir_params["netcdf_dir"]
    )

    # Parse whichcasts argument
    prop.whichcasts = prop.whichcasts.replace("[", "")
    prop.whichcasts = prop.whichcasts.replace("]", "")
    prop.whichcasts = prop.whichcasts.split(",")


    #########################################################

    ## Constants:
    cellarea = 1.33807829*1.40637584    # GLSEA cell area in km^2.
                                        #GLSEA Dx=0.01617deg(~1.338km),
                                        #DY=0.01263deg(~1.406km)
    brdr = 0.2                          # margin for plotting
    shouldimakemaps = 'yesplease'       #Should i make maps? 'yesplease' or
                                        #'nothanks'
    shouldimakeplots = 'yesplease'      #Should i make plots? 'yesplease' or
                                        #'nothanks'
    prop.ice_dt = 'daily'               #daily or hourly --
                                        #Default is daily!
    dailyplotdays = 10                  #Number of days before end date to
                                        #start making daily plots. If you want
                                        #plots every day, set it to a big 'ol
                                        #number like 999
    seasonrun = 'yes'                   #Run for the current ice season? This
                                        #option makes sure forecast fields files
                                        #are available; if not, exits.

    ## Ice SA related settings and thresholds:
    threshold_exte = 10    # threshold ice concentration in % at for ice extent
    stathresh = 1           # threshold for stats & calcs

    ########################################################
    # Loop through whichcasts --> GO!
    for cast in prop.whichcasts:
        prop.whichcast = cast.lower()
        # Adjust start date if using forecast and doing a current season run.
        # Set the start date for 60 days before the end date, as that is how
        # long fields forecast files are retained.
        if prop.whichcast == 'forecast_b' and seasonrun == 'yes':
            prop.oldstartdate = prop.start_date_full
            if ((datetime.strptime(prop.end_date_full,
                                   "%Y-%m-%dT%H:%M:%SZ") -
                datetime.strptime(prop.start_date_full,
                                  "%Y-%m-%dT%H:%M:%SZ")).days > 60):
                logger.info("Adjusting start date for forecast_b...")
                start_date_forecast = date.today() - timedelta(days=60)
                prop.start_date_full = datetime.strftime(start_date_forecast,
                                                         "%Y-%m-%dT%H:%M:%SZ")
                logger.info("Forecast_b start date changed to %s from %s",
                            prop.start_date_full,prop.oldstartdate)
                # Make sure start and end months are in ice season
                if (prop.start_date_full.split('-')[1] not in monthlist
                    or
                    prop.end_date_full.split('-')[1] not in monthlist):
                    logger.error("Start and/or end months not in ice season %s",
                                 monthlist)
                    sys.exit(-1)
    #-------------------------------------------------------------------------
        # Download, concatenate, and mask/clip satellite and 2D climatology
        obsice,ice_clim = get_icecover_observations.get_icecover_observations(
            prop,logger)
        logger.info('Grabbed ice cover observations')
        # Concatenate existing model output
        modelice = get_icecover_model.get_icecover_model(prop,logger)
        logger.info('Grabbed ice cover model output')
    #-------------------------------------------------------------------------

        # -- Read lat, lon and ice cover from FVCOM nowcast output (model)
        lon_m = np.array(modelice.variables['lon'][:])
        lat_m = np.array(modelice.variables['lat'][:])
        try:
            icecover_m = np.array(modelice.variables['aice'][:])
        except:
            logger.error('No modeled ice concentration available! Abort')
            sys.exit(-1)

        time_m = np.array(modelice.variables['time'][:])
        # depth = np.array(modelice.variables['h'][:])

        # -- Read lat, lon and ice cover from GLSEA netCDF file (observations)
        lon_o = np.array(obsice.variables['lon'][:])
        lon_o = lon_o + 360
        lat_o = np.array(obsice.variables['lat'][:])
        icecover_o = np.array(obsice.variables['ice_concentration'][:,:,:])
        time_o = np.array(obsice.variables['time'][:])
        # Tile lat & lon into arrays
        latsize = np.size(lat_o)
        lonsize = np.size(lon_o)
        lon_o = np.tile(lon_o, (latsize, 1))
        lat_o = np.tile(lat_o, (lonsize, 1))
        lat_o = np.transpose(lat_o)
        logger.info('GLSEA parsing complete')

        # Pair model output to observations, get master time arrays
        icecover_o,icecover_m,time_all,time_all_dt,icecover_hist,\
            icecover_hist_2d = pair_ice(
            time_m,
            icecover_m,
            time_o,
            icecover_o,
            prop,
            logger,
            ice_clim
            )
        logger.info('Done with data pairing')


        # Get OFS station inventory, find model & obs lat & lon nearest to
        #stations, extract 1D time series, and save to file for each whichcast
        inventory = find_ofs_ice_stations.find_ofs_ice_stations(
              prop,lon_m,lat_m,lon_o,lat_o,time_all_dt,time_all,icecover_o,\
                  icecover_m,logger)
        logger.info('Completed inventory and .int files')

    #-------------------------------------------------------------------------
        # ---> Let's do some statistics now, ok? ok.

        # -- 1D model statistics through time
        mod_meanicecover = []
        mod_stdmic = []
        mod_extent = []
        mod_icearea = []

        # -- 1D observation statistics through time
        obs_meanicecover = []
        obs_stdmic = []
        obs_extent = []
        obs_icearea = []

        # -- 1D comparison stats through time
        hitrate = []
        hitrate_obs = []
        #r_overlap = []
        r_all = []
        rmse_overlap = []
        rmse_either = []
        rmse_all = []
        SS = []

        # -- 2D statistics through time
        obsmoddiff_all = []
        icecover_m_interp_all = []
        # depth_all = []
        obs_all = []
        mod_all = []
        #ice_dist_all = []
        obs_extent_map_all = []
        mod_extent_map_all = []
        overlap_map_all = []
        obs_icedays_all = []
        mod_icedays_all = []
        noiceobs_all = []
        noicemod_all = []
        noiceobs_ext_all = []
        noicemod_ext_all = []
        openwater_all = []
        openwater_ext_all = []
        csi_all = []
        falarm_map_all = []
        miss_map_all = []
        #icecover_m_overlap_all = []
        #icecover_o_overlap_all = []

        #Loop through each day in date range and compare GLSEA and model output
        dayrange = ((time_all_dt[-1] - time_all_dt[0]).days)+1
        for i in range(0, len(time_all)):
            #print(i)
            #percentcomplete = ((i+1)/dayrange)*100
            logger.info("%s percent complete: %s",
                        prop.whichcast,
                        np.round((i/dayrange)*100,decimals=0))
            # Extract ice concentration info from GLSEA data
            icecover_o_mask = np.array(icecover_o[i][:][:])


            # ---------INTERPOLATION-----------------
            # Create a map
            map = Basemap(projection='merc',
                          resolution = 'i', area_thresh = 1.0,
                          llcrnrlon=lon_o.min()-brdr,
                          llcrnrlat=lat_o.min()-brdr,
                          urcrnrlon=lon_o.max()+brdr,
                          urcrnrlat=lat_o.max()+brdr)

            # Project GLSEA lon&lat onto xo&yo
            xo,yo = map(lon_o,lat_o)
            # Project FVCOM lon&lat onto xm&ym
            xm,ym = map(lon_m,lat_m)
            # Interpolate FVCOM data to GLSEA grid
            icecover_m_interp = interp.griddata(
                (xm,ym),np.array(icecover_m[i,:]*100),(xo,yo),method='nearest')
            # depth_interp = interp.griddata(
            #     (xm,ym),depth,(xo,yo),method='nearest')
            icecover_m_interp_all.append(icecover_m_interp)
            # depth_all.append(depth_interp)
            # Apply land mask to interpolated model grid
            # (this is a sneaky way to do it!)
            icecover_m_mask = icecover_m_interp - (icecover_o_mask*0)
            #-----------------------------------------

            ##############
            # Statistics
            ##############

            ############## Pre-processing ##############
            ############################################
            logger.info('Stats pre-processing -- make masks for open water...')
            # Mask where there is open water (both model AND
            # observation have no ice!!)
            # First do openwater mask for conc
            icecover_add = np.array(icecover_o_mask + icecover_m_mask)
            openwater = np.array(icecover_o_mask)*0
            openwater[icecover_add<stathresh] = np.nan
            openwater_all.append(openwater)
            # Now do openwater mask for extent
            openwater_ext = np.array(icecover_o_mask)*0
            openwater_ext[icecover_add<threshold_exte] = np.nan
            openwater_ext_all.append(openwater_ext)
            # Now remove ice conc below stathresh
            icecover_o_mask2 = np.array(icecover_o_mask)
            icecover_o_mask2[icecover_add<stathresh] = np.nan
            icecover_m_mask2 = np.array(icecover_m_mask)
            icecover_m_mask2[icecover_add<stathresh] = np.nan
            # Mask where open water for observation only, ice conc
            noiceobs = np.array(icecover_o_mask)
            noiceobs[noiceobs<stathresh] = np.nan
            noiceobs_all.append(noiceobs)
            # Mask where open water for model only, ice conc
            noicemod = np.array(icecover_m_mask)
            noicemod[noicemod<stathresh] = np.nan
            noicemod_all.append(noicemod)

            # Mask where open water for observation only, ice extent
            noiceobs_ext = np.array(icecover_o_mask)
            noiceobs_ext[noiceobs_ext<threshold_exte] = np.nan
            noiceobs_ext_all.append(noiceobs)
            # Mask where open water for model only, ice extent
            noicemod_ext = np.array(icecover_m_mask)
            noicemod_ext[noicemod_ext<threshold_exte] = np.nan
            noicemod_ext_all.append(noicemod)

            # Find overlap between model and observations
            mod_icefind = np.argwhere(icecover_m_mask>stathresh)
            obs_icefind = np.argwhere(icecover_o_mask>stathresh)
            modset = set((tuple(i) for i in mod_icefind))
            obsset = set((tuple(i) for i in obs_icefind))
            overlap = modset.intersection(obsset)
            overlap_obs = obsset.intersection(modset)
            # Now get overlap values and deal with NaNs
            overlaplist = list(overlap)
            index_overlap = np.array(np.moveaxis(np.array(overlaplist), -1, 0))
            mod_overlapvalues = np.array(
                icecover_m_mask[tuple(index_overlap)])
            obs_overlapvalues = np.array(icecover_o_mask[tuple(index_overlap)])
            index = ~isnan(mod_overlapvalues) * ~isnan(obs_overlapvalues)
            mod_overlapvalues = np.array(mod_overlapvalues[index])
            obs_overlapvalues = np.array(obs_overlapvalues[index])

            # Flatten arrays to calculate corr coefficient amd remove nans
            obs_flat = icecover_o_mask2.flatten()
            mod_flat = icecover_m_mask2.flatten()
            badnans = ~np.logical_or(np.isnan(obs_flat), np.isnan(mod_flat))
            obs_flat = np.array(np.compress(badnans, obs_flat))
            mod_flat = np.array(np.compress(badnans, mod_flat))

            # Calculate stats
            logger.info('Calculating stats')
            ###############################################
            # Mean ice cover & standard deviation
            mod_meanicecover.append(np.nanmean(icecover_m_mask))
            mod_stdmic.append(np.nanstd(icecover_m_mask))
            obs_meanicecover.append(np.nanmean(icecover_o_mask))
            obs_stdmic.append(np.nanstd(icecover_o_mask))

            # Percent ice cover (extent)
            if (((icecover_m_mask>=0).sum())*100) > 0:
                mod_extent.append(((icecover_m_mask>=threshold_exte).sum()/
                                   (icecover_m_mask>=0).sum())*100)
            else:
                mod_extent.append(0)
            if (((icecover_o_mask>=0).sum())*100) > 0:
                obs_extent.append(((icecover_o_mask>=threshold_exte).sum()/
                                   (icecover_o_mask>=0).sum())*100)
            else:
                obs_extent.append(0)

            # Ice area
            mod_icearea.append((((icecover_m_mask>stathresh).sum())
                                *cellarea))
            obs_icearea.append((((icecover_o_mask>stathresh).sum())*cellarea))

            # Hit rate
            if len(mod_icefind) > 0:
                hitrate.append((len(overlap)/len(mod_icefind))*100)
            else:
                hitrate.append(np.nan)
            if len(obs_icefind) > 0:
                hitrate_obs.append((len(overlap_obs)/len(obs_icefind))*100)
            else:
                hitrate_obs.append(np.nan)

            # Pearson's R where either model or observations have ice
            if np.nansum(~isnan(icecover_m_mask2)) > 5 and np.nansum(
                    ~isnan(icecover_o_mask2)) > 5:
                r_value1 = stats.pearsonr(obs_flat,mod_flat)[0]

                r_all.append(np.around(r_value1, decimals=3))
            else:
                r_all.append(np.nan)

            # RMSE all pixels
            if np.nansum(~isnan(icecover_m_mask)) > 5 and np.nansum(
                    ~isnan(icecover_o_mask)) > 5:
                rmse_all.append(np.sqrt(np.nanmean((
                    icecover_m_mask-icecover_o_mask)**2)))
            else:
                rmse_all.append(np.nan)

            # RMSE ice where either model or observations
            if np.nansum(~isnan(icecover_m_mask2)) > 5 and np.nansum(
                    ~isnan(icecover_o_mask2)) > 5:
                rmse_either.append(np.sqrt(np.nanmean((
                    icecover_m_mask2-icecover_o_mask2)**2)))
            else:
                rmse_either.append(np.nan)

            # RMSE overlapping pixels
            if index_overlap.size>5:
                rmse_overlap.append(np.sqrt(np.nanmean((
                    mod_overlapvalues-obs_overlapvalues)**2)))
            else:
                rmse_overlap.append(np.nan)

            # Skill score (SS) from Hebert et al. (2015)
            # DOI: 10.1002/2015JC011283
            # Do it in 2D
            if np.nansum(~isnan(icecover_m_mask)) > 5 and np.nansum(
                    ~isnan(icecover_o_mask)) > 5:
                mse2_fO = rmse_all[i]**2
                mse2_fC = np.nanmean((
                    icecover_m_mask-icecover_hist_2d[i,:,:])**2)
                if mse2_fC > 0:
                    skillscore = 1 - (mse2_fO/mse2_fC)
                else:
                    skillscore = np.nan
                SS.append(skillscore)
            else:
                SS.append(np.nan)

            # Daily ice extent & total ice days
            # Do obs
            obs_extent_map = np.array(icecover_o_mask)
            obs_extent_map[obs_extent_map < threshold_exte] = 0
            obs_extent_map[obs_extent_map >= threshold_exte] = 1
            obs_extent_map_all.append(obs_extent_map)
            # Do mod
            mod_extent_map = np.array(icecover_m_mask)
            mod_extent_map[mod_extent_map < threshold_exte] = 0
            mod_extent_map[mod_extent_map >= threshold_exte] = 1
            mod_extent_map_all.append(mod_extent_map)
            # Do extent overlap (hits), misses, and false alarms
            overlap_map = np.array(mod_extent_map + obs_extent_map)
            overlap_map[overlap_map <= 1] = 0
            overlap_map[overlap_map == 2] = 1
            overlap_map_all.append(overlap_map)
            csi_map = np.array(mod_extent_map - obs_extent_map)
            falarm_map = np.array(csi_map)
            falarm_map[falarm_map != 1] = 0
            falarm_map_all.append(falarm_map)
            miss_map = np.array(csi_map)
            miss_map[miss_map != -1] = 0
            miss_map = miss_map * -1
            miss_map_all.append(miss_map)
            # Do CSI
            #hits: cm[1][1]
            #false alarms: cm[0][1]
            #misses: cm[1][0]
            try:
                cm = confusion_matrix(
                    obs_extent_map[~isnan(obs_extent_map)].flatten(),
                    mod_extent_map[~isnan(mod_extent_map)].flatten(),
                    labels=[0,1])
                if (cm[1][1] + cm[0][1] + cm[1][0]) > 0:
                    csi = cm[1][1]/(cm[1][1] + cm[0][1] + cm[1][0])
                else:
                    csi = 0
                csi_all.append(csi)
            except ValueError:
                csi_all.append(np.nan)

            # 2D -- diff between obs and mod
            obsmoddiff_single = icecover_m_mask2-\
                icecover_o_mask2
            obsmoddiff_all.append(obsmoddiff_single)
            # Also keep model interpolated and observation arrays
            obs_all.append(icecover_o_mask)
            mod_all.append(icecover_m_mask)

            # Make a map once each day, and save it
            if shouldimakemaps == 'yesplease':
                if ((prop.ice_dt == 'hourly' and
                     time_all[i].hour == 12)
                    or
                    (prop.ice_dt == 'daily' and
                     (time_all_dt[-1]-time_all_dt[i]).days <= dailyplotdays)):
                    # Static maps
                    mapdata = np.stack(
                        (icecover_o_mask2,icecover_m_mask2,
                         obsmoddiff_single))
                    make_ice_map.make_ice_map(prop,lon_o,lat_o,xo,yo,mapdata,
                                              time_all[i],
                                              'daily',logger)

            ###If last time step, do 2D stats and maps and plots etc.
            #    over time period
            ###
            if i == len(time_all)-1 and dayrange >= 5:
                obsmoddiff_all = np.stack(obsmoddiff_all)
                icecover_m_interp_all = np.stack(
                    icecover_m_interp_all)
                obs_all = np.stack(obs_all)
                mod_all = np.stack(mod_all)

                # Average 3D arrays through time to make 2D arrays
                # First make new masks to mask out open water
                # for obs and/or model
                # Ice concentration masks
                noiceobs_mask = np.stack(noiceobs_all)
                noiceobs_mask = np.array(np.nanmean(
                    noiceobs_mask,axis=0))*0
                noicemod_mask = np.stack(noicemod_all)
                noicemod_mask = np.array(np.nanmean(
                    noicemod_mask,axis=0))*0
                openwater_mask = np.stack(openwater_all)
                openwater_mask = np.array(np.nanmean(
                    openwater_mask,axis=0)) #Already multiplied by zero earlier
                # Ice extent masks
                noiceobs_ext_mask = np.stack(noiceobs_ext_all)
                noiceobs_ext_mask = np.array(np.nanmean(
                    noiceobs_ext_mask,axis=0))*0
                noicemod_ext_mask = np.stack(noicemod_ext_all)
                noicemod_ext_mask = np.array(np.nanmean(
                    noicemod_ext_mask,axis=0))*0
                openwater_ext_mask = np.stack(openwater_ext_all)
                openwater_ext_mask = np.array(np.nanmean(
                    openwater_ext_mask,axis=0)) #Already multiplied by zero earlier

                # Now proceed
                obsmoddiff_allmean = np.array(
                    np.nanmean(obsmoddiff_all,axis=0))
                obsmoddiff_allmax = np.array(
                    np.nanmax(obsmoddiff_all,axis=0))
                obsmoddiff_allmin = np.array(
                    np.nanmin(obsmoddiff_all,axis=0))
                obs_allmean = np.array(
                    np.nanmean(obs_all,axis=0)) + openwater_mask
                mod_allmean = np.array(
                    np.nanmean(mod_all,axis=0)) + openwater_mask
                # ice_dist_allmean = np.array(
                #     np.nanmean(ice_dist_all,axis=0))
                # ice_dist_allmean = ice_dist_allmean + openwater_mask

                # Do RMSE through time
                rmse_2d = np.array(np.sqrt(np.nanmean(
                    ((obsmoddiff_all)**2),axis=0)))
                rmse_2d = rmse_2d + openwater_mask

                # Make ice extents & days of ice cover
                #NOTE! numpy nansum returns zeros when
                # summing across nans! So we gotta re-apply
                # masks.
                # Do obs --
                obs_extent_map_allsum = np.array(
                    np.nansum(obs_extent_map_all,axis=0))
                obs_icedays_all = np.array(
                    obs_extent_map_allsum+(noiceobs_ext_mask))
                obs_extent_map_allsum[
                    obs_extent_map_allsum>0] = 1
                obs_extent_map_allsum = np.array(
                    obs_extent_map_allsum+(noiceobs_ext_mask))
                # Do model --
                mod_extent_map_allsum = np.array(
                    np.nansum(mod_extent_map_all,axis=0))
                mod_icedays_all = np.array(
                    mod_extent_map_allsum+(noicemod_ext_mask))
                mod_extent_map_allsum[
                    mod_extent_map_allsum>0] = 1
                mod_extent_map_allsum = np.array(
                    mod_extent_map_allsum+(noicemod_ext_mask))

                # Do Critical Success Index mapping -->
                # First, map hits
                hit_map_allsum = np.array(
                    np.nansum(overlap_map_all,axis=0)/dayrange)*100 +\
                    openwater_ext_mask
                miss_map_allsum = np.array(
                    np.nansum(miss_map_all,axis=0)/dayrange)*100 +\
                    openwater_ext_mask
                falarm_map_allsum = np.array(
                    np.nansum(falarm_map_all,axis=0)/dayrange)*100 +\
                    openwater_ext_mask


                # Find ice-on and ice-off dates
                logger.info("Starting ice onset/thaw date-finding routine...")
                obs_iceon,obs_iceoff = iceonoff(time_all_dt,obs_meanicecover,
                                                logger)
                mod_iceon,mod_iceoff = iceonoff(time_all_dt,mod_meanicecover,
                                                logger)
                clim_iceon,clim_iceoff = iceonoff(time_all_dt,icecover_hist,
                                                  logger)
                logger.info("Completed ice onset/thaw! Back in main.")
                if mod_iceon is not None and obs_iceon is not None:
                    iceondiff = (mod_iceon-obs_iceon).days
                else:
                    iceondiff = None
                if mod_iceoff is not None and obs_iceoff is not None:
                    iceoffdiff = (mod_iceoff-obs_iceoff).days
                else:
                    iceoffdiff = None
                logger.info("Calculated ice onset/thaw error!")
                # Combine ice on/off dates and diff to format for pandas table
                logger.info("Writing ice onset/thaw table...")
                iceonoffall = [
                    [' ','Ice onset','Ice thaw'],
                    ['Observed',str(obs_iceon),str(obs_iceoff)],
                    ['Modeled',str(mod_iceon),str(mod_iceoff)],
                    ['Climatology',str(clim_iceon),str(clim_iceoff)],
                    ['Model-obs difference (days)',str(iceondiff),str(iceoffdiff)]
                ]

                # Write to csv
                title = r"" +\
                    f"{prop.data_skill_stats_path}/skill_{prop.ofs}_iceonoff_{prop.whichcast}.csv"
                with open(title, "w", newline="") as csvfile:
                    writer = csv.writer(csvfile)

                    # Write the header row (column labels)
                    writer.writerow(iceonoffall[0])

                    # Write the data rows
                    for row in iceonoffall[1:]:
                        writer.writerow(row)

                logger.info("Ice on/off table saved!")

                # Do 2D stats maps
                if shouldimakemaps == 'yesplease':
                    logger.info("Starting all stats maps...")
                    # Map conc: mean model, mean obs, and rmse
                    mapdata = np.stack(
                        (obs_allmean,
                         mod_allmean,
                         rmse_2d))
                    make_ice_map.make_ice_map(
                        prop,lon_o,lat_o,xo,yo,mapdata,
                        time_all[i],
                        'rmse means',logger)
                    # Map extent: total ice days model,
                    # ice days obs, ice distance
                    mapdata = np.stack(
                        (obs_icedays_all,
                         mod_icedays_all))
                    make_ice_map.make_ice_map(
                        prop,lon_o,lat_o,xo,yo,mapdata,
                        time_all[i],
                        'extents',logger)
                    # Map diff conc: mean diff, max diff,
                    # min diff
                    mapdata = np.stack(
                        (obsmoddiff_allmean,
                         obsmoddiff_allmax,
                         obsmoddiff_allmin))
                    make_ice_map.make_ice_map(
                        prop,lon_o,lat_o,xo,yo,mapdata,
                        time_all[i],
                        'diff',logger)
                    # Map CSI metrics
                    mapdata = np.stack(
                        (hit_map_allsum,
                         falarm_map_allsum,
                         miss_map_allsum))
                    make_ice_map.make_ice_map(
                        prop,lon_o,lat_o,xo,yo,mapdata,
                        time_all[i],
                        'csi',logger)
                    logger.info("All stats maps complete!")
                if shouldimakeplots == 'yesplease':
                    #logger.info("Starting histograms...")
                    # ---HISTOGRAMS/PDFs-----------------------------------
                    # Make distributions of errors
                    ## Do all RMSEs
                    make_ice_boxplots.make_ice_boxplots(obs_all,mod_all,
                                                        time_all_dt,prop,
                                                        logger)
                    plt.close('all')
                    logger.info("Box plots complete!")
            elif i == len(time_all)-1 and dayrange < 5:
                logger.info("Day range is < 5, so no maps or cumulative stats!")

        ##Before plotting, make pandas dataframe with stats time series
        logger.info("Compiling time series stats for table output...")
        pd.DataFrame(
            {
                "time_all_dt": time_all_dt,
                "obs_meanicecover": obs_meanicecover,
                "mod_meanicecover": mod_meanicecover,
                "obs_stdmic": obs_stdmic,
                "mod_stdmic": mod_stdmic,
                "icecover_hist": icecover_hist,
                "SS": SS,
                "rmse_all": rmse_all,
                "rmse_either": rmse_either,
                "rmse_overlap": rmse_overlap,
                "hitrate_mod": hitrate,
                "hitrate_obs": hitrate_obs,
                "obs_extent": obs_extent,
                "mod_extent": mod_extent,
                "r_all": r_all,
                #"r_overlap": r_overlap,
                "csi_all": csi_all
            }
        ).to_csv(
            r"" + f"{prop.data_skill_stats_path}/"
                  f"skill_{prop.ofs}_"
            f"icestatstseries_{prop.whichcast}.csv"
        )

        logger.info(
            "Time series of OFS-wide skill stats is created successfully."
        )

        #Switch date back to user-inputted start date if doing a season run
        if seasonrun == 'yes' and prop.whichcast == 'forecast_b':
            prop.start_date_full = prop.oldstartdate

    if shouldimakeplots == 'yesplease':
        if dayrange >= 5:
            ##Send 1D stats, model, and obs time series to plotting module
            create_1dplot_ice.create_1dplot_icestats(prop,\
                                                 time_all,\
                                                 logger)
            create_1dplot_ice.create_1dplot_ice(prop, inventory,\
                                                 time_all,\
                                                 logger)
        else:
            logger.info("Need >=5 days for stats & 1D time series plots! "
                        "No 1D plots made.")

    logger.info("Program complete! Go get coffee and bagel")

# Execution:
if __name__ == "__main__":
    # Arguments:
    # Parse (optional and required) command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-o", "--ofs", required=True, help="""Choose from the list on the
        ofs_extents/ folder, you can also create your own shapefile,
        add it at the ofs_extents/ folder and call it here""", )
    parser.add_argument(
        "-p", "--path", required=True,
        help="Inventory File path where ofs_extents/ folder is located", )
    parser.add_argument(
        "-s", "--StartDate_full", required=True,
        help="Start Date_full YYYY-MM-DDThh:mm:ssZ e.g.'2023-01-01T12:34:00Z'")
    parser.add_argument(
        "-e", "--EndDate_full", required=True,
        help="End Date_full YYYY-MM-DDThh:mm:ssZ e.g. '2023-01-01T12:34:00Z'")
    parser.add_argument(
        "-ws", "--Whichcasts", required=True,
        help="whichcasts: 'Nowcast', 'Forecast_A', 'Forecast_B'", )
    parser.add_argument(
        "-pt", "--PlotType", required=False,
        help="Plot type: 'static' or 'interactive'", )
    args = parser.parse_args()

    prop1 = model_properties.ModelProperties()
    prop1.ofs = args.ofs.lower()
    prop1.path = args.path
    prop1.start_date_full = args.StartDate_full
    prop1.end_date_full = args.EndDate_full
    prop1.whichcasts = args.Whichcasts


    ##Do forecast_a to assess a single forecast cycle
    if 'forecast_a' in prop1.whichcasts:
        if args.Forecast_Hr is None:
            print('No forecast cycle input -- defaulting to 00Z')
            prop1.forecast_hr = '00hr'
        elif args.FileType is not None:
            prop1.forecast_hr = args.Forecast_Hr

    ##Make interactive plots default, unless user specifies static
    if args.PlotType is None:
        prop1.plottype = 'interactive'
    elif args.PlotType is not None:
        prop1.plottype = args.PlotType.lower()

    do_iceskill(prop1, None)
