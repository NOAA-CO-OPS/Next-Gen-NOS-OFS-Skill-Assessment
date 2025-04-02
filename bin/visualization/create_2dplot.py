"""
-*- coding: utf-8 -*-

Documentation for Scripts create_2dplot.py

Directory Location:   /path/to/ofs_dps/server/bin/visualization

Technical Contact(s): Name:  AJK & PWL

This is the main script of the 2d visualizations module.

Language:  Python 3.11

Estimated Execution Time: <5 min

usage: python bin/visualization/create_2dplot.py -s 2023-11-29T00:00:00Z -e
2023-11-30T00:00:00Z -p ./ -o cbofs -cs
/path/to/ofs_dps/data/observations/2d_satellite/cbofs.nc -ws Forecast_b

Arguments:
  -h, --help            show this help message and exit
  -o OFS, --ofs OFS     Choose from the list on the ofs_Extents folder, you
                        can also create your own shapefile, add it top the
                        ofs_Extents folder and call it here
  -p PATH, --path PATH  Path to home
  -s STARTDATE_FULL, --StartDate_full STARTDATE_FULL
                        Start Date_full YYYYMMDD-hh:mm:ss e.g.
                        '20220115-05:05:05'
  -e ENDDATE_FULL, --EndDate_full ENDDATE_FULL
                        End Date_full YYYYMMDD-hh:mm:ss e.g.
                        '20230808-05:05:05'
  -cs ConcatSat,   --ConcatSatellite
                        This is the path to the concatenated Satellite file
                        i.e. the output from the 2D observations module
  -ws whichcast, --Whichcast
                       'Nowcast', 'Forecast_A', 'Forecast_B'

Author Name:  FC       Creation Date:  03/19/2024

Revisions:
    Date          Author             Description
    09/2024       AJK                Added leaflet contour plot output
    11/2024       PWL                Added 2D stats module(s), new plotting,
                                     new stats table
    03/2025       AJK                Updates for intake

"""
import sys
import argparse
import logging
import logging.config
import os
from pathlib import Path
from datetime import datetime
import json

import pandas as pd
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import numpy as np
# Add parent directory to sys.path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from obs_retrieval import utils
from model_processing import model_source
from model_processing import model_properties
from model_processing import list_of_files
from model_processing import intake_scisa
from skill_assessment import metrics_two_d
from skill_assessment import make_2d_skill_maps
from visualization import processing_2d

def param_val(prop1):
    '''
    This function validates the inputs and creates
    the missing directories
    '''
    logger = None
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

        # Creater logger
        logging.config.fileConfig(log_config_file)
        logger = logging.getLogger("root")
        logger.info("Using config %s",config_file)
        logger.info("Using log config %s",log_config_file)

    logger.info("--- Starting Visualization Process ---")

    dir_params = utils.Utils().read_config_section("directories",logger)

    # Start Date and End Date validation
    try:
        datetime.strptime(prop1.start_date_full,"%Y-%m-%dT%H:%M:%SZ")
        datetime.strptime(prop1.end_date_full,"%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        error_message = (f"Please check Start Date - "
                         f"{prop1.start_date_full}, End Date - "
                         f"{prop1.end_date_full}. Abort!")
        logger.error(error_message)
        print(error_message)
        sys.exit(-1)

    if datetime.strptime(
            prop1.start_date_full,"%Y-%m-%dT%H:%M:%SZ") > datetime.strptime(
        prop1.end_date_full,"%Y-%m-%dT%H:%M:%SZ"):
        error_message = (f"End Date {prop1.end_date_full} "
                         f"is before Start Date {prop1.end_date_full}. Abort!")
        logger.error(error_message)
        sys.exit(-1)

    if prop1.path is None:
        prop1.path = dir_params["home"]

    # prop.path validation
    ofs_extents_path = os.path.join(
        prop1.path,dir_params["ofs_extents_dir"])
    if not os.path.exists(ofs_extents_path):
        error_message = (f"ofs_extents/ folder is not found. "
                         f"Please check prop.path - {prop1.path}. Abort!")
        logger.error(error_message)
        sys.exit(-1)

    # prop.ofs validation
    shape_file = f"{ofs_extents_path}/{prop1.ofs}.shp"
    if not os.path.isfile(shape_file):
        error_message = (f"Shapefile {prop1.ofs} is not found at "
                         f"the folder {ofs_extents_path}. Abort!")
        logger.error(error_message)
        sys.exit(-1)
    # 2D satellite path
    prop1.data_observations_2d_satellite_path = os.path.join(
        prop1.path,dir_params["data_dir"],dir_params["observations_dir"],
        dir_params["2d_satellite_dir"],)
    os.makedirs(prop1.data_observations_2d_satellite_path,exist_ok = True)
    # 2D maps 'n plots path
    prop1.visuals_2d_station_path = os.path.join(
        prop1.path,dir_params["data_dir"],dir_params["visual_dir"],)
    os.makedirs(prop1.visuals_2d_station_path,exist_ok = True)
    # 2D skill JSON file save path
    prop1.data_skill_2d_json_path = os.path.join(
        prop1.path,
        dir_params["data_dir"],
        dir_params["skill_dir"],
        '2d')
    os.makedirs(prop1.data_skill_2d_json_path,exist_ok = True)
    # 2D satellite JSON file save path
    prop1.data_observations_2d_json_path = os.path.join(
        prop1.path,
        dir_params["data_dir"],
        dir_params["observations_dir"],
        '2d')
    os.makedirs(prop1.data_observations_2d_json_path,exist_ok = True)
    # 2D model JSON file save path
    prop1.data_model_2d_json_path = os.path.join(
        prop1.path,
        dir_params["data_dir"],
        dir_params["model_dir"],
        '2d')
    os.makedirs(prop1.data_model_2d_json_path,exist_ok = True)
    # 2D skill table save path
    prop1.data_skill_2d_table_path = os.path.join(
        prop1.path,
        dir_params["data_dir"],
        dir_params["skill_dir"],
        dir_params["stats_dir"],
        )
    os.makedirs(prop1.data_skill_2d_table_path,exist_ok = True)

    return prop1,logger

def parameter_validation(prop, dir_params, logger):
    """Parameter validation"""
    # Start Date and End Date validation

    try:
        start_dt = datetime.strptime(
            prop.start_date_full, "%Y-%m-%dT%H:%M:%SZ")
        end_dt = datetime.strptime(
            prop.end_date_full, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        error_message = f"Please check Start Date - " \
                        f"'{prop.start_date_full}', End Date -" \
                        f" '{prop.end_date_full}'. Abort!"
        logger.error(error_message)
        print(error_message)
        sys.exit(-1)

    if start_dt > end_dt:
        error_message = f"End Date {prop.end_date_full} " \
                        f"is before Start Date " \
                        f"{prop.start_date_full}. Abort!"
        logger.error(error_message)
        sys.exit(-1)

    if prop.path is None:
        prop.path = dir_params["home"]

    # Path validation
    ofs_extents_path = os.path.join(prop.path, dir_params["ofs_extents_dir"])
    if not os.path.exists(ofs_extents_path):
        error_message = f"ofs_extents/ folder is not found. " \
                        f"Please check Path - " \
                        f"'{prop.path}'. Abort!"
        logger.error(error_message)
        sys.exit(-1)

    # OFS validation
    shapefile = f"{ofs_extents_path}/{prop.ofs}.shp"
    if not os.path.isfile(shapefile):
        error_message = f"Shapefile '{prop.ofs}' " \
                        f"is not found at the folder" \
                        f" {ofs_extents_path}. Abort!"
        logger.error(error_message)
        sys.exit(-1)

    # Whichcast validation
    if (prop.whichcast is not None) and (
        prop.whichcast not in ["nowcast", "forecast_a", "forecast_b"]
    ):
        error_message = f"Please check Whichcast - " \
                        f"'{prop.whichcast}'. Abort!"
        logger.error(error_message)
        sys.exit(-1)

    if prop.whichcast == "forecast_a" and prop.forecast_hr is None:
        error_message = "Forecast_Hr is required if " \
                        "Whichcast is forecast_a. Abort!"
        logger.error(error_message)
        sys.exit(-1)

def write_2dskill_csv(prop1,stats,time_all,logger):
    '''Put stats into pandas dataframe, and write it to csv!
    [obs_mean, obs_std, mod_mean, mod_std, modobs_bias, modobs_bias_std,
                r_value, rmse, cf, pof, nof]

    '''
    # Pandas, go!
    ## Might need to reformat dates first
    # Make time array
    date_all = []
    for i in range(0,len(time_all)):
        date_all.append(datetime.strptime(time_all[i],'%Y%m%d-%Hz'))

    variable='temperature'
    stats = np.round(stats,decimals=2)
    pd.DataFrame(
        {
            "Date": date_all,
            "Obs mean": list(zip(*stats))[0],
            "Obs stdev": list(zip(*stats))[1],
            "Model mean": list(zip(*stats))[2],
            "Model stdev": list(zip(*stats))[3],
            "Bias": list(zip(*stats))[4],
            "Bias stdev": list(zip(*stats))[5],
            "R": list(zip(*stats))[6],
            "RMSE": list(zip(*stats))[7],
            "Central frequency (%)": list(zip(*stats))[8],
            "Negative outlier freq (%)": list(zip(*stats))[9],
            "Positive outlier freq (%)": list(zip(*stats))[10],
        }
    ).to_csv(
        r"" + f"{prop1.data_skill_2d_table_path}/"
              f"skill_2d_{prop1.ofs}_"
        f"{variable}_{prop1.whichcast}.csv"
    )

    logger.info(
        "2D summary skill table for %s and variable %s "
        "is created successfully",
        prop1.ofs,
        variable,
    )
    logger.info("Program complete!")

def get_intersection(list1,list2):
    '''this little guy gets the intersecting values & indices from list1
        compared to list2, and sorts them by date. This is used to make sure
        the obs and model data are paired correctly.
    '''
    # Get intersection and indices of intersecting values
    ind_dict = dict((k,i) for i,k in enumerate(list1))
    inter_values = set(ind_dict).intersection(list2)
    indices = [ind_dict[x] for x in inter_values]
    # Zip values and indices together for sorting
    tupfiles = tuple(zip(indices,inter_values))
    # Sort by date
    tupfiles = tuple(sorted(tupfiles, key=lambda x: (x[0])))
    # Unzip, get sorted values & index lists back
    inter_values_sort = list(zip(*tupfiles))[1]
    inter_values_sort = list(inter_values_sort)
    indices_sort = list(zip(*tupfiles))[0]
    indices_sort = list(indices_sort)
    # Give 'em back
    return indices_sort, inter_values_sort

def list_of_json_files(filepath, prop1):
    '''Peek in JSON dirs and return sorted list of files'''
    all_files = os.listdir(filepath)
    spltstr = []
    files = []
    for af_name in all_files:
        if ((datetime.strptime(af_name.split("_")[1],"%Y%m%d-%Hz") >=
              datetime.strptime(prop1.start_date_full, "%Y%m%d-%H:%M:%S"))
            and (datetime.strptime(af_name.split("_")[1],"%Y%m%d-%Hz") <=
                  datetime.strptime(prop1.end_date_full, "%Y%m%d-%H:%M:%S"))
            and af_name.split("_")[0] == prop1.ofs):
            spltstr.append(af_name.split("_")[1]) # Date info for sorting
            files.append(filepath + "/" + af_name) # Full file path
    # Sort file list
    tupfiles = tuple(zip(spltstr,files))
    # Sort by year, month, day, then hour
    tupfiles = tuple(sorted(tupfiles, key=lambda x: (x[0][-3:-1])))
    tupfiles = tuple(sorted(tupfiles, key=lambda x: (x[0][6:8])))
    tupfiles = tuple(sorted(tupfiles, key=lambda x: (x[0][4:6])))
    tupfiles = tuple(sorted(tupfiles, key=lambda x: (x[0][0:4])))

    # Unzip, get sorted file list back
    spltstr = list(zip(*tupfiles))[0]
    spltstr = list(spltstr)
    files = list(zip(*tupfiles))[1]
    files = list(files)

    return files, spltstr

def json_to_numpy(files):
    '''Takes sorted file list of JSON files and converts to numpy.
    Needs to load files in correct (sorted) chronological order!!! Which is
    handled by function list_of_json_files'''
    z_all = []
    for index,value in enumerate(files):
        with open(value, 'r') as file:
            jsondata = json.load(file)
        if index == 0:
            x = np.array(jsondata['lons'],dtype=float)
            y = np.array(jsondata['lats'],dtype=float)
        z = np.array(jsondata['sst'],dtype=float)
        z_all.append(z)
    try:
        z_all = np.stack(z_all)
    except ValueError:
        logger.error("Can't stack arrays with different shapes!")
        sys.exit(-1)

    return x,y,z_all

def plot_2dstats(stats1d_all, time_all, prop, logger):
    ''' Make plotly plot of OFS-wide stats '''
    # Make pandas dataframe
    df = pd.DataFrame(stats1d_all, columns =
                      ['Observation mean',
                       'Observation stdev',
                       'Model mean',
                       'Model stdev',
                       'Bias mean',
                       'Bias stdev',
                       'R',
                       'RMSE',
                       'CF',
                       'POF',
                       'NOF'
                       ])
    # Make time array
    date_all = []
    for i in range(0,len(time_all)):
        date_all.append(datetime.strptime(time_all[i],'%Y%m%d-%Hz'))

    # Put time in dataframe
    df['Date'] = date_all

    fig = make_subplots(
    rows=4, cols=1, vertical_spacing = 0.055,
    subplot_titles=("Model and Observation means", "Model-observation bias",
                    "RMSE", "Frequency statistics"),
    shared_xaxes=True,
    )

    fig.add_trace(go.Scatter(x=df['Date'], y=df['Observation mean'],
                             name='Observation mean',
                             hovertemplate='%{y:.2f}',
                             mode='lines',
                             line=dict(
                                 color='rgba(0,0,0,1)',
                                 width=2)
                             ), row=1, col=1)
    fig.add_trace(go.Scatter(x=df['Date'], y=df['Observation mean']+
                             df['Observation stdev'],
                             name='Obs +1 sigma',
                             hovertemplate='%{y:.2f}',
                             mode='lines',
                             line=dict(
                                 color='rgba(0,0,0,0.1)',
                                 width=0),
                             showlegend=False
                             ), row=1, col=1)
    fig.add_trace(go.Scatter(x=df['Date'], y=df['Observation mean']-
                             df['Observation stdev'],
                             name='Obs -1 sigma',
                             hovertemplate='%{y:.2f}',
                             #marker=dict(color="#444"),
                             line=dict(width=0),
                             mode='lines',
                             fillcolor='rgba(0,0,0,0.1)',
                             fill='tonexty',
                             showlegend=False
                             ), row=1, col=1)
    fig.add_trace(go.Scatter(x=df['Date'], y=df['Model mean'],
                             name='Model mean',
                             hovertemplate='%{y:.2f}',
                             mode='lines',
                             line=dict(
                                 color='rgba(0,0,255,1)',
                                 width=2)
                             ), row=1, col=1)
    fig.add_trace(go.Scatter(x=df['Date'], y=df['Model mean']+
                             df['Model stdev'],
                             name='Model +1 sigma',
                             hovertemplate='%{y:.2f}',
                             mode='lines',
                             line=dict(
                                 color='rgba(0,0,255,0.1)',
                                 width=0),
                             showlegend=False
                             ), row=1, col=1)
    fig.add_trace(go.Scatter(x=df['Date'], y=df['Model mean']-
                             df['Model stdev'],
                             name='Model -1 sigma',
                             hovertemplate='%{y:.2f}',
                             #marker=dict(color="#444"),
                             line=dict(width=0),
                             mode='lines',
                             fillcolor='rgba(0,0,255,0.1)',
                             fill='tonexty',
                             showlegend=False
                             ), row=1, col=1)

    fig.add_trace(go.Scatter(x=df['Date'], y=df['RMSE'],
                             name='RMSE',
                             hovertemplate='%{y:.2f}',
                             mode='lines',
                             line=dict(
                                 color='darkgreen',
                                 width=2),
                             showlegend=False
                             ), row=3, col=1)

    fig.add_trace(go.Scatter(x=df['Date'], y=df['Bias mean'],
                             name='Bias mean',
                             hovertemplate='%{y:.2f}',
                             mode='lines',
                             line=dict(
                                 color='rgba(255,0,0,1)',
                                 width=2),
                             showlegend=False
                             ), row=2, col=1)
    fig.add_hline(y=0, row=2, col=1)
    fig.add_trace(go.Scatter(x=df['Date'], y=df['Bias mean']+
                             df['Bias stdev'],
                             name='Bias +1 sigma',
                             hovertemplate='%{y:.2f}',
                             mode='lines',
                             line=dict(
                                 color='rgba(255,0,0,0.1)',
                                 width=0),
                             showlegend=False
                             ), row=2, col=1)
    fig.add_trace(go.Scatter(x=df['Date'], y=df['Bias mean']-
                             df['Bias stdev'],
                             name='Bias -1 sigma',
                             hovertemplate='%{y:.2f}',
                             #marker=dict(color="#444"),
                             line=dict(width=0),
                             mode='lines',
                             fillcolor='rgba(255,0,0,0.1)',
                             fill='tonexty',
                             showlegend=False
                             ), row=2, col=1)
    df_filt = df[df['CF']<90]
    fig.add_trace(go.Scatter(x=df['Date'], y=df['CF'],
                             name='Central frequency',
                             hovertemplate='%{y:.2f}',
                             mode='lines',
                             line=dict(
                                 color='rgba(64,224,208,1)',
                                 width=2)    ,
                             showlegend=False
                             ), row=4, col=1)
    fig.add_trace(go.Scatter(x=df_filt['Date'], y=df_filt['CF'],
                             name='Central frequency fail',
                             #hovertemplate='%{y:.2f}',
                             hoverinfo='skip',
                             mode='markers',
                             marker=dict(
                                 color='rgba(255,0,0,0.5)',
                                 size=12,
                                 line=dict(
                                     color='black',
                                     width=0)),
                             showlegend=False,
                             ), row=4, col=1)
    fig.add_trace(go.Scatter(x=df['Date'], y=df['POF'],
                             name='Positive outlier frequency',
                             hovertemplate='%{y:.2f}',
                             mode='lines',
                             line=dict(
                                 color='rgba(13,90,107,1)',
                                 width=2),
                             showlegend=False
                             ), row=4, col=1)
    fig.add_trace(go.Scatter(x=df['Date'], y=df['NOF'],
                             name='Negative outlier frequency',
                             hovertemplate='%{y:.2f}',
                             mode='lines',
                             line=dict(
                                 color='rgba(92,231,175,1)',
                                 width=2),
                             showlegend=False
                             ), row=4, col=1)
    fig.add_hline(y=90,row=4,col=1,line_width=0.5,line_dash="dash",
                  line_color="black")
    fig.add_hline(y=1,row=4,col=1,line_width=0.5,line_dash="dash",
                  line_color="black")
    fig.update_yaxes(title_text="SST (\u00b0C)",
                     title_font=dict(size=16, color='black'),
                     #range=[min(), 1],
                     row=1, col=1)
    fig.update_yaxes(title_text="RMSE (\u00b0C)",
                     title_font=dict(size=16, color='black'),
                     #range=[0, 5],
                     row=3, col=1)
    ##### Set y limits for bias so 0 is in the middle of the plot
    y_lim_down = max(abs(df['Bias mean']-df['Bias stdev']))
    y_lim_up = max(abs(df['Bias mean']+df['Bias stdev']))
    if y_lim_down >= y_lim_up:
        y_lim = y_lim_down
    else:
        y_lim = y_lim_up
    #####
    fig.update_yaxes(title_text="SST (\u00b0C)",
                     title_font=dict(size=16, color='black'),
                     # range=[-max(abs(df['Bias mean']+df['Bias stdev'])),
                     #        max(abs(df['Bias mean']+df['Bias stdev']))],
                     range = [-y_lim,y_lim],
                     row=2, col=1)
    fig.update_yaxes(title_text="Frequency statistics (%)",
                     title_font=dict(size=16, color='black'),
                     range=[0,100],
                     row=4, col=1)
    fig.update_xaxes(range=[df['Date'].iloc[0],df['Date'].iloc[-1]])

    # make title dates
    datestrend = (prop.end_date_full).split('T')[0]
    datestrbeg = (prop.start_date_full).split('T')[0]
    # Do title and cosmetic thingies
    fig.update_layout(
        title=dict(
             #text='CBOFS nowcast sea surface temp, 5/29/24 - 5/30/24',
             text=str(prop1.ofs.upper() + ' ' + prop.whichcast + ' ' +\
                 'Water Temperature 2D Skill Statistics' + ' ' + datestrbeg +\
                     ' - ' + datestrend),
             font=dict(size=20, color='black'),
             y=1,  # new
             x=0.5, xanchor='center', yanchor='top'),
        yaxis = dict(tickfont = dict(size=16)),
        yaxis2 = dict(tickfont = dict(size=16)),
        yaxis3 = dict(tickfont = dict(size=16)),
        yaxis4 = dict(tickfont = dict(size=16)),
        xaxis4 = dict(tickfont = dict(size=16)),
        transition_ordering="traces first", dragmode="zoom",
        hovermode="x unified", height=700, width=900,
        template="plotly_white", margin=dict(
            t=50, b=50), legend=dict(
            font=dict(size=16, color='black'),
            bgcolor = 'rgba(0,0,0,0)',
            orientation="h", yanchor="top",
            y=0.98, xanchor="left", x=0.0))

    fig.update_xaxes(showline=True, linewidth=1, linecolor='black', mirror=True)
    fig.update_yaxes(showline=True, linewidth=1, linecolor='black', mirror=True)
    savepath = os.path.join(prop1.visuals_2d_station_path, str(prop1.ofs +\
                            '_' + prop1.whichcast + '_1D_stat_series.html'))
    fig.write_html(savepath)
        #r"" + f"{prop1.visuals_2d_station_path}/" +\
        #f"{prop1.ofs}_{prop1.whichcast}_1D_stat_series.html")

def plot_2d(prop1,logger):
    """
    this big 'ol function takes the ofs and satellite data, does stats,
    saves maps to JSON format, and saves 1D time series to plots.
    """
    # Should we make plotly maps for offline viewing? True or False
    make_plotly_maps = False
    logger.info("Make plotly express maps? %s.", make_plotly_maps)

    #
    #First get sorted list of JSON files and dates within the input date range
    #
    logger.info("Fetching list of JSON files for satellite...")
    sat_files, sat_dates = list_of_json_files(
        prop1.data_observations_2d_json_path,prop1
        )
    logger.info("Fetching list of JSON files for model...")
    mod_files, mod_dates = list_of_json_files(
        prop1.data_model_2d_json_path,prop1
        )

    #
    #Pair satellite and model files/dates,
    #if not paired already from the previous step
    #
    if list(set(sat_dates).difference(mod_dates)):
        #Oops there must be missing sat or mod data, let's correct it
        # Get satellite indices & dates that intersect model dates
        sat_ind,sat_dates = get_intersection(sat_dates,mod_dates)
        sat_files = [sat_files[i] for i in sat_ind]
        # Get model indices & dates that intersect satellite dates
        mod_ind,mod_dates = get_intersection(mod_dates,sat_dates)
        mod_files = [mod_files[i] for i in mod_ind]
        # Check pairing again to make sure
        if list(set(sat_dates).difference(mod_dates)):
            logger.error('Cannot pair satellite and model data! Abort!')
            sys.exit(-1)

    #
    #Now convert available JSON files to numpy arrays for lat, lon, and z (sst)
    #
    logger.info("Converting JSON datasets to 3D numpy arrays")
    x_sat,y_sat,z_sat = json_to_numpy(sat_files)
    _,_,z_mod = json_to_numpy(mod_files)

    #Get time steps for looping, if model and satellite shapes are the same
    if ((z_mod.shape == z_sat.shape)
        and (z_mod.shape[0] == len(mod_dates))
        and (z_sat.shape[0] == len(sat_dates))):
        time_steps = len(sat_dates)
    else:
        logger.info('Error -- satellite and model arrays are different shapes!')
        sys.exit(-1)

    #
    #Loop & do stats
    #'''
    stats1d_all = []
    for k in range(time_steps):
        logger.info("Main time loop: %s percent complete",
                    ((k+1)/(len(sat_dates)))*100)
        stats1d = metrics_two_d.return_one_d(z_sat[k,:,:],z_mod[k,:,:],logger)
        stats1d_all.append(stats1d)
        diff = z_mod[k,:,:] - z_sat[k,:,:]
        #Write 2D diff for each time step to JSON file
        out_file = os.path.join(prop1.data_skill_2d_json_path,
                                str(prop1.ofs + '_' + prop1.whichcast + '_' +
                                sat_dates[k] + '_diff_stats.json'))
        processing_2d.write_2d_arrays_to_json(y_sat,x_sat,diff,out_file)

    # Make 2D arrays from 3D arrays
    diff_mean,diff_max,rmse,diff_stdev,cf2d,pof2d,nof2d,diff_min =\
        metrics_two_d.return_two_d(z_sat,z_mod,logger)
    #Write 2D stats calculated over time period to JSON files
    out_file = os.path.join(prop1.data_skill_2d_json_path, str(prop1.ofs +
                            '_' + prop1.whichcast + '_' + sat_dates[0] +
                            '--' + sat_dates[-1] + '_rmse_stats.json'))
    processing_2d.write_2d_arrays_to_json(y_sat,x_sat,rmse,out_file)

    out_file = os.path.join(prop1.data_skill_2d_json_path, str(prop1.ofs +
                            '_' + prop1.whichcast + '_' + sat_dates[0] +
                            '--' + sat_dates[-1] + '_diffmean_stats.json'))
    processing_2d.write_2d_arrays_to_json(y_sat,x_sat,diff_mean,out_file)

    out_file = os.path.join(prop1.data_skill_2d_json_path, str(prop1.ofs +
                            '_' + prop1.whichcast + '_' + sat_dates[0] +
                            '--' + sat_dates[-1] + '_diffmax_stats.json'))
    processing_2d.write_2d_arrays_to_json(y_sat,x_sat,diff_max,out_file)

    out_file = os.path.join(prop1.data_skill_2d_json_path, str(prop1.ofs +
                            '_' + prop1.whichcast + '_' + sat_dates[0] +
                            '--' + sat_dates[-1] + '_diffstdev_stats.json'))
    processing_2d.write_2d_arrays_to_json(y_sat,x_sat,diff_stdev,out_file)

    out_file = os.path.join(prop1.data_skill_2d_json_path, str(prop1.ofs +
                            '_' + prop1.whichcast + '_' + sat_dates[0] +
                            '--' + sat_dates[-1] + '_cf2d_stats.json'))
    processing_2d.write_2d_arrays_to_json(y_sat,x_sat,cf2d,out_file)

    out_file = os.path.join(prop1.data_skill_2d_json_path, str(prop1.ofs +
                            '_' + prop1.whichcast + '_' + sat_dates[0] +
                            '--' + sat_dates[-1] + '_pof2d_stats.json'))
    processing_2d.write_2d_arrays_to_json(y_sat,x_sat,pof2d,out_file)

    out_file = os.path.join(prop1.data_skill_2d_json_path, str(prop1.ofs +
                            '_' + prop1.whichcast + '_' + sat_dates[0] +
                            '--' + sat_dates[-1] + '_nof2d_stats.json'))
    processing_2d.write_2d_arrays_to_json(y_sat,x_sat,nof2d,out_file)

    out_file = os.path.join(prop1.data_skill_2d_json_path, str(prop1.ofs +
                            '_' + prop1.whichcast + '_' + sat_dates[0] +
                            '--' + sat_dates[-1] + '_diffmin_stats.json'))
    processing_2d.write_2d_arrays_to_json(y_sat,x_sat,nof2d,out_file)

    #Do some plotting of 1D stats averaged across 2D domains
    plot_2dstats(stats1d_all, sat_dates, prop1, logger)

    #Make Plotly Express maps of daily diff, mean diff, rmse
    if make_plotly_maps:
        diff_all = np.array(z_mod-z_sat)
        make_2d_skill_maps.make_2d_skill_maps\
            (diff_all,y_sat,x_sat,sat_dates,'diffall',prop1,logger)
        make_2d_skill_maps.make_2d_skill_maps\
            (rmse,y_sat,x_sat,sat_dates,'rmse',prop1,logger)
        make_2d_skill_maps.make_2d_skill_maps\
            (diff_mean,y_sat,x_sat,sat_dates,'diffmean',prop1,logger)
        make_2d_skill_maps.make_2d_skill_maps\
            (diff_max,y_sat,x_sat,sat_dates,'diffmax',prop1,logger)
        make_2d_skill_maps.make_2d_skill_maps\
            (diff_min,y_sat,x_sat,sat_dates,'diffmin',prop1,logger)
        make_2d_skill_maps.make_2d_skill_maps\
            (diff_stdev,y_sat,x_sat,sat_dates,'diffstd',prop1,logger)
        make_2d_skill_maps.make_2d_skill_maps\
            (cf2d,y_sat,x_sat,sat_dates,'cf',prop1,logger)
        make_2d_skill_maps.make_2d_skill_maps\
            (pof2d,y_sat,x_sat,sat_dates,'pof',prop1,logger)
        make_2d_skill_maps.make_2d_skill_maps\
            (nof2d,y_sat,x_sat,sat_dates,'nof',prop1,logger)


    #Write 2D skill csv
    write_2dskill_csv(prop1,stats1d_all,sat_dates,logger)


if __name__ == "__main__":

    # Parse (optional and required) command line arguments
    parser = argparse.ArgumentParser(
        prog="python write_obs_ctlfile.py",
        usage="%(prog)s",
        description="ofs write Station Control File",
    )
    parser.add_argument(
        "-o",
        "--ofs",
        required=True,
        help="Choose from the list on the ofs_Extents folder, you can also "
             "create your own shapefile, add it top the ofs_Extents folder and "
             "call it here",
    )
    parser.add_argument("-p", "--path", required=True,
                        help="Path to /opt/ofs_dps")
    parser.add_argument("-s", "--StartDate_full", required=True,
        help="Start Date_full YYYY-MM-DDThh:mm:ssZ e.g. '2023-01-01T12:34:00Z'")
    parser.add_argument("-e", "--EndDate_full", required=True,
        help="End Date_full YYYY-MM-DDThh:mm:ssZ e.g. '2023-01-01T12:34:00Z'")
    parser.add_argument("-cs", "--ConcatSatellite", required=True,
        help="This is the path to the concatenated Satellite data")
    parser.add_argument("-ws", "--whichcast", required=True,
        help="whichcast: 'Nowcast', 'Forecast_A', 'Forecast_B'", )

    args = parser.parse_args()

    prop1 = model_properties.ModelProperties()
    prop1.ofs = args.ofs.lower()
    prop1.path = args.path
    prop1.ofs_extents_path = r"" + prop1.path + "ofs_extents" + "/"
    prop1.start_date_full = args.StartDate_full
    prop1.end_date_full = args.EndDate_full
    prop1.whichcast = args.whichcast.lower()
    prop1.model_source = model_source.model_source(args.ofs)

    ''' Set up paths & assign to prop1, do date validation '''
    prop1,logger = param_val(prop1)[0],param_val(prop1)[-1]

    prop1.ofsfiletype='fields' #hardcoding - 2d always uses fields

    dir_params = utils.Utils().read_config_section("directories", logger)

    parameter_validation(prop1, dir_params, logger)

    prop1.model_path = os.path.join(
        dir_params["model_historical_dir"], prop1.ofs, dir_params["netcdf_dir"])

    prop1.model_path = Path(prop1.model_path).as_posix()
    prop1.start_date_full = prop1.start_date_full.replace("-", "")
    prop1.end_date_full = prop1.end_date_full.replace("-", "")
    prop1.start_date_full = prop1.start_date_full.replace("Z", "")
    prop1.end_date_full = prop1.end_date_full.replace("Z", "")
    prop1.start_date_full = prop1.start_date_full.replace("T", "-")
    prop1.end_date_full = prop1.end_date_full.replace("T", "-")

    prop1.startdate = (datetime.strptime(
                prop1.start_date_full.split("-")[0], "%Y%m%d")).strftime(
                "%Y%m%d") + "00"
    prop1.enddate = (datetime.strptime(
                prop1.end_date_full.split("-")[0], "%Y%m%d")).strftime(
                "%Y%m%d") + "23"

    dir_list = list_of_files.list_of_dir(prop1, logger)
    list_files = list_of_files.list_of_files(prop1, dir_list)
    logger.info('Calling intake_scisa from create_2dplot.')
    model = intake_scisa.intake_model(list_files, prop1, logger)
    logger.info('Returned from call to intake_scisa inside of create_2dplot.')
    processing_2d.parse_leaflet_json(model, args.ConcatSatellite, prop1)

    plot_2d(prop1,logger)
