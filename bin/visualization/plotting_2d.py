"""
-*- coding: utf-8 -*-

Documentation for plotting_functions.py

Script Name: plotting_2d.py

Technical Contact(s): Name: AJK

Abstract:
   This module contains 2d plotting functions used in the skill assessment
   routine, called by create_2dplots.py.

Language:  Python 3.8+

Estimated Execution Time:

Usage:
    Called by create_2dplots.py

Author Name:  AJK

Revisions:
Date          Author     Description
"""

import os
import sys
from datetime import datetime
import pandas as pd
import numpy as np
from plotly.subplots import make_subplots
import plotly.graph_objects as go

from .create_2dplot import list_of_json_files, get_intersection, \
    json_to_numpy, write_2dskill_csv
from .processing_2d import write_2d_arrays_to_json
from skill_assessment import make_2d_skill_maps, metrics_two_d

def plot_2dstats(stats1d_all, time_all, prop1, logger):
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
    datestrend = (prop1.end_date_full).split('T')[0]
    datestrbeg = (prop1.start_date_full).split('T')[0]
    # Do title and cosmetic thingies
    fig.update_layout(
        title=dict(
             #text='CBOFS nowcast sea surface temp, 5/29/24 - 5/30/24',
             text=str(prop1.ofs.upper() + ' ' + prop1.whichcast + ' ' +\
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

    #Loop & do stats
    stats1d_all = []
    for k in range(time_steps):
        logger.info("Main time loop: %s percent complete",
                    round((((k+1)/(len(sat_dates)))*100),2))
        stats1d = metrics_two_d.return_one_d(z_sat[k,:,:],z_mod[k,:,:],logger)
        stats1d_all.append(stats1d)
        diff = z_mod[k,:,:] - z_sat[k,:,:]
        #Write 2D diff for each time step to JSON file
        out_file = os.path.join(prop1.data_skill_2d_json_path,
                                str(prop1.ofs + '_' + prop1.whichcast + '_' +
                                sat_dates[k] + '_diff_stats.json'))
        write_2d_arrays_to_json(y_sat,x_sat,diff,out_file)

    # Make 2D arrays from 3D arrays by aggregating long time axis for stats
    all_stats = metrics_two_d.return_two_d(z_sat,z_mod,logger)

    # Write 2D stats calculated over time period to JSON files & plotly maps
    statlist = ['rmse','diffmean','diffmax','diffmin','diffstd','cf','pof',
                'nof']
    for statname,stat in zip(statlist,all_stats):
        out_file = os.path.join(prop1.data_skill_2d_json_path, str(prop1.ofs +\
                                '_' + prop1.whichcast + '_' + sat_dates[0] +\
                                '--' + sat_dates[-1] + '_' + statname + \
                                    '_stats.json'))
        write_2d_arrays_to_json(y_sat,x_sat,stat,out_file)
        if make_plotly_maps:
            make_2d_skill_maps.make_2d_skill_maps\
                (stat,y_sat,x_sat,sat_dates,statname,prop1,logger)
    # Finally write the time slider maps to file
    if make_plotly_maps:
        make_2d_skill_maps.make_2d_skill_maps\
            (z_sat,y_sat,x_sat,sat_dates,'obs',prop1,logger)
        make_2d_skill_maps.make_2d_skill_maps\
            (z_mod,y_sat,x_sat,sat_dates,'mod',prop1,logger)
        diff_all = np.array(z_mod-z_sat)
        make_2d_skill_maps.make_2d_skill_maps\
            (diff_all,y_sat,x_sat,sat_dates,'diffall',prop1,logger)

    #Do some plotting of 1D stats averaged across 2D domains
    #plot_2dstats(stats1d_all, sat_dates, prop1, logger)

    #Write 2D skill csv
    write_2dskill_csv(prop1,stats1d_all,sat_dates,logger)
