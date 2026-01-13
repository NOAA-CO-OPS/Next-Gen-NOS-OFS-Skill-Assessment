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

import json
import os
import sys
from datetime import datetime

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from ofs_skill.skill_assessment import make_2d_skill_maps, metrics_two_d

# Lazy import to avoid pyinterp dependency issues on Windows
# from ofs_skill.visualization.processing_2d import write_2d_arrays_to_json


# Utility functions for 2D plotting
def write_2dskill_csv(prop1, stats, time_all, logger):
    '''Put stats into pandas dataframe, and write it to csv!
    [obs_mean, obs_std, mod_mean, mod_std, modobs_bias, modobs_bias_std,
                r_value, rmse, cf, pof, nof]

    '''
    # Pandas, go!
    ## Might need to reformat dates first
    # Make time array
    date_all = []
    for i in range(0, len(time_all)):
        date_all.append(datetime.strptime(time_all[i], '%Y%m%d-%Hz'))

    variable = 'temperature'
    stats = np.round(stats, decimals=2)
    pd.DataFrame(
        {
            'Date': date_all,
            'Obs mean': list(zip(*stats))[0],
            'Obs stdev': list(zip(*stats))[1],
            'Model mean': list(zip(*stats))[2],
            'Model stdev': list(zip(*stats))[3],
            'Bias': list(zip(*stats))[4],
            'Bias stdev': list(zip(*stats))[5],
            'R': list(zip(*stats))[6],
            'RMSE': list(zip(*stats))[7],
            'Central frequency (%)': list(zip(*stats))[8],
            'Negative outlier freq (%)': list(zip(*stats))[9],
            'Positive outlier freq (%)': list(zip(*stats))[10],
        }
    ).to_csv(
        r'' + f'{prop1.data_skill_2d_table_path}/'
              f'skill_2d_{prop1.ofs}_'
        f'{variable}_{prop1.whichcast}.csv'
    )

    logger.info(
        '2D summary skill table for %s and variable %s '
        'is created successfully',
        prop1.ofs,
        variable,
    )
    logger.info('Program complete!')


def get_intersection(list1, list2):
    '''this little guy gets the intersecting values & indices from list1
        compared to list2, and sorts them by date. This is used to make sure
        the obs and model data are paired correctly.
    '''
    # Get intersection and indices of intersecting values
    ind_dict = {k: i for i, k in enumerate(list1)}
    inter_values = set(ind_dict).intersection(list2)
    indices = [ind_dict[x] for x in inter_values]
    # Zip values and indices together for sorting
    tupfiles = tuple(zip(indices, inter_values))
    # Sort by date
    tupfiles = tuple(sorted(tupfiles, key=lambda x: (x[0])))
    # Unzip, get sorted values & index lists back
    inter_values_sort = list(zip(*tupfiles))[1]
    inter_values_sort = list(inter_values_sort)
    indices_sort = list(zip(*tupfiles))[0]
    indices_sort = list(indices_sort)
    # Give 'em back
    return indices_sort, inter_values_sort


def list_of_json_files(filepath, prop1, logger):
    '''Peek in JSON dirs and return sorted list of files'''
    all_files = os.listdir(filepath)
    if len(all_files) == 0:
        logger.error('The JSON directory (%s) is totally empty!', filepath)
        sys.exit(-1)
    spltstr = []
    files = []
    for af_name in all_files:
        if 'model' in af_name and 'daily' not in af_name and 'SPoRT' not in af_name and 'ssh' not in af_name and 'sss' not in af_name and 'ssu' not in af_name and 'ssv' not in af_name:  # ignore daily avg and ssh, sss, ssu, and ssv
            if ((datetime.strptime(af_name.split('_')[1], '%Y%m%d-%Hz') >=
                  datetime.strptime(prop1.start_date_full, '%Y%m%d-%H:%M:%S'))
                and (datetime.strptime(af_name.split('_')[1], '%Y%m%d-%Hz') <=
                      datetime.strptime(prop1.end_date_full, '%Y%m%d-%H:%M:%S'))
                and af_name.split('_')[0] == prop1.ofs
                and prop1.whichcast in af_name.split('.')[-2]):
                spltstr.append(af_name.split('_')[1])  # Date info for sorting
                files.append(filepath + '/' + af_name)  # Full file path
        elif 'model' not in af_name and 'daily' not in af_name and 'lnc' not in af_name:  # ignore daily avg
            if ((datetime.strptime(af_name.split('_')[1], '%Y%m%d-%Hz') >=
                  datetime.strptime(prop1.start_date_full, '%Y%m%d-%H:%M:%S'))
                and (datetime.strptime(af_name.split('_')[1], '%Y%m%d-%Hz') <=
                      datetime.strptime(prop1.end_date_full, '%Y%m%d-%H:%M:%S'))
                and af_name.split('_')[0] == prop1.ofs):
                spltstr.append(af_name.split('_')[1])  # Date info for sorting
                files.append(filepath + '/' + af_name)  # Full file path
    try:
        files[0]
    except IndexError:
        logger.error('No JSON files found in directory %s! Exiting.',
                     filepath)
        sys.exit(-1)

    # Sort file list
    tupfiles = tuple(zip(spltstr, files))
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


def json_to_numpy(files, logger):
    '''Takes sorted file list of JSON files and converts to numpy.
    Needs to load files in correct (sorted) chronological order!!! Which is
    handled by function list_of_json_files'''
    z_all = []
    x = None
    y = None
    for index, value in enumerate(files):
        with open(value) as file:
            jsondata = json.load(file)
        if index == 0:
            x = np.array(jsondata['lons'], dtype=float)
            y = np.array(jsondata['lats'], dtype=float)
        z = np.array(jsondata['sst'], dtype=float)
        z_all.append(z)
    try:
        z_all = np.stack(z_all)
    except ValueError:
        logger.error("Can't stack arrays with different shapes!")
        sys.exit(-1)

    return x, y, z_all


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
    subplot_titles=('Model and Observation means', 'Model-observation bias',
                    'RMSE', 'Frequency statistics'),
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
    fig.add_hline(y=90,row=4,col=1,line_width=0.5,line_dash='dash',
                  line_color='black')
    fig.add_hline(y=1,row=4,col=1,line_width=0.5,line_dash='dash',
                  line_color='black')
    fig.update_yaxes(title_text='SST (\u00b0C)',
                     title_font=dict(size=16, color='black'),
                     #range=[min(), 1],
                     row=1, col=1)
    fig.update_yaxes(title_text='RMSE (\u00b0C)',
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
    fig.update_yaxes(title_text='SST (\u00b0C)',
                     title_font=dict(size=16, color='black'),
                     # range=[-max(abs(df['Bias mean']+df['Bias stdev'])),
                     #        max(abs(df['Bias mean']+df['Bias stdev']))],
                     range = [-y_lim,y_lim],
                     row=2, col=1)
    fig.update_yaxes(title_text='Frequency statistics (%)',
                     title_font=dict(size=16, color='black'),
                     range=[0,100],
                     row=4, col=1)
    fig.update_xaxes(range=[df['Date'].iloc[0],df['Date'].iloc[-1]])

    # make title dates
    datestrend = (prop1.end_date_full).split('T')[0]
    datestrbeg = (prop1.start_date_full).split('T')[0]
    # Do title and cosmetic thingies
    figheight=700
    figwidth=900
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
        transition_ordering='traces first', dragmode='zoom',
        hovermode='x unified', height=figheight, width=figwidth,
        template='plotly_white', margin=dict(
            t=50, b=50), legend=dict(
            font=dict(size=16, color='black'),
            bgcolor = 'rgba(0,0,0,0)',
            orientation='h', yanchor='top',
            y=0.98, xanchor='left', x=0.0))

    fig.update_xaxes(showline=True, linewidth=1, linecolor='black', mirror=True)
    fig.update_yaxes(showline=True, linewidth=1, linecolor='black', mirror=True)
    #savepath = os.path.join(prop1.visuals_2d_station_path, str(prop1.ofs +\
    #                        '_' + prop1.whichcast + '_1D_stat_series.html'))

    naming_ws = '_'.join(prop1.whichcasts)
    output_file = (
        f'{prop1.visuals_2d_station_path}/{prop1.ofs}_'
        f'{naming_ws}_1D_stat_series'
        )
    fig_config = {
    'toImageButtonOptions': {
        'format': 'png',
        'filename': output_file.split('/')[-1],
        'height': figheight,
        'width': figwidth,
        'scale': 1
        }
    }
    logger.debug(f'Writing file: {output_file}')
    fig.write_html(output_file+'.html',config=fig_config)
    logger.debug(f'Finished writing file: {output_file}')


def plot_2d(prop1,logger):
    """
    this big 'ol function takes the ofs and satellite data, does stats,
    saves maps to JSON format, and saves 1D time series to plots.
    """
    # Lazy import to avoid pyinterp dependency issues at module load time
    from ofs_skill.visualization.processing_2d import write_2d_arrays_to_json

    # Should we make plotly maps for offline viewing? True or False
    make_plotly_maps = False
    logger.info('Make plotly express maps? %s.', make_plotly_maps)

    #
    #First get sorted list of JSON files and dates within the input date range
    #
    logger.info('Fetching list of JSON files for satellite...')
    sat_files, sat_dates = list_of_json_files(
        prop1.data_observations_2d_json_path,prop1,logger
        )
    logger.info('Fetching list of JSON files for model...')
    mod_files, mod_dates = list_of_json_files(
        prop1.data_model_2d_json_path,prop1,logger
        )

    """
    Parse l3c files from SPoRT files
    """
    sat_files_l3c=[]
    sat_files_SPo=[]
    sat_dates_l3c=[]
    sat_dates_SPo=[]
    for i, f in enumerate(sat_files):
        if 'SPo' in f:
            sat_files_SPo.append(f)
            sat_dates_SPo.append(sat_dates[i])
        elif 'l3c' in f:
            sat_files_l3c.append(f)
            sat_dates_l3c.append(sat_dates[i])

    """
    Run through this routine for l3c, then SPoRT
    """
    sat_list = []
    if prop1.l3c:
        sat_list.append('l3c')
    if prop1.sport:
        sat_list.append('SPo')
    try:
        sat_list[0]
    except IndexError:
        logger.error('No satellite data available for '
                     'stats! Exiting.')
        sys.exit(-1)
    for sat_source in sat_list:
        sat_files=None
        sat_dates=None
        if sat_source=='SPo':
            sat_dates=sat_dates_SPo
            sat_files=sat_files_SPo
            mod_dates_SPo = []
            mod_files_SPo = []
            #clunky search for model files matching sport times
            for i, sd in enumerate(sat_dates):
                for ii, md in enumerate(mod_dates):
                    if sd in md:
                        mod_dates_SPo.append(md)
                        mod_files_SPo.append(mod_files[ii])
            mod_dates=mod_dates_SPo
            mod_files=mod_files_SPo
        elif sat_source=='l3c':
            sat_dates=sat_dates_l3c
            sat_files=sat_files_l3c
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
        logger.info('Converting JSON datasets to 3D numpy arrays')
        x_sat,y_sat,z_sat = json_to_numpy(sat_files,logger)
        _,_,z_mod = json_to_numpy(mod_files,logger)

        # Check if sat array has data in it -- if not, exit because no stats
        # can be calculated.
        is_it_nans = np.all(np.isnan(z_sat))
        if is_it_nans:
            logger.error('Satellite data is entirely NaNs for each time step! '
                         'No stats can be calculated.')
            logger.info('Even though satellite data is blank, we can still '
                        'make plotly express maps of the model data.')
            make_2d_skill_maps.make_2d_skill_maps\
                (z_mod,y_sat,x_sat,sat_dates,'mod',sat_source,prop1,logger)
            return

        #Get time steps for looping, if model and satellite shapes are the same
        if ((z_mod.shape == z_sat.shape)
            and (z_mod.shape[0] == len(mod_dates))
            and (z_sat.shape[0] == len(sat_dates))):
            time_steps = len(sat_dates)
        else:
            logger.info('Error -- satellite and model arrays are different shapes!')
            sys.exit(-1)

        if len(sat_dates) > 2: #skip if not enough time steps
            #Loop & do stats
            stats1d_all = []
            for k in range(time_steps):
                logger.info('Main time loop: %s percent complete',
                            round((((k+1)/(len(sat_dates)))*100),2))
                try:
                    stats1d = metrics_two_d.return_one_d(z_sat[k,:,:],z_mod[k,:,:],logger)
                except:
                    stats1d=None
                stats1d_all.append(stats1d)
                diff = z_mod[k,:,:] - z_sat[k,:,:]
                #Write 2D diff for each time step to JSON file
                out_file = os.path.join(prop1.data_skill_2d_json_path,
                                        str(prop1.ofs + '_' + prop1.whichcast + '_' +
                                        sat_dates[k] + '_' + sat_source +
                                        '_diff_stats.json'))
                write_2d_arrays_to_json(y_sat,x_sat,diff,out_file)

            # Make 2D arrays from 3D arrays by aggregating long time axis for stats
            all_stats = metrics_two_d.return_two_d(z_sat,z_mod,logger)

            # Write 2D stats calculated over time period to JSON files & plotly maps
            statlist = ['rmse','diffmean','diffmax','diffmin','diffstd','cf','pof',
                        'nof']
            for statname,stat in zip(statlist,all_stats):
                out_file = os.path.join(prop1.data_skill_2d_json_path, str(prop1.ofs +'_' +
                                        prop1.whichcast + '_' + sat_dates[0] + '--' +
                                        sat_dates[-1] + '_' + statname + '_' + sat_source +
                                        '_stats.json'))
                write_2d_arrays_to_json(y_sat,x_sat,stat,out_file)
                if make_plotly_maps:
                    make_2d_skill_maps.make_2d_skill_maps\
                        (stat,y_sat,x_sat,sat_dates,statname,sat_source,prop1,logger)
            # Finally write the time slider maps to file
            if make_plotly_maps:
                make_2d_skill_maps.make_2d_skill_maps\
                    (z_sat,y_sat,x_sat,sat_dates,'obs',sat_source,prop1,logger)
                make_2d_skill_maps.make_2d_skill_maps\
                    (z_mod,y_sat,x_sat,sat_dates,'mod',sat_source,prop1,logger)
                diff_all = np.array(z_mod-z_sat)
                make_2d_skill_maps.make_2d_skill_maps\
                    (diff_all,y_sat,x_sat,sat_dates,'diffall',sat_source,prop1,logger)

            #Do some plotting of 1D stats averaged across 2D domains
            #plot_2dstats(stats1d_all, sat_dates, prop1, logger)

            #Write 2D skill csv
            try:
                write_2dskill_csv(prop1,stats1d_all,sat_dates,logger)
            except:
                logger.error('Problem writting 2D skill csv.')
        else:
            logger.error('Only %s %s satellite times available, skipping statistics.',
                         len(sat_dates), sat_source)
