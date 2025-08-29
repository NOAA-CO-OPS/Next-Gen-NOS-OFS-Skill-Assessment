# -*- coding: utf-8 -*-
"""
Created July 2025

@author: PWL
"""
import sys
import os
from pathlib import Path
import copy
import numpy as np
import pandas as pd
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Add parent directory to sys.path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from visualization import plotting_functions

def make_table(grouped,info,prop,stat):
    '''
    Writes a table to file for a given statistic. Each table row is a station
    ID, and each column is a model cycle. This needs to be updated in a future
    release to include forecast horizon bins as a option for the columns
    instead of model cycles.
    Called by plotting functions below.

    PARAMETERS:
    ----------
    grouped: a grouped pandas dataframe created using 'groupby'
    info: list of station info strings -->
        info[0] = variable
        info[1] = model
        info[2] = station ID
        info[3] = station full name
        info[4] = station provider/owner
    prop: model properties object containing date range, etc.
    stat: a string describing the statistic used in the grouped dataframe, e.g.
    'cf' for central frequency. Used in the file name.
    logger: logging interface.

    RETURNS:
    --------
    NOTHING.
    Writes a table to file.
    '''

    # Get error range
    #X1, X2 = plotting_functions.get_error_range(info[0],prop,logger)
    # Make dataframe from 'groupby' pandas series
    df_grouped = pd.DataFrame(grouped)
    df_grouped = df_grouped.transpose()
    # Add station ID
    df_grouped.insert(loc=0,column='ID',value=info[2])
    # Append to existing file if there is one
    filename = f"{prop.ofs}_{info[0]}_modelcycles_{stat}.csv"
    filepath = os.path.join(prop.data_horizon_1d_pair_path,filename)
    if os.path.isfile(filepath):
        df_file = pd.read_csv(filepath)
        df_grouped = pd.concat([df_file, df_grouped])
        df_grouped.to_csv(filepath,index=False)
    else:
        df_grouped.to_csv(filepath,index=False)

def get_yaxis_label(name_var, logger):
    '''
    Simple function that takes a variable name (wl, salt, temp, cu) and returns
    strings that are then used for figure y-axis labels.
    Called by plotting functions below.
    '''
    if name_var == 'wl':
        label_text = 'Water level'
    elif name_var == 'salt':
        label_text = 'Salinity'
    elif name_var == 'temp':
        label_text = 'Water temperature'
    elif name_var == 'cu':
        label_text = 'Current speed'
    else:
        logger.error("Unknown name_var for labeling y-axis!")
        label_text = 'Unknown'

    # Units
    if name_var == 'wl':
        units = ' (<i>meters<i>)'
    elif name_var == 'temp':
        units = ' (<i>\u00b0C<i>)'
    elif name_var == 'salt':
        units = ' (<i>PSU<i>)'
    elif name_var == 'cu':
        units = ' (<i>m/s<i>)'

    return label_text, units

def make_flag_images(prop,logger):
    '''
    Plotting function that writes a 'scorecard' or 'flag' plot based
    on pass/fail acceptance criteria for central frequency statistics. Each
    flag plot is an i x j matrix, where the rows are station ID and the columns
    are model cycle. The matrix cells are colored red for 'fail', green for
    'pass', and clear for 'no data'.
    Called by get_forecast_hours.merge_obs_series_scalar

    PARAMETERS:
    ----------
    info: list of station info strings -->
        info[0] = variable
        info[1] = model
        info[2] = station ID
        info[3] = station full name
        info[4] = station provider/owner
    prop: model properties object containing date range, etc.
    logger: logging interface.

    RETURNS:
    --------
    NOTHING.
    Writes a plot to file.

    '''
    # Get error range
    #X1, X2 = plotting_functions.get_error_range(info[0],prop,logger)
    # Loop & load all CSVs
    files = os.listdir(prop.data_horizon_1d_pair_path)
    #binary_colorscale = [[0, 'chartreuse'], [1, 'crimson']]
    colorscale = [
    [0, '#e35336'], # dark red
    [0.89999, '#ffcccb'], # light red
    [0.9, '#92ddc8'],  # light green
    [1, '#5aa17f'], # dark green
    ]
    for i in range(len(files)):
        try:
            name_var = files[i].split('_')[1]
            title_var, _ = get_yaxis_label(name_var, logger)
            filepath = os.path.join(prop.data_horizon_1d_pair_path,files[i])
            df = pd.read_csv(filepath)
            col_labels = df.columns[1:]
            col_labels_f = []
            for i in range(len(col_labels)):
                temp = col_labels[i].split('-')[0][4:6] + '/' + \
                    col_labels[i].split('-')[0][6:8] + ' ' + \
                            col_labels[i].split('-')[1][0:2] + ':00'
                col_labels_f.append(temp)
            row_labels = df['ID']
            df = df.set_index('ID')
            # Make data binary
            df_np = df.to_numpy()
            #df_np[df_np<90] = 0
            #df_np[df_np>=90] = 1
            #df_np = np.nan_to_num(df_np, nan=-10)
            if 'T' in prop.start_date_full:
                start_date_title = prop.start_date_full.split('T')[0]
                end_date_title = prop.end_date_full.split('T')[0]
            else:
                start_date_title = prop.start_date_full.split('-')[0]
                start_date_title = start_date_title[0:4] + '-' +\
                    start_date_title[4:6] + '-' + start_date_title[6:]
                end_date_title = prop.end_date_full.split('-')[0]
                end_date_title = end_date_title[0:4] + '-' +\
                    end_date_title[4:6] + '-' + end_date_title[6:]
            titlestr = prop.ofs.upper() + ' ' + title_var.lower() + \
                " central frequency<br>" + start_date_title + ' to ' + \
                    end_date_title
            # Make figure
            fig = px.imshow(df_np,
                y=row_labels,
                x=col_labels_f,
                color_continuous_scale=colorscale,
                labels={"x":'Forecast model cycle', "y":'Station ID'},
                title=titlestr,
                aspect='equal',
                #width=800, height=1000
                )
            # Update layout
            fig.update_layout(
                    paper_bgcolor='white',
                    plot_bgcolor='white',
                    coloraxis={
                        "colorscale":colorscale,
                        "cmin":0,
                        "cmax":100,
                        "colorbar":dict(
                            tickvals= [0,25,50,75,90,100],
                            ticktext=["0%","25%","50%","75%","90%","100%"],
                            outlinecolor="black",
                            outlinewidth=0.75,
                            orientation="h",
                            x=0.5,
                            y=1,
                            xanchor="center",
                            yanchor="bottom",
                            #tickangle=90,
                            )
                        },
                    #coloraxis_colorbar_x=0.85,
                    coloraxis_showscale=True,
                    coloraxis_colorbar_tickfont_color="black",
                    coloraxis_colorbar_tickfont_size=14,
                    #coloraxis_colorbar_title_text = ' %  ',
                    coloraxis_colorbar={
                        "len":0.2,  # Sets length to % of the plot height
                        "thickness": 15,  # Sets thickness in pixels
                        "lenmode":'fraction' # Can also be 'pixels'
                        },
                    margin={"l":50, "r": 50, "b": 50, "t": 100},
                    xaxis_title_font={"size":18},
                    yaxis_title_font={"size":18},
                    title_font_size=18,
                    title_x=0.5,
                    title_font_color='black',
                    title_font_family='Open Sans',
                    )
            fig.update_traces(
                hovertemplate="Station ID: %{y}<br>"
                      "Model cycle: %{x}<br>"
                      "Central freq.: %{z}<br>"
                      "<extra></extra>",
                      xgap=5,
                      ygap=5
                      )
            fig.update_yaxes(type='category',tickfont={
                "size":14, "color":'black'},)
            fig.update_xaxes(tickangle=45,tickfont={
                "size":14, "color":'black'},)
            fig.write_html(
                r"" + f"{prop.visuals_horizon_path}/{prop.ofs}_"
                f"{name_var}_cf_flags.html")
            logger.info("Wrote scorecard/flag plot to file for variable %s.",
                        title_var)
        except Exception as e_x:
            logger.error("Caught exception trying to make flag image! "
                         "Error: %s", e_x)
            return

def make_horizonbin_plots(df_all, info, prop, logger):
    '''
    Here we make bar subplots for each OFS station with 2 rows and 1 column.
    The first row is a bar plot showing RMSE and mean error (y axis) across
    6-hour forecast horizon bins (x axis), with each variable's target error
    range superimposed. The second row is a bar plot showing RMSE and mean
    error (y axis) across each model cycle (x axis), with each variable's
    target error range superimposed.
    Called by get_forecast_hours.horizon_skill

    PARAMETERS:
    ----------
    df_all: giant pandas dataframe that was reshaped in
    get_forecast_hours.horizon_skill.
    info: list of station info strings -->
        info[0] = variable
        info[1] = model
        info[2] = station ID
        info[3] = station full name
        info[4] = station provider/owner
    prop: model properties object containing date range, etc.
    logger: logging interface.

    RETURNS:
    --------
    NOTHING.
    Writes a plot to file.
    '''

    # Get error range
    error_range, _ = plotting_functions.get_error_range(info[0],prop,logger)

    # Stats
    n_threshold = 10
    # Filter out groups with n < n_threshold
    df_filt_mc = df_all.groupby('model_cycle')
    df_filt_mc = df_filt_mc.filter(lambda x: x['error'].count() > n_threshold)
    df_filt_hb = df_all.groupby('hour_bins')
    df_filt_hb = df_filt_hb.filter(lambda x: x['error'].count() > n_threshold)

    rmse_hours = np.round(np.sqrt(df_filt_hb.groupby(
        'hour_bins')['square_error'].mean()),
        decimals=2)
    error_hours = np.round(df_filt_hb.groupby('hour_bins')['error'].mean(),
                           decimals=2)
    rmse_hours_cycle = np.round(np.sqrt(df_filt_mc.groupby(
        'model_cycle')['square_error'].mean()),decimals=2)
    error_hours_cycle = np.round(df_filt_mc.groupby('model_cycle')['error'].\
                                 mean(), decimals=2)

    #r_hours = df_all.groupby('hour_bins')[['OBS','OFS']].corr().iloc[0::2,-1]
    #mean_hours = df_all.groupby('hour_bins')[['OFS','OBS']].mean()
    #mean_obs_hours = df_all.groupby('hour_bins')['OBS'].mean()
    #std_hours = df_all.groupby('hour_bins')[['OFS','OBS']].std()
    #std_obs_hours = df_all.groupby('hour_bins')['OBS'].std()
    # Plots
    # Make hour bin (x axis) labels
    barlabels = []
    bins = np.insert(rmse_hours.index,0,0)
    for i in range(len(bins)-1):
        barstring = str(bins[i]) + '-' + str(bins[i+1])
        barlabels.append(barstring)
    # Make model cycle bin (x axis) labels
    model_cycles = sorted(df_filt_mc['model_cycle'].unique())
    cyclelabels = []
    for i in range(len(model_cycles)):
        cyclestr = model_cycles[i].split('-')[0][4:6] + '/' + \
            model_cycles[i].split('-')[0][6:8] + ' ' + \
                    model_cycles[i].split('-')[1][0:2] + ':00'
        cyclelabels.append(cyclestr)

    # Create lists for looping
    xlabels = [barlabels,cyclelabels]
    ydatahours = [np.array(rmse_hours),np.array(error_hours)]
    ydatacycles = [np.array(rmse_hours_cycle),np.array(error_hours_cycle)]
    colorramps = ['deep','dense']
    # Figure set-up
    nrows=2
    fig = make_subplots(rows=nrows, cols=1, vertical_spacing = 0.2,
    #shared_xaxes=True
    )
    showlegend = [True,False]
    # Loop 'n plot the things 'n stuff
    for i in range(nrows):
        try:
            if i == 0:
                ydata = ydatahours
            else:
                ydata = ydatacycles
            n_colors = len(ydata[0])
            colors = px.colors.sample_colorscale(
                colorramps[i], [n/(n_colors -1) for n in range(n_colors)])
            fig.add_trace(
                go.Bar(x=xlabels[i],
                           y=ydata[0],
                           name='RMSE',
                           marker_color=colors,
                           marker_line_color='black',
                           marker_line_width=1.5,
                           textposition='outside',
                           showlegend=showlegend[i],
                           ), row=i+1, col=1)
            fig.add_trace(
                go.Bar(x=xlabels[i],
                           y=ydata[1],
                           name='Mean error',
                           marker_color=colors,
                           marker_line_color='magenta',
                           marker_line_width=1.5,
                           #line=dict(color='magenta'),
                           showlegend=showlegend[i],
                           ), row=i+1, col=1)
            if ydata[1] is None:
                fig.add_annotation(
                    text="<b>Not enough data points to calculate RMSE!</b>",
                    xref="x domain", yref="y domain",
                    font={"size":14, "color":'red'},
                    x=0, y=0.0,
                    showarrow=False,
                    row=i+1, col=1
                    )
                logger.info("Added low data points warning label to plot "
                             "for station %s", info[2])
            fig.add_hline(y=error_range, line_color="darkorange",
                          line_width=1.25,
                          line_dash='dash',
                          annotation_text= '<b>Target error range</b>',
                          annotation_position='top left',
                          annotation_font_color='black',
                          annotation_font_size=13,
                          row=i+1, col=1)
            fig.add_hline(y=-error_range, line_color="darkorange",
                          line_width=1.25,
                          line_dash='dash',
                          annotation_text= '<b>Target error range</b>',
                          annotation_position='bottom right',
                          annotation_font_color='black',
                          annotation_font_size=13,
                          row=i+1, col=1)
            fig.add_hline(y=0, line_color="black",
                          line_width=1,
                          row=i+1, col=1)
        except Exception as e_x:
            logger.error("Caught exception in make_horizonbin_plots loop! "
                         "Error: %s. Skipping plot!", e_x)
            return
    try:
        yaxis_label, unit_label = get_yaxis_label(info[0], logger)
        fig.update_yaxes(title_text=yaxis_label + '<br>RMSE or error' + unit_label,
                         title_font={"size":16, "color":'black'},
                         tickfont={"size":16, "color":'black'},
                         tickfont_family='Open Sans',
                         titlefont_family='Open Sans',
                         )
        fig.update_yaxes(showline=True, linewidth=1, linecolor='black',
                         mirror=True)
        fig.update_xaxes(showline=True,
                         linewidth=1,
                         linecolor='black',
                         mirror=True,
                         tickfont={"size":14, "color":'black'},
                         tickfont_family='Open Sans'
                         )
        fig.update_xaxes(title_text="Forecast horizon (hours)",
                         title_font={"size":16, "color":'black'},
                         titlefont_family='Open Sans',
                         row=1,col=1)
        fig.update_xaxes(title_text="Model cycle",
                         title_font={"size":16, "color":'black'},
                         titlefont_family='Open Sans',
                         row=2,col=1)
        # Update layout
        prop111 = copy.deepcopy(prop)
        prop111.start_date_full = datetime.strftime(df_all['DateTime'].min(),
                                                    "%Y-%m-%dT%H:%M:%SZ")
        prop111.end_date_full = datetime.strftime(df_all['DateTime'].max(),
                                                    "%Y-%m-%dT%H:%M:%SZ")
        figtitle = plotting_functions.get_title(prop111,
                                                info[1],
                                                info[2:5],
                                                info[0],
                                                logger)
        fig.update_layout(
            title={
                  "text":figtitle,
                  "font":dict(size=14, color='black',family='Open Sans'),
                  "y":1,  # new
                  "x":0.5, "xanchor":'center', "yanchor":'top'},
            yaxis1 = {"tickfont": dict(size=16),
                          "range": [-error_range*2,error_range*2]},
            yaxis2 = {"tickfont": dict(size=16),
                          "range": [-error_range*2,error_range*2]},
            transition_ordering="traces first",
            dragmode="zoom",
            hovermode="x unified",
            height=700,
            width=800,
            xaxis1={"tickangle":45},
            template="plotly_white",
            barmode='group',
            margin={"t":80, "b":50},
            legend={
                "font":dict(
                    family='Open Sans',
                    size=14,
                    color='black'
                    )
                }
            )
        fig.write_html(
            r"" + f"{prop.visuals_horizon_path}/{prop.ofs}_{info[2]}_"
            f"{info[1]}_{info[0]}_rmse_bars.html")
    except Exception as e_x:
        logger.error("Caught exception in make_horizonbin_plots formatting! "
                     "Error: %s. Skipping plot!", e_x)
        return
    # logger.info("Wrote bar plot for %s from make_horizonbin_plots",
    #             info[2])

def make_horizonbin_freq_plots(df_all, info, prop, logger):
    '''
    Here we make bar subplots for each OFS station with 2 rows and 1 column.
    The first row is a bar plot showing central frequency (y axis) across
    6-hour forecast horizon bins (x axis), with the 90% acceptance criteria
    superimposed. The second row is a bar plot showing central frequency
    (y axis) across each model cycle (x axis), with the 90% acceptance criteria
    superimposed
    Called by get_forecast_hours.horizon_skill

    PARAMETERS:
    ----------
    df_all: giant pandas dataframe that was reshaped in
    get_forecast_hours.horizon_skill.
    info: list of station info strings -->
        info[0] = variable
        info[1] = model
        info[2] = station ID
        info[3] = station full name
        info[4] = station provider/owner
    prop: model properties object containing date range, etc.
    logger: logging interface.

    RETURNS:
    --------
    NOTHING.
    Writes a plot to file.
    '''
    # Get error range
    error_range, _ = plotting_functions.get_error_range(info[0],prop,logger)

    # Stats
    n_threshold = 20
    # Filter out groups with n < n_threshold
    df_filt_mc = df_all.groupby('model_cycle')
    df_filt_mc = df_filt_mc.filter(lambda x: x['error'].count() > n_threshold)
    df_filt_hb = df_all.groupby('hour_bins')
    df_filt_hb = df_filt_hb.filter(lambda x: x['error'].count() > n_threshold)

    # Stats
    cf_hours = np.round(100*(df_filt_hb.groupby('hour_bins')['error'].\
        apply(lambda x: ((x <= error_range)&(x >= -error_range)).sum()))/\
        df_filt_hb.groupby('hour_bins')['error'].count(), decimals=2)
    cf_hours_cycle = np.round(100*(df_filt_mc.groupby('model_cycle')['error'].\
        apply(lambda x: ((x <= error_range)&(x >= -error_range)).sum()))/\
        df_filt_mc.groupby('model_cycle')['error'].count(), decimals=2)
    # Make table
    make_table(cf_hours_cycle,info,prop,'cf')
    # Plots
    # Make hour bin (x axis) labels
    barlabels = []
    bins = np.insert(cf_hours.index,0,0)
    for i in range(len(bins)-1):
        barstring = str(bins[i]) + '-' + str(bins[i+1])
        barlabels.append(barstring)
    # Make model cycle bin (x axis) labels
    model_cycles = sorted(df_filt_mc['model_cycle'].unique())
    cyclelabels = []
    for i in range(len(model_cycles)):
        cyclestr = model_cycles[i].split('-')[0][4:6] + '/' + \
            model_cycles[i].split('-')[0][6:8] + ' ' + \
                    model_cycles[i].split('-')[1][0:2] + ':00'
        cyclelabels.append(cyclestr)

    # Create lists for looping
    xlabels = [barlabels,cyclelabels]
    ydatahours = [np.array(cf_hours)]
    ydatacycles = [np.array(cf_hours_cycle)]
    colorramps = ['deep','dense']
    # Figure set-up
    nrows=2
    fig = make_subplots(rows=nrows, cols=1, vertical_spacing = 0.2)
    showlegend = [False,False]
    # Loop 'n plot the things 'n stuff
    for i in range(nrows):
        try:
            if i == 0:
                ydata = ydatahours
            else:
                ydata = ydatacycles
            n_colors = len(ydata[0])
            colors = px.colors.sample_colorscale(
                colorramps[i], [n/(n_colors -1) for n in range(n_colors)])
            fig.add_trace(
                go.Bar(x=xlabels[i],
                           y=ydata[0],
                           name='Central frequency',
                           marker_color=colors,
                           marker_line_color='black',
                           marker_line_width=0.75,
                           textposition='outside',
                           showlegend=showlegend[i],
                           ), row=i+1, col=1)
            if ydata[0] is None:
                fig.add_annotation(
                    text="<b>Not enough data points to calculate stats!</b>",
                    xref="x domain", yref="y domain",
                    font={"size":14, "color":'red'},
                    x=0, y=0.0,
                    showarrow=False,
                    row=i+1, col=1
                    )
                logger.error("Added low data points warning label to plot "
                             "for station %s", info[2])
            fig.add_hline(y=90, line_color="darkred",
                          line_width=1.25,
                          line_dash='dash',
                          annotation_text= '<b>90% acceptance criteria</b>',
                          annotation_position='top left',
                          annotation_font_color='black',
                          annotation_font_size=13,
                          row=i+1, col=1)
            fig.add_hline(y=0, line_color="black",
                          line_width=1,
                          row=i+1, col=1)
        except Exception as e_x:
            logger.error("Caught exception in make_horizonbin_freq_plots loop! "
                         "Error: %s. Skipping plot!", e_x)
            return
    try:
        yaxis_label, _ = get_yaxis_label(info[0], logger)
        fig.update_yaxes(title_text=yaxis_label + '<br>central frequency' + \
                         ' (%)',
                         title_font={"size":16, "color":'black'},
                         tickfont={"size":16, "color":'black'},
                         tickfont_family='Open Sans',
                         titlefont_family='Open Sans',
                         )
        fig.update_yaxes(showline=True, linewidth=1, linecolor='black',
                         mirror=True)
        fig.update_xaxes(showline=True,
                         linewidth=1,
                         linecolor='black',
                         mirror=True,
                         tickfont={"size":14, "color":'black'},
                         tickfont_family='Open Sans'
                         )
        fig.update_xaxes(title_text="Forecast horizon (hours)",
                         title_font={"size":16, "color":'black'},
                         titlefont_family='Open Sans',
                         row=1,col=1)
        fig.update_xaxes(title_text="Model cycle",
                         title_font={"size":16, "color":'black'},
                         titlefont_family='Open Sans',
                         row=2,col=1)
        ##update layout
        prop111 = copy.deepcopy(prop)
        prop111.start_date_full = datetime.strftime(df_all['DateTime'].min(),
                                                    "%Y-%m-%dT%H:%M:%SZ")
        prop111.end_date_full = datetime.strftime(df_all['DateTime'].max(),
                                                    "%Y-%m-%dT%H:%M:%SZ")
        figtitle = plotting_functions.get_title(prop111,
                                                info[1],
                                                info[2:5],
                                                info[0],
                                                logger)
        fig.update_layout(
            title={
                  "text":figtitle,
                  "font":dict(size=14, color='black',family='Open Sans'),
                  "y":1,  # new
                  "x":0.5, "xanchor":'center', "yanchor":'top'},
            yaxis1 = {"tickfont": dict(size=16),
                          "range": [0,100]},
            yaxis2 = {"tickfont": dict(size=16),
                          "range": [0,100]},
            transition_ordering="traces first",
            dragmode="zoom",
            hovermode="x unified",
            height=700,
            width=800,
            xaxis1={"tickangle":45},
            template="plotly_white",
            #barmode='group',
            margin={"t":80, "b":50},
            legend={
                "font":dict(
                    family='Open Sans',
                    size=14,
                    color='black'
                    )
                }
            )
        fig.write_html(
            r""+f"{prop.visuals_horizon_path}/{prop.ofs}_{info[2]}_{info[1]}_"
            f"{info[0]}_cfreq_bars.html")
    except Exception as e_x:
        logger.error("Caught exception in make_horizonbin_freq_plots "
                     "formatting! Error: %s. Skipping plot!", e_x)
        return
    # logger.info("Wrote bar plot for %s from make_horizonbin_freq_plots",
    #             info[2])

def make_timeseries_plots(df_all, forecast_cols_sort, info, prop, logger):
    '''
    Here we make subplots (2x1) time series of observations and all model
    cycles for each OFS station. First row is a time series of obs and model
    data. Second row is a time series of error (model minus obs) for each
    model cycle, with each variable's target error range superimposed.
    Called by get_forecast_hours.horizon_skill

    PARAMETERS:
    ----------
    df_all: giant pandas dataframe that was reshaped in
    get_forecast_hours.horizon_skill.
    forecast_cols_sort: list of model cycle dates as datetime objects sorted in
    ascending order. The main loop below iterates over this list.
    info: list of station info strings -->
        info[0] = variable
        info[1] = model
        info[2] = station ID
        info[3] = station full name
        info[4] = station provider/owner
    prop: model properties object containing date range, etc.
    logger: logging interface.

    RETURNS:
    --------
    NOTHING.
    Writes a plot to file.
    '''

    # Get error range
    error_range, _ = plotting_functions.get_error_range(info[0],prop,logger)

    #Sort out observation data
    df_obs = df_all[['DateTime','OBS']]
    df_obs = df_obs.sort_values(by='DateTime')
    df_obs = df_obs.drop_duplicates(subset='DateTime', keep='first')
    # Figure set-up
    nrows = 2
    fig = make_subplots(rows=nrows, cols=1, vertical_spacing = 0.055,
    #subplot_titles=(prop.whichcasts),
    shared_xaxes=True)
    n_colors = len(forecast_cols_sort)
    colors = px.colors.sample_colorscale(
        "Turbo", [n/(n_colors -1) for n in range(n_colors)])
    # Add traces -- first do observation
    try:
        fig.add_trace(
            go.Scatter(x=df_obs['DateTime'],
                       y=df_obs['OBS'],
                       mode='lines',
                       name='Observations',
                       line={"color":'black', "width":2, "dash":'dash'},
                       ), row=1, col=1)

        # Next add all model error time series
        for i,fcst_col in enumerate(forecast_cols_sort):
            trace_name = fcst_col.split('-')[0][4:6] + '/' + \
                        fcst_col.split('-')[0][6:] + '/' + \
                        fcst_col.split('-')[0][0:4] + ' ' + \
                            fcst_col.split('-')[1][0:2] + 'Z'
            df_filt = df_all[df_all['model_cycle']==fcst_col]
            fig.add_trace(
                go.Scatter(x=df_filt['DateTime'],
                           y=df_filt['OFS'],
                           mode='lines',
                           showlegend=False,
                           name=trace_name,
                           line={"color":colors[i], "width":1.25},
                           ), row=1, col=1)
            fig.add_trace(
                go.Scatter(x=df_filt['DateTime'],
                           y=df_filt['error'],
                           mode='lines',
                           name=trace_name,
                           line={"color":colors[i], "width":1.25},
                           ), row=2, col=1)
    except Exception as e_x:
        logger.error("Caught exception in make_timeseries_plots loop!"
                     "Skipping plot. Error: %s", e_x)
    try:
        # add target error ranges
        fig.add_hline(y=0,line={"width":1},
                      row=2,col=1)
        fig.add_hline(y=error_range, line_color="red",
                      line_width=1,
                      line_dash='dash',
                      annotation_text= 'Target error range',
                      annotation_position='top left',
                      annotation_font_color='black',
                      annotation_font_size=12,
                      row=2, col=1)
        fig.add_hline(y=-error_range, line_color="red",
                      line_width=1,
                      line_dash='dash',
                      annotation_text= 'Target error range',
                      annotation_position='bottom right',
                      annotation_font_color='black',
                      annotation_font_size=12,
                      row=2, col=1)
        # Set figure properties
        yaxis_label, unit_label = get_yaxis_label(info[0], logger)
        #yrange_error = np.ceil(np.nanmax(np.abs(df_all['error'])))
        fig.update_yaxes(title_text=yaxis_label + unit_label,
                         #range=[0, 100],
                         row=1,col=1)
        fig.update_yaxes(title_text=yaxis_label + ' error' + unit_label,
                         range=[-error_range*3, error_range*3],
                         row=2,col=1)
        fig.update_yaxes(showline=True, linewidth=1, linecolor='black',
                         mirror=True, title_font={"size":16, "color":'black'})
        fig.update_xaxes(showline=True,
                         linewidth=1,
                         linecolor='black',
                         mirror=True,
                         tickfont={"size":16},
                         )
        fig.update_xaxes(title_text="Time",
                         title_font={"size":16, "color":'black'},
                         row=2)
        ##update layout
        prop111 = copy.deepcopy(prop)
        prop111.start_date_full = datetime.strftime(df_obs['DateTime'].min(),
                                                    "%Y-%m-%dT%H:%M:%SZ")
        prop111.end_date_full = datetime.strftime(df_obs['DateTime'].max(),
                                                    "%Y-%m-%dT%H:%M:%SZ")
        figtitle = plotting_functions.get_title(prop111,
                                                info[1],
                                                info[2:5],
                                                info[0],
                                                logger)
        figwidth = 900
        figheight = 600
        fig.update_layout(
            title={
                  "text":figtitle,
                  "font": dict(size=14, color='black',family='Open Sans'),
                  "y":0.97,  # new
                  "x":0.5, "xanchor":'center', "yanchor":'top'},
            yaxis1 = {"tickfont": dict(size=16)},
            yaxis2 = {"tickfont": dict(size=16),
                          "range": [-error_range*2,error_range*2]},
            transition_ordering="traces first",
            dragmode="zoom",
            hovermode="x unified",
            height=figheight,
            width=figwidth,
            legend_tracegroupgap = 140,
            legend_traceorder="normal",
            template="plotly_white",
            margin={"t":100, "b":50},
            legend={
                "font":dict(size=16, color='black'),
                "bgcolor": 'rgba(0,0,0,0)',
                }
            )
        fig.write_html(
            r"" + f"{prop.visuals_horizon_path}/{prop.ofs}_{info[2]}_"
            f"{info[1]}_{info[0]}_cycle_series.html")
    except Exception as e_x:
        logger.error("Caught exception in make_timeseries_plots "
                     "formatting! Error: %s. Skipping plot!", e_x)
        return
    # logger.info("Wrote time series plot for %s from make_timeseries_plots",
    #             info[2])
