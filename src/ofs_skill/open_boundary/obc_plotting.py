"""
Created on Wed Apr  1 11:09:54 2026

@author: PWL
"""

import os
from datetime import datetime

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.io as pio
from plotly.subplots import make_subplots

from ofs_skill.open_boundary.obc_processing import (
    _decode_time,
    make_x_labels,
    transform_to_z,
)


def plot_fvcom_obc(prop,ds,logger):
    """
    Generates a comprehensive suite of plots for FVCOM open boundary conditions.

    This function produces:
    1. Temperature and Salinity vertical transects (animated over time).
    2. Water level (elevation/zeta) spatial transects along the boundary.
    3. Node-by-node water level time series with a dropdown selection menu.
    4. An interactive Mapbox map showing the geographic locations of OBC nodes.

    The resulting plots are saved as interactive HTML files in the directory
    defined by 'prop.visuals_1d_station_path'.

    Args:
        prop (ModelProperties): Configuration object containing model metadata
            and output paths.
        ds (xarray.Dataset): The FVCOM OBC netCDF dataset.
        logger (logging.Logger): Logger for status updates.
    """
    logger.info('Starting FVCOM plots...')
    # Decode time via CF metadata (units/calendar), FVCOM Itime/Itime2,
    # or legacy MJD fallback — see obc_processing._decode_time.
    time_dt = _decode_time(ds, logger)

    # First do temp & salt
    logger.info('Plotting FVCOM temp & salt transects')
    nrows = 1
    ncols = 1
    for name_var in ['temp','salinity']:
        if name_var == 'temp':
            cbar_title = 'Water Temperature<br>(\u00b0C)'
            plot_title_name = 'temperature'
        else:
            cbar_title = 'Salinity (<i>PSU<i>)'
            plot_title_name = 'salinity'

        # Figure out dataset variable name
        matches = [item for item in ds.keys() if name_var in item]
        if not matches:
            logger.error(
                "Variable containing '%s' not found in dataset; skipping.",
                name_var)
            continue
        var = str(matches[0])

        # Make x-axis labels
        x_labels = make_x_labels(ds,logger)
        # Transform sigma layers to z-coordinates & interpolate transect
        z, y_labels, x_labels = transform_to_z(ds,var,x_labels,logger)

        # Figures
        fig = make_subplots(rows=nrows, cols=ncols)
        # Make df from z for plotly express animations
        plot_title = prop.ofs.upper() + ' open boundary ' + plot_title_name + ', ' +\
            prop.start_date_full.split('T')[0] + ' ' + \
            prop.model_cycle + 'Z' + ' cycle'
        fig = px.imshow(z,
                animation_frame=0,
                aspect=0.33,
                x=x_labels,
                y=y_labels,
                labels=dict(x='Distance along transect (km)', y='Depth (m)',
                            color=cbar_title),
                range_color=[np.nanpercentile(z, 2),
                             np.nanpercentile(z, 98)],
                color_continuous_scale='Turbo',
                title=plot_title
                )
        for i,step in enumerate(fig.layout.sliders[0].steps):
            step.label = datetime.strftime(time_dt[i],'%m/%d/%Y %H:%M:%S')
        fig.update_layout(
            plot_bgcolor='white',
            paper_bgcolor='white',
            title_font=dict(
                family='Open Sans',
                size=20,
                color='black'
            )
        )
        # Update the x-axis title font
        fig.update_xaxes(
            title_font=dict(
                family='Open Sans',
                size=16,
                color='black'
            ),
            showline=True,
            linewidth=1,
            linecolor='black',
            gridcolor='lightgray',
            mirror=True,
            zeroline=True,
            zerolinecolor='lightgray',
            zerolinewidth=1
        )
        # Update the y-axis title font
        fig.update_yaxes(
            title_font=dict(
                family='Open Sans',
                size=16,
                color='black'
            ),
            showline=True,
            linewidth=1,
            linecolor='black',
            gridcolor='lightgray',
            mirror=True,
            zeroline=True,
            zerolinecolor='black',
            zerolinewidth=1
        )
        fig.layout.updatemenus[0].buttons[0].args[1]['frame']['duration'] = 50
        filename = prop.ofs + '_' + name_var + '_OBC.html'
        savepath = os.path.join(prop.visuals_1d_station_path, filename)
        fig.write_html(savepath, auto_play=False)

    # Now do water level
    logger.info('Plotting FVCOM water level transect')
    time_iterator = int(np.ceil(len(ds['time'])/55))
    try:
        z = np.asarray(ds['elevation'])
    except KeyError:
        z = np.asarray(ds['zeta'])
    try:
        nodes = np.asarray(ds['obc_nodes'])
    except KeyError:
        nodes = np.linspace(0,len(ds['lat'])-1,len(ds['lat']))
        nodes = nodes.astype(int)
    # Make x-axis labels
    x_labels = make_x_labels(ds,logger)
    # Build big dataframe
    df = None
    for t in range(0,len(time_dt),time_iterator):
        temp_ = pd.DataFrame(
            {
            'Time': time_dt[t],
            'Distance along boundary (km)': x_labels,
            'Water level (m)': z[t,:],
            'Node': nodes
            }
        )
        df = pd.concat([df, temp_], ignore_index=True)
        df['Time'] = df['Time'].dt.round('1s')
        df['Node'] = df['Node'].astype('string')

    # Figures
    nrows = 1
    ncols = 1
    fig = make_subplots(rows=nrows, cols=ncols)
    # Make df from z for plotly express animations
    plot_title = prop.ofs.upper() + ' open boundary water levels, ' +\
        prop.start_date_full.split('T')[0] + ' ' + \
            prop.model_cycle + 'Z' + ' cycle'
    fig = px.scatter(
        df,
        x='Distance along boundary (km)',
        y='Water level (m)',
        color='Node',
        color_discrete_sequence=px.colors.qualitative.Dark24,
        animation_frame='Time',
        hover_data={
            'Distance along boundary (km)': True,
            'Water level (m)': True,
            'Node': True,
            },
        title=plot_title,
        range_y=[np.nanmin(df['Water level (m)']),
                 np.nanmax(df['Water level (m)'])]
        )
    fig.update_layout(
        plot_bgcolor='white',
        paper_bgcolor='white',
        title_font=dict(
            family='Open Sans',
            size=20,
            color='black'
        )
    )
    # Update the x-axis title font
    fig.update_xaxes(
        title_font=dict(
            family='Open Sans',
            size=16,
            color='black'
        ),
        showline=True,
        linewidth=1,
        linecolor='black',
        gridcolor='lightgray',
        mirror=True,
        zeroline=True,
        zerolinecolor='lightgray',
        zerolinewidth=1
    )
    # Update the y-axis title font
    fig.update_yaxes(
        title_font=dict(
            family='Open Sans',
            size=16,
            color='black'
        ),
        showline=True,
        linewidth=1,
        linecolor='black',
        gridcolor='lightgray',
        mirror=True,
        zeroline=True,
        zerolinecolor='black',
        zerolinewidth=1
    )
    filename = prop.ofs + '_' + 'water_level' + '_OBC.html'
    savepath = os.path.join(prop.visuals_1d_station_path, filename)
    fig.write_html(savepath, auto_play=False)

    # Now make time series plot with dropdown menu
    logger.info('Plotting FVCOM water level node-by-node time series')
    df = None
    for n in range(len(nodes)):
        temp = pd.DataFrame(
            {
            'Time': time_dt,
            'Water level (m)': z[:,n],
            'Node': nodes[n]
            }
        )
        df = pd.concat([df, temp], ignore_index=True)
        df['Time'] = df['Time'].dt.round('1s')
    fig = make_subplots(rows=nrows, cols=ncols)
    plot_title = prop.ofs.upper() + ' open boundary water levels, ' +\
        prop.start_date_full.split('T')[0] + ' ' + \
            prop.model_cycle + 'Z' + ' cycle'
    fig = px.line(
        df,
        x='Time',
        y='Water level (m)',
        color='Node',
        title=plot_title,
        )
    # create dropdown buttons
    buttons = []
    # Add a button to show all series
    buttons.append(dict(
        label='All nodes',
        method='update',
        args=[{'visible': [True] * len(nodes)},
              {'title': (f'Water level for all {prop.ofs.upper()} OBC nodes, '
                         f"{prop.start_date_full.split('T')[0]} "
                         f'{prop.model_cycle}Z')}]
        )
    )
    for i, node in enumerate(nodes):
        # make a list of booleans to control trace visibility
        visibility_list = [False] * len(nodes)
        visibility_list[i] = True
        button = dict(
            label='Node ' + str(node),
            method='update',
            args=[{'visible': visibility_list},
                  {'title': (f'Water level for {prop.ofs.upper()} OBC node {node}, '
                             f"{prop.start_date_full.split('T')[0]} "
                             f'{prop.model_cycle}Z')}]
        )
        buttons.append(button)

    # update layout to include the dropdown menu
    fig.update_layout(
        updatemenus=[
            dict(
                buttons=buttons,
                active=0,
                direction='down',
                pad={'r': 0, 't': 0},
                showactive=True,
                x=0,
                xanchor='left',
                y=1.1,
                yanchor='top'
            ),
        ],
        margin={'t': 125, 'b': 50, 'l': 50, 'r': 50},
        plot_bgcolor='white',
        paper_bgcolor='white',
        title_font=dict(
            family='Open Sans',
            size=20,
            color='black'
        )
    )
    # Update the x-axis title font
    fig.update_xaxes(
        title_font=dict(
            family='Open Sans',
            size=16,
            color='black'
        ),
        showline=True,
        linewidth=1,
        linecolor='black',
        gridcolor='lightgray',
        mirror=True,
        zeroline=True,
        zerolinecolor='lightgray',
        zerolinewidth=1
    )
    # Update the y-axis title font
    fig.update_yaxes(
        title_font=dict(
            family='Open Sans',
            size=16,
            color='black'
        ),
        showline=True,
        linewidth=1,
        linecolor='black',
        gridcolor='lightgray',
        mirror=True,
        zeroline=True,
        zerolinecolor='black',
        zerolinewidth=1
    )
    filename = prop.ofs + '_' + 'water_level' + '_OBC_node_series.html'
    savepath = os.path.join(prop.visuals_1d_station_path, filename)
    fig.write_html(savepath)

    # Map of OBC nodes
    logger.info('Mapping FVCOM OBC nodes')
    df = None
    df = pd.DataFrame(
        {
        'X': np.asarray(ds['lon']),
        'Y': np.asarray(ds['lat']),
        'Node': nodes
        }
    )
    df['Node'] = df['Node'].astype('string')
    plot_title = prop.ofs.upper() + ' open boundary nodes'
    pio.renderers.default = 'browser'
    fig = px.scatter_mapbox(df, lat='Y', lon='X',
                     color='Node',  # which column to use to set the color of markers
                     color_discrete_sequence=px.colors.qualitative.Dark24,
                     mapbox_style='carto-positron',
                     hover_data={'X': ':.2f',
                                 'Y': ':.2f',
                                 'Node': ':.0f',
                                 },
                     zoom=8,
                     title=plot_title,
                     height=700,
                     width=1000,
                     )
    # update layout to include the dropdown menu
    fig.update_layout(
        title_font=dict(
            family='Open Sans',
            size=20,
            color='black'
        )
    )
    filename = prop.ofs + '_OBC_node_map.html'
    savepath = os.path.join(prop.visuals_1d_station_path, filename)
    fig.write_html(savepath,config={'scrollZoom': True})
    logger.info('Done with FVCOM plotting!')
