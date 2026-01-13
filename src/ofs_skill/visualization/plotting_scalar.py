"""
Scalar variable plotting module for OFS skill assessment.

This module provides plotting routines for scalar oceanographic variables
including water level, temperature, and salinity. It generates interactive
Plotly visualizations with time series, box plots, and error analysis.

Key Features:
    - Time series plots with observations and model guidance
    - Statistical box plots for data distribution
    - Error/bias analysis with target error ranges
    - Optional tidal predictions for water level plots
    - Automatic marker sizing based on data density
    - Accessibility-optimized color palettes

Functions:
    oned_scalar_plot: Create standard scalar variable plots with obs/model comparison

Author: AJK
Created: 05/09/2025
Last Modified: 10/2025 - Split ice plotting into separate file
"""
from __future__ import annotations

import configparser
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import ofs_skill.visualization.make_static_plots as make_static_plots
from ofs_skill.obs_retrieval import (
    find_nearest_tidal_stations,
    retrieve_tidal_predictions,
)
from ofs_skill.obs_retrieval.retrieve_properties import RetrieveProperties
from ofs_skill.visualization.plotting_functions import (
    find_max_data_gap,
    get_error_range,
    get_markerstyles,
    get_title,
    make_cubehelix_palette,
)

if TYPE_CHECKING:
    from logging import Logger

    import pandas as pd


def oned_scalar_plot(
    now_fores_paired: list[pd.DataFrame],
    name_var: str,
    station_id: tuple,
    node: str,
    prop,
    logger: Logger
) -> None:
    """
    Create standard scalar variable plots (water level, temp, salinity).

    Generates a multi-panel interactive plot with:
    - Top left: Time series of observations and model outputs
    - Top right: Box plots showing data distributions
    - Bottom left: Error/bias time series
    - Bottom right: Error distribution box plots

    For water level plots, attempts to add tidal predictions from CO-OPS API.

    Args:
        now_fores_paired: List of DataFrames with paired obs/model data for each cast
        name_var: Variable name ('wl', 'temp', or 'salt')
        station_id: Tuple of (station_number, station_name, source)
        node: Model node identifier
        prop: Properties object with configuration settings
        logger: Logger instance for output messages

    Returns:
        None (writes HTML plot to file)

    Notes:
        - Updated 5/28/24 for accessibility
        - Marker sizes scale inversely with data count for readability
        - Gaps > 10 points disable line connection
        - Datum mismatches trigger warning annotation
    """
    # Get marker styles so they can be assigned to different time series below
    allmarkerstyles = get_markerstyles()

    # Get target error range
    X1, X2 = get_error_range(name_var, prop, logger)

    """
    Adjust marker sizes dynamically based on the number of data points.
    If the number of DateTime entries in the first element of now_fores_paired
    exceeds data_count, scale the marker sizes inversely proportional to the
    data count. Otherwise, use the default marker sizes.

    """
    modetype = 'lines+markers'
    lineopacity = 1
    marker_opacity = 0.5
    data_count = 48
    min_size = 1
    gap_length = 10
    if len(list(now_fores_paired[0].DateTime)) > data_count:
        marker_size = (
            6**(
                data_count/len(list(now_fores_paired[0].DateTime))
            )
        ) + (min_size-1)
        marker_size_obs = (
            9**(
                data_count/len(list(now_fores_paired[0].DateTime))
            )
        ) + (min_size-1)
    else:
        marker_size = 6
        marker_size_obs = 9
    # Check for long data gaps
    if find_max_data_gap(now_fores_paired[0].OBS) > gap_length:
        connectgaps = False
    else:
        connectgaps = True

    if name_var == 'wl':
        plot_name = 'Water Level ' + f'at {prop.datum} (<i>meters<i>)'
        save_name = 'water_level'
    elif name_var == 'temp':
        plot_name = 'Water Temperature (<i>\u00b0C<i>)'
        save_name = 'water_temperature'
    elif name_var == 'salt':
        plot_name = 'Salinity (<i>PSU<i>)'
        save_name = 'salinity'

    '''
    Make a color palette with entries for each whichcast plus observations
    and tides. The 'cubehelix' palette linearly varies hue AND intensity
    so that colors can be distingushed by colorblind users or in greyscale.
    '''
    ncolors = (len(prop.whichcasts)*1) + 2
    palette, _ = make_cubehelix_palette(ncolors, 2.5, 0.9, 0.65)
    # Observations are dashed, while model now/forecasts are solid (default)
    obslinestyle = 'dash'
    obsname = 'Observations'
    linewidth = 1.5
    nrows = 2
    ncols = 2
    column_widths = [
        1 - len(prop.whichcasts) * 0.03,
        len(prop.whichcasts) * 0.125,
    ]
    xaxis_share = True

    # Create figure
    fig = make_subplots(
        rows=nrows, cols=ncols, column_widths=column_widths,
        shared_yaxes=True, horizontal_spacing=0.05,
        vertical_spacing=0.05,
        shared_xaxes=xaxis_share,
        # subplot_titles = ['Observed','OFS Model'],
    )

    fig.add_trace(
        go.Scattergl(
            x=list(now_fores_paired[0].DateTime),
            y=list(now_fores_paired[0].OBS), name=obsname,
            hovertemplate='%{y:.2f}', mode=modetype,
            opacity=lineopacity,
            connectgaps=connectgaps,
            line=dict(color=palette[0], width=linewidth, dash=obslinestyle),
            legendgroup='obs', marker=dict(
                symbol=allmarkerstyles[0], size=marker_size_obs,
                color=palette[0],opacity=marker_opacity,
                line=dict(width=0, color='black'),
            ),
        ), 1, 1,
    )

    # Adding boxplots
    fig.add_trace(
        go.Box(
            y=now_fores_paired[0]['OBS'], boxmean='sd',
            name=obsname, showlegend=False, legendgroup='obs',
            width=.7, line=dict(color=palette[0], width=1.5),
            # fillcolor = 'black',
            marker=dict(color=palette[0]),
        ), 1, 2,
    )

    for i in range(len(prop.whichcasts)):
        # Change name of model time series to make more explanatory
        if prop.whichcasts[i][-1].capitalize() == 'B':
            seriesname = 'Model Forecast Guidance'
        elif prop.whichcasts[i][-1].capitalize() == 'A':
            seriesname = 'Model Forecast Guidance, ' + prop.forecast_hr[:-2] +\
                'z cycle'
        elif prop.whichcasts[i].capitalize() == 'Nowcast':
            seriesname = 'Model Nowcast Guidance'
        else:
            seriesname = prop.whichcasts[i].capitalize() + ' Guidance'

        fig.add_trace(
            go.Scattergl(
                x=list(now_fores_paired[i].DateTime),
                y=list(now_fores_paired[i].OFS),
                name=seriesname,
                opacity=lineopacity,
                # Updated hover text to show Obs/Fore/Now, not bias
                hovertemplate='%{y:.2f}',
                mode=modetype, line=dict(
                    color=palette[i+1],
                    width=linewidth,
                ),
                legendgroup=seriesname,
                # i+1 because observations already used first marker type
                marker=dict(
                    symbol=allmarkerstyles[i+1], size=marker_size,
                    color=palette[i+1],
                    # 'firebrick',
                    opacity=marker_opacity, line=dict(
                        width=0, color='black',
                    ),
                ),
            ), 1, 1,
        )

        fig.add_trace(
            go.Box(
                y=now_fores_paired[i]['OFS'], boxmean='sd',
                name=seriesname,
                showlegend=False,
                legendgroup=seriesname,
                width=.7,
                line=dict(
                    color=palette[i+1],
                    width=1.5,
                ),
                marker_color=palette[i+1],
            ),
            1, 2,
        )

    #####################################################################
    ## Tide retrieval and plotting                                     ##
    #  Add tidal predictions for water level plots excluding glofs     ##
    #####################################################################
    if name_var == 'wl' and prop.ofs[0] != 'l':
      try:
          import os

          retrieve_input = RetrieveProperties()
          obs_station_id = str(station_id[0])
          station_source = str(station_id[2]) if len(station_id) > 2 else 'CO-OPS'

          # Get date range from actual paired data to ensure tidal predictions cover plotted range
          data_times = now_fores_paired[0].DateTime
          start_dt = data_times.min().to_pydatetime()
          end_dt = data_times.max().to_pydatetime()
          #start_dt = datetime.strptime(prop.start_date_full, "%Y-%m-%dT%H:%M:%SZ")
          #end_dt = datetime.strptime(prop.end_date_full, "%Y-%m-%dT%H:%M:%SZ")
          retrieve_input.start_date = start_dt.strftime('%Y%m%d%H%M%S')
          retrieve_input.end_date = end_dt.strftime('%Y%m%d%H%M%S')
          #if (end_dt - start_dt) > timedelta(hours=48):
          #      raise ValueError("Run is more than 48 hours - skipping tide retrieval.")

          # Try requested datum first, then fallback datums if needed
          requested_datum = prop.datum
          fallback_datums = ['MLLW', 'MHHW', 'MHW', 'MLW', 'NAVD88', 'IGLD85', 'LWD', 'XGEOID20B']
          # Read fallback datums from config file
          config = configparser.ConfigParser()
          config_file = Path(__file__).resolve().parent.parent.parent.parent / 'conf' / 'ofs_dps.conf'
          try:
                config.read(config_file)
                if config.has_option('datums', 'datum_list'):
                    datum_list_str = config.get('datums', 'datum_list')
                    fallback_datums = [d.strip() for d in datum_list_str.split()]
          except Exception as ex:
                logger.warning('Could not read datum_list from config, using defaults: %s',ex)

          datums_to_try = [requested_datum] + [d for d in fallback_datums if d != requested_datum]

          tidal_data = None
          used_datum = None
          tidal_station_id = None
          tidal_station_name = None
          tidal_station_distance = None

          # Helper function to try getting tidal data from a station
          def try_get_tidal_data(station_id_to_try):
              retrieve_input.station = station_id_to_try
              for datum in datums_to_try:
                  retrieve_input.datum = datum
                  data = retrieve_tidal_predictions(
                      retrieve_input, logger)
                  if data is False:
                      # Station doesn't support predictions
                      return None, None
                  if data is not None and len(data) > 0:
                      return data, datum
              return None, None

          # Get station coordinates from control file
          lat, lon = None, None
          try:
              ctl_file = os.path.join(prop.control_files_path, f'{prop.ofs}_wl_station.ctl')
              with open(ctl_file) as f:
                  lines = f.readlines()
              for i, line in enumerate(lines):
                  if line.strip().startswith(obs_station_id):
                      coords = lines[i+1].split()
                      lat, lon = float(coords[0]), float(coords[1])
                      break
          except Exception as ex:
              logger.warning('Could not find coordinates for station %s: %s', obs_station_id, ex)

          # For CO-OPS stations, try the station itself first
          if station_source.upper() in ['CO-OPS', 'COOPS', 'TC', 'TAC']:
              tidal_data, used_datum = try_get_tidal_data(obs_station_id)
              if tidal_data is not None:
                  tidal_station_id = obs_station_id
                  tidal_station_distance = 0.0

          # If no data yet (non-CO-OPS or CO-OPS failed), try nearby tidal stations
          if tidal_data is None and lat is not None and lon is not None:
              logger.info('Finding nearby tidal stations for %s station %s...',
                         station_source, obs_station_id)
              nearby_stations = find_nearest_tidal_stations(
                  lat, lon, logger, max_stations=10)

              for candidate_id, candidate_name, candidate_dist in nearby_stations:
                  # Skip if same as observation station (already tried for CO-OPS)
                  if candidate_id == obs_station_id:
                      continue
                  logger.info('Trying tidal station %s (%s) at %.1f km...',
                             candidate_id, candidate_name, candidate_dist)
                  tidal_data, used_datum = try_get_tidal_data(candidate_id)
                  if tidal_data is not None:
                      tidal_station_id = candidate_id
                      tidal_station_name = candidate_name
                      tidal_station_distance = candidate_dist
                      logger.info('Using tidal station %s (%s) at %.1f km for station %s',
                                 tidal_station_id, tidal_station_name, tidal_station_distance, obs_station_id)
                      break

          if tidal_data is not None and len(tidal_data) > 0:
              # Build hover text with source information including distance
              if tidal_station_name:
                  source_text = f'CO-OPS Station {tidal_station_id} ({tidal_station_name})'
              else:
                  source_text = f'CO-OPS Station {tidal_station_id}'

              if tidal_station_distance is not None and tidal_station_distance > 0:
                  distance_text = f'<br>Distance: {tidal_station_distance:.1f} km'
              else:
                  distance_text = ''

              if used_datum == requested_datum:
                    hover_text = f'Tidal Prediction: %{{y:.2f}}<br>Source: {source_text}{distance_text}<br>Datum: {used_datum}<extra></extra>'
              else:
                    hover_text = f'Tidal Prediction: %{{y:.2f}}<br>Source: {source_text}{distance_text}<br>Datum: {used_datum}(requested: {requested_datum})<extra></extra>'

              fig.add_trace(
                  go.Scattergl(
                      x=list(tidal_data.DateTime),
                      y=list(tidal_data.TIDE),
                      name='Tidal Predictions',
                      hovertemplate=hover_text,
                      mode='lines',
                      opacity=0.7,
                      line=dict(color=palette[-1], width=1.5, dash='dot'),
                      legendgroup='tide',
                      marker=dict(size=0)), 1, 1)
              logger.info('Tidal predictions added to water level plot for station %s using tidal station %s (datum:%s)',
                         obs_station_id, tidal_station_id, used_datum)
              # Adding boxplots for tides
              fig.add_trace(
                  go.Box(
                        y=tidal_data['TIDE'], boxmean='sd',
                        name='Tidal Prediction', showlegend=False, legendgroup='tide',
                        width=.7, line=dict(color=palette[-1], width=1.5),
                        # fillcolor = 'black',
                        marker=dict(color=palette[-1]),
                  ), 1, 2,
               )

      except Exception as ex:
          logger.warning('Could not retrieve tidal predictions for station %s: %s',
                        station_id[0], ex)

      logger.debug('Finished adding tidal predictions added to water level plot for station %s using tidal station %s (datum:%s)',
           obs_station_id, tidal_station_id, used_datum)
    #####################################################################
    ## Done tide retrieval and plotting                                ##
    #####################################################################

    for i in range(len(prop.whichcasts)):
        if prop.whichcasts[i].capitalize() == 'Nowcast':
            sdboxName = 'Nowcast - Obs.'
        elif prop.whichcasts[i].capitalize() == 'Forecast_b':
            sdboxName = 'Forecast - Obs.'
        else:
            sdboxName = 'Model'+str(i+1)+' - Obs.'
        fig.add_trace(
            go.Scattergl(
                x=list(now_fores_paired[i].DateTime),
                y=[
                    ofs - obs for ofs, obs in zip(
                        now_fores_paired[i].OFS,
                        now_fores_paired[i].OBS,
                    )
                ],
                name=sdboxName,
                connectgaps=connectgaps,
                hovertemplate='%{y:.2f}',
                mode=modetype, line=dict(
                    color=palette[i+1],
                    width=1.5, dash='dash',
                ),
                legendgroup=sdboxName,
                marker=dict(
                    # i+1 because observations already used first marker type
                    symbol=allmarkerstyles[i+1], size=marker_size,
                    color=palette[i+1],
                    # 'firebrick',
                    opacity=marker_opacity, line=dict(width=0, color='black'),
                ),
            ), 2,
            1,
        )

        fig.add_hline(
            y=0, line_width=1,
            line_color='black',
            # line_dash='dash',
            row=2, col=1,
        )
        fig.add_hline(
            y=X1, line_color='orange',
            line_width=0.75,
            line_dash='dash',
            annotation_text='Target error range',
            annotation_position='top left',
            annotation_font_color='black',
            annotation_font_size=12,
            row=2, col=1,
        )
        fig.add_hline(
            y=-X1, line_color='orange',
            line_width=0.75,
            line_dash='dash',
            annotation_text='Target error range',
            annotation_position='bottom right',
            annotation_font_color='black',
            annotation_font_size=12,
            row=2, col=1,
        )
        fig.add_hline(
            y=X1*2, line_color='red',
            line_width=0.75,
            line_dash='dash',
            annotation_text='2x target error range',
            annotation_position='top left',
            annotation_font_color='black',
            annotation_font_size=12,
            row=2, col=1,
        )
        fig.add_hline(
            y=-X1*2, line_color='red',
            line_width=0.75,
            line_dash='dash',
            annotation_text='2x target error range',
            annotation_position='bottom right',
            annotation_font_color='black',
            annotation_font_size=12,
            row=2, col=1,
        )

        fig.add_trace(
            go.Box(
                y=[
                    ofs - obs for ofs, obs in zip(
                        now_fores_paired[i].OFS,
                        now_fores_paired[i].OBS,
                    )
                ],
                boxmean='sd',
                name=sdboxName,
                showlegend=False,
                legendgroup=sdboxName,
                width=.7,
                line=dict(
                    color=palette[i+1],
                    width=linewidth,
                ),
                marker_color=palette[i+1],
            ),
            2, 2,
        )

    # Figure Config
    figheight = 700
    figwidth  = 950
    yoffset = 1.01
    fig.update_layout(
        title=dict(
            text=get_title(prop, node, station_id, name_var, logger),
            font=dict(size=14, color='black', family='Open Sans'),
            y=0.97,  # new
            x=0.5, xanchor='center', yanchor='top',
        ),
        xaxis=dict(
            mirror=True,
            ticks='inside',
            showline=True,
            linecolor='black',
            linewidth=1,
        ),
        xaxis2=dict(tickangle=45),
        xaxis3=dict(
            mirror=True,
            ticks='inside',
            showline=True,
            linecolor='black',
            linewidth=1,
        ),
        xaxis4=dict(tickangle=45),
        yaxis=dict(
            mirror=True,
            ticks='inside',
            showline=True,
            linecolor='black',
            linewidth=1,
        ),
        yaxis3=dict(
            mirror=True,
            ticks='inside',
            showline=True,
            linecolor='black',
            linewidth=1,
            range=[-X1*4, X1*4],
            tickmode='array',
            tickvals=[-X1*4, -X1*3, -X1*2, -X1, 0, X1, X1*2, X1*3, X1*4],
        ),
        transition_ordering='traces first', dragmode='zoom',
        hovermode='x unified', height=figheight, width=figwidth,
        template='plotly_white', margin=dict(
            t=150, b=100,
        ),
        legend=dict(
            orientation='h', yanchor='bottom',
            y=yoffset, xanchor='left', x=-0.05,
            itemsizing='constant',
            font=dict(
                family='Open Sans',
                size=12,
                color='black',
            ),
        ),
    )
    # Add annotation if datum mismatch
    if name_var == 'wl':
        filename = f'{prop.control_files_path}/{prop.ofs}_wl_datum_report.csv'
        try:
            df = pd.read_csv(filename)
            has_fail = df.loc[df[df['Station ID'] == int(
                station_id[0])].index]['Datum conversion pass/fail'] == 'fail'
            if has_fail.bool():
                fig.add_annotation(
                    text='<b>Warning:<br>datum mismatch</b>',
                    xref='x domain', yref='y domain',
                    font=dict(size=14, color='red'),
                    x=0, y=0.0,
                    showarrow=False,
                    row=1, col=1,
                )
        except Exception as e_x:
            logger.error(
                'Cannot find station ID in datum report! '
                'Exception: %s', e_x,
            )

    # Set x-axis moving bar
    fig.update_xaxes(
        showspikes=True,
        spikemode='across',
        spikesnap='cursor',
        showline=True,
        showgrid=True,
        tickfont=dict(size=14,
                      family='Open Sans',
                      color='black'),
    )
    # Set y-axes titles
    fig.update_yaxes(
        mirror=True,
        title_text=f'{plot_name}',
        title_font=dict(
            family='Open Sans',
            #size=18,
            color='black'
            ),
        tickfont=dict(size=14,
                      family='Open Sans',
                      color='black'),
        row=1, col=1,
    )
    fig.update_yaxes(
        title_text=f"Error {plot_name.split(' ')[-1]}",
        title_font=dict(
            family='Open Sans',
            #size=18,
            color='black'
            ),
        tickfont=dict(size=14,
                      family='Open Sans',
                      color='black'),
        row=2, col=1,
    )

    # naming whichcasts
    naming_ws = '_'.join(prop.whichcasts)
    output_file = (
        f'{prop.visuals_1d_station_path}/{prop.ofs}_'
        f'{station_id[0]}_{save_name}_timeseries_'
        f'{naming_ws}_{prop.ofsfiletype}'
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
    # Finally make a static plot and write it to file for O&M dashboard
    if prop.static_plots:
        logger.debug('prop.static_plots set to True, calling make_static_plots routine ... ')
        make_static_plots.scalar_plots(now_fores_paired, name_var, station_id,
                                       node, prop, logger)
