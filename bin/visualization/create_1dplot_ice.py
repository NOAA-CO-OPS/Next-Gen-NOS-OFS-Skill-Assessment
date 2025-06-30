# -*- coding: utf-8 -*-
"""
Created on Tue Nov  5 15:17:25 2024

@author: PWL

for reference:

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
"r_overlap": r_overlap,
"csi_all": csi_all
"""
import os
import sys
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.offsetbox import AnchoredText
import matplotlib.colors as mcolors
import pandas as pd
import math
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import seaborn as sns
#import datetime
from datetime import datetime
#import imageio

# Add parent directory to sys.path
from pathlib import Path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from visualization import plotting_scalar

def make_cubehelix_palette(
        ncolors, start_val, rot_val, light_val
        ):
    '''
    Function makes and returns a custom cubehelix color palette for plotting.
    The colors within the cubehelix palette (and therefore plots) can be
    distinguished in greyscale to improve accessibility. Colors are returned as
    HEX values because it's easier to handle HEX compared to RGB values.

    Arguments:
    -ncolors = number of dicrete colors in color palette, should correspond to
        number of time series in the plot. integer, 1 <= ncolors <= 1000(?)
    -start_val = starting hue for color palette. float, 0 <= start_val <= 3
    -rot_val = rotations around the hue wheel over the range of the palette.
        Larger (smaller) absolute values increase (decrease) number of different
        colors in palette. float, positive or negative
    -light_val = Intensity of the lightest color in the palette.
        float, 0 (darker) <= light <= 1 (lighter)

    More details:
        https://seaborn.pydata.org/generated/seaborn.cubehelix_palette.html

    '''
    palette = sns.cubehelix_palette(
        n_colors=ncolors,start=start_val,rot=rot_val,gamma=1.0,
        hue=0.8,light=light_val,dark=0.15,reverse=False,as_cmap=False
        )
    #Convert RGB to HEX numbers
    palette = palette.as_hex()
    return palette

def create_1dplot_icestats(prop,time_all,logger):
    '''Make time series plots of lake-wide ice concentration
    and extent skill stats, let's go'''

    # First load nowcast and/or forecast stats from file!
    dflist = []
    counter = 0
    for cast in prop.whichcasts:
        df = None
        #df2 = None
        # Here we try to open the paired data set.
        # If not found, return without making plot
        prop.whichcast = cast.lower()
        if (os.path.isfile(
                f"{prop.data_skill_stats_path}/skill_{prop.ofs}_"
                f"icestatstseries_{prop.whichcast}.csv") is False):
            logger.error(
                "Ice stats time series dataset not found in %s. ",
                prop.data_skill_stats_path)

        # If csv is there, proceed
        else:
            counter = counter + 1 #Keep track of # whichcasts
            df = pd.read_csv(
                r"" +  f"{prop.data_skill_stats_path}/skill_{prop.ofs}_"
                 f"icestatstseries_{prop.whichcast}.csv")
            logger.info(
                "Ice stats time series csv found in %s",
                prop.data_skill_stats_path)


        # Put nowcast & forecast together in list -- not used as of 12/6/24,
        # but will come in handy for future updates...
        if df is not None:
            dflist.append(df)
        # if df2 is not None:
        #     df2list.append(df2)

    # Get dates and make title for figures
    datestrend = str(time_all[len(time_all)-1]).split()
    #datestrbegin = str(time_all[0]).split()
    datestrbegin = prop.start_date_full.split('T')
    titlewc = "_".join(prop.whichcasts)

    figtitle = prop.ofs.upper()+' '+datestrbegin[0]+\
                 ' - ' + datestrend[0]

    # Figure out what x-axis tick spacing should be --
    # it depends on length of run so axis doesn't overcrowd!
    df['time_all_dt'] = pd.to_datetime(df['time_all_dt'])
    #dayselapsed=((df['time_all_dt'].iloc[-1]-df['time_all_dt'].iloc[0]).days)+1
    dayselapsed = (datetime.strptime(prop.end_date_full,"%Y-%m-%dT%H:%M:%SZ") -
                   datetime.strptime(prop.start_date_full,"%Y-%m-%dT%H:%M:%SZ"
                                     )).days + 1
    xtickspace = dayselapsed/10
    if xtickspace >= 1:
        xtickspace = math.floor(xtickspace)
    elif xtickspace < 1:
        xtickspace = math.ceil(xtickspace)

    if prop.plottype == 'static':
        # --- Do plots, huzzah --------------------------------------------
        # Do figure stuff, enjoy
        fig, axs = plt.subplots(4,1)
        fig.set_figheight(10)
        fig.set_figwidth(8)
        fig.suptitle(figtitle,fontsize=20)

        axs[0].plot(df['time_all_dt'],df['obs_meanicecover'],label="GLSEA",
                    color='deeppink',linewidth=1.5)
        axs[0].plot(df['time_all_dt'],df['mod_meanicecover'],label="FVCOM",
                    color='dodgerblue')
        axs[0].plot(df['time_all_dt'],df['icecover_hist'],
                    label="Climatology",color='black')
        axs[0].fill_between(df['time_all_dt'],
                            np.subtract(df['obs_meanicecover'],
                                        df['obs_stdmic']),
                            np.add(df['obs_meanicecover'],df['obs_stdmic']),
                            alpha=0.2,
                            linewidth=0, facecolor='deeppink')
        axs[0].fill_between(df['time_all_dt'],
                            np.subtract(df['mod_meanicecover'],
                                        df['mod_stdmic']),
                            np.add(df['mod_meanicecover'],
                                   df['mod_stdmic']),
                            alpha=0.2,
                            linewidth=0, facecolor='dodgerblue')
        axs[0].xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        axs[0].xaxis.set_major_locator(mdates.DayLocator())
        axs[0].grid(True, color = 'grey', linestyle = '--', linewidth = 0.5)
        #axs[2].set_title('Mean ice cover')
        axs[0].set_ylim([0,100])
        axs[0].set_xlim([df['time_all_dt'].min(),df['time_all_dt'].max()])
        axs[0].legend(loc="upper center",fontsize=14,ncol=3,frameon=False,
                      bbox_to_anchor=(0.37, 1.23))
        axs[0].set_ylabel('Mean ice cover (%)',fontsize=16)
        axs[0].set_xticks(axs[0].get_xticks()[::xtickspace])
        axs[0].set_yticks(axs[0].get_yticks()[::1])
        axs[0].tick_params(axis='both', which='major', labelsize=14)
        plt.gcf().autofmt_xdate()

        axs[1].plot(df['time_all_dt'], df['SS'], label = "Skill Score",
                    color='darkviolet',linewidth=1.5)
        axs[1].axhline(y = 0, color = 'black',linewidth=0.8)
        axs[1].axhline(y = 0.36, color = 'green', linewidth=0.8)
        #axs[1].plot(time_all_dt, r_overlap,
        #label = "Pearson's R, overlap only", color='orange')
        axs[1].xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        axs[1].xaxis.set_major_locator(mdates.DayLocator())
        axs[1].grid(True, color = 'grey', linestyle = '--', linewidth = 0.5)
        axs[1].set_ylim([np.floor(df['SS'].min()),1])
        axs[1].set_xlim([df['time_all_dt'].min(),df['time_all_dt'].max()])
        #axs[3].legend(loc="upper left",fontsize=14)
        axs[1].set_ylabel('Skill score', fontsize=16)
        #axs[1].set_xlabel("Time", )
        axs[1].set_xticks(axs[1].get_xticks()[::xtickspace])
        axs[1].set_yticks(axs[1].get_yticks()[::1])
        axs[1].tick_params(axis='both', which='major', labelsize=14)
        line2 = '> 0.36: very skillful'
        line3 = '> 0: skillful'
        line4 = '< 0: not skillful'
        summary = line2 + '\n' + line3 + '\n' + line4
        # Add the cool annotation to plot showing skill metrics
        anchored_text = AnchoredText(summary, loc="lower left",
                                      prop=dict(fontsize=12))
        anchored_text.patch.set_alpha(0.5)
        axs[1].add_artist(anchored_text)
        plt.gcf().autofmt_xdate()

        axs[2].plot(df['time_all_dt'], df['rmse_all'], label = "RMSE",
                    color='red',linewidth=1.5)
        #axs[0].plot(time_all_dt, rmse_overlap,
        #label = "RMSE overlap only", color='red')
        axs[2].plot(df['time_all_dt'],df['rmse_either'],
                    label="RMSE ice only",
                    color='sienna',linestyle='--')
        axs[2].xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        axs[2].xaxis.set_major_locator(mdates.DayLocator())
        axs[2].grid(True, color = 'grey', linestyle = '--', linewidth = 0.5)
        #axs[0].set_title('RMSE (%)')
        ymax = (math.ceil(np.nanmax(df['rmse_either'])/10))*10
        axs[2].set_ylim([0,ymax])
        axs[2].set_xlim([df['time_all_dt'].min(),df['time_all_dt'].max()])
        axs[2].legend(loc="upper center",fontsize=14,ncol=2,frameon=False,
                      bbox_to_anchor=(0.26, 1.23))
        axs[2].set_ylabel('RMSE (%)', fontsize=16)
        axs[2].set_xticks(axs[2].get_xticks()[::xtickspace])
        axs[2].set_yticks(axs[2].get_yticks()[::1])
        axs[2].tick_params(axis='both', which='major', labelsize=14)
        plt.gcf().autofmt_xdate()

        axs[3].plot(df['time_all_dt'], df['hitrate_mod'],
                    label = "Model overlap percent",
                    color='green',linewidth=1.5)
        axs[3].plot(df['time_all_dt'], df['hitrate_obs'],
                    label = "Obs overlap percent",
                    color='lightseagreen',linestyle = '--',linewidth=1.5)
        axs[3].xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        axs[3].xaxis.set_major_locator(mdates.DayLocator())
        axs[3].grid(True, color = 'grey', linestyle = '--', linewidth = 0.5)
        axs[3].set_ylim([0,100])
        axs[3].set_xlim([df['time_all_dt'].min(),df['time_all_dt'].max()])
        axs[3].legend(loc="upper center",fontsize=14,ncol=2,frameon=False,
                      bbox_to_anchor=(0.42, 1.23))
        axs[3].set_xlabel('Time', fontsize=16)
        axs[3].set_ylabel('Overlap (%)',fontsize=16)
        axs[3].set_xticks(axs[3].get_xticks()[::xtickspace])
        axs[3].set_yticks(axs[3].get_yticks()[::1])
        axs[3].tick_params(axis='both', which='major', labelsize=14)
        plt.gcf().autofmt_xdate()

        fig.tight_layout()
        fig.align_ylabels()

        savenameplot=str(prop.ofs)+'_'+titlewc+'_concseries_ice'+'.png'
        savepath = os.path.join(prop.visuals_stats_ice_path,savenameplot)
        fig.savefig(savepath,format='png',dpi=1000,bbox_inches='tight')

    elif prop.plottype == 'interactive':
        nrows = 3
        ##Do stats time series
        fig = make_subplots(
        rows=nrows, cols=len(prop.whichcasts), vertical_spacing = 0.055,
        subplot_titles=(prop.whichcasts),
        shared_xaxes=True,
        )
        showlegend = [True,False]
        for i in range(len(prop.whichcasts)):
            prop.whichcast = prop.whichcasts[i]
            if prop.whichcast == 'nowcast':
                if (os.path.isfile(
                        f"{prop.data_skill_stats_path}/skill_{prop.ofs}_"
                        f"iceonoff_{prop.whichcast}.csv") is False):
                    logger.error(
                        "Ice on/off csv not found in %s. ",
                        prop.data_skill_stats_path
                        )
                # If csv is there, proceed
                else:
                    #counter = counter + 1 #Keep track of # whichcasts
                    df2 = pd.read_csv(
                        r"" +  f"{prop.data_skill_stats_path}/skill_{prop.ofs}_"
                          f"iceonoff_{prop.whichcast}.csv")
                    logger.info(
                        "Ice on/off csv found in %s",
                        prop.data_skill_stats_path
                        )

            ###subplot 1
            fig.add_trace(go.Scatter(x=dflist[i].time_all_dt,
                                     y=dflist[i].obs_meanicecover,
                                     name='GLSEA analysis mean',
                                     hovertemplate='%{y:.2f}',
                                     mode='lines',
                                     legendgroup = '1',
                                     showlegend = showlegend[i],
                                     line=dict(
                                         color='rgba(' +\
                                             str(mcolors.to_rgb('dodgerblue'))
                                             .strip('()') + ', ' + str(1) +')',
                                         width=2)
                                     ), row=1, col=i+1)
            fig.add_trace(go.Scatter(x=dflist[i].time_all_dt,
                                     y=dflist[i].obs_meanicecover+
                                     dflist[i].obs_stdmic,
                                     name='GLSEA +1 sigma',
                                     #hovertemplate='%{y:.2f}',
                                     hoverinfo='skip',
                                     #hovertemplate='%{y:.2f}',
                                     mode='lines',
                                     #marker=dict(color="#444"),
                                     #alpha=0.5,
                                     line=dict(
                                         color='rgba(' +\
                                             str(mcolors.to_rgb('dodgerblue'))
                                             .strip('()') + ', '+str(0.1)+ ')',
                                         width=0),
                                     showlegend=False
                                     ), row=1, col=i+1)
            fig.add_trace(go.Scatter(x=dflist[i].time_all_dt,
                                     y=dflist[i].obs_meanicecover-
                                      dflist[i].obs_stdmic,
                                      name='GLSEA -1 sigma',
                                      #hovertemplate='%{y:.2f}',
                                      hoverinfo='skip',
                                      #marker=dict(color="#444"),
                                      line=dict(width=0),
                                      mode='lines',
                                      fillcolor='rgba(' +\
                                          str(mcolors.to_rgb('dodgerblue'))
                                          .strip('()') + ', ' + str(0.1) + ')',
                                      fill='tonexty',
                                      #alpha=0.1,
                                      showlegend=False
                                      ), row=1, col=i+1)
            fig.add_trace(go.Scatter(x=dflist[i].time_all_dt,
                                     y=dflist[i].mod_meanicecover,
                                     name='OFS mean',
                                     hovertemplate='%{y:.2f}',
                                     mode='lines',
                                     legendgroup = '1',
                                     showlegend = showlegend[i],
                                     line=dict(
                                         color='rgba(' +\
                                             str(mcolors.to_rgb('magenta'))
                                             .strip('()') + ', '+ str(1) + ')',
                                         width=2)
                                     ), row=1, col=i+1)
            fig.add_trace(go.Scatter(x=dflist[i].time_all_dt,
                                     y=dflist[i].mod_meanicecover+
                                     dflist[i].mod_stdmic,
                                     name='OFS +1 sigma',
                                     #hovertemplate='%{y:.2f}',
                                     hoverinfo='skip',
                                     #hovertemplate='%{y:.2f}',
                                     mode='lines',
                                     #marker=dict(color="#444"),
                                     #alpha=0,
                                     line=dict(
                                         color='rgba(' +\
                                             str(mcolors.to_rgb('magenta'))
                                             .strip('()') + ', ' +\
                                                 str(0.1) + ')',
                                         width=0),
                                     showlegend=False
                                     ), row=1, col=i+1)
            fig.add_trace(go.Scatter(x=dflist[i].time_all_dt,
                                     y=dflist[i].mod_meanicecover-
                                      dflist[i].mod_stdmic,
                                      name='OFS -1 sigma',
                                      #hovertemplate='%{y:.2f}',
                                      hoverinfo='skip',
                                      #marker=dict(color="#444"),
                                      line=dict(width=0),
                                      mode='lines',
                                      fillcolor='rgba(' +\
                                          str(mcolors.to_rgb('magenta'))
                                          .strip('()') + ', ' + str(0.1) + ')',
                                      #alpha = 0.1,
                                      fill='tonexty',
                                      showlegend=False
                                      ), row=1, col=i+1)
            fig.add_trace(go.Scatter(x=dflist[i].time_all_dt,
                                     y=dflist[i].icecover_hist,
                                     name='Climatology (1973-2024)',
                                     hovertemplate='%{y:.2f}',
                                     mode='lines',
                                     legendgroup = '1',
                                     showlegend = showlegend[i],
                                     line=dict(
                                         color='rgba(0,0,0,1)',
                                         width=2)
                                     ), row=1, col=i+1)
            # Ice on/off dates
            #df_filt = df[df['time_all_dt']==df2['Ice onset'][0]]
            if prop.whichcast == 'nowcast':
                df = pd.DataFrame(dflist[i])
                df['time_all_dt'] = pd.to_datetime(df['time_all_dt'])
                if df2['Ice onset'].notna()[0]:
                    obs_iceon = pd.to_datetime(df2.iloc[0]['Ice onset'])
                    df_ind_obs_on = df[df['time_all_dt']==obs_iceon].index
                    logger.info("obs ice on index: %s", df_ind_obs_on.values)
                    logger.info("obs ice on value: %s", obs_iceon)
                    #obs_iceon = obs_iceon.timestamp() * 1000
                    fig.add_trace(go.Scatter(x=df['time_all_dt']
                                              [df_ind_obs_on],
                                              y=df['obs_meanicecover']
                                              [df_ind_obs_on],
                                  name='GLSEA ice onset',
                                  hovertemplate='%{x}',
                                  #hoverinfo='skip',
                                  mode='markers',
                                  marker=dict(
                                      color ='rgba(' +\
                                          str(mcolors.to_rgb('dodgerblue'))
                                          .strip('()') + ', ' + str(0.6) + ')',
                                      size=10,
                                      line=dict(
                                          color='black',
                                          width=0.5)),
                                #legendgroup = '1',
                                showlegend=False,
                                ), row=1, col=i+1)
                if df2['Ice thaw'].notna()[0]:
                    obs_iceoff = pd.to_datetime(df2.iloc[0]['Ice thaw'])
                    df_ind_obs_off = df[df['time_all_dt']==obs_iceoff].index
                    logger.info("obs ice off index: %s", df_ind_obs_off.values)
                    logger.info("obs ice off value: %s", obs_iceoff)
                    #obs_iceon = obs_iceon.timestamp() * 1000
                    fig.add_trace(go.Scatter(x=df['time_all_dt']
                                              [df_ind_obs_off],
                                              y=df['obs_meanicecover']
                                              [df_ind_obs_off],
                                  name='GLSEA ice thaw',
                                  hovertemplate='%{x}',
                                  #hoverinfo='skip',
                                  mode='markers',
                                  marker=dict(
                                      color ='rgba(' +\
                                          str(mcolors.to_rgb('dodgerblue'))
                                          .strip('()') + ', ' + str(0.6) + ')',
                                      size=14,
                                      symbol='x',
                                      line=dict(
                                          color='black',
                                          width=0.5)),
                                #legendgroup = '1',
                                showlegend=False,
                                ), row=1, col=i+1)
                if df2['Ice onset'].notna()[1]:
                    mod_iceon = pd.to_datetime(df2.iloc[1]['Ice onset'])
                    df_ind_mod_on = df[df['time_all_dt']==mod_iceon].index
                    logger.info("mod ice on index: %s", df_ind_mod_on.values)
                    logger.info("mod ice on value: %s", mod_iceon)
                    #obs_iceon = obs_iceon.timestamp() * 1000
                    fig.add_trace(go.Scatter(x=df['time_all_dt']
                                              [df_ind_mod_on],
                                              y=df['mod_meanicecover']
                                              [df_ind_mod_on],
                                  name='OFS ice onset',
                                  hovertemplate='%{x}',
                                  #hoverinfo='skip',
                                  mode='markers',
                                  marker=dict(
                                      color ='rgba(' +\
                                          str(mcolors.to_rgb('deeppink'))
                                          .strip('()') + ', ' + str(0.6) + ')',
                                      size=10,
                                      line=dict(
                                          color='black',
                                          width=0.5)),
                                #legendgroup = '1',
                                showlegend=False,
                                ), row=1, col=i+1)
                if df2['Ice thaw'].notna()[1]:
                    mod_iceoff = pd.to_datetime(df2.iloc[1]['Ice thaw'])
                    df_ind_mod_off = df[df['time_all_dt']==mod_iceoff].index
                    logger.info("mod ice off index: %s", df_ind_mod_off.values)
                    logger.info("mod ice off value: %s", mod_iceoff)
                    #obs_iceon = obs_iceon.timestamp() * 1000
                    fig.add_trace(go.Scatter(x=df['time_all_dt']
                                              [df_ind_mod_off],
                                              y=df['mod_meanicecover']
                                              [df_ind_mod_off],
                              name='OFS ice thaw',
                              hovertemplate='%{x}',
                              #hoverinfo='skip',
                              mode='markers',
                              marker=dict(
                                  color ='rgba(' +\
                                      str(mcolors.to_rgb('deeppink'))
                                      .strip('()') + ', ' + str(0.6) + ')',
                                  size=14,
                                  symbol='x',
                              line=dict(
                                  color='black',
                                  width=0.5)),
                              #legendgroup = '1',
                              showlegend=False,
                              ), row=1, col=i+1)
                if df2['Ice onset'].notna()[2]:
                    clim_iceon = pd.to_datetime(df2.iloc[2]['Ice onset'])
                    df_ind_clim_on = df[df['time_all_dt']==clim_iceon].index
                    logger.info("clim ice on index: %s", df_ind_clim_on.values)
                    logger.info("clim ice on value: %s", clim_iceon)
                    #obs_iceon = obs_iceon.timestamp() * 1000
                    fig.add_trace(go.Scatter(x=df['time_all_dt']
                                              [df_ind_clim_on],
                                              y=df['icecover_hist']
                                              [df_ind_clim_on],
                                  name='Climatology ice onset',
                                  hovertemplate='%{x}',
                                  #hoverinfo='skip',
                                  mode='markers',
                                  marker=dict(
                                      color ='rgba(' +\
                                          str(mcolors.to_rgb('black'))
                                          .strip('()') + ', ' + str(0.5) + ')',
                                      size=10,
                                      line=dict(
                                          color='black',
                                          width=0.5)),
                                  #legendgroup = '1',
                                  showlegend=False,
                                  ), row=1, col=i+1)
                if df2['Ice thaw'].notna()[2]:
                    clim_iceoff = pd.to_datetime(df2.iloc[2]['Ice thaw'])
                    df_ind_clim_off = df[df['time_all_dt']==clim_iceoff].index
                    logger.info("clim ice off index: %s", df_ind_clim_off.values)
                    logger.info("clim ice off value: %s", clim_iceoff)
                    #obs_iceon = obs_iceon.timestamp() * 1000
                    fig.add_trace(go.Scatter(x=df['time_all_dt']
                                              [df_ind_clim_off],
                                              y=df['icecover_hist']
                                              [df_ind_clim_off],
                                  name='Climatology ice thaw',
                                  hovertemplate='%{x}',
                                  #hoverinfo='skip',
                                  mode='markers',
                                  marker=dict(
                                      color ='rgba(' +\
                                          str(mcolors.to_rgb('black'))
                                          .strip('()') + ', ' + str(0.5) + ')',
                                      size=14,
                                      symbol='x',
                                      line=dict(
                                          color='black',
                                          width=0.5)),
                                  #legendgroup = '1',
                                  showlegend=False,
                                  ), row=1, col=i+1)

        ###subplot 2
        for i in range(len(prop.whichcasts)):
            fig.add_trace(go.Scatter(x=dflist[i].time_all_dt,
                                     y=dflist[i].rmse_all,
                                     name='RMSE',
                                     hovertemplate='%{y:.2f}',
                                     mode='lines',
                                     legendgroup = '2',
                                     showlegend = showlegend[i],
                                     line=dict(
                                         color='rgba(' +\
                                             str(mcolors.to_rgb('red'))
                                             .strip('()') + ', ' + str(1)+ ')',
                                         width=2),
                                     #showlegend=False
                                     ), row=2, col=i+1)
            fig.add_trace(go.Scatter(x=dflist[i].time_all_dt,
                                     y=dflist[i].rmse_either,
                                     name='RMSE, ice only',
                                     hovertemplate='%{y:.2f}',
                                     legendgroup = '2',
                                     showlegend = showlegend[i],
                                     mode='lines',
                                     line=dict(
                                         color='rgba(' +\
                                             str(mcolors.to_rgb('sienna'))
                                             .strip('()') + ', ' + str(1) +')',
                                         width=2),
                                     #showlegend=False
                                     ), row=2, col=i+1)
        ###subplot 3
        for i in range(len(prop.whichcasts)):
            fig.add_trace(go.Scatter(x=dflist[i].time_all_dt,
                                     y=dflist[i].SS,
                                     name='Skill score',
                                     hovertemplate='%{y:.2f}',
                                     mode='lines',
                                     legendgroup = '3',
                                     showlegend = showlegend[i],
                                     line=dict(
                                         color='rgba(' +\
                                             str(mcolors.to_rgb('darkviolet'))
                                             .strip('()') + ', ' + str(1) +')',
                                         width=2)    ,
                                     #showlegend=False
                                     ), row=3, col=i+1)
            fig.add_hline(y=0, row=3, col=i+1)

        # fig.add_trace(go.Scatter(df['Date'], y=[7000, 8000, 9000]),
        #           row=2, col=2)

        ##update axes
        title_text = [
            ['Mean ice conc. (%)',None],
            ['RMSE (%)',None],
            ['Skill score',None]
                     ]
        for i in range(len(prop.whichcasts)):
            fig.update_yaxes(title_text=title_text[0][i],
                             title_font=dict(size=16, color='black'),
                             range=[0, 100],
                             row=1,col=i+1)
            fig.update_yaxes(title_text=title_text[1][i],
                             title_font=dict(size=16, color='black'),
                             range=[0, 100],
                             row=2,col=i+1)
            ss_ymin = np.floor(np.nanmin(dflist[i].SS))
            fig.update_yaxes(title_text=title_text[2][i],
                             title_font=dict(size=16, color='black'),
                             range=[ss_ymin, 1],
                             row=3,col=i+1)

        fig.update_yaxes(showline=True, linewidth=1, linecolor='black',
                         mirror=True)
        fig.update_xaxes(showline=True,
                         linewidth=1,
                         linecolor='black',
                         mirror=True,
                         tickfont=dict(size=16),
                         dtick=86400000*xtickspace,
                         tickformat="%m/%d",
                         range=[datetime.strptime(prop.start_date_full,
                                "%Y-%m-%dT%H:%M:%SZ"), time_all[-1]],
                         tick0 = datetime.strptime(prop.start_date_full,
                                "%Y-%m-%dT%H:%M:%SZ")
                         )
        fig.update_xaxes(title_text="Time",
                         title_font=dict(size=16, color='black'),
                         #range=[-1, 1],
                         row=3)
        ##update layout
        if len(prop.whichcasts) == 1:
            figwidth = 900
        elif len(prop.whichcasts) > 1:
            figwidth = 1200
        else:
            figwidth = 900
        fig.update_layout(
            title=dict(
                 text=figtitle,
                 font=dict(size=20, color='black'),
                 y=1,  # new
                 x=0.5, xanchor='center', yanchor='top'),
            yaxis1 = dict(tickfont = dict(size=16)),
            yaxis2 = dict(tickfont = dict(size=16)),
            yaxis3 = dict(tickfont = dict(size=16)),
            yaxis4 = dict(tickfont = dict(size=16)),
            yaxis5 = dict(tickfont = dict(size=16)),
            yaxis6 = dict(tickfont = dict(size=16)),
            transition_ordering="traces first",
            dragmode="zoom",
            hovermode="x unified",
            height=700,
            width=figwidth,
            legend_tracegroupgap = 140,
            xaxis_tickangle=-45,
            template="plotly_white",
            margin=dict(
                t=50, b=50),
            legend=dict(
                font=dict(size=16, color='black'),
                bgcolor = 'rgba(0,0,0,0)',
                #orientation="h",
                #yanchor="top",
                #y=1,
                #xanchor="left",
                #x=0.0
                )
            )
        ##write to file
        savename = prop.ofs + '_' + titlewc +\
            '_iceconcseries' + '.html'
        savepath = os.path.join(
            prop.visuals_stats_ice_path,savename)
        fig.write_html(
            savepath,
            auto_open=False)

        ######
        ##Now do ice extents plot
        ######
        nrows = 2
        fig = make_subplots(
        rows=nrows, cols=len(prop.whichcasts), vertical_spacing = 0.055,
        subplot_titles=(prop.whichcasts),
        shared_xaxes=True,
        #title = ('CBOFS nowcast sea surface temp, 5/29/24 - 5/30/24')
        )
        for i in range(len(prop.whichcasts)):
            prop.whichcast = prop.whichcasts[i]
            ##subplot 1
            fig.add_trace(go.Scatter(x=dflist[i].time_all_dt,
                                     y=dflist[i].obs_extent,
                                      name='GLSEA analysis ice extent',
                                      hovertemplate='%{y:.2f}',
                                      mode='lines',
                                      legendgroup = '1',
                                      showlegend = showlegend[i],
                                      line=dict(
                                          color='rgba(' +\
                                              str(mcolors.to_rgb('dodgerblue'))
                                              .strip('()') + ', ' + str(1)+')',
                                          width=2)
                                      ), row=1, col=i+1)
            fig.add_trace(go.Scatter(x=dflist[i].time_all_dt,
                                     y=dflist[i].mod_extent,
                                      name='OFS ice extent',
                                      hovertemplate='%{y:.2f}',
                                      legendgroup = '1',
                                      showlegend = showlegend[i],
                                      #hoverinfo='skip',
                                      #hovertemplate='%{y:.2f}',
                                      mode='lines',
                                      #marker=dict(color="#444"),
                                      #alpha=0.5,
                                      line=dict(
                                          color='rgba(' +\
                                              str(mcolors.to_rgb('magenta'))
                                              .strip('()') + ', ' + str(1)+')',
                                          width=2),
                                      #showlegend=False
                                      ), row=1, col=i+1)
            fig.add_trace(go.Scatter(x=dflist[i].time_all_dt,
                                     y=dflist[i].mod_extent
                                      -dflist[i].obs_extent,
                                      name='Ice extent error',
                                      hovertemplate='%{y:.2f}',
                                      legendgroup = '1',
                                      showlegend = showlegend[i],
                                      mode='lines',
                                      line=dict(
                                          color='rgba(' +\
                                              str(mcolors.to_rgb('black'))
                                              .strip('()') + ', ' + str(1)+')',
                                          width=2)
                                      ), row=1, col=i+1)
        ##subplot 2
        for i in range(len(prop.whichcasts)):
            fig.add_trace(go.Scatter(x=dflist[i].time_all_dt,
                                     y=dflist[i].csi_all,
                                      name='Critical Success Index',
                                      hovertemplate='%{y:.2f}',
                                      legendgroup = '2',
                                      showlegend = showlegend[i],
                                      mode='lines',
                                      line=dict(
                                          color='rgba(' +\
                                              str(mcolors.to_rgb(
                                                  'lightseagreen'))
                                              .strip('()') + ', ' + str(1)+')',
                                          width=2),
                                      #showlegend=False
                                      ), row=2, col=i+1)

        ##update axes
        title_text = [
            ['Ice extent (%)',None],
            ['Critical Success Index',None]
                     ]
        for i in range(len(prop.whichcasts)):
            fig.update_yaxes(title_text=title_text[0][i],
                              title_font=dict(size=16, color='black'),
                              range=[0, 100],
                              row=1, col=i+1)
            fig.update_yaxes(title_text=title_text[1][i],
                              title_font=dict(size=16, color='black'),
                              range=[0, 1],
                              row=2, col=i+1)
        fig.update_yaxes(showline=True, linewidth=1, linecolor='black',
                          mirror=True)
        fig.update_xaxes(showline=True,
                          linewidth=1,
                          linecolor='black',
                          mirror=True,
                          tickfont=dict(size=16),
                          dtick=86400000*xtickspace,
                          tickformat="%m/%d",
                          range=[datetime.strptime(prop.start_date_full,
                                "%Y-%m-%dT%H:%M:%SZ"), time_all[-1]]
                          )
        fig.update_xaxes(title_text="Time",
                          title_font=dict(size=16, color='black'),
                          #range=[-1, 1],
                          row=2)
        ##update layout
        if len(prop.whichcasts) == 1:
            figwidth = 900
        elif len(prop.whichcasts) > 1:
            figwidth = 1200
        else:
            figwidth = 900
        fig.update_layout(
            title=dict(
                  text=figtitle,
                  font=dict(size=20, color='black'),
                  y=1,  # new
                  x=0.5, xanchor='center', yanchor='top'),
            yaxis1 = dict(tickfont = dict(size=16)),
            yaxis2 = dict(tickfont = dict(size=16)),
            yaxis3 = dict(tickfont = dict(size=16)),
            yaxis4 = dict(tickfont = dict(size=16)),
            legend_tracegroupgap = 130,
            transition_ordering="traces first",
            dragmode="zoom",
            hovermode="x unified",
            height=500,
            width=figwidth,
            xaxis_tickangle=-45,
            template="plotly_white",
                margin=dict(t=50, b=50),
                legend=dict(
                font=dict(size=16, color='black'),
                bgcolor = 'rgba(0,0,0,0)',
                #orientation="h", yanchor="top",
                #y=1, xanchor="left", x=0.0
                )
                )
        ##write to file
        savename = prop.ofs + '_' + titlewc +\
            '_iceextentseries' + '.html'
        filepath = os.path.join(
            prop.visuals_stats_ice_path, savename)
        fig.write_html(
            filepath,
            auto_open=False)


def create_1dplot_ice(prop,inventory,time_all,logger):
    '''Create 1D time series at obs station locations, and save a plot'''

    #First load nowcast and/or forecast stats from file!
    counter = 0
    for i in range(0,len(inventory['ID'])):
        dflist = []
        for cast in prop.whichcasts:
            df = None
            # Here we try to open the paired data set(s).
            # If not found, return without making plot
            prop.whichcast = cast.lower()
            if (os.path.isfile(
                  f"{prop.data_skill_ice1dpair_path}/"
                  f"{prop.ofs}_"
                  f"iceconc_"
                  f"{inventory.at[i,'ID']}_"
                  f"{inventory.at[i,'NODE']}_"
                  f"{prop.whichcast}_pair.int"
                    ) is False):
                logger.error(
                    "Ice 1D time series .int NOT found in %s. ",
                    prop.data_skill_ice1dpair_path)

            # If csv is there, proceed
            else:
                counter = counter + 1 #Keep track of # whichcasts
                df = pd.read_csv(
                    r"" + f"{prop.data_skill_ice1dpair_path}/"
                          f"{prop.ofs}_"
                          f"iceconc_"
                          f"{inventory.at[i,'ID']}_"
                          f"{inventory.at[i,'NODE']}_"
                          f"{prop.whichcast}_pair.int"
                     )
                logger.info(
                    "Ice 1D time series .int for %s found in %s",
                    inventory.at[i,'ID'],
                    prop.data_skill_ice1dpair_path)

                df['DateTime'] = pd.to_datetime(df['DateTime'])
            # Put nowcast & forecast together in list
            if df is not None:
                dflist.append(df)

        # Date range
        datestrend = str(time_all[len(time_all)-1]).split()
        datestrbegin = str(time_all[0]).split()

        # Do figure stuff, enjoy
        if prop.plottype == 'static':

            # Figure out what x-axis tick spacing should be --
            # it depends on length of run so axis doesn't overcrowd!
            #dayselapsed=(time_all_dt[len(time_all_dt)-1]-time_all_dt[0]).days
            dayselapsed = (df.at[df.index[-1],'DateTime'] -\
                           df.at[df.index[0],'DateTime']).days
            xtickspace = dayselapsed/10
            if xtickspace >= 1:
                xtickspace = math.floor(xtickspace)
            elif xtickspace < 1:
                xtickspace = math.ceil(xtickspace)

            # Set forecast and nowcast colors
            castcolors = ['dodgerblue','mediumseagreen']

            title = prop.ofs.upper()+' '+datestrbegin[0]+' to '+\
                datestrend[0]
            fig, axs = plt.subplots()
            fig.set_figheight(5)
            fig.set_figwidth(10)

            textbox = '\n Station ' + inventory.at[i,'ID']+' '+\
                inventory.at[i,'Name']
            fig.suptitle(title+textbox, fontsize=20)

            axs.plot(dflist[0].DateTime, dflist[0].OBS, label = "GLSEA",
                        color='deeppink',linewidth=1.5)
            for j in range(len(prop.whichcasts)):
                # Change name of model time series to make more explanatory
                if prop.whichcasts[j][0].capitalize() == 'F':
                    seriesname='Model Forecast Guidance'
                elif prop.whichcasts[j].capitalize() == 'Nowcast':
                    seriesname='Model Nowcast Guidance'
                else:
                    seriesname=prop.whichcasts[j].capitalize() + " Guidance"
                axs.plot(dflist[j].DateTime,
                         dflist[j].OFS,
                         label=seriesname,
                         color=castcolors[j])
                axs.fill_between(dflist[j].DateTime,
                                 np.subtract(dflist[j].OFS,dflist[j].STDEV),
                                 np.add(dflist[j].OFS,dflist[j].STDEV),
                                 alpha=0.2,
                                 linewidth=0,
                                 facecolor=castcolors[j])
            #axs.text(0.5, 0.5, textbox, color='black',
            #bbox=dict(facecolor='none', edgecolor='black'))
            axs.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d/%Y'))
            axs.xaxis.set_major_locator(mdates.DayLocator())
            axs.grid(True, color = 'grey', linestyle = '--', linewidth = 0.5)
            #axs[2].set_title('Mean ice cover')
            axs.set_ylim([0,100])
            axs.set_xlim([min(df['DateTime']),max(df['DateTime'])])
            axs.legend(loc="upper left",fontsize=14,ncol=1,frameon=False,)
            axs.set_ylabel('Ice cover (%)',fontsize=16)
            axs.set_xticks(axs.get_xticks()[::xtickspace])
            axs.set_yticks(axs.get_yticks()[::1])
            axs.tick_params(axis='both', which='major', labelsize=14)
            plt.gcf().autofmt_xdate()

            fig.tight_layout()
            #plt.show()
            naming_ws = "_".join(prop.whichcasts)
            savename = f"{prop.ofs}_{inventory.at[i,'ID']}_{inventory.at[i,'NODE']}_iceconc_scalar2_{naming_ws}.png"
            savepath = os.path.join(prop.visuals_1d_ice_path,savename)
            fig.savefig(savepath, format='png', dpi=800, bbox_inches='tight')
            fig.align_ylabels()
            #plt.plot(time_all_dt,modeldata[:,mod_rowcol[20]])

        elif prop.plottype == 'interactive':
            plotting_scalar.oned_scalar_plot(dflist,
                                            'ice_conc',
                                            [inventory.at[i,'ID'],
                                             inventory.at[i,'Name'],
                                             inventory.at[i,'Source']],
                                            inventory.at[i,'NODE'],
                                            prop, logger
                                            )
