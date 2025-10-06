# -*- coding: utf-8 -*-
"""
Created on Mon Mar 24 21:35:41 2025

@author: patrick.limber
"""

import os
import numpy as np
from numpy import isnan
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

def make_ice_boxplots(ice_o,ice_m,time_all_dt,prop,logger):
    '''
    Boxplots of RMSE! This writes an interactive plotly .html plot with
    boxplots of RMSE for ranges of ice concentration to show if the model
    is 'better' at predicting lower or higher values of ice concentration
    (or neither!)
    '''

    ice_o_copy2 = np.copy(ice_o)
    ice_m_copy2 = np.copy(ice_m)
    ## Do pre-processing, if necessary
    thresholds = np.array([[1,25],[50,75]])
    ice_o_copy2[ice_o_copy2<thresholds[0][0]] = np.nan
    ice_m_copy2[ice_m_copy2<thresholds[0][0]] = np.nan
    # Do figure stuff, enjoy
    datestrend = str(time_all_dt[len(time_all_dt)-1]).split()
    datestrbegin = str(time_all_dt[0]).split()
    figtitle = (prop.ofs.upper()+' '+prop.whichcast+\
                  ' RMSE box/violin plots, ' +
                  datestrbegin[0]+ ' - ' + datestrend[0])
    counter = -1
    for i in range(0,int(len(thresholds))):
        for k in range(0,int(len(thresholds))):
            counter = counter + 1
            ice_o_thresh = []
            ice_m_thresh = []
            data = []
            data_all = []
            for j in range(0,len(time_all_dt)):
                o_temp = np.array(ice_o[j,:,:])
                m_temp = np.array(ice_m[j,:,:])
                o_temp[o_temp<thresholds[0][0]] = np.nan
                m_temp[m_temp<thresholds[i][k]] = np.nan
                if k == 0:
                    m_temp[m_temp>=thresholds[i][k+1]] =\
                        np.nan
                elif i == 0 and k > 0:
                    m_temp[m_temp>=thresholds[i+1][k-1]] =\
                        np.nan
                ice_o_thresh.append(o_temp)
                ice_m_thresh.append(m_temp)

            # Stack data
            data_all = np.array(np.sqrt(np.nanmean(
                ((ice_m_copy2-ice_o_copy2)**2),axis=0)))
            ice_o_thresh = np.stack(ice_o_thresh)
            ice_m_thresh = np.stack(ice_m_thresh)
            data = np.array(np.nanmean(
                ((ice_m_thresh-ice_o_thresh)**2),axis=0))
            data = np.sqrt(data)
            # Put all data into a loooooooong 1D array
            y = data.reshape(1,-1)
            y_all = data_all.reshape(1,-1)
            badnans = ~isnan(y)
            badnans = ~isnan(y_all)
            y = y[badnans]
            y_all = y_all[badnans]
            temp_thresh = thresholds + (thresholds[1][1]-\
                                        thresholds[1][0])
            temp_thresh[0][0] = temp_thresh[0][0] - 1
            if counter == 0:
                df = pd.DataFrame(
                    {
                        "RMSE": y,
                        "Ice concentration": str(thresholds[i][k]) + '-' +\
                            str(temp_thresh[i][k]) + '%',
                    }
                )
                df2 = pd.DataFrame(
                    {
                        "RMSE": y_all,
                        "Ice concentration": '1-100%',
                    }
                )
                df_all = pd.concat([df2, df], ignore_index=True)
            else:
                df = pd.DataFrame(
                    {
                        "RMSE": y,
                        "Ice concentration": str(thresholds[i][k]) + '-' +\
                            str(temp_thresh[i][k]) + '%',
                    }
                )
                df_all = pd.concat([df_all, df], ignore_index=True)

    fig = go.Figure()
    fig = px.violin(df_all,
                 x="Ice concentration",
                 y="RMSE",
                 box=True,
                 #points="all",
                 #cut=0,
                 color="Ice concentration",
                 template="plotly_white"
                 )
    fig.update_layout(
                      xaxis_title_font_size=20,
                      yaxis_title_font_size=20,
                      xaxis = dict(tickfont = dict(size=15)),
                      yaxis = dict(tickfont = dict(size=15)),
                      title = figtitle
                      )

    savename = str(prop.ofs) + '_' + prop.whichcast + '_' +\
        'rmseboxplots.html'
    savepath = os.path.join(
        prop.visuals_stats_ice_path,savename)
    fig.write_html(
        savepath,
        auto_open=False
        )
