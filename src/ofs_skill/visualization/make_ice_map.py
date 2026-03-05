"""
Created on Mon Jan 13 08:31:05 2025

@author: PL

Main map-making function for do_iceskill.py. This function takes in stacked
map data (2D arrays), and loops through each array to make subplots and JSON
maps.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime

import matplotlib as mpl
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
from mpl_toolkits.basemap import Basemap

# Import from ofs_skill package
import ofs_skill.visualization.processing_2d as processing_2d


def make_ice_map(
    prop, lon_o, lat_o, x_o, y_o, mapdata, time,
    mapstuff, logger,
):
    '''
    Main map-making function, makes figures with subplotted
    maps. Use 'mapstuff' to specify what type of map so
    titles, labels, etc. will be correct.
    '''

    makestatic = False
    makejson = True

    brdr = 0.2

    # Generate titles and colorbar properties
    datestr = str(time).split()
    startdatetime = datetime.strptime(
        prop.start_date_full, '%Y-%m-%dT%H:%M:%SZ')
    enddatetime = datetime.strptime(prop.end_date_full, '%Y-%m-%dT%H:%M:%SZ')
    timeelapsed = enddatetime - startdatetime
    dayselapsed = timeelapsed.days + 1
    logger.info('Making maps for %s', mapstuff)
    if mapstuff == 'rmse means':
        logger.info('Configuring %s maps', mapstuff)
        if makestatic:
            datestrstart = prop.start_date_full.split('T')[0]
            rmsemaptitle = prop.ofs.upper() +\
                ' RMSE, ' + datestrstart + ' - ' + datestr[0]
            obsmaptitle = prop.ofs.upper() +\
                ' mean observed ice coverage, ' + ' ' +\
                datestrstart + ' - ' + datestr[0]
            modmaptitle = prop.ofs.upper() +\
                ' mean modeled ice coverage, ' +\
                ' ' + datestrstart + ' - ' + datestr[0]
            minmax_vals = np.array([
                [0, 100],
                [0, 100],
                [0, 100],
            ])
            all_titles = [
                obsmaptitle,
                modmaptitle,
                rmsemaptitle,
            ]
            cbartitles = [
                'Ice coverage (%)',
                'Ice coverage (%)',
                'RMSE (%)',
            ]
            cmaptypes = [
                'terrain',
                'terrain',
                'plasma',
            ]
            savenamemap = str(prop.ofs) + '_' + str(prop.whichcast) +\
                '_map_' + 'conc_' + '.png'
            fig, axs = plt.subplots(len(mapdata), 1)
        if makejson:
            savenamejson = str(prop.ofs) + '_' + str(prop.whichcast) + '_' +\
                prop.start_date_full.split('T')[0] + '--' +\
                prop.end_date_full.split('T')[0] + '_map_'
            jsonmaptype = [
                'mean_iceconc_obs',
                'mean_iceconc_mod',
                'rmse',
            ]
        logger.info('Done configuring %s maps', mapstuff)
    elif mapstuff == 'daily':
        logger.info('Configuring %s maps', mapstuff)
        if makestatic:
            diffmaptitle = prop.ofs.upper() +\
                ' Modeled - observed ice coverage, ' + datestr[0]
            obsmaptitle = prop.ofs.upper() +\
                ' observed ice coverage, ' + ' ' + datestr[0]
            modmaptitle = prop.ofs.upper() +\
                ' modeled ice coverage, ' + ' ' + datestr[0]
            minmax_vals = np.array([
                [0, 100],
                [0, 100],
                [-100, 100],
            ])
            all_titles = [
                obsmaptitle,
                modmaptitle,
                diffmaptitle,
            ]
            cbartitles = [
                'Ice coverage (%)',
                'Ice coverage (%)',
                'Error (%)',
            ]
            cmaptypes = [
                'cool',
                'cool',
                'seismic',
            ]
            savenamemap = ('%02d' % time.month) + '-' + ('%02d' % time.day) +\
                '-' + ('%02d' % time.year) + '-' +\
                ('%02d' % time.hour) + 'Z-' +\
                str(prop.ofs) + '_' + str(prop.whichcast) +\
                '_daily_' + 'map' + '.png'
            fig, axs = plt.subplots(len(mapdata), 1)

        if makejson:
            savenamejson = ('%02d' % time.month) + '-' + ('%02d' % time.day) +\
                '-' + ('%02d' % time.year) + '-' +\
                ('%02d' % time.hour) + 'Z-' + str(prop.ofs) + '_' +\
                str(prop.whichcast)
            jsonmaptype = [
                '_daily_obs',
                '_daily_mod',
                '_daily_diff',
            ]

        logger.info('Done configuring %s maps', mapstuff)
    elif mapstuff == 'extents':
        logger.info('Configuring %s maps', mapstuff)
        if makestatic:
            datestrstart = prop.start_date_full.split('T')[0]
            obsmaptitle = prop.ofs.upper() +\
                ' total observed ice days, ' + '\n' + datestrstart +\
                ' - ' + datestr[0] + ' (total days elapsed = ' + \
                    str(dayselapsed) +\
                ')'
            modmaptitle = prop.ofs.upper() +\
                ' total modeled ice days, ' + '\n' + datestrstart +\
                ' - ' + datestr[0] + ' (total days elapsed = ' + \
                    str(dayselapsed) +\
                ')'
            minmax_vals = np.array([
                [0, dayselapsed],
                [0, dayselapsed],
                # [0,dayselapsed]
            ])
            all_titles = [
                obsmaptitle,
                modmaptitle,
                # distmaptitle
            ]
            cbartitles = [
                'Ice days',
                'Ice days',
                # 'Ice days'
            ]
            cmaptypes = [
                'viridis',
                'viridis',
                # 'viridis'
            ]
            savenamemap = str(prop.ofs) + '_' + str(prop.whichcast) +\
                '_map_' + 'extent_' + '.png'
            fig, axs = plt.subplots(len(mapdata), 1)

        if makejson:
            savenamejson = str(prop.ofs) + '_' + str(prop.whichcast) + '_' +\
                prop.start_date_full.split('T')[0] + '--' +\
                prop.end_date_full.split('T')[0] + '_map_'
            jsonmaptype = [
                'obs_ice_days',
                'model_ice_days',
                # 'icedays_error'
            ]

        logger.info('Done configuring %s maps', mapstuff)
    elif mapstuff == 'diff':
        logger.info('Configuring %s maps', mapstuff)
        if makestatic:
            datestrstart = prop.start_date_full.split('T')[0]
            meandiffmaptitle = prop.ofs.upper() +\
                ' mean error, ' + datestrstart + ' - ' + datestr[0]
            maxdiffmaptitle = prop.ofs.upper() +\
                ' maximum error, ' + ' ' +\
                datestrstart + ' - ' + datestr[0]
            mindiffmaptitle = prop.ofs.upper() +\
                ' minimum error, ' +\
                ' ' + datestrstart + ' - ' + datestr[0]
            minmax_vals = np.array([
                [-100, 100],
                [-100, 100],
                [-100, 100],
            ])
            all_titles = [
                meandiffmaptitle,
                maxdiffmaptitle,
                mindiffmaptitle,
            ]
            cbartitles = [
                'Error (%)',
                'Error (%)',
                'Error (%)',
            ]
            cmaptypes = [
                'Spectral',
                'Spectral',
                'Spectral',
            ]
            savenamemap = str(prop.ofs) + '_' + str(prop.whichcast) +\
                '_map_' + 'error_' + '.png'
            fig, axs = plt.subplots(len(mapdata), 1)

        if makejson:
            savenamejson = str(prop.ofs) + '_' + str(prop.whichcast) + '_' +\
                prop.start_date_full.split('T')[0] + '--' +\
                prop.end_date_full.split('T')[0] + '_map_'
            jsonmaptype = [
                'mean_error',
                'max_error',
                'min_error',
            ]

        logger.info('Done configuring %s maps', mapstuff)
    elif mapstuff == 'csi':
        logger.info('Configuring %s maps', mapstuff)
        if makestatic:
            datestrstart = prop.start_date_full.split('T')[0]
            hitmaptitle = prop.ofs.upper() +\
                ' Critical Success Index hits, ' + '\n' +\
                datestrstart + ' - ' + datestr[0] +' (total days elapsed = ' +\
                str(dayselapsed) + ')'
            missmaptitle = prop.ofs.upper() +\
                ' Critical Success Index misses, ' + '\n' + datestrstart +\
                ' - ' + datestr[0] + ' (total days elapsed = ' + \
                    str(dayselapsed) +\
                ')'
            falarmmaptitle = prop.ofs.upper() +\
                ' Critical Success Index false alarms, ' + '\n' + \
                    datestrstart +\
                ' - ' + datestr[0] + ' (total days elapsed = ' + \
                    str(dayselapsed) +\
                ')'
            minmax_vals = np.array([
                [0, 100],
                [0, 100],
                [0, 100],
            ])
            all_titles = [
                hitmaptitle,
                falarmmaptitle,
                missmaptitle,
            ]
            cbartitles = [
                '% of ice days',
                '% of ice days',
                '% of ice days',
            ]
            cmaptypes = [
                'brg',
                'brg',
                'brg',
            ]
            savenamemap = str(prop.ofs) + '_' + str(prop.whichcast) +\
                '_map_' + 'csi_' + '.png'
            fig, axs = plt.subplots(len(mapdata), 1)

        if makejson:
            savenamejson = str(prop.ofs) + '_' + str(prop.whichcast) + '_' +\
                prop.start_date_full.split('T')[0] + '--' +\
                prop.end_date_full.split('T')[0] + '_map_'
            jsonmaptype = [
                'csi_hit',
                'csi_falarm',
                'csi_miss',
            ]

        logger.info('Done configuring %s maps', mapstuff)

    else:
        logger.info('Incorrect map type (%s) specified! Exiting.', mapstuff)
        sys.exit(-1)

    # Set figure dimensions
    if makestatic:
        fig.set_figheight(10)
        fig.set_figwidth(8)
    for j in range(0, len(mapdata)):
        logger.info('Looping through #%s layer of map stack', j)
        if makestatic:
            # create a map
            axs[j].set_title(all_titles[j], fontsize=14)
            map = Basemap(
                projection='merc',
                resolution='i',
                area_thresh=1.0,
                ax=axs[j],
                llcrnrlon=lon_o.min()-brdr,
                llcrnrlat=lat_o.min()-brdr,
                urcrnrlon=lon_o.max()+brdr,
                urcrnrlat=lat_o.max()+brdr,
            )

            # draw map
            map.drawmapboundary()
            # draw coastlines
            map.drawcoastlines()
            parallels = np.arange(0., 90., 1.)
            map.drawparallels(
                parallels, labels=[1, 0, 0, 0],
                fontsize=10,
                linewidth=0.25,
                color='black',
            )
            # Draw meridians
            meridians = np.arange(180., 360., 2.)
            map.drawmeridians(
                meridians, labels=[0, 0, 0, 1],
                fontsize=10,
                linewidth=0.25,
                color='black',
            )
            map.fillcontinents(color='grey', lake_color='aqua')
            if cmaptypes[j] == 'viridis':
                # Define categories and corresponding colors
                cmap = mpl.cm.viridis
                binsize = 5
                norm = mpl.colors.BoundaryNorm(
                    np.arange(
                        0, dayselapsed+binsize, binsize,
                    ), cmap.N,
                )
                i_m = map.pcolor(
                    x_o, y_o, mapdata[j, :, :],
                    cmap=cmap,
                    norm=norm,
                    shading='auto',
                )
            else:
                i_m = map.pcolor(
                    x_o, y_o, mapdata[j, :, :],
                    cmap=cmaptypes[j],
                    vmin=minmax_vals[j, 0],
                    vmax=minmax_vals[j, 1],
                    shading='auto',
                )
            cbar = map.colorbar(i_m)
            cbar.set_label(cbartitles[j], fontsize=13)
            if cmaptypes[j] != 'terrain':
                no_ice = mpatches.Patch(
                    color='aqua',
                    label='Open water',
                )
                plt.legend(handles=[no_ice], loc='lower right')
            fig.tight_layout()
            logger.info('Made static map subplot #%s', j)
        # Now do JSON!
        if makejson:
            logger.info('Starting JSON from map stack layer #%s', j)
            savenameloop = savenamejson + jsonmaptype[j] + '.json'
            out_file = os.path.join(
                prop.visuals_json_maps_ice_path,
                savenameloop,
            )
            processing_2d.write_2d_arrays_to_json(
                lat_o, lon_o,
                mapdata[j, :, :],
                out_file,
            )
            logger.info('Finished JSON from map stack layer #%s', j)

    if makestatic:
        logger.info('Saving static map')
        savepath = os.path.join(
            prop.visuals_maps_ice_path,
            savenamemap,
        )
        fig.savefig(
            savepath, format='png',
            dpi=250, bbox_inches='tight',
        )
        plt.close('all')

    logger.info('Finished with %s maps! Back to main...', mapstuff)
