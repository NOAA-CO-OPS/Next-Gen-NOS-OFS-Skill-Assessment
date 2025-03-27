'''
This script is used to find the closest model node to a station point.
To do that the distance between a station and all model nodes is
calculated.
'''

from model_processing import station_distance
import numpy as np


def index_nearest_station(ctl_file_extract, model_netcdf, model_source, name_var, logger):

    '''
    The inputs for this function are the observations lat and long,
    and the model netcdf file
    Thus the outputs from ctl_file_extract(ctlfile) and the outputs
    from fvcom_netcdf(dataset) (or the concatenation concat_FVCOM) can
    serve as inputs here

    This lines are used to
    1) open the ctl file,
    2) split it in lines,
    3) grab the lines that have lat and long (every other line),
    4) split these line based on spaces,
    5) remove the spaces,
    6) create a new list only with lat and lon
    '''

    if model_source=="fvcom":
        max_dist = 2 # cutoff for distance between obs station and model station
        index_min_dist = []
        min_dist = []
        length = len(ctl_file_extract)
        lon_np = np.array(model_netcdf['lon'])[1]
        lat_np = np.array(model_netcdf['lat'])[1]

        for obs_p in range(0, length):
            dist = []
            nearby_nodes = np.argwhere(
      (lon_np > (float(ctl_file_extract[obs_p][1])+360)-.3) \
    & (lon_np < (float(ctl_file_extract[obs_p][1])+360)+.3) \
    & (lat_np > (float(ctl_file_extract[obs_p][0])-.3)) \
    & (lat_np < (float(ctl_file_extract[obs_p][0])+.3))
    )
            if nearby_nodes.size > 0:
                for mod_p in nearby_nodes[:,0]:
                    dvalue = station_distance.station_distance(
                        lat_np[int(mod_p)], lon_np[int(mod_p)],
                        float(ctl_file_extract[obs_p][0])+360,
                        float(ctl_file_extract[obs_p][1]))
                    dist.append(dvalue)
                if np.nanmin(dist) <= max_dist:
                    index_min_dist.append(int(nearby_nodes[dist.index(min(dist))]))
                    min_dist.append(np.nanmin(dist))
                else:
                    index_min_dist.append(np.nan)
                    min_dist.append(np.nan)

                logger.info(
                    'Nearest ofs station found: station %s of %s', obs_p + 1,
                    len(ctl_file_extract))
            else:
                index_min_dist.append(np.nan)
                min_dist.append(np.nan)

    elif model_source=="roms":
        # Find model station nearest to observation station
        max_dist = 2 # cutoff for distance between obs station and model station
        index_min_dist = []
        min_dist = []
        lon_rho_np = np.array(model_netcdf['lon_rho'])
        lat_rho_np = np.array(model_netcdf['lat_rho'])
        for obs_p in range(len(ctl_file_extract)):
            #print(ctl_file_extract[obs_p][1])
            #print(ctl_file_extract[obs_p][0])
            #for obs_p in range(len(ctl_file_extract) - (len(ctl_file_extract)-2)): #limit stations for debug
            dist = np.empty((len(model_netcdf['lon_rho'])))
            dist[:]=np.nan #set to nans in order to disregard land points
            nearby_nodes = np.argwhere(
                  (lon_rho_np < float(ctl_file_extract[obs_p][1])+0.1) \
                & (lon_rho_np > float(ctl_file_extract[obs_p][1])-0.1) \
                & (lat_rho_np < float(ctl_file_extract[obs_p][0])+0.1) \
                & (lat_rho_np > float(ctl_file_extract[obs_p][0])-0.1)
                )
            if nearby_nodes.size > 0:
                for i_index in nearby_nodes:
                    distance = station_distance.station_distance(
                        lat_rho_np[i_index],
                        lon_rho_np[i_index],
                        float(ctl_file_extract[obs_p][0]),
                        float(ctl_file_extract[obs_p][1]))
                    dist[i_index] = distance
                if np.nanmin(dist) <= max_dist:
                    index_min_dist.append(np.nanargmin(dist))
                    min_dist.append(np.nanmin(dist))
                else:
                    index_min_dist.append(np.nan)
                    min_dist.append(np.nan)
                logger.info(
                    'Nearest node found: station %s of %s', obs_p + 1,
                    len(ctl_file_extract))
            else:
                index_min_dist.append(np.nan)
                min_dist.append(np.nan)
    #print('debug pause')
    return index_min_dist, min_dist
