'''
This script is used to find the closest model node to a station point.
To do that the distance between a station and all model nodes is
calculated.
'''
import sys
from pathlib import Path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from model_processing import station_distance
import numpy as np

def index_nearest_node(ctl_file_extract, model_netcdf, model_source, name_var,
                       logger):
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
        index_min_dist = []
        length = len(ctl_file_extract)
        lonc_np = np.array(model_netcdf['lonc'])
        latc_np = np.array(model_netcdf['latc'])
        lon_np = np.array(model_netcdf['lon'])
        lat_np = np.array(model_netcdf['lat'])

        if name_var == "cu":
            for obs_p in range(0, length):
                dist = []
                nearby_ele = np.argwhere(
          (lonc_np > (float(ctl_file_extract[obs_p][1])+360)-.1) \
        & (lonc_np < (float(ctl_file_extract[obs_p][1])+360)+.1) \
        & (latc_np > (float(ctl_file_extract[obs_p][0])-.1)) \
        & (latc_np < (float(ctl_file_extract[obs_p][0])+.1))
        )

                for mod_p in nearby_ele[:,0]:
                    dvalue = station_distance.station_distance(
                        latc_np[int(mod_p)],
                        lonc_np[int(mod_p)],
                        float(ctl_file_extract[obs_p][0]),
                        float(ctl_file_extract[obs_p][1]))
                    dist.append(dvalue)

                index_min_dist.append(int(nearby_ele[dist.index(min(dist))]))

                logger.info(
                    'Nearest element found: station %s of %s', obs_p + 1,
                    len(ctl_file_extract))

        else:
            for obs_p in range(0, length):
                dist = []
                nearby_nodes = np.argwhere(
          (lon_np > (float(ctl_file_extract[obs_p][1])+360)-.1) \
        & (lon_np < (float(ctl_file_extract[obs_p][1])+360)+.1) \
        & (lat_np > (float(ctl_file_extract[obs_p][0])-.1)) \
        & (lat_np < (float(ctl_file_extract[obs_p][0])+.1))
        )

                for mod_p in nearby_nodes[:,0]:
                    dvalue = station_distance.station_distance(
                        lat_np[int(mod_p)],
                        lon_np[int(mod_p)],
                        float(ctl_file_extract[obs_p][0]),
                        float(ctl_file_extract[obs_p][1]))
                    dist.append(dvalue)

                index_min_dist.append(int(nearby_nodes[dist.index(min(dist))]))

                logger.info(
                    'Nearest node found: station %s of %s', obs_p + 1,
                    len(ctl_file_extract))

    elif model_source=="roms":
        index_min_dist = []
        lat_rho_np = np.array(model_netcdf['lat_rho'])
        lon_rho_np = np.array(model_netcdf['lon_rho'])
        mask_rho_np = np.array(model_netcdf['mask_rho'])
        for obs_p in range(len(ctl_file_extract)):
        #for obs_p in range(len(ctl_file_extract) - (len(ctl_file_extract)-2)): #limit stations for debug
            dist = np.empty((len(lon_rho_np),len(lon_rho_np[0])))
            dist[:]=np.nan #set to nans in order to disregard land points
            nearby_nodes = np.argwhere(
                  (lon_rho_np < float(ctl_file_extract[obs_p][1])+0.1) \
                & (lon_rho_np > float(ctl_file_extract[obs_p][1])-0.1) \
                & (lat_rho_np < float(ctl_file_extract[obs_p][0])+0.1) \
                & (lat_rho_np > float(ctl_file_extract[obs_p][0])-0.1)
                )

            for i_index, j_index in nearby_nodes:
                if bool(mask_rho_np[0,i_index,j_index])==False:
                        continue
                elif bool(mask_rho_np[0,i_index,j_index])==True:
                        distance = station_distance.station_distance(
                            lat_rho_np[i_index,j_index],
                            lon_rho_np[i_index,j_index],
                            float(ctl_file_extract[obs_p][0]),
                            float(ctl_file_extract[obs_p][1]))
                        dist[i_index,j_index] = distance

            index_min_dist.append(np.nanargmin(dist))

            logger.info(
                'Nearest node found: station %s of %s', obs_p + 1,
                len(ctl_file_extract))

    return index_min_dist
