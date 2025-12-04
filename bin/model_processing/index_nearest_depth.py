'''
This script is used to find the nearest model depth layers to the corresponding
observation depths. It loops through each obs station and returns a list of
the model depth layer indices, not actual depths.
'''

import numpy as np

def index_nearest_depth(
        prop, index_min_dist, model_netcdf,station_ctl_file_extract,
        model_source, name_var, logger
        ):

    if prop.ofsfiletype == 'fields':
        index_min_depth = []
        depth_value = []
        length = len(index_min_dist)
        if model_source == 'fvcom':
            zc_np = np.array(model_netcdf['zc'])
            z_np = np.array(model_netcdf['z'])
        elif model_source == 'roms':
            lon_rho_np = np.array(model_netcdf['lon_rho'])
            s_rho_np = np.array(model_netcdf['s_rho'])
            h_np = np.array(model_netcdf['h'])
        for idx in range(0, length):
            if model_source=="fvcom":
                if name_var == "cu":
                    ele = index_min_dist[idx]
                    model_depths = zc_np[:, ele]
                else:
                    node = index_min_dist[idx]
                    model_depths = z_np[node,:]
                station_depth = np.array(station_ctl_file_extract)[:, 3][idx]
                dist = []
                # this is positive here because model depths (depth) are negative
                # values and obs depths (station_depth) are positive
                for depth in model_depths:
                    dist.append(float(station_depth) + depth)

                dist = [abs(i) for i in dist]
                index_min_depth_node = dist.index(min(dist))
                depth_value.append(model_depths[
                index_min_depth_node])
                index_min_depth.append(index_min_depth_node)

                logger.info(
                 'Nearest depth found: node %s of %s', idx + 1,
                 len(index_min_dist))

            elif model_source=="roms":
                i_index,j_index = np.unravel_index(
                    index_min_dist[idx],np.shape(lon_rho_np))
                model_depths = np.asarray(
                    np.asarray(s_rho_np)*h_np[0,i_index,j_index])
                station_depth = np.array(station_ctl_file_extract)[:, 3][idx]
                dist = []
                # this is positive here because model depths (depth) are negative
                # values and obs depths (station_depth) are positive
                for depth in model_depths:
                    dist.append(float(station_depth) + depth)

                dist = [abs(i) for i in dist]
                index_min_depth_node = dist.index(min(dist))
                depth_value.append(model_depths[
                index_min_depth_node])
                index_min_depth.append(index_min_depth_node)

                logger.info(
                 'Nearest depth found: node %s of %s', idx + 1,
                 len(index_min_dist))

            elif model_source=="schism": 
                if name_var == "wl":
                    index_min_depth.append('0') 
                else:  
                    # we assume layers are consistance at all the time steps, 
                    # therefore, we use depth layes at time 0
                    #z_coords_1d = model_netcdf['zCoordinates'].load()
                    z_coords_1d = model_netcdf['zCoordinates'].isel(time=0).load()  # to handle memory error
                    node = index_min_dist[idx]
               
                    if np.isnan(node) or np.isnan(float(np.array(station_ctl_file_extract)[:, 3][idx])):
                       logger.warning(f"No nearby depth found for node {idx + 1}")
                       index_min_dist.append(-1)
                       depth_value.append(-1)
                       continue
                  
                    #model_depths = z_coords_1d[0,node,:]
                    model_depths = z_coords_1d[node,:].values 
                    station_depth = np.array(station_ctl_file_extract)[:, 3][idx]
                    dist = []
                    # this is positive here because model depths (depth) are negative
                    # values and obs depths (station_depth) are positive
                    for depth in model_depths:

                        if  not np.isnan(depth):
                            dist.append(float(station_depth) + depth)
                        else:
                            dist.append(np.nan)
                    dist = [np.abs(i) for i in dist]
                    index_min_depth_node = dist.index(np.nanmin(dist))
                    index_min_depth.append(index_min_depth_node)
                    depth_value.append(model_depths[
                    index_min_depth_node])
                logger.info(
                  'Nearest depth found: node %s of %s', idx + 1,
                  len(index_min_dist))

    elif prop.ofsfiletype == 'stations':
        index_min_depth = []
        depth_value = []
        length = len(index_min_dist)
        if model_source == 'fvcom':
            z_np = np.array(model_netcdf['z'])
        elif model_source == 'roms':
            s_rho_np = np.array(model_netcdf['s_rho'])
            h_np = np.array(model_netcdf['h'])

        for idx in range(0, length):
            if np.isnan(index_min_dist[idx]) == 0:
                node = index_min_dist[idx]
                if model_source=="fvcom":
                    model_depths = np.asarray(z_np[:,node,0])
                elif model_source=="roms":
                    model_depths = np.asarray(
                        s_rho_np*h_np[0,node]
                        )

                station_depth = np.array(station_ctl_file_extract)[:, 3][idx]
                dist = []
                # this is positive here because model depths (depth) are negative
                # values and obs depths (station_depth) are positive
                for depth in model_depths:
                    dist.append(float(station_depth) + depth)

                dist = [abs(i) for i in dist]
                index_min_depth_node = dist.index(min(dist))
                depth_value.append(model_depths[
                    index_min_depth_node])
                index_min_depth.append(index_min_depth_node)
            else:
                index_min_depth.append(np.nan)
                depth_value.append(np.nan)
            logger.info(
                'Nearest depth found: node %s of %s', idx + 1,
                len(index_min_dist))

    return index_min_depth, np.abs(depth_value)
