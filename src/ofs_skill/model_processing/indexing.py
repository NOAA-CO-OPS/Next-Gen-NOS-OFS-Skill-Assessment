"""
Spatial Indexing Functions

Functions for matching observation stations to model nodes and depth levels.
Calculates spatial distances and finds nearest neighbors for different model types
(FVCOM, ROMS, SCHISM).
"""

import logging
from typing import Any

import numpy as np

from ofs_skill.model_processing.station_distance import calculate_station_distance


def index_nearest_node(
    ctl_file_extract: list[list[str]],
    model_netcdf: dict[str, Any],
    model_source: str,
    name_var: str,
    logger: logging.Logger
) -> list[int]:
    """
    Find the closest model node to each observation station.

    Calculates the distance between observation stations and all model nodes,
    returning the index of the nearest node for each station. Supports different
    model frameworks (FVCOM, ROMS, SCHISM).

    Parameters
    ----------
    ctl_file_extract : List[List[str]]
        Observation station metadata extracted from control file
        Format: [[lat, lon, ...], ...]
    model_netcdf : Dict[str, Any]
        Dictionary containing model grid information including:
        - FVCOM: 'lonc', 'latc', 'lon', 'lat'
        - ROMS: 'lon_rho', 'lat_rho', 'mask_rho'
        - SCHISM: 'lon', 'lat'
    model_source : str
        Model type: 'fvcom', 'roms', or 'schism'
    name_var : str
        Variable name: 'wl', 'temp', 'salt', or 'cu'
    logger : logging.Logger
        Logger for tracking progress

    Returns
    -------
    List[int]
        List of model node indices, one for each observation station

    Notes
    -----
    - For FVCOM currents ('cu'), uses element centers (lonc, latc)
    - For other FVCOM variables, uses node coordinates (lon, lat)
    - Handles longitude wrapping for global models
    - For ROMS, handles land masking
    - Uses haversine formula via calculate_station_distance()

    Examples
    --------
    >>> ctl_extract = [['37.0', '-76.0'], ['37.1', '-76.1']]
    >>> model_data = {
    ...     'lon': np.array([...]),
    ...     'lat': np.array([...])
    ... }
    >>> indices = index_nearest_node(
    ...     ctl_extract, model_data, 'fvcom', 'wl', logger
    ... )
    >>> print(f"Found {len(indices)} nearest nodes")

    See Also
    --------
    calculate_station_distance : Geographic distance calculation
    index_nearest_depth : Find nearest depth level
    """
    if model_source == 'fvcom':
        index_min_dist = []
        length = len(ctl_file_extract)
        lonc_np = np.array(model_netcdf['lonc'])
        latc_np = np.array(model_netcdf['latc'])
        lon_np = np.array(model_netcdf['lon'])
        lat_np = np.array(model_netcdf['lat'])

        # Handle longitude wrapping for global models
        if np.min(lonc_np) < 0:
            lonc_np = lonc_np + 360
        if np.min(lon_np) < 0:
            lon_np = lon_np + 360

        if name_var == 'cu':
            # For currents, use element centers
            for obs_p in range(0, length):
                dist = []
                obs_lon = float(ctl_file_extract[obs_p][1]) + 360
                obs_lat = float(ctl_file_extract[obs_p][0])

                # Find nearby elements within 0.1 degree window
                nearby_ele = np.argwhere(
                    (lonc_np > obs_lon - 0.1) &
                    (lonc_np < obs_lon + 0.1) &
                    (latc_np > obs_lat - 0.1) &
                    (latc_np < obs_lat + 0.1)
                )

                for mod_p in nearby_ele[:, 0]:
                    dvalue = calculate_station_distance(
                        latc_np[int(mod_p)],
                        lonc_np[int(mod_p)],
                        obs_lat,
                        obs_lon
                    )
                    dist.append(dvalue)

                index_min_dist.append(int(nearby_ele[dist.index(min(dist))]))
                logger.info(
                    f'Nearest element found: station {obs_p + 1} of {len(ctl_file_extract)}'
                )
        else:
            # For other variables, use nodes
            for obs_p in range(0, length):
                dist = []
                obs_lon = float(ctl_file_extract[obs_p][1]) + 360
                obs_lat = float(ctl_file_extract[obs_p][0])

                # Find nearby nodes within 0.1 degree window
                nearby_nodes = np.argwhere(
                    (lon_np > obs_lon - 0.1) &
                    (lon_np < obs_lon + 0.1) &
                    (lat_np > obs_lat - 0.1) &
                    (lat_np < obs_lat + 0.1)
                )

                for mod_p in nearby_nodes[:, 0]:
                    dvalue = calculate_station_distance(
                        lat_np[int(mod_p)],
                        lon_np[int(mod_p)],
                        obs_lat,
                        obs_lon
                    )
                    dist.append(dvalue)

                index_min_dist.append(int(nearby_nodes[dist.index(min(dist))]))
                logger.info(
                    f'Nearest node found: station {obs_p + 1} of {len(ctl_file_extract)}'
                )

    elif model_source == 'roms':
        index_min_dist = []
        lat_rho_np = np.array(model_netcdf['lat_rho'])
        lon_rho_np = np.array(model_netcdf['lon_rho'])
        mask_rho_np = np.array(model_netcdf['mask_rho'])

        # Squeeze out any singleton dimensions (e.g., time dimension)
        # Grid arrays should be 2D: (eta_rho, xi_rho)
        lat_rho_np = np.squeeze(lat_rho_np)
        lon_rho_np = np.squeeze(lon_rho_np)
        mask_rho_np = np.squeeze(mask_rho_np)

        # Ensure all arrays are 2D by extracting first time slice if needed
        if lat_rho_np.ndim == 3:
            logger.info(
                f'lat_rho has 3 dimensions {lat_rho_np.shape}, extracting 2D grid '
                f'from first time slice: lat_rho[0, :, :]'
            )
            lat_rho_np = lat_rho_np[0, :, :]
        elif lat_rho_np.ndim != 2:
            logger.error(
                f'lat_rho has unexpected {lat_rho_np.ndim} dimensions (shape {lat_rho_np.shape})'
            )
            raise ValueError(f'lat_rho must be 2D or 3D, got {lat_rho_np.ndim}D')

        if lon_rho_np.ndim == 3:
            logger.info(
                f'lon_rho has 3 dimensions {lon_rho_np.shape}, extracting 2D grid '
                f'from first time slice: lon_rho[0, :, :]'
            )
            lon_rho_np = lon_rho_np[0, :, :]
        elif lon_rho_np.ndim != 2:
            logger.error(
                f'lon_rho has unexpected {lon_rho_np.ndim} dimensions (shape {lon_rho_np.shape})'
            )
            raise ValueError(f'lon_rho must be 2D or 3D, got {lon_rho_np.ndim}D')

        if mask_rho_np.ndim == 3:
            logger.info(
                f'mask_rho has 3 dimensions {mask_rho_np.shape}, extracting 2D grid '
                f'from first time slice: mask_rho[0, :, :]'
            )
            mask_rho_np = mask_rho_np[0, :, :]
        elif mask_rho_np.ndim != 2:
            logger.error(
                f'mask_rho has unexpected {mask_rho_np.ndim} dimensions (shape {mask_rho_np.shape})'
            )
            raise ValueError(f'mask_rho must be 2D or 3D, got {mask_rho_np.ndim}D')

        # Validate that all arrays have the same shape
        if lat_rho_np.shape != lon_rho_np.shape or lat_rho_np.shape != mask_rho_np.shape:
            logger.error(
                f'Shape mismatch in ROMS grid arrays: '
                f'lat_rho {lat_rho_np.shape}, lon_rho {lon_rho_np.shape}, '
                f'mask_rho {mask_rho_np.shape}'
            )
            raise ValueError('ROMS grid arrays have inconsistent shapes')

        # Log the final shapes being used for indexing
        logger.debug(
            f'ROMS node indexing - Grid array shapes: '
            f'lat_rho {lat_rho_np.shape}, lon_rho {lon_rho_np.shape}, '
            f'mask_rho {mask_rho_np.shape}'
        )

        for obs_p in range(len(ctl_file_extract)):
            obs_lat = float(ctl_file_extract[obs_p][0])
            obs_lon = float(ctl_file_extract[obs_p][1])

            # Calculate distances to all points
            dist = np.empty(np.shape(lon_rho_np))
            dist[:] = np.nan  # Set to NaN to disregard land points

            # Find nearby nodes within 0.1 degree window
            nearby_nodes = np.argwhere(
                (lon_rho_np < obs_lon + 0.1) &
                (lon_rho_np > obs_lon - 0.1) &
                (lat_rho_np < obs_lat + 0.1) &
                (lat_rho_np > obs_lat - 0.1)
            )

            if nearby_nodes.size > 0:
                for i_index, j_index in nearby_nodes:
                    # Validate indices before accessing arrays
                    if (i_index >= mask_rho_np.shape[0] or
                        j_index >= mask_rho_np.shape[1]):
                        logger.warning(
                            f'Invalid indices [{i_index}, {j_index}] for grid shape '
                            f'{mask_rho_np.shape} at station {obs_p + 1}'
                        )
                        continue

                    if mask_rho_np[i_index, j_index] == 1:  # Water point
                        distance = calculate_station_distance(
                            lat_rho_np[i_index, j_index],
                            lon_rho_np[i_index, j_index],
                            obs_lat,
                            obs_lon
                        )
                        dist[i_index, j_index] = distance

                min_idx = np.nanargmin(dist)
                index_min_dist.append(min_idx)
                logger.info(
                    f'Nearest node found: station {obs_p + 1} of {len(ctl_file_extract)}'
                )
            else:
                index_min_dist.append(np.nan)
                logger.warning(
                    f'No nearby nodes found: station {obs_p + 1} of {len(ctl_file_extract)}'
                )

    elif model_source == 'schism':
        index_min_dist = []
        lon_np = np.array(model_netcdf['lon'])
        lat_np = np.array(model_netcdf['lat'])

        for obs_p in range(len(ctl_file_extract)):
            obs_lat = float(ctl_file_extract[obs_p][0])
            obs_lon = float(ctl_file_extract[obs_p][1])

            # Find nearby nodes
            dist = []
            nearby_nodes = np.argwhere(
                (lon_np > obs_lon - 0.1) &
                (lon_np < obs_lon + 0.1) &
                (lat_np > obs_lat - 0.1) &
                (lat_np < obs_lat + 0.1)
            )

            for mod_p in nearby_nodes[:, 0]:
                dvalue = calculate_station_distance(
                    lat_np[int(mod_p)],
                    lon_np[int(mod_p)],
                    obs_lat,
                    obs_lon
                )
                dist.append(dvalue)

            if dist:
                index_min_dist.append(int(nearby_nodes[dist.index(min(dist))]))
                logger.info(
                    f'Nearest node found: station {obs_p + 1} of {len(ctl_file_extract)}'
                )
            else:
                index_min_dist.append(np.nan)
                logger.warning(
                    f'No nearby nodes found: station {obs_p + 1} of {len(ctl_file_extract)}'
                )

    else:
        raise ValueError(f'Unknown model source: {model_source}')

    return index_min_dist


def index_nearest_depth(
    prop: Any,
    index_min_dist: list[int],
    model_netcdf: dict[str, Any],
    station_ctl_file_extract: Any,
    model_source: str,
    name_var: str,
    logger: logging.Logger
) -> tuple[list[int], list[float]]:
    """
    Find the nearest depth level for each model node.

    For 3D models, finds the vertical layer index closest to the observation
    depth for each station.

    Parameters
    ----------
    prop : ModelProperties
        Model properties object
    index_min_dist : List[int]
        Node indices from index_nearest_node
    model_netcdf : Dict[str, Any]
        Model data including depth/sigma information
    station_ctl_file_extract : array-like
        Station information including depths
    model_source : str
        Model type: 'fvcom', 'roms', or 'schism'
    name_var : str
        Variable name
    logger : logging.Logger
        Logger instance

    Returns
    -------
    Tuple[List[int], List[float]]
        Tuple of (depth level indices, depth values)
        - First element: List of depth level indices for each station
        - Second element: List of actual depth values at those indices

    Notes
    -----
    Only applies to 'fields' file type (3D output).
    For 'stations' file type, returns empty lists.
    Model depths are typically negative (below surface).
    """
    if prop.ofsfiletype != 'fields':
        return [], []

    index_min_depth = []
    depth_value = []
    length = len(index_min_dist)

    if model_source == 'fvcom':
        zc_np = np.array(model_netcdf['zc'])  # Element center depths
        z_np = np.array(model_netcdf['z'])    # Node depths
    elif model_source == 'roms':
        lon_rho_np = np.array(model_netcdf['lon_rho'])
        s_rho_np = np.array(model_netcdf['s_rho'])
        h_np = np.array(model_netcdf['h'])

        # Squeeze out singleton dimensions for grid arrays
        lon_rho_np = np.squeeze(lon_rho_np)
        h_np = np.squeeze(h_np)
        s_rho_np = np.squeeze(s_rho_np)

        # For ROMS, h (bathymetry) is time-independent but may have a time dimension
        # If h is 3D (time, eta_rho, xi_rho), extract the first time slice
        if h_np.ndim == 3:
            logger.info(
                f'h array has 3 dimensions {h_np.shape}, extracting 2D grid '
                f'from first time slice: h[0, :, :] -> {h_np[0, :, :].shape}'
            )
            h_np = h_np[0, :, :]

        # Similarly for lon_rho and lat_rho if they have time dimension
        if lon_rho_np.ndim == 3:
            logger.info(
                f'lon_rho has 3 dimensions {lon_rho_np.shape}, extracting 2D grid '
                f'from first time slice'
            )
            lon_rho_np = lon_rho_np[0, :, :]

        # s_rho is 1D (vertical levels), ensure it stays that way
        if s_rho_np.ndim > 1:
            logger.warning(
                f's_rho has {s_rho_np.ndim} dimensions {s_rho_np.shape}, '
                f'extracting 1D array'
            )
            s_rho_np = s_rho_np.flatten() if s_rho_np.size == s_rho_np.shape[-1] else s_rho_np[0, :]

        # Log shapes for debugging
        logger.debug(
            f'ROMS depth indexing - Array shapes after processing: '
            f'lon_rho {lon_rho_np.shape}, h {h_np.shape}, s_rho {s_rho_np.shape}'
        )

        # Ensure h_np and lon_rho_np have the same 2D shape
        if h_np.ndim == 2 and lon_rho_np.ndim == 2:
            if h_np.shape != lon_rho_np.shape:
                logger.error(
                    f'Critical shape mismatch: h_np {h_np.shape} != '
                    f'lon_rho_np {lon_rho_np.shape}. '
                    f'Attempting to transpose h_np to match.'
                )
                # Try transposing h_np to match lon_rho_np
                if h_np.shape == lon_rho_np.shape[::-1]:
                    logger.info('Transposing h_np to match lon_rho_np shape')
                    h_np = h_np.T
                else:
                    logger.error(
                        f'Cannot reconcile shapes: h_np {h_np.shape}, '
                        f'lon_rho_np {lon_rho_np.shape}'
                    )
                    raise ValueError(
                        f'Incompatible grid array shapes: h_np {h_np.shape} '
                        f'vs lon_rho_np {lon_rho_np.shape}'
                    )

    for idx in range(0, length):
        if model_source == 'fvcom':
            if name_var == 'cu':
                ele = index_min_dist[idx]
                model_depths = zc_np[:, ele]
            else:
                node = index_min_dist[idx]
                model_depths = z_np[:, node]

            # Get station depth
            if hasattr(prop, 'user_input_location') and prop.user_input_location:
                station_depth = station_ctl_file_extract
            else:
                station_depth = np.array(station_ctl_file_extract)[:, 3][idx]

            # Find nearest depth level
            # Model depths are negative, station depths are positive
            dist = [abs(float(station_depth) + depth) for depth in model_depths]
            index_min_depth_node = dist.index(min(dist))
            depth_value.append(model_depths[index_min_depth_node])
            index_min_depth.append(index_min_depth_node)

            logger.info(
                f'Nearest depth found: node {idx + 1} of {len(index_min_dist)}'
            )

        elif model_source == 'roms':
            # Check if this station had a valid nearest node
            if np.isnan(index_min_dist[idx]):
                index_min_depth.append(np.nan)
                depth_value.append(np.nan)
                logger.warning(
                    f'No valid node for station {idx + 1}, skipping depth calculation'
                )
                continue

            try:
                # Ensure h_np is 2D before unraveling
                if h_np.ndim != 2:
                    logger.error(
                        f'h_np has {h_np.ndim} dimensions (shape {h_np.shape}), '
                        f'expected 2D for unraveling indices'
                    )
                    index_min_depth.append(np.nan)
                    depth_value.append(np.nan)
                    continue

                # Unravel using lon_rho shape (same as used in index_nearest_node)
                # to ensure consistency
                if lon_rho_np.ndim == 2:
                    unravel_shape = lon_rho_np.shape
                else:
                    # Fallback to h_np shape if lon_rho_np isn't 2D
                    unravel_shape = h_np.shape

                i_index, j_index = np.unravel_index(
                    int(index_min_dist[idx]), unravel_shape
                )

                # Validate indices are within bounds
                if i_index >= h_np.shape[0] or j_index >= h_np.shape[1]:
                    logger.warning(
                        f'Unraveled indices [{i_index}, {j_index}] exceed h_np shape '
                        f'{h_np.shape} for station {idx + 1}'
                    )
                    index_min_depth.append(np.nan)
                    depth_value.append(np.nan)
                    continue

            except (TypeError, ValueError) as e:
                logger.warning(
                    f'Cannot unravel index {index_min_dist[idx]} for station {idx + 1}: {e}'
                )
                index_min_depth.append(np.nan)
                depth_value.append(np.nan)
                continue

            # Calculate depth levels for this location
            model_depths = np.asarray(s_rho_np) * h_np[i_index, j_index]
            station_depth = np.array(station_ctl_file_extract)[:, 3][idx]

            # Find nearest depth level
            dist = [abs(float(station_depth) + depth) for depth in model_depths]
            index_min_depth_node = dist.index(min(dist))
            depth_value.append(model_depths[index_min_depth_node])
            index_min_depth.append(index_min_depth_node)

            logger.info(
                f'Nearest depth found: node {idx + 1} of {len(index_min_dist)}'
            )

        elif model_source == 'schism':
            # SCHISM-specific depth indexing would go here
            logger.warning('SCHISM depth indexing not fully implemented')
            index_min_depth.append(0)  # Default to surface
            depth_value.append(0.0)

    return index_min_depth, depth_value


def index_nearest_station(
    ctl_file_extract: list[list[str]],
    model_netcdf: dict[str, Any],
    model_source: str,
    name_var: str,
    logger: logging.Logger
) -> list[int]:
    """
    Find the closest model station output location to observation stations.

    For models that output at specific station locations, finds the nearest
    model station to each observation station.

    Parameters
    ----------
    ctl_file_extract : List[List[str]]
        Observation station locations
    model_netcdf : Dict[str, Any]
        Model station locations
    model_source : str
        Model type
    name_var : str
        Variable name
    logger : logging.Logger
        Logger instance

    Returns
    -------
    List[int]
        List of model station indices (or NaN if no match within threshold)

    Notes
    -----
    Uses a maximum distance threshold of 2 km.
    Stations beyond this threshold are marked as NaN.
    """
    max_dist = 2.0  # km - cutoff for distance matching
    index_min_dist = []
    min_dist = []

    if model_source == 'fvcom':
        length = len(ctl_file_extract)
        lon_np = np.array(model_netcdf['lon'])[1]
        lat_np = np.array(model_netcdf['lat'])[1]

        # Handle longitude wrapping
        if np.min(lon_np) < 0:
            lon_np = lon_np + 360

        for obs_p in range(0, length):
            dist = []
            obs_lon = float(ctl_file_extract[obs_p][1]) + 360
            obs_lat = float(ctl_file_extract[obs_p][0])

            # Find nearby stations within 0.3 degree window
            nearby_nodes = np.argwhere(
                (lon_np > obs_lon - 0.3) &
                (lon_np < obs_lon + 0.3) &
                (lat_np > obs_lat - 0.3) &
                (lat_np < obs_lat + 0.3)
            )

            if nearby_nodes.size > 0:
                for mod_p in nearby_nodes[:, 0]:
                    dvalue = calculate_station_distance(
                        lat_np[int(mod_p)], lon_np[int(mod_p)],
                        obs_lat, obs_lon
                    )
                    dist.append(dvalue)

                if np.nanmin(dist) <= max_dist:
                    index_min_dist.append(int(nearby_nodes[dist.index(min(dist))]))
                    min_dist.append(np.nanmin(dist))
                    logger.info(
                        f'Nearest station found: station {obs_p + 1} of {len(ctl_file_extract)}'
                    )
                else:
                    index_min_dist.append(np.nan)
                    min_dist.append(np.nan)
                    logger.info(
                        f'Nearest station NOT found (>{max_dist}km): station {obs_p + 1}'
                    )
            else:
                index_min_dist.append(np.nan)
                min_dist.append(np.nan)
                logger.info(
                    f'Nearest station NOT found: station {obs_p + 1} of {len(ctl_file_extract)}'
                )

    elif model_source == 'roms':
        lon_rho_np = np.array(model_netcdf['lon_rho'])
        lat_rho_np = np.array(model_netcdf['lat_rho'])

        for obs_p in range(len(ctl_file_extract)):
            dist = np.empty(len(model_netcdf['lon_rho']))
            dist[:] = np.nan
            obs_lon = float(ctl_file_extract[obs_p][1])
            obs_lat = float(ctl_file_extract[obs_p][0])

            nearby_nodes = np.argwhere(
                (lon_rho_np < obs_lon + 0.1) &
                (lon_rho_np > obs_lon - 0.1) &
                (lat_rho_np < obs_lat + 0.1) &
                (lat_rho_np > obs_lat - 0.1)
            )

            if nearby_nodes.size > 0:
                for i_index in nearby_nodes:
                    distance = calculate_station_distance(
                        lat_rho_np[i_index],
                        lon_rho_np[i_index],
                        obs_lat,
                        obs_lon
                    )
                    dist[i_index] = distance

                if np.nanmin(dist) <= max_dist:
                    index_min_dist.append(np.nanargmin(dist))
                    min_dist.append(np.nanmin(dist))
                    logger.info(
                        f'Nearest station found: station {obs_p + 1} of {len(ctl_file_extract)}'
                    )
                else:
                    index_min_dist.append(np.nan)
                    min_dist.append(np.nan)
                    logger.info(
                        f'Nearest station NOT found (>{max_dist}km): station {obs_p + 1}'
                    )
            else:
                index_min_dist.append(np.nan)
                min_dist.append(np.nan)
                logger.info(
                    f'Nearest station NOT found: station {obs_p + 1} of {len(ctl_file_extract)}'
                )

    return index_min_dist
