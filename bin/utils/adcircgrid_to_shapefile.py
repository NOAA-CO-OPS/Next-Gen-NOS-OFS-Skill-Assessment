"""Generate extent shapefile from ADCIRC grid file.

The format of the input grid files is described in 
the documentation at:
    https://adcirc.org/home/documentation/users-manual-v53/parameter-definitions#NOPE

Example input grid file format for STOFS-2D-Global:

OceanMesh2D
24875336 12785004
< 12785004 lines of:
    NodeID Longitude Latitude Depth
>
< 24875336 lines of:
    ElementID NodeID1 NodeID2 NodeID3
>
0 = Number of open boundaries
0 = Total number of open boundary nodes
262 = Number of land boundaries
12421 = Total number of land boundary nodes
< 262 groups of:
    Either (type-24, internal boundary):
        < 1 line per boundary node of:
            NodeID BackFaceNodeID BarrierHeight BarrierCoefficient BarrierCoefficient
        >
    or (type-20, external boundary):
        < 1 line per boundary node of:
            NodeID     
        >
>

"""


import pandas as pd
import shapely
import geopandas as gpd
import argparse
import tempfile


STOFS_2D_GLO_V2P1_GRID_URL = "https://noaa-gestofs-pds.s3.amazonaws.com/staticfiles/v2.1/stofs_2d_glo_grid"
STOFS_2D_GLO_V2P1_GRID_FILENAME = "/home/jre/data/noaa-gestofs-pds/staticfiles/v2.1/stofs_2d_glo_grid"
BOUNDARY_HEADER_STRING = "= Number of nodes "


def get_number_elements_nodes(line):
    (n_elements, n_nodes) = (int(n) for n in line.strip().split())
    return (n_elements, n_nodes)


def parse_boundary_header_line(line):
    parts = line.strip().split()
    boundary_n_nodes = int(parts[0])
    boundary_type = int(parts[1])
    boundary_id = int(parts[-1])
    return (boundary_id, boundary_n_nodes, boundary_type)


def parse_grid_file_line_by_line(filename):
    node_id = []
    node_lat = []
    node_lon = []
    boundary_id = []
    boundary_n_nodes = []
    boundary_type = []
    boundary_node_list = []
    boundary_section = False
    with open(filename, 'r', encoding='utf-8') as file:
        for line_num, line in enumerate(file):
            # Print every 100000 lines.
            if line_num % 100000 == 0:
                print(f"Processing line {line_num}: {line.strip()}")
            # Ignore the first line.
            if line_num == 0:
                continue
            # Get the number of elements and nodes from the second line.
            if line_num == 1:
                N_e, N_nodes = get_number_elements_nodes(line)
                continue
            # Next get the node info from the subsequent N_nodes lines.
            if line_num <= N_nodes + 1:
                parts = line.strip().split()
                node_id.append(int(parts[0]))
                node_lon.append(float(parts[1]))
                node_lat.append(float(parts[2]))
                continue
            # Identify lines that start a boundary section.
            if BOUNDARY_HEADER_STRING in line:
                print(f"Found boundary header on line {line_num}: {line.strip()}")
                boundary_section = True
                b_nodes = []
                (b, b_n_nodes, b_type) = parse_boundary_header_line(line)
                boundary_id.append(b)
                boundary_n_nodes.append(b_n_nodes)
                boundary_type.append(b_type)
                boundary_line_num = line_num
                boundary_lines_start = boundary_line_num + 1
                boundary_lines_end = boundary_line_num + b_n_nodes
                continue
            # If we are in a boundary section, parse the node info.
            if boundary_section and line_num >= boundary_lines_start and line_num < boundary_lines_end:
                b_nodes.append(int(line.strip().split()[0]))
                continue
            # If we are at the end of the boundary section, parse the node info,
            # append the node list to the list of node lists, and set boundary_section 
            # back to False.
            if boundary_section and line_num == boundary_lines_end :
                    b_nodes.append(int(line.strip().split()[0]))
                    boundary_node_list.append(b_nodes)
                    boundary_section = False
                    continue
    return {
        'node_id': node_id,
        'node_lat': node_lat,
        'node_lon': node_lon,
        'boundary_id': boundary_id,
        'boundary_node_list': boundary_node_list,
        'boundary_type': boundary_type,
        'boundary_n_nodes': boundary_n_nodes
    }


def get_node_data_frame(node_dict):
    """Get data frame containing node info from dictionary."""
    try:
        node_id = node_dict['node_id']
        node_lat = node_dict['node_lat']
        node_lon = node_dict['node_lon']
    except KeyError as e:
        print(f"Error: Missing key in node dictionary: {e}")
        raise
    df = pd.DataFrame(data={
        'node_id': node_id,
        'node_lat': node_lat,
        'node_lon': node_lon
    })
    df.set_index('node_id', inplace=True)
    return df


def get_node_geo_data_frame(df_node):
    gdf = gpd.GeoDataFrame(
        df_node, 
        geometry=gpd.points_from_xy(
            df_node.node_lon, 
            df_node.node_lat
        ), 
        crs="EPSG:4326"
    )
    return gdf


def boundary_to_polygon(
    boundary_node_list,
    df_node
):
    """Convert a list of boundary nodes to a polygon."""
    coords = []
    # Loop over the boundary nodes and get their coordinates from the data frame.
    for node_id in boundary_node_list:
        coords.append((df_node.loc[node_id, 'node_lon'], df_node.loc[node_id, 'node_lat']))
    # If necessary, add the first node again to close the polygon.
    if coords[0] != coords[-1]:
        print('Appending first node to end of coordinates to close the polygon.')
        coords.append((df_node.loc[boundary_node_list[0], 'node_lon'], df_node.loc[boundary_node_list[0], 'node_lat']))
    else:
        print('First and last coordinates are the same, no need to append first node again.')
    return shapely.geometry.Polygon(coords)


def boundaries_to_shapefile(boundaries_nodes, shapefile_name):
    """Create a shapefile from boundary and node data."""  
    # Convery node data to a data frame for easier access.
    df_nodes = get_node_data_frame(stofs_boundaries_nodes)
    geometries = []
    for boundary_node_list in boundaries_nodes['boundary_node_list']:
        geometries.append(boundary_to_polygon(boundary_node_list, df_nodes))
    # Create a geodataframe.
    # Note we truncate columns names to 10 characters for shapefile compatibility.
    gdf = gpd.GeoDataFrame(
        data = {
            'boundaryID': boundaries_nodes['boundary_id'],
            'type': boundaries_nodes['boundary_type'],
            'N_nodes': boundaries_nodes['boundary_n_nodes']
        },
        geometry=geometries,
        crs="EPSG:4326"
    )
    gdf.to_file(shapefile_name)
    return gdf


def boundary_nodes_to_shapefile(boundaries_nodes):
    """Create a shapefile that has all boundary nodes as individual points."""
    shapefile_name = "/home/jre/dev-Next-Gen-NOS-OFS-Skill-Assessment/ofs_extents/stofs_2d_glo_points.shp"
    # Convery node data to a data frame for easier access.
    df_nodes = get_node_data_frame(stofs_boundaries_nodes)
    geometries = []
    bID_list = []
    btyp_list = []
    bnn_list = []
    for ib, boundary_node_list in enumerate(boundaries_nodes['boundary_node_list']):
        for node in boundary_node_list:
            geometries.append(shapely.geometry.Point(df_nodes.loc[node, 'node_lon'], df_nodes.loc[node, 'node_lat']))
            bID_list.append(boundaries_nodes['boundary_id'][ib])
            btyp_list.append(boundaries_nodes['boundary_type'][ib])
            bnn_list.append(boundaries_nodes['boundary_n_nodes'][ib])
    # Create a geodataframe.
    # Note we truncate columns names to 10 characters for shapefile compatibility.
    gdf = gpd.GeoDataFrame(
        data = {
            'boundaryID': bID_list,
            'type': btyp_list,
            'N_nodes': bnn_list
        },
        geometry=geometries,
        crs="EPSG:4326"
    )
    gdf.to_file(shapefile_name)
    return gdf


def write_global_extent_shapefile(shapefile_name):
    """Write a shapefile containing the global extent of the STOFS-2D-Global grid."""
    # Define the global extent as a polygon.
    global_extent_polygon = shapely.geometry.Polygon([
        (-180, -90),
        (-180, 90),
        (180, 90),
        (180, -90),
        (-180, -90)
    ])
    # Create a geodataframe.
    gdf = gpd.GeoDataFrame(
        data = {
            'name': ['STOFS-2D-Global Extent']
        },
        geometry=[global_extent_polygon],
        crs="EPSG:4326"
    )
    gdf.to_file(shapefile_name)


if __name__ == "__main__":
    raise NotImplementedError('This script it not yet ready to use. '
                              'STOFS-2D-Global currently has a copy of the '
                              'GOMOFS extent file, for testing purposes.')
    # Parse arguments.
    parser = argparse.ArgumentParser(description='Generate extent shapefile from ADCIRC grid file.')

    # Grid file path.
    # (Ignored for now since we are hardcoding the path to the STOFS-2D-Global grid file.)
    parser.add_argument('grid_file', type=str, default=None, help='Path to the ADCIRC grid file.')
    
    # Output shapefile path.
    # (Ignored for now since we are hardcoding the path to the output shapefile.)
    parser.add_argument('shapefile_path', type=str, default=None, help='Path to the output shapefile.')

    # Model name (e.g., STOFS-2D-Global).
    # Could be used to download grid file if not provided.
    # (Ignored for now since we are hardcoding the path to the STOFS-2D-Global grid file.)
    parser.add_argument('model_name', type=str, default=None, help='Name of the model (e.g., STOFS-2D-Global).')

    # Get the arguments.
    args = parser.parse_args()

    # Validate the arguments.
    if args.model_name is not None:
        if args.grid_file is not None:
            raise ValueError("Cannot specify both grid_file and model_name.")
        else:
            # We could add stuff here to temporarily download the grid file based on the model name, but that's for another day.
            raise NotImplementedError("Downloading grid file based on model name argument is not implemented yet. Please specify the grid file path directly.")
    else:
        if args.grid_file is None:
            # Usually this would cause an error, but we are hard-coding for now so just commenting this out.
            # If we get this working properly eventually, uncomment this line
            # \/   \/   \/   \/
            #raise ValueError("Must specify either grid_file or model_name.")
            # /\   /\   /\   /\
            pass
        else:
            grid_file_path = args.grid_file

    if args.shapefile_path is not None:
        shapefile_path = args.shapefile_path
    else:
        if args.model_name is None:
            # Usually this would cause an error, but we are hard-coding for now so just commenting this out.
                # If we get this working properly eventually, uncomment this line
                # \/   \/   \/   \/
            #raise ValueError("Must specify shapefile_path (unless you specify model_name, in which case shapefile_path is constructed from it).")
                # /\   /\   /\   /\
            shapefile_path = "/home/jre/dev-Next-Gen-NOS-OFS-Skill-Assessment/ofs_extents/stofs_2d_glo.shp"
        else:
            shapefile_path = f"/home/jre/dev-Next-Gen-NOS-OFS-Skill-Assessment/ofs_extents/{args.model_name}.shp"
    #

    if (
        (args.shapefile_name is None) and 
        (args.model_name is None) and 
        (args.grid_file is None)
    ):
        write_global_extent_shapefile(shapefile_path)
    else:
        stofs_boundaries_nodes = parse_grid_file_line_by_line(grid_file_path)
        gdf = boundaries_to_shapefile(stofs_boundaries_nodes, shapefile_path)