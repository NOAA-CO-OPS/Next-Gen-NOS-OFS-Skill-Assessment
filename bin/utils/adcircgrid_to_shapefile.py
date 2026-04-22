"""Script to find the boundary edges of the STOFS-2D-Global mesh and save them as a shapefile.

This workflow is broken into quite a lot of  functions, which are called in 
sequence at the bottom of the script. This is really for readability 
and modularity, since the workflow is quite complex and there are a lot of steps.
However, the individual functions are not really meant to be used independently.
So, beware if re-using these functions elsewhere!

"""



import pandas as pd
import numpy as np
import shapely
from shapely.ops import unary_union
import geopandas as gpd
import tempfile
import time
import requests


STOFS_2D_GLO_V2P1_GRID_URL = "https://noaa-gestofs-pds.s3.amazonaws.com/staticfiles/v2.1/stofs_2d_glo_grid"
STOFS_2D_GLO_V2P1_GRID_FILENAME = None # Set this to the file path of the grid file if you have it downloaded, or leave as None to download it to a temporary location each time (slow).
BOUNDARY_HEADER_STRING = "= Number of nodes "


def get_grid_filepath():
    """Get the file path for the grid file."""
    if STOFS_2D_GLO_V2P1_GRID_FILENAME is not None:
        return STOFS_2D_GLO_V2P1_GRID_FILENAME
    # If the file path is not set, download the file to a temporary location and return the path.
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file: 
        print('Variable STOFS_2D_GLO_V2P1_GRID_FILENAME is not set. Downloading grid file to temporary location. Move the temporary file somewhere safe and edit the script to set the variable if you want to avoid downloading the file every time.')
        print(f"Downloading grid file from {STOFS_2D_GLO_V2P1_GRID_URL} to temporary file {tmp_file.name}...")
        print('Pausing for 10 seconds to abort if this is not intended...')
        time.sleep(10)
        print('Downloading now...')
        response = requests.get(STOFS_2D_GLO_V2P1_GRID_URL)
        response.raise_for_status()  # Check if the download was successful
        tmp_file.write(response.content)
        return tmp_file.name


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
    """Parse the STOFS-2D-Global grid file line by line to extract node and edge information.
    
    This returns the 'node_dict' used by several other functions, which contains
    lists of node IDs, latitudes, longitudes, and edge node pairs. 
    
    We parse the file line by line to avoid memory issues with loading the entire 
    file at once, since it is very large.

    """
    node_id = []
    node_lat = []
    node_lon = []
    edge_node_1 = []
    edge_node_2 = []
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
            elif line_num <= N_nodes + 1 + N_e:
                parts = line.strip().split()
                element_node_count = int(parts[1])
                node_ids = [int(parts[i]) for i in range(2, 2 + element_node_count)]
                for i in range(element_node_count):
                    for j in range(i + 1, element_node_count):
                        edge_node_1.append(min(node_ids[i], node_ids[j]))
                        edge_node_2.append(max(node_ids[i], node_ids[j]))
                continue
    return {
        'node_id': node_id,
        'node_lat': node_lat,
        'node_lon': node_lon,
        'edge_node_1': edge_node_1,
        'edge_node_2': edge_node_2
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


def remove_duplicate_edges_in_chunks(node_dict):
    """Remove duplicate edges from the edge lists in the node dictionary, in chunks to avoid memory issues.
    
    The purpose of this is simply to find the edges that are on the boundary 
    of the mesh, which are the edges that only appear once in the edge list.
    
    """
    df_boundary_edges = pd.DataFrame()
    n_edges = len(node_dict['edge_node_1'])
    chunk_size = 1000000
    for i in range(0, n_edges, chunk_size):
        print(f"Processing edges {i} to {min(i + chunk_size, n_edges)}")
        chunk = pd.DataFrame({
            'node_id1': node_dict['edge_node_1'][i:i + chunk_size],
            'node_id2': node_dict['edge_node_2'][i:i + chunk_size]
        })
        # Check for duplicates in the chunk.
        # We use keep=False to mark all duplicates, not just the first occurrence.
        duplicates = chunk.duplicated(subset=['node_id1', 'node_id2'], keep=False)
        keep = chunk[~duplicates]
        print(f"Keeping {len(keep)} edges from this chunk.")
        # Add non-duplicate edges to the boundary edge data frame.
        df_boundary_edges = pd.concat([df_boundary_edges, keep], ignore_index=True)
        # Check combined data frame for duplicates.
        # We use keep=False to mark all duplicates, not just the first occurrence.
        grand_duplicates = df_boundary_edges.duplicated(subset=['node_id1', 'node_id2'], keep=False)
        df_boundary_edges = df_boundary_edges[~grand_duplicates]
        print(f"Total boundary edges so far: {len(df_boundary_edges)}")
    return df_boundary_edges


def get_boundary_edges_geopandas(df_boundary_edges, df_node):
    """Get GeoPandas data frame containing boundary edges."""
    gdf = gpd.GeoDataFrame(df_boundary_edges)
    gdf['geometry'] = gdf.apply(lambda row: shapely.geometry.LineString([
        (df_node.loc[row['node_id1'], 'node_lon'], df_node.loc[row['node_id1'], 'node_lat']),
        (df_node.loc[row['node_id2'], 'node_lon'], df_node.loc[row['node_id2'], 'node_lat'])
    ]), axis=1)
    gdf = gdf.set_geometry('geometry')
    return gdf


def split_df_at_antimeridian(gdf):
    """Format dataframe by splitting any 2-node LineStrings that cross the antimeridian.

    Adds new nodes at the crossing points on the map edges (180 or -180 longitude) 
    and creates two new LineStrings that stop at the map edges.

    """
    result = gdf.copy()
    len_orig = len(gdf)
    # Extract the LineStrings that cross the anitmeridian.
    lon_span = gdf['geometry'].apply(lambda geom: abs(geom.coords[0][0] - geom.coords[1][0]))
    to_replace = result[lon_span > 180]
    
    # Iterate over rows and get new rows for each crossing edge.
    rows_to_add = pd.DataFrame()
    for index, row in to_replace.iterrows():
        max_node_id = max(result['node_id1'].max(), result['node_id2'].max())
        new_rows = split_linestring_at_antimeridian(row, max_node_id)
        # Replace the original row with the new rows.
        print(f'Replacing row\n {row.to_frame().T} \nwith new rows\n {new_rows}.')
        rows_to_add = pd.concat([rows_to_add, new_rows], ignore_index=True)
   
    # Add all the new rows and drop the old ones.
    result = pd.concat([result[lon_span <= 180], rows_to_add], ignore_index=True)
    len_new = len(result)
    print(f"Split {len(to_replace)} edges that crossed the antimeridian into {len_new - len_orig} new edges.")
    print(f"Total edges before splitting: {len_orig}.\nTotal edges after splitting: {len_new}.")
   
    # Check for longitude jumps > 180 in the result, and print a warning if any are found.
    result_lon_span = result['geometry'].apply(lambda geom: abs(geom.coords[0][0] - geom.coords[1][0]))
    if (result_lon_span > 180).any():
        print("Warning: Found edges that still cross the antimeridian after splitting. This may indicate an issue with the splitting logic.")
    return result


def split_linestring_at_antimeridian(row, max_node_id):
    """Split a LineString into two LineStrings if it crosses the antimeridian
    
    ...and calculate new nodes at the crossing points on the map edges (180 
    or -180 longitude).

    Parameters:
        row: A row from the GeoDataFrame containing the LineString to split. 
        Must have columns 'node_id1', 'node_id2', and 'geometry'.
        max_node_id: The maximum node ID currently in the data frame, used to 
        assign new node IDs for the crossing points.

    Returns: A new data frame containing the two new rows to replace the original 
    row, with new node IDs and geometries for the split LineStrings. If the 
    original row does not cross the antimeridian, returns a data frame containing
    just the original row.
    
    """
    # Get the geometry of the row, which should be a LineString.
    geom = row['geometry']
    # Extract coordinates from the geometry
    coords = list(geom.coords)
    (existing_node_lon1, existing_node_lat1), (existing_node_lon2, existing_node_lat2) = coords
    # Get the node IDs.
    existing_node_id1 = row['node_id1']
    existing_node_id2 = row['node_id2']
    
    # Failsafe: Only apply to exactly 2-node linestrings
    if len(coords) != 2:
        print(f"Warning: Geometry with node IDs {existing_node_id1} and {existing_node_id2} does not have exactly 2 nodes. Skipping.")
        print(row)
        return geom
    
    # A longitude jump of > 180 degrees indicates an antimeridian crossing
    if abs(existing_node_lon1 - existing_node_lon2) > 180:
        if existing_node_lon1 > 0:
            # Segment originates in the Eastern Hemisphere and crosses East (e.g., 170 to -170)
            new_node_lon1 = 180.0
            new_node_lon2 = -180.0
            dist1 = 180.0 - existing_node_lon1
            dist2 = existing_node_lon2 + 180.0 # equivalent to lon2 - (-180.0)
        else:
            # Segment originates in the Western Hemisphere and crosses West (e.g., -170 to 170)
            new_node_lon1 = -180.0
            new_node_lon2 = 180.0
            dist1 = existing_node_lon1 + 180.0 # equivalent to lon1 - (-180.0)
            dist2 = 180.0 - existing_node_lon2
            
        total_dist = dist1 + dist2
        
        # Interpolate the latitude at the exact crossing point (+/- 180)
        fraction = dist1 / total_dist if total_dist != 0 else 0.5
        new_node_lat = existing_node_lat1 + fraction * (existing_node_lat2 - existing_node_lat1)
        
        # Create the two new broken LineStrings that stop at the map edges
        segment1 = shapely.LineString([
            (existing_node_lon1, existing_node_lat1), 
            (new_node_lon1, new_node_lat)
        ])
        segment2 = shapely.LineString([
            (new_node_lon2, new_node_lat), 
            (existing_node_lon2, existing_node_lat2)
        ])

        # Put everything together into a new data frame to replace row.
        new_node_id1 = max_node_id + 1
        new_node_id2 = max_node_id + 2
        new_rows = pd.DataFrame({
            'node_id1': [existing_node_id1, new_node_id2],
            'node_id2': [new_node_id1, existing_node_id2],
            'geometry': [segment1, segment2]
        })
        
        # Package back into a MultiLineString
        return new_rows
        
    # If it doesn't cross, return the original geometry
    return row.to_frame().T


def add_north_pole_edge(gdf):
    """Add a LineString edge across the north pole to help in polygonization.
    
    Not currently used.
    """
    north_pole_node_id1 = max(gdf['node_id1'].max(), gdf['node_id2'].max()) + 1
    north_pole_node_id2 = north_pole_node_id1 + 1
    north_pole_edge = shapely.LineString([(-180.0, 90.0), (180.0, 90.0)])
    north_pole_row = pd.DataFrame({
        'node_id1': [north_pole_node_id1],
        'node_id2': [north_pole_node_id2],
        'geometry': [north_pole_edge]
    })
    result = pd.concat([gdf, north_pole_row], ignore_index=True)
    return result


def add_south_pole_edge(gdf):
    """Add a LineString edge along the south pole to ensure that Antarctica is included in the polygonization."""
    south_pole_node_id1 = max(gdf['node_id1'].max(), gdf['node_id2'].max()) + 1
    south_pole_node_id2 = south_pole_node_id1 + 1
    south_pole_edge = shapely.LineString([(-180.0, -90.0), (180.0, -90.0)])
    south_pole_row = pd.DataFrame({
        'node_id1': [south_pole_node_id1],
        'node_id2': [south_pole_node_id2],
        'geometry': [south_pole_edge]
    })
    result = pd.concat([gdf, south_pole_row], ignore_index=True)
    return result


def join_antimeridian_edges(gdf):
    """Closes the open loops at the antimeridian.
    
    After splitting edges at the antimeridian, we have pairs of edges that touch 
    the -180 and 180 longitude lines. We need to join these up with new 
    north-south edges to create closed loops for polygonization.
    
    We start at the south pole and work northwards, because we know Antartica 
    is the first land area. We assume that the edges are well-behaved and 
    that we can just join up the first two edges that touch the -180 longitude 
    line, then the next two, etc. We do the same for the 180 longitude line. 
    If this is not the case, we print a warning and continue anyway, 
    which may lead to issues with polygonization.
    """
    # Get the edges that touch the -180 longitude edge.
    west_edges = gdf[gdf['geometry'].apply(lambda geom: geom.coords[0][0] == -180.0 or geom.coords[1][0] == -180.0)]
    # Order them by latitude, increasing from the south pole to the north pole.
    west_edges = west_edges.sort_values(by='geometry', ascending=True, key=lambda geoms: geoms.apply(lambda geom: max(geom.coords[0][1], geom.coords[1][1])))
    # Get the edges that touch the 180 longitude edge.
    east_edges = gdf[gdf['geometry'].apply(lambda geom: geom.coords[0][0] == 180.0 or geom.coords[1][0] == 180.0)]
    # Order them by latitude, increasing from the south pole to the north pole.
    east_edges = east_edges.sort_values(by='geometry', ascending=True, key=lambda geoms: geoms.apply(lambda geom: max(geom.coords[0][1], geom.coords[1][1])))
    # Check that len of east_edges and west_edges is the same, and that both are even.
    if len(west_edges) != len(east_edges):
        print(f"Warning: Number of west edges ({len(west_edges)}) does not match number of east edges ({len(east_edges)}). This may indicate an issue with the data.")
    if len(west_edges) % 2 != 0:
        print(f"Warning: Number of west edges ({len(west_edges)}) is not even. This may indicate an issue with the data.")
    # We now assume that everything is nicely behaved, so we can work
    # down the list of west and east edges and join 0 to 1, 2 to 3, etc. at the north pole.
    for (edge_set, edge_lon) in zip([east_edges, west_edges], [180.0, -180.0]):
        for i in range(0, len(edge_set), 2):
            edge1 = edge_set.iloc[i]
            edge2 = edge_set.iloc[i + 1]
            # Create a new LineString that goes from the point on the first edge that is on the antimeridian, to the point on the second edge that is on the antimeridian.
            edge1_coords = list(edge1['geometry'].coords)
            edge2_coords = list(edge2['geometry'].coords)
            if edge1_coords[0][0] == edge_lon:
                edge1_antimeridian_point = edge1_coords[0]
                edge1_antimeridian_node = edge1['node_id1']
            else:
                edge1_antimeridian_point = edge1_coords[1]
                edge1_antimeridian_node = edge1['node_id2']
            if edge2_coords[0][0] == edge_lon:
                edge2_antimeridian_point = edge2_coords[0]
                edge2_antimeridian_node = edge2['node_id1']
            else:                
                edge2_antimeridian_point = edge2_coords[1]
                edge2_antimeridian_node = edge2['node_id2']
            new_edge = shapely.LineString([edge1_antimeridian_point, edge2_antimeridian_point])
            # Add the new edge to the data frame.
            new_row = pd.DataFrame({
                'node_id1': [edge1_antimeridian_node],
                'node_id2': [edge2_antimeridian_node],
                'geometry': [new_edge]
            })
            print(f'Adding new edge to join antimeridian edges:\n {new_row}.')
            gdf = pd.concat([gdf, new_row], ignore_index=True)
    return gdf   


def create_global_extent_geodataframe():
    # Create a GeoDataFrame with a polygon that covers the entire globe.
    global_extent_edge = shapely.LineString([(-180.0, -90.0), (-180.0, 90.0), (180.0, 90.0), (180.0, -90.0), (-180.0, -90.0)])
    global_extent_edge = shapely.Polygon(global_extent_edge)
    gdf_global_extent = gpd.GeoDataFrame({'id': ['global_extent'], 'geometry': [global_extent_edge]})
    gdf_global_extent.crs = "EPSG:4326"
    return gdf_global_extent                     


def poylgonize_global_extent(gdf):
    """Convert geopandas data frame containing edges to polygons."""
    # Split any edges that cross the antimeridian into two edges.
    print('Splitting edges that cross the antimeridian...')
    gdf = split_df_at_antimeridian(gdf)
    # Add north pole nodes (south pole already dealt with by Antarctic boundary edges).
    print('Adding south pole...')
    gdf = add_south_pole_edge(gdf)
    # Join up the -180 and 180 edges at the north pole to create a closed loop for polygonization.
    print('Joining edges at the antimeridian...')
    gdf = join_antimeridian_edges(gdf)
    # Polygonize the edges.
    print('Converting edges to polygons...')
    (
        valid,
        cut_edges,
        dangles,
        invalid
    ) = gdf['geometry'].polygonize(node=False, full=True)
    if len(invalid) > 0:
        print(f"Warning: Found {len(invalid)} invalid geometries during polygonization.")
    if len(dangles) > 0:
        print(f"Warning: Found {len(dangles)} dangles during polygonization.")
    if len(cut_edges) > 0:
        print(f"Warning: Found {len(cut_edges)} cut edges during polygonization.")
    # Subtract the valid polygons from the global extent to get the final combined 
    # GeoDataFrame of valid polygons and remaining holes.
    valid.crs = "EPSG:4326"   
    print('Creating global extent GeoDataFrame...')
    gdf_global_extent = create_global_extent_geodataframe()
    print('Subtracting valid polygons from global extent to get holes. This can be quite slow -- take a coffee break or go for a walk...')
    gdf_combined = gpd.overlay(gdf_global_extent, valid.to_frame(), how='difference')
    return gdf_combined


if __name__ == "__main__":
    grid_filepath = get_grid_filepath()
    node_dict = parse_grid_file_line_by_line(grid_filepath)
    df_boundary_edges = remove_duplicate_edges_in_chunks(node_dict)
    df_node = get_node_data_frame(node_dict)
    gdf_boundary_edges = get_boundary_edges_geopandas(df_boundary_edges, df_node)
    gdf_combined = poylgonize_global_extent(gdf_boundary_edges)
    filename = "stofs_2d_glo.shp"
    print(f"Saving to shapefile {filename}...")
    gdf_combined.to_file(filename)