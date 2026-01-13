"""
OFS Control File Parsing

Functions for reading and parsing OFS control files that map observation
stations to model nodes.
"""

from pathlib import Path

import numpy as np


def parse_ofs_ctlfile(filename: str) -> tuple[list[list[str]], list[int], list[int], list[float], list[str]]:
    """
    Read and parse an OFS control file.

    Control files contain mappings between observation stations and model nodes,
    including node indices, depth levels, bias corrections, and station IDs.

    Parameters
    ----------
    filename : str
        Path to the control file to be parsed

    Returns
    -------
    lines : List[List[str]]
        Raw parsed lines from the control file, each line split into fields
    nodes : List[int]
        Model node indices (column 0)
    depths : List[int]
        Depth level indices (column 1)
    shifts : List[float]
        Bias correction shifts to apply to model data (last column)
    ids : List[str]
        Observation station IDs (second-to-last column)

    Raises
    ------
    FileNotFoundError
        If the control file does not exist
    ValueError
        If the control file format is invalid

    Notes
    -----
    Control file format (space-delimited):
        <node> <depth> <lat> <lon> <station_id> <shift>

    Example line:
        145 0 37.5 -76.3 8573364 0.0

    where:
        - node: Model node index (145)
        - depth: Depth level index (0 for surface)
        - lat, lon: Station location
        - station_id: Observation station ID (8573364)
        - shift: Bias correction in meters (0.0)

    Examples
    --------
    >>> filename = "control_files/cbofs_wl_model_station.ctl"
    >>> lines, nodes, depths, shifts, ids = parse_ofs_ctlfile(filename)
    >>> print(f"Found {len(nodes)} stations")
    Found 42 stations
    >>> print(f"First node: {nodes[0]}, station: {ids[0]}")
    First node: 145, station: 8573364

    See Also
    --------
    write_ofs_ctlfile : Generate control files
    """
    # Validate file exists
    file_path = Path(filename)
    if not file_path.exists():
        raise FileNotFoundError(f'Control file not found: {filename}')

    # Read and parse the control file
    with open(filename, encoding='utf-8') as file:
        model_ctlfile = file.read()

    # Split into lines and parse
    lines = model_ctlfile.split('\n')
    lines = [line.split(' ') for line in lines]
    # Remove empty strings from each line
    lines = [list(filter(None, line)) for line in lines]

    # Filter out empty lines (which would be empty lists after filtering)
    lines = [line for line in lines if line]

    # Extract data columns
    lines_array = np.array(lines)

    # Node indices (column 0)
    nodes = lines_array[:, 0].astype(int).tolist()

    # Depth level indices (column 1)
    depths = lines_array[:, 1].astype(int).tolist()

    # Bias correction shifts (last column)
    # This is the shift that can be applied to the OFS timeseries,
    # for instance if there is a known bias in the model
    shifts = lines_array[:, -1].astype(float).tolist()

    # Station IDs (second-to-last column)
    # This is the station ID of the nearest observation station to the mesh node
    ids = lines_array[:, -2].astype(str).tolist()

    return lines, nodes, depths, shifts, ids
