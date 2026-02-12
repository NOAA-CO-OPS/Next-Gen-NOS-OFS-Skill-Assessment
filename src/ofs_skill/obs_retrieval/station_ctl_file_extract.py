"""
Station Control File Extraction

Extract and parse observation station information from control files.
"""

import os
from typing import Optional


def station_ctl_file_extract(ctlfile_path: str) -> Optional[tuple[list[list[str]], list[list[str]]]]:
    """
    Extract station information from an observation control file.

    Control files contain station metadata in alternating lines:
    - Even lines (0, 2, 4...): Station IDs, names, and source
    - Odd lines (1, 3, 5...): Geographic coordinates and datum info

    Parameters
    ----------
    ctlfile_path : str
        Path to the station control file

    Returns
    -------
    tuple or None
        (station_info, coord_info) if file exists and is valid, None otherwise
        - station_info: List of [ID, source_ID, name, source]
        - coord_info: List of [lat, lon, depth, datum, ...]

    Examples
    --------
    >>> result = station_ctl_file_extract('control_files/cbofs_wl_station.ctl')
    >>> if result:
    ...     station_info, coord_info = result
    ...     print(f"Found {len(station_info)} stations")
    ...     print(f"First station: {station_info[0]}")
    Found 42 stations
    First station: ['8573364', '8573364_COOPS', 'Chesapeake Bay Bridge Tunnel', 'COOPS']

    Notes
    -----
    Control file format (two lines per station):

    Line 1 (station info):
        <ID> <source_ID> "<station name>"

    Line 2 (coordinates):
        <lat> <lon> <depth> <other_params> <datum>

    Example:
        8573364 8573364_COOPS "Chesapeake Bay Bridge Tunnel"
        37.0 -76.0 0.0 0.0 MLLW

    The function handles missing or malformed entries gracefully.
    """
    # Check if file exists and has content
    if not os.path.exists(ctlfile_path):
        return None

    if os.path.getsize(ctlfile_path) == 0:
        return None

    # Read the control file
    with open(ctlfile_path, encoding='utf-8') as f:
        ctlfile = f.read()

    # Split into lines
    lines = ctlfile.split('\n')

    # Extract station info (even lines: 0, 2, 4, ...)
    lines1 = lines[0::2]
    lines1 = [i.split('"') for i in lines1]
    lines1 = [list(filter(None, i)) for i in lines1]

    # Format station information
    lines1_format = []
    for i in lines1:
        try:
            # Parse: <ID> <source_ID> "<station name>"
            first = i[0].split(' ')[0]  # Station ID
            second = i[0].split(' ')[1]  # Source ID (e.g., "8573364_COOPS")
            source = second.split('_')[-1]  # Extract source (e.g., "COOPS")
            third = i[1]  # Station name
            lines1_format.append([first, second, third, source])
        except (IndexError, AttributeError):
            # Skip malformed entries
            pass

    lines1 = lines1_format

    # Extract coordinate info (odd lines: 1, 3, 5, ...)
    lines2 = lines[1::2]
    lines2 = [i.split(' ') for i in lines2]
    lines2 = [list(filter(None, i)) for i in lines2]

    # Return both lists
    return lines1, lines2
