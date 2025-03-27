"""
-*- coding: utf-8 -*-

Documentation for Script parse_ofs_ctlfile.py

Directory Location:   path/to/ofs_dps/server/bin/model_processing

Technical Contact(s): Name:  FC, XC

This script contains the function to parse OFS control files.

Language:  Python 3.11

usage: Called by bin/visualization/create_1dplot.py 
             and bin/model_processing/get_node_ofs.py

Arguments:
    - filename: filename of control file to be parse
Returns: 
    - Parsed lines
    - nodes
    - depths
    - shifts
    - ids

Author Name:  XC       Creation Date:  03/06/2025

Revisions:
    Date          Author             Description
"""



def parse_ofs_ctlfile(filename):
    """
    Method A: Reads and parses the OFS control file.

    :param filename: Path to the control file.
    :return: Parsed lines, nodes, depths, shifts, and ids.
    """
    import numpy as np
    with open(filename, mode="r", encoding="utf-8") as file:
        model_ctlfile = file.read()
        lines = model_ctlfile.split("\n")
        lines = [i.split(" ") for i in lines]
        lines = [list(filter(None, i)) for i in lines]

        nodes = np.array(lines[:-1])[:, 0]
        nodes = [int(i) for i in nodes]

        depths = np.array(lines[:-1])[:, 1]
        depths = [int(i) for i in depths]

        # This is the shift that can be applied to the ofs timeseries,
        # for instance if there is a known bias in the model
        shifts = np.array(lines[:-1])[:, -1]
        shifts = [float(i) for i in shifts]

        # This is the station id, of the nearest station to the mesh node
        ids = np.array(lines[:-1])[:, -2]
        ids = [str(i) for i in ids]

    return lines, nodes, depths, shifts, ids


