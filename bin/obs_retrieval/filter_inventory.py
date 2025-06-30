"""
-*- coding: utf-8 -*-

Documentation for Scripts filter_inventory.py

Script Name: filter_inventory.py

Directory Location:   /path/to/ofs_dps/server/bin/obs_retrieval/

Technical Contact(s): Name:  AJK     Org:    NOS/CO-OPS

Abstract:

   This script is used to filter the final inventory file, 
   and remove duplicates based on station ID 
   (developed to handle stations duplicated between NDBC and CO-OPS.
   Precedent is given to CO-OPS stations)

Language:  Python 3.8

Estimated Execution Time: < 1 second 

Scripts/Programs Called:

Usage: python filter_inventory.py

Filter Inventory

Arguments:

Output:
Name                 Description
dataset_dropped       Filtered Pandas Dataframe with ID, X, Y, Source, and
                      Name info for all stations withing lat and lon 1 and 2

Author Name:  AJK       Creation Date:  01/02/2024

Revisions:
Date          Author     Description

Remarks:

"""
# Libraries:
import pandas as pd
import numpy as np

def filter_inventory(dataset):
    droplist = []
    
    for i, id in enumerate(dataset['ID']):
        indx = dataset['Name'].str.find(id)
        if np.max(indx)==0:
            droplist.append(*np.argwhere(indx == 0)[0])

    dataset_dropped = dataset.drop(dataset.index[droplist])
    return dataset_dropped 
