'''
This script gives the model used in any OFS
'''
from __future__ import annotations


def model_source(ofs):
    '''
    This is a simple if function that relates ofs with the model
    '''
    if ofs in ('cbofs', 'dbofs', 'gomofs', 'tbofs', 'ciofs', 'wcofs'):
        model_source = 'roms'

    elif ofs in ('nyofs', 'sjrofs'):
        model_source = 'pom'

    elif ofs in ('necofs', 'ngofs2', 'ngofs', 'leofs', 'lmhofs', 'loofs', 'lsofs',
                 'sfbofs', 'sscofs'):
        model_source = 'fvcom'

    elif ofs in ('stofs_3d_atl', 'stofs_3d_pac', 'loofs-nextgen'):
        model_source = 'schism'

    return model_source
