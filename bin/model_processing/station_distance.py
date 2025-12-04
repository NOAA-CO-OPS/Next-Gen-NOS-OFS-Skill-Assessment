'''
This script is used to calculated the distance between two points
(e.g., station and model node)
on a sphere with diameter = diameter of the Earth
'''

from math import cos, asin, sqrt


def station_distance(lat1, lon1, lat2, lon2):
    '''
    p is the factor to convert an angle expressed in degrees to
    radians: π/180 = 0.017453292519943295...\
    hav is the haversine calculated using the above formula\
    12742 is the diameter of the earth expressed in km, and is thus
    the value of 2𝑟 in the above formula.
    '''

    pvalue = 0.017453292519943295
    hav = 0.5 - cos((lat2 - lat1) * pvalue) / 2 + cos(lat1 * pvalue) * cos(
        lat2 * pvalue) * (1 - cos((lon2 - lon1) * pvalue)) / 2
    return 12742 * asin(sqrt(hav))
