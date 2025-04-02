'''

Properties for USGS observation
retrieval. This is called by 'retrieve_usgs_station.py'
and returns a list of pre-allocated properties to be filled
in by retrieve_usgs_station.py.
'''



class USGSProperties:
    ''' Properties for USGS'''
    base_url = ""
    url = ""
    obs_final = None
    start = ""
    end = ""
    start_year = ""
    start_month = ""
    start_day = ""
    end_year = ""
    end_month = ""
    end_day = ""
    start_str = ""
    end_str = ""
