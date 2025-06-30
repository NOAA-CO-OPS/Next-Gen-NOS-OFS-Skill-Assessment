'''
Properties for CO-OPS Tides and Currents observation
retrieval. This is called by 'retrieve_t_and_c_station.py'
and returns a list of pre-allocated properties to be filled
in by retrieve_t_and_c_station.py.
'''


class TidesandCurrentsProperties:
    ''' properties for Tides and Currents'''
    mdapi_url = ""
    api_url = ""
    start_dt_0 = None
    end_dt_0 = None
    start_dt = None
    end_dt = None
    total_date = []
    total_var = []
    total_dir = []
    station_url = ""
    station_url_2 = ""
    depth = 0.0
    depth_url = None
    delta = None
    date = []
    var = []
    drt = []
