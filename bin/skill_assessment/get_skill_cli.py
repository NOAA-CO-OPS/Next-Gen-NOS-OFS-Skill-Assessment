"""
Created on Mon Apr 13 21:23:02 2026

@author: PWL

This is the entry point for the main skill assessment module. It loads .prd
and .obs files, pairs them, and then writes .int files and skill tables.

It can only be used as an entry point if .prd and .obs files are already present!

"""

import argparse

from ofs_skill.model_processing import model_properties
from ofs_skill.skill_assessment.get_skill import get_skill

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        prog='python get_skill.py', usage='%(prog)s',
        description='Run skill assessment'
    )

    parser.add_argument(
        '-o',
        '--OFS',
        required=True,
        help='Choose from the list on the prop.ofs_Extents folder, '
        'you can also create your own shapefile, add it to the '
        'prop.ofs_Extents folder and call it here',
    )
    parser.add_argument(
        '-p',
        '--Path',
        required=False,
        help='Use /home as the default. User can specify path',
    )
    parser.add_argument(
        '-s',
        '--StartDate_full',
        required=True,
        help="Start Date YYYY-MM-DDThh:mm:ssZ e.g. '2023-01-01T12:34:00Z'",
    )
    parser.add_argument(
        '-e',
        '--EndDate_full',
        required=True,
        help="End Date YYYY-MM-DDThh:mm:ssZ e.g. '2023-01-01T12:34:00Z'",
    )
    parser.add_argument(
        '-w',
        '--Whichcast',
        required=False,
        help='nowcast, forecast_a, '
             'forecast_b (it is the forecast between cycles)',
    )
    parser.add_argument(
        '-d',
        '--Datum',
        required=True,
        help="datum: 'MHHW', 'MHW', 'MLW', 'MLLW', 'NAVD88', 'XGEOID20B', 'IGLD85','LWD'"
    )
    parser.add_argument(
        '-f',
        '--Forecast_Hr',
        required=False,
        help="'02z', '06z', '12z', '24z' ... ",
    )
    parser.add_argument(
        '-t', '--FileType', required=True,
        help="OFS output file type to use: 'fields' or 'stations'", )
    parser.add_argument(
        '-so',
        '--Station_Owner',
        required=False,
        default='co-ops,ndbc,usgs,chs',
        help='Input station provider to use in skill assessment: '
        "'CO-OPS', 'NDBC', 'USGS', 'CHS',", )
    parser.add_argument(
        '-vs',
        '--Var_Selection',
        required=False,
        default='water_level,water_temperature,salinity,currents',
        help='Which variables do you want to skill assess? Options are: '
            'water_level, water_temperature, salinity, and currents. Choose '
            'any combination. Default (no argument) is all variables.')
    parser.add_argument(
        '-hs',
        '--Horizon_Skill',
        action='store_true',
        help='Use all available forecast horizons between the '
        'start and end dates? True or False (boolean)')

    args = parser.parse_args()
    prop1 = model_properties.ModelProperties()
    prop1.ofs = args.OFS
    prop1.path = args.Path
    prop1.start_date_full = args.StartDate_full
    prop1.end_date_full = args.EndDate_full
    prop1.whichcast = args.Whichcast
    prop1.datum = args.Datum.upper()
    prop1.ofsfiletype = args.FileType.lower()
    prop1.stationowner = args.Station_Owner
    prop1.var_list = args.Var_Selection
    prop1.horizonskill = args.Horizon_Skill
    prop1.user_input_location = False

    prop1.forecast_hr = None
    if prop1.whichcast == 'forecast_a':
        prop1.forecast_hr = args.Forecast_Hr

    get_skill(prop1, None)
