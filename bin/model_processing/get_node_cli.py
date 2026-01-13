"""
This is the entry point for the final model 1d extraction function,
it opens the path and looks for the model ctl file,if model ctl file is
found, then the script uses it for extracting the model timeseries if
model ctl file is not found, all the predefined function for finding the
nearest node and depth are applied and a new model ctl file is created along
with the time series
"""

import argparse

from ofs_skill.model_processing.get_node_ofs import get_node_ofs

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        prog='python get_node_ofs.py',
        usage='%(prog)s',
        description='Create model control files & time series',
    )
    parser.add_argument(
        '-o', '--OFS',
        required=True,
        help="""Choose from the list on the ofs_extents/folder,
        you can also create your own shapefile, add it at the
        ofs_extents/folder and call it here""", )
    parser.add_argument(
        '-p', '--Path',
        required=True,
        help='Inventory File path where ofs_extents/folder is located', )
    parser.add_argument(
        '-s', '--StartDate_full',
        required=True,
        help='Assessment start date: YYYY-MM-DDThh:mm:ssZ '
        "e.g. '2023-01-01T12:34:00Z'")
    parser.add_argument(
        '-e', '--EndDate_full',
        required=True,
        help='Assessment end date: YYYY-MM-DDThh:mm:ssZ '
        "e.g. '2023-01-01T12:34:00Z'")
    parser.add_argument(
        '-d', '--Datum',
        required=False,
        default='MLLW',
        help="datum options: 'MHW', 'MHHW' \
        'MLW', 'MLLW', 'NAVD88', 'XGEOID20B', 'IGLD85', 'LWD'")
    parser.add_argument(
        '-ws', '--Whichcast',
        required=False,
        default='nowcast',
        help="whichcasts: 'nowcast', 'forecast_b', 'forecast_a'", )
    parser.add_argument(
        '-t', '--FileType',
        required=False,
        default='stations',
        help="OFS model output file type to use: 'fields' or 'stations'", )
    parser.add_argument(
        '-f',
        '--Forecast_Hr',
        required=False,
        default='00hr',
        help='Specify model cycle to assess. Used with forecast_a mode only: '
        "'02hr', '06hr', '12hr', ... ", )
    parser.add_argument(
        '-hs',
        '--Horizon_Skill',
        action='store_true',
        help='Use all available forecast horizons between the '
        'start and end dates? True or False (boolean)')
    parser.add_argument(
        '-vs',
        '--Var_Selection',
        required=False,
        default='water_level,water_temperature,salinity,currents',
        help='Which variables do you want to skill assess? Options are: '
            'water_level, water_temperature, salinity, and currents. Choose '
            'any combination. Default (no argument) is all variables.')
    parser.add_argument(
        '-ui',
        '--User_Input',
        action='store_true',
        help='Input custom coordinates for model time series extraction? '
        'True or False (boolean)')


    args = parser.parse_args()
    prop1 = ModelProperties()
    prop1.ofs = args.OFS.lower()
    prop1.path = args.Path
    prop1.start_date_full = args.StartDate_full
    prop1.end_date_full = args.EndDate_full
    prop1.whichcast = args.Whichcast
    prop1.datum = args.Datum.upper()
    prop1.model_source = get_model_source(args.OFS)
    prop1.ofsfiletype = args.FileType
    prop1.horizonskill = args.Horizon_Skill
    prop1.forecast_hr = args.Forecast_Hr
    prop1.var_list = args.Var_Selection
    prop1.user_input_location = args.User_Input

    # Switch default datum if GLOFS
    if 'l' in prop1.ofs[0] and prop1.datum == 'MLLW':
        prop1.datum = 'IGLD85'

    get_node_ofs(prop1, None)
