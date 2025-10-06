"""
This script is called by get_node_ofs.py in case the ofs control file is
not found
if model ctl file is not found, all the predefined function for finding
the nearest node and depth are applied to create the ofs control file
"""

import os
import sys
import logging
import logging.config
import argparse
from datetime import datetime
import pandas as pd
import xarray as xr

from pathlib import Path
# Add parent directory to sys.path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from model_processing import model_properties
from model_processing import model_source
from obs_retrieval import utils
from model_processing import list_of_files
from model_processing import intake_scisa


def get_icecover_model(prop, logger):
    """
    write_ofs_ctlfile
    """
    prop.model_source = model_source.model_source(prop.ofs)
    if logger is None:
        config_file = utils.Utils().get_config_file()
        log_config_file = "conf/logging.conf"
        log_config_file = (Path(__file__).parent.parent.parent / log_config_file).resolve()

        # Check if log file exists
        if not os.path.isfile(log_config_file):
            sys.exit(-1)
        # Check if config file exists
        if not os.path.isfile(config_file):
            sys.exit(-1)

        # Creater logger
        logging.config.fileConfig(log_config_file)
        logger = logging.getLogger("root")
        logger.info("Using config %s", config_file)
        logger.info("Using log config %s", log_config_file)

    logger.info("--- Starting OFS Model ice cover process ---")

    start_date_full = prop.start_date_full.replace("-", "")
    end_date_full = prop.end_date_full.replace("-", "")
    start_date_full = start_date_full.replace("Z", "")
    end_date_full = end_date_full.replace("Z", "")
    start_date_full = start_date_full.replace("T", "-")
    end_date_full = end_date_full.replace("T", "-")

    prop.startdate = (datetime.strptime(
        start_date_full.split("-")[0], "%Y%m%d")).strftime(
        "%Y%m%d") + "00"
    prop.enddate = (datetime.strptime(
        end_date_full.split("-")[0], "%Y%m%d")).strftime(
        "%Y%m%d") + "23"

    if os.path.isfile(
        prop.data_model_ice_path + f"/{prop.ofs}_{prop.whichcast}_"
        f"{str(prop.startdate)}"
        f"_{str(prop.enddate)}.nc"
    ):
        concated_model = xr.open_dataset(
            prop.data_model_ice_path + f"/{prop.ofs}_{prop.whichcast}_"
            f"{str(prop.startdate)}_"
            f"{str(prop.enddate)}.nc",
            decode_times=False,
        )

        if prop.model_source=="fvcom":
            concated_model["time"] = pd.date_range(
                start=concated_model.time.attrs["units"].split("since")[-1],
                periods=concated_model.sizes["time"],
                freq="D",
            )
        elif prop.model_source=="roms":
            concated_model["time"] = pd.date_range(
                start=concated_model.ocean_time.attrs["units"].split("since")[-1],
                periods=concated_model.sizes["ocean_time"],
                freq="H",
            )

        logger.info(
            "Model Concatenated File (%s_%s_%s_%s.nc) "
            "found in %s",
            prop.ofs,
            prop.whichcast,
            str(prop.startdate),
            str(prop.enddate),
            prop.data_model_ice_path,
        )

    else:
        logger.info(
            "Model Concatenated File (%s_%s_%s_%s.nc) not found "
            "in %s.Searching for hourly files",
            prop.ofs,
            prop.whichcast,
            prop.startdate,
            prop.enddate,
            prop.data_model_ice_path,
        )
    dir_list = list_of_files.list_of_dir(prop, logger)
    list_files = list_of_files.list_of_files(prop, dir_list, logger)

    # If daily resolution, pare down list of files to one per day
    logger.info(
        "Trimming list of model output files from hourly to daily..."
        )
    if prop.ice_dt == 'daily':
        list_files_daily = []
        list_files_len = len(list_files)
        for i in range(0, list_files_len):
            if (prop.whichcast == 'nowcast'
                and 't12z' in list_files[i]
                and 'n006' in list_files[i]
                ):
                list_files_daily.append(list_files[i])
            elif (prop.whichcast == 'forecast_b'
                  #Use previous cycle and grab fcast file @ 12Z -- makes
                  #a 6-hour forecast window
                  and 't06z' in list_files[i]
                  and 'f006' in list_files[i]
                  ):
                list_files_daily.append(list_files[i])
            elif prop.whichcast == 'forecast_a':
                logger.error("Ice error: forecast_a must be hourly resolution!")
                sys.exit(-1)


    if len(list_files) > 0 and len(list_files_daily) > 0:
        list_files = list_files_daily
        logger.info(
            "Model %s **icecover** netcdf files for the OFS %s found for the period "
            "from %s to %s",
            prop.whichcast,
            prop.ofs,
            prop.startdate,
            prop.enddate,
        )
        logger.info("Starting the concatenation")
        #Concat and save file
        # concated_model = concat_ofs_icecover.concat_ofs_icecover(
        #     prop, logger)
        logger.info('About to call intake_scisa from write_ofs_ctlfile.')
        concated_model = intake_scisa.intake_model(list_files, prop, logger)
        logger.info('Returned from call to intake_scisa inside of write_ofs_ctlfile.')

    return concated_model

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        prog="python get_icecover_model.py",
        usage="%(prog)s",
        description="OFS Create Model CTL File",
    )

    parser.add_argument(
        "-o",
        "--OFS",
        required=True,
        help="Choose from the list on the OFS_Extents folder, you can also "
             "create your own shapefile, add it top the OFS_Extents folder "
             "and call it here",
    )
    parser.add_argument(
        "-p",
        "--Path",
        required=False,
        help="Use /home as the default. User can specify path",
    )
    parser.add_argument(
        "-s",
        "--StartDate",
        required=True,
        help="Start Date YYYY-MM-DDThh:mm:ssZ e.g. '2023-01-01T12:34:00Z'",
    )
    parser.add_argument(
        "-e",
        "--EndDate",
        required=True,
        help="End Date YYYY-MM-DDThh:mm:ssZ e.g. '2023-01-01T12:34:00Z'",
    )
    parser.add_argument(
        "-w",
        "--Whichcasts",
        required=False,
        help="nowcast, forecast_a, "
             "forecast_b(it is the forecast between cycles)",
    )
    parser.add_argument(
        "-f",
        "--Forecast_Hr",
        required=False,
        help="'02hr', '06hr', '12hr', '24hr' ... ",
    )

    args = parser.parse_args()
    prop1 = model_properties.ModelProperties()
    prop1.ofs = args.OFS
    prop1.path = args.Path
    prop1.ofs_extents_path = r"" + prop1.path + "ofs_extents" + "/"
    prop1.start_date_full = args.StartDate
    prop1.end_date_full = args.EndDate
    prop1.whichcast = args.Whichcast

    prop1.model_source = model_source.model_source(args.OFS)

    if prop1.whichcast != "forecast_a":
        prop1.forecast_hr = None
    else:
        prop1.forecast_hr = args.Forecast_Hr

    get_icecover_model(prop1, None)
