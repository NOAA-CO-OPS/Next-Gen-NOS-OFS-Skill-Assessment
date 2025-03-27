"""
This is the final model 1d extraction function, it opens the path and looks
 for the model ctl file,
if model ctl file is found, then the script uses it for extracting the model
 timeseries
if model ctl file is not found, all the predefined function for finding the
nearest node and depth are applied and a new model ctl file is created along
with the time series
"""

import sys
import os
import logging
import logging.config
from datetime import datetime, timedelta
import argparse
import math
import xarray as xr
import numpy as np
import pandas as pd

from pathlib import Path
# Add parent directory to sys.path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from obs_retrieval import format_obs_timeseries
from obs_retrieval import utils
from model_processing import model_format_properties
from model_processing import model_properties
from model_processing import write_ofs_ctlfile
from model_processing import model_source

from model_processing import intake_scisa
from model_processing import list_of_files


def name_convent(variable):
    """
    name convent
    """
    if variable == "water_level":
        name_var = "wl"
        model_var = "zeta"

    elif variable == "water_temperature":
        name_var = "temp"
        model_var = "temp"

    elif variable == "salinity":
        name_var = "salt"
        model_var = "salinity"

    elif variable == "currents":
        name_var = "cu"
        model_var = "currents"

    return name_var, model_var


def ofs_ctlfile_extract(prop, name_var, logger):
    """
    The input here is the path.
    Extracts data from an OFS control file. If the file does not exist,
    it generates it first.
    """

    if prop.ofsfiletype == 'fields':
        filename = f"{prop.control_files_path}/{prop.ofs}_{name_var}_model.ctl"
        if (os.path.isfile(
                filename)) is False:
                # or os.path.isfile(
                # f"{prop.data_model_1d_node_path}/{prop.ofs}_{prop.whichcast}"
                # f"_{str(prop.startdate)}_{str(prop.enddate)}.nc") is False):
            model = write_ofs_ctlfile.write_ofs_ctlfile(prop, logger)
    elif prop.ofsfiletype == 'stations':
        filename = f"{prop.control_files_path}/{prop.ofs}_{name_var}_model_station.ctl"
        if (os.path.isfile(
                filename)) is False:
                # or os.path.isfile(
                # f"{prop.data_model_1d_node_path}/{prop.ofs}_{prop.whichcast}"
                # f"_{str(prop.startdate)}_{str(prop.enddate)}.nc") is False):
            model = write_ofs_ctlfile.write_ofs_ctlfile(prop, logger)

    with open(
            filename, mode="r", encoding="utf-8"
    ) as file:
        model_ctlfile = file.read()
        lines = model_ctlfile.split("\n")
        lines = [i.split(" ") for i in lines]
        lines = [list(filter(None, i)) for i in lines]
        nodes = np.array(lines[:-1])[:, 0]
        nodes = [int(i) for i in nodes]
        depths = np.array(lines[:-1])[:, 1]
        depths = [int(i) for i in depths]

        # this is the shift that can be applied to the ofs timeseries,
        # for instance if there is a known bias in the model
        shifts = np.array(lines[:-1])[:, -1]
        shifts = [float(i) for i in shifts]

        # This is the station id, of the nearest station to the mesh node
        ids = np.array(lines[:-1])[:, -2]
        ids = [str(i) for i in ids]

        return lines, nodes, depths, shifts, ids


def roms_nodes(model, node_num):
    """
    This function converts the node from the ofs control file
    into i and j for ROMS
    """
    i_index,j_index = np.unravel_index(int(node_num),np.shape(model['lon_rho']))

    return i_index,j_index


def format_temp_salt(prop, model, ofs_ctlfile, model_var, i):
    """
    format temp salt
    """

    if prop.model_source=="fvcom":
        if prop.ofsfiletype == 'fields':
            model_time = np.array(model["time"])
            model_obs = np.array(
                model[model_var][:, int(ofs_ctlfile[2][i]), int(ofs_ctlfile[1][i])]
            )
            model_obs = model_obs + ofs_ctlfile[3][i]
        elif prop.ofsfiletype == 'stations':
            # Dimensions: time x siglay x station
            model_time = np.array(model["time"])
            if int(ofs_ctlfile[1][i]) > -999:
                model_obs = np.array(
                    model[model_var][:, int(ofs_ctlfile[2][i]), int(ofs_ctlfile[1][i])]
                )
                model_obs = model_obs + ofs_ctlfile[3][i]
            else:
                model_obs = None
    elif prop.model_source=="roms":
        if model_var=="salinity":
            model_var="salt"
        if prop.ofsfiletype == 'fields':
            i_index,j_index = roms_nodes(model, int(ofs_ctlfile[1][i]))
            model_time = np.array(model["ocean_time"])
            model_obs = np.array(model[model_var][:, int(ofs_ctlfile[2][i]), i_index,j_index])
            model_obs = model_obs + ofs_ctlfile[3][i]
        elif prop.ofsfiletype == 'stations':
            # Dimensions: time x station x s_rho
            model_time = np.array(model["ocean_time"])
            if int(ofs_ctlfile[1][i]) > -999:
                model_obs = np.array(model[model_var]
                                     [:, int(ofs_ctlfile[1][i]),
                                      int(ofs_ctlfile[2][i])])
                model_obs = model_obs + ofs_ctlfile[3][i]
            else:
                model_obs = None

    data_model = pd.DataFrame(
        {"DateTime": model_time,
         "OBS": model_obs}, columns=["DateTime", "OBS"]
    )

    start_date = (
        str(
            (
                datetime.strptime(prop.start_date_full.split("-")[0], "%Y%m%d")
                - timedelta(days=2)
            ).strftime("%Y%m%d")
        )
        + "-01:01:01"
    )
    end_date = (
        str(
            (
                datetime.strptime(prop.end_date_full.split("-")[0], "%Y%m%d")
                + timedelta(days=2)
            ).strftime("%Y%m%d")
        )
        + "-01:01:01"
    )

    formatted_series = \
        format_obs_timeseries.scalar(data_model, start_date, end_date)
    return formatted_series


def format_currents(prop, model, ofs_ctlfile, i):
    """
    format currents
    """

    if prop.model_source=="fvcom":
        mfp = model_format_properties.ModelFormatProperties()
        mfp.model_time = np.array(model["time"])
        if prop.ofsfiletype == 'fields':
            u_i = np.array(
                model["u"][:, int(ofs_ctlfile[2][i]), int(ofs_ctlfile[1][i])]
            )
            v_i = np.array(
                model["v"][:, int(ofs_ctlfile[2][i]), int(ofs_ctlfile[1][i])]
            )

            mfp.model_obs = np.array(u_i**2 + v_i**2) ** 0.5

            mfp.model_ang = np.array(
                [math.atan2(u_i[t], v_i[t]) / math.pi * 180 % 360.0 for t in range(
                    len(np.array(mfp.model_time)))])

            mfp.model_obs = mfp.model_obs + ofs_ctlfile[3][i]
        elif prop.ofsfiletype == 'stations':
            if int(ofs_ctlfile[1][i]) > -999:
                mfp = model_format_properties.ModelFormatProperties()
                mfp.model_time = np.array(model["time"])

                u_i = np.array(
                    model["u"][:, int(ofs_ctlfile[2][i]), int(ofs_ctlfile[1][i])]
                )
                v_i = np.array(
                    model["v"][:, int(ofs_ctlfile[2][i]), int(ofs_ctlfile[1][i])]
                )

                mfp.model_obs = np.array(u_i**2 + v_i**2) ** 0.5

                mfp.model_ang = np.array(
                    [math.atan2(u_i[t], v_i[t]) / math.pi * 180 % 360.0 for t in range(
                        len(np.array(mfp.model_time)))])

                mfp.model_obs = mfp.model_obs + ofs_ctlfile[3][i]
            else:
                mfp.model_obs = None
                mfp.model_ang = None
    elif prop.model_source=="roms":
        mfp = model_format_properties.ModelFormatProperties()
        mfp.model_time = np.array(model["ocean_time"])
        if prop.ofsfiletype == 'fields':
            i_index,j_index = roms_nodes(model, int(ofs_ctlfile[1][i]))
            u_i = np.array(model['u_east'][:, int(ofs_ctlfile[2][i]), i_index,j_index])
            v_i = np.array(model['v_north'][:, int(ofs_ctlfile[2][i]), i_index,j_index])

            mfp.model_obs = np.array(u_i**2 + v_i**2) ** 0.5
            mfp.model_ang = np.array(
                [math.atan2(u_i[t], v_i[t]) / math.pi * 180 % 360.0 for t in range(
                    len(np.array(mfp.model_time)))])

            mfp.model_obs = mfp.model_obs + ofs_ctlfile[3][i]
        elif prop.ofsfiletype == 'stations':
            # Dimensions: time x station x s_rho
            if int(ofs_ctlfile[1][i]) > -999:
                u_i = np.array(model['u_east'][:, int(ofs_ctlfile[1][i]), int(ofs_ctlfile[2][i])])
                v_i = np.array(model['v_north'][:, int(ofs_ctlfile[1][i]), int(ofs_ctlfile[2][i])])
                mfp.model_obs = np.array(u_i**2 + v_i**2) ** 0.5
                mfp.model_ang = np.array(
                    [math.atan2(u_i[t], v_i[t]) / math.pi * 180 % 360.0 for t in range(
                        len(np.array(mfp.model_time)))])

                mfp.model_obs = mfp.model_obs + ofs_ctlfile[3][i]
            else:
                mfp.model_obs = None
                mfp.model_ang = None

    mfp.data_model = pd.DataFrame(
        {"DateTime": mfp.model_time,
         "DIR": mfp.model_ang,
         "OBS": mfp.model_obs},
        columns=["DateTime", "DIR", "OBS"],
    )

    start_date = (
        str(
            (
                datetime.strptime(prop.start_date_full.split("-")[0], "%Y%m%d")
                - timedelta(days=2)
            ).strftime("%Y%m%d")
        )
        + "-01:01:01"
    )
    end_date = (
        str(
            (
                datetime.strptime(prop.end_date_full.split("-")[0], "%Y%m%d")
                + timedelta(days=2)
            ).strftime("%Y%m%d")
        )
        + "-01:01:01"
    )
    formatted_series = \
        format_obs_timeseries.vector(mfp.data_model, start_date, end_date)

    return formatted_series


def format_waterlevel(prop, model, ofs_ctlfile, model_var, i):
    """
    format waterlevel
    """

    if prop.model_source=="fvcom":
        if prop.ofsfiletype == 'fields':
            model_time = np.array(model["time"])
            model_obs = np.array(model[model_var][:, int(ofs_ctlfile[1][i])])
            model_obs = model_obs + ofs_ctlfile[3][i]
        elif prop.ofsfiletype == 'stations':
            model_time = np.array(model["time"])
            if int(ofs_ctlfile[1][i]) > -999:
                model_obs = np.array(model[model_var][:, int(ofs_ctlfile[1][i])])
                model_obs = model_obs + ofs_ctlfile[3][i]
            else:
                model_obs = None
    elif prop.model_source=="roms":
        if prop.ofsfiletype == 'fields':
            i_index,j_index = roms_nodes(model, int(ofs_ctlfile[1][i]))
            model_time = np.array(model["ocean_time"])
            model_obs = np.array(model[model_var][:, i_index,j_index])
            model_obs = model_obs + ofs_ctlfile[3][i]
        elif prop.ofsfiletype == 'stations':
            # Dimensions: time x stations
            #i_index = roms_station_nodes(model, int(ofs_ctlfile[1][i]))
            model_time = np.array(model["ocean_time"])
            if int(ofs_ctlfile[1][i]) > -999:
                model_obs = np.array(model[model_var][:,int(ofs_ctlfile[1][i])])
                model_obs = model_obs + ofs_ctlfile[3][i]
            else:
                model_obs = None

    data_model = pd.DataFrame(
        {"DateTime": model_time,
         "OBS": model_obs}, columns=["DateTime", "OBS"]
    )

    start_date = (
        str(
            (
                datetime.strptime(prop.start_date_full.split("-")[0], "%Y%m%d")
                - timedelta(days=2)
            ).strftime("%Y%m%d")
        )
        + "-01:01:01"
    )
    end_date = (
        str(
            (
                datetime.strptime(prop.end_date_full.split("-")[0], "%Y%m%d")
                + timedelta(days=2)
            ).strftime("%Y%m%d")
        )
        + "-01:01:01"
    )

    formatted_series = \
        format_obs_timeseries.scalar(data_model, start_date, end_date)

    return formatted_series


def parameter_validation(prop, dir_params, logger):
    """Parameter validation"""
    # Start Date and End Date validation

    try:
        start_dt = datetime.strptime(
            prop.start_date_full, "%Y-%m-%dT%H:%M:%SZ")
        end_dt = datetime.strptime(
            prop.end_date_full, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        error_message = f"Please check Start Date - " \
                        f"'{prop.start_date_full}', End Date -" \
                        f" '{prop.end_date_full}'. Abort!"
        logger.error(error_message)
        print(error_message)
        sys.exit(-1)

    if start_dt > end_dt:
        error_message = f"End Date {prop.end_date_full} " \
                        f"is before Start Date " \
                        f"{prop.start_date_full}. Abort!"
        logger.error(error_message)
        sys.exit(-1)

    if prop.path is None:
        prop.path = dir_params["home"]

    # Path validation
    ofs_extents_path = os.path.join(prop.path, dir_params["ofs_extents_dir"])
    if not os.path.exists(ofs_extents_path):
        error_message = f"ofs_extents/ folder is not found. " \
                        f"Please check Path - " \
                        f"'{prop.path}'. Abort!"
        logger.error(error_message)
        sys.exit(-1)

    # OFS validation
    shapefile = f"{ofs_extents_path}/{prop.ofs}.shp"
    if not os.path.isfile(shapefile):
        error_message = f"Shapefile '{prop.ofs}' " \
                        f"is not found at the folder" \
                        f" {ofs_extents_path}. Abort!"
        logger.error(error_message)
        sys.exit(-1)

    # Whichcast validation
    if (prop.whichcast is not None) and (
        prop.whichcast not in ["nowcast", "forecast_a", "forecast_b"]
    ):
        error_message = f"Please check Whichcast - " \
                        f"'{prop.whichcast}'. Abort!"
        logger.error(error_message)
        sys.exit(-1)

    if prop.whichcast == "forecast_a" and prop.forecast_hr is None:
        error_message = "Forecast_Hr is required if " \
                        "Whichcast is forecast_a. Abort!"
        logger.error(error_message)
        sys.exit(-1)


def get_node_ofs(prop, logger):
    """
    get_node_ofs
    """
    prop.model_source = model_source.model_source(prop.ofs)
    if logger is None:
        log_config_file = "conf/logging.conf"
        log_config_file = os.path.join(os.getcwd(), log_config_file)

        # Check if log file exists
        if not os.path.isfile(log_config_file):
            sys.exit(-1)

        # Creater logger
        logging.config.fileConfig(log_config_file)
        logger = logging.getLogger("root")
        logger.info("Using log config %s", log_config_file)

    logger.info("--- Starting OFS Model process ---")

    dir_params = utils.Utils().read_config_section("directories", logger)


    parameter_validation(prop, dir_params, logger)

    prop.model_path = os.path.join(
        dir_params["model_historical_dir"], prop.ofs, dir_params["netcdf_dir"]
    )
    prop.model_path = Path(prop.model_path).as_posix()

    prop.control_files_path = os.path.join(
        prop.path, dir_params["control_files_dir"]
    )

    os.makedirs(prop.control_files_path, exist_ok=True)

    prop.data_model_1d_node_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['model_dir'],
        dir_params['1d_node_dir'])
    prop.data_model_1d_node_path = Path(prop.data_model_1d_node_path).as_posix()
    os.makedirs(prop.data_model_1d_node_path, exist_ok = True)

    prop.start_date_full = prop.start_date_full.replace("-", "")
    prop.end_date_full = prop.end_date_full.replace("-", "")
    prop.start_date_full = prop.start_date_full.replace("Z", "")
    prop.end_date_full = prop.end_date_full.replace("Z", "")
    prop.start_date_full = prop.start_date_full.replace("T", "-")
    prop.end_date_full = prop.end_date_full.replace("T", "-")

#    dir_list = list_of_files.list_of_dir(prop, logger)
#    list_files = list_of_files.list_of_files(prop, dir_list)
#    logging.info("About to start intake_scisa from get_node ...")
#    model = intake_scisa.intake_model(list_files, prop, logger)
#    logging.info("Lazily loaded dataset complete in get_node_ofs!")

    for variable in ["water_level",
                     "water_temperature",
                     "salinity",
                     "currents"]:
        try:
            name_conventions = name_convent(variable)

            control_file = f"{prop.control_files_path}/{prop.ofs}_" \
                           f"{name_conventions[0]}_station.ctl"
            if os.path.isfile(control_file) is False:
                logger.info("%s is not found.", control_file)
                sys.exit(-1)
            prop.startdate = (datetime.strptime(
                prop.start_date_full.split("-")[0], "%Y%m%d")).strftime(
                "%Y%m%d") + "00"
            prop.enddate = (datetime.strptime(
                prop.end_date_full.split("-")[0], "%Y%m%d")).strftime(
                "%Y%m%d") + "23"

            ofs_ctlfile = ofs_ctlfile_extract(
                prop, name_conventions[0], logger) #this function calls write_ofs_ctl, which returns the model... if we return the model again through ofs_ctlfile_extract, we don't need to call intake again here.

            dir_list = list_of_files.list_of_dir(prop, logger)
            list_files = list_of_files.list_of_files(prop, dir_list)
            logging.info("About to start intake_scisa from get_node ...")
            model = intake_scisa.intake_model(list_files, prop, logger)
            logging.info("Lazily loaded dataset complete in get_node_ofs!")

            if prop.model_source=="fvcom":

                if prop.ofsfiletype == 'fields':
                    if prop.ofs == "ngofs2":
                        model["time"] = pd.date_range(
                            start=datetime.strptime(model['time'][0].
                                        values.astype(str).
                                        split('.')[0],"%Y-%m-%dT%H:%M:%S"),
                            periods=model.sizes["time"],
                            freq="3H",
                        )
                    else:
                        model["time"] = pd.date_range(
                            start=datetime.strptime(model['time'][0].
                                        values.astype(str).
                                        split('.')[0],"%Y-%m-%dT%H:%M:%S"),
                            periods=model.sizes["time"],
                            freq="H",
                        )
                elif prop.ofsfiletype == 'stations':
                    if prop.ofs == "ngofs2":
                        model["time"] = pd.date_range(
                            start=datetime.strptime(model['time'][0].
                                        values.astype(str).
                                        split('.')[0],"%Y-%m-%dT%H:%M:%S"),
                            periods=model.sizes["time"],
                            freq="6min",
                        )
                    else:
                        model["time"] = pd.date_range(
                            start=datetime.strptime(model['time'][0].
                                        values.astype(str).
                                        split('.')[0],"%Y-%m-%dT%H:%M:%S"),
                            periods=model.sizes["time"],
                            freq="6min",
                        )
            elif prop.model_source=="roms":

                if prop.ofsfiletype == 'fields':
                     if prop.ofs == "gomofs" or prop.ofs == "wcofs":
                          model["ocean_time"] = pd.date_range(
                              start=datetime.strptime(model['ocean_time'][0].
                                         values.astype(str).
                                         split('.')[0],"%Y-%m-%dT%H:%M:%S"),
                              periods=model.sizes["ocean_time"],
                              freq="3H",
                          )

                     else:
                          model["ocean_time"] = pd.date_range(
                              start=datetime.strptime(model['ocean_time'][0].
                                         values.astype(str).
                                         split('.')[0],"%Y-%m-%dT%H:%M:%S"),
                              periods=model.sizes["ocean_time"],
                              freq="H",
                          )
                elif prop.ofsfiletype == 'stations':
                     if prop.ofs == "gomofs" or prop.ofs == "wcofs":
                         model["ocean_time"] = pd.date_range(
                             start=datetime.strptime(model['ocean_time'][0].
                                         values.astype(str).
                                         split('.')[0],"%Y-%m-%dT%H:%M:%S"),
                             periods=model.sizes["ocean_time"],
                             freq="6min",
                         )
                     else:
                         model["ocean_time"] = pd.date_range(
                             start=datetime.strptime(model['ocean_time'][0].
                                         values.astype(str).
                                         split('.')[0],"%Y-%m-%dT%H:%M:%S"),
                             periods=model.sizes["ocean_time"],
                             freq="6min",
                         )

            for i in range(len(ofs_ctlfile[1])):
                if variable in ("salinity", "water_temperature"):
                    formatted_series = format_temp_salt(
                        prop,
                        model,
                        ofs_ctlfile,
                        name_conventions[-1],
                        i,
                    )

                elif variable == "currents":
                    formatted_series = format_currents(prop, model,
                                                       ofs_ctlfile,
                                                       i)

                else:
                    formatted_series = format_waterlevel(
                        prop,
                        model,
                        ofs_ctlfile,
                        name_conventions[-1],
                        i,
                    )

                if prop.whichcast == "forecast_a":

                    with open(
                        r""
                        + f"{prop.data_model_1d_node_path}"
                          f"/{ofs_ctlfile[4][i]}_"
                          f"{prop.ofs}_{name_conventions[0]}_"
                          f"{ofs_ctlfile[1][i]}_"
                          f"{prop.whichcast}_{prop.forecast_hr}_model.prd",
                        "w",
                        encoding="utf-8",
                    ) as output:
                        for line in formatted_series:
                            output.write(str(line) + "\n")
                        logger.info(
                            "%s/%s_%s_%s_%s_%s_%s_model.prd created "
                            "successfully",
                            prop.data_model_1d_node_path,
                            ofs_ctlfile[4][i],
                            prop.ofs,
                            name_conventions[0],
                            ofs_ctlfile[1][i],
                            prop.whichcast,
                            prop.forecast_hr,
                        )
                else:
                    with open(
                        r""
                        + f"{prop.data_model_1d_node_path}/{ofs_ctlfile[4][i]}_"
                          f"{prop.ofs}_"
                          f"{name_conventions[0]}_{ofs_ctlfile[1][i]}"
                          f"_{prop.whichcast}_model.prd",
                        "w",
                        encoding="utf-8",
                    ) as output:
                        for line in formatted_series:
                            output.write(str(line) + "\n")
                        logger.info(
                            "%s/%s_%s_%s_%s_%s_model.prd created successfully",
                            prop.data_model_1d_node_path,
                            ofs_ctlfile[4][i],
                            prop.ofs,
                            name_conventions[0],
                            ofs_ctlfile[1][i],
                            prop.whichcast,
                        )
        except Exception as ex:
            logger.error("Error happened when process %s - %s",
                         variable,
                         str(ex))


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        prog="python get_node_ofs.py",
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
        "--Whichcast",
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
    parser.add_argument(
        "-t", "--FileType", required=False,
        help="OFS output file type to use: 'fields' or 'stations'", )

    args = parser.parse_args()
    prop1 = model_properties.ModelProperties()
    prop1.ofs = args.OFS
    prop1.path = args.Path
    prop1.start_date_full = args.StartDate
    prop1.end_date_full = args.EndDate
    prop1.whichcast = args.Whichcast

    prop1.model_source = model_source.model_source(args.OFS)

    ''' Make stations files default, unless user specifies fields '''
    if args.FileType is None:
        prop1.ofsfiletype = 'stations'
    elif args.FileType is not None:
        prop1.ofsfiletype = args.FileType.lower()
    else:
        print('Check OFS file type argument! Abort')
        sys.exit(-1)

    ''' Do forecast_a to assess a single forecast cycle'''
    if 'forecast_a' in prop1.whichcasts:
        if args.Forecast_Hr is None:
            print('No forecast cycle input -- defaulting to 00Z')
            prop1.forecast_hr = '00hr'
        elif args.FileType is not None:
            prop1.forecast_hr = args.Forecast_Hr

    get_node_ofs(prop1, None)
