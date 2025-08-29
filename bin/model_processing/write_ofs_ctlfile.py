"""
This script is called by get_node_ofs.py in case the ofs control file is
not found
if model ctl file is not found, all the predefined function for finding
the nearest node and depth are applied to create the ofs control file
"""

import os
from pathlib import Path
import sys
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from obs_retrieval import station_ctl_file_extract
from obs_retrieval import utils
from model_processing import index_nearest_node
from model_processing import index_nearest_depth
from model_processing import index_nearest_station

import numpy as np


def write_ofs_ctlfile(prop, model, logger):
    """
    write_ofs_ctlfile
    """

    dir_params = utils.Utils().read_config_section("directories", logger)

    prop.model_path = os.path.join(
        dir_params["model_historical_dir"], prop.ofs, dir_params["netcdf_dir"]
    )
    prop.model_path = Path(prop.model_path).as_posix()

    name_var = []

    for variable in ["water_level",
                     "water_temperature",
                     "salinity",
                     "currents"]:

        if variable == "water_level":
            name_var = "wl"

        elif variable == "water_temperature":
            name_var = "temp"

        elif variable == "salinity":
            name_var = "salt"

        elif variable == "currents":
            name_var = "cu"

        if (
            (os.path.isfile(
                f"{prop.control_files_path}/{prop.ofs}_"
                f"{name_var}_model.ctl") is False
                and prop.ofsfiletype == 'fields') or (os.path.isfile(
                    f"{prop.control_files_path}/{prop.ofs}_"
                    f"{name_var}_model_station.ctl") is False
                    and prop.ofsfiletype == 'stations')
                    ):

            logger.info(
                f"Model Control File ({prop.ofs}_{name_var}_model.ctl) not "
                f"found")

            logger.info(
                "Creating Model Control File for %s. This might take a couple "
                "of minutes", variable, )

            logger.info(
                "Searching for the nearest nodes and their respective "
                "depths in relation to the stations found "
                "in the station_ctl_file.ctl"
            )

            control_file = f"{prop.control_files_path}/{prop.ofs}_" \
                               f"{name_var}_station.ctl"

            extract = station_ctl_file_extract.station_ctl_file_extract(
                control_file)
            if extract is not None:
                if prop.ofsfiletype == 'fields':
                    list_of_nearest_node =\
                        index_nearest_node.index_nearest_node(
                        extract[-1],
                        model,
                        prop.model_source,
                        name_var,
                        logger,
                    )
                    list_of_nearest_layer, list_of_depths =\
                        index_nearest_depth.index_nearest_depth(
                        prop,
                        list_of_nearest_node,
                        model,
                        extract[-1],
                        prop.model_source,
                        name_var,
                        logger,
                    )
                elif prop.ofsfiletype == 'stations':
                    list_of_nearest_node =\
                        index_nearest_station.index_nearest_station(
                        extract[-1],
                        model,
                        prop.model_source,
                        name_var,
                        logger,
                    )
                    list_of_nearest_layer, list_of_depths =\
                        index_nearest_depth.index_nearest_depth(
                        prop,
                        list_of_nearest_node,
                        model,
                        extract[-1],
                        prop.model_source,
                        name_var,
                        logger,
                    )

                logger.info("Extracting data found in the Model Control File")

                # This loop is used to write every line of every model ctl file
                model_ctl_file = []

                length = len(list_of_nearest_layer)
                if prop.model_source=="fvcom":
                    for i in range(0, length):
                        if np.isnan(list_of_nearest_node[i]) == False:
                            if prop.ofsfiletype == 'fields':
                                if name_var == 'cu':
                                    model_ctl_file.append(
                                        f"{list_of_nearest_node[i]} "
                                        f"{list_of_nearest_layer[i]} "
                                        f"{model['latc'][list_of_nearest_node[i]].data.compute():.3f}  "
                                        f"{model['lonc'][list_of_nearest_node[i]].data.compute() - 360:.3f}  "
                                        f"{extract[0][i][0]}  {list_of_depths[i]:.1f}\n"
                                        )
                                else:
                                    model_ctl_file.append(
                                        f"{list_of_nearest_node[i]} "
                                        f"{list_of_nearest_layer[i]} "
                                        f"{model['lat'][list_of_nearest_node[i]].data.compute():.3f}  "
                                        f"{model['lon'][list_of_nearest_node[i]].data.compute() - 360:.3f}  "
                                        f"{extract[0][i][0]}  {list_of_depths[i]:.1f}\n"
                                        )
                            else:
                                model_ctl_file.append(
                                    f"{list_of_nearest_node[i]} "
                                    f"{list_of_nearest_layer[i]} "
                                    f"{model['lat'][0,list_of_nearest_node[i]].data.compute():.3f}  "
                                    f"{model['lon'][0,list_of_nearest_node[i]].data.compute() - 360:.3f}  "
                                    f"{extract[0][i][0]}  {list_of_depths[i]:.1f}\n"
                                )
                        else:
                            logger.info("No matching model station found for "
                                        "obs station %s.", extract[0][i][0])

                elif prop.model_source=="roms":
                    for i in range(length):
                        if np.isnan(list_of_nearest_node[i]) == False:
                            model_ctl_file.append(
                            f"{list_of_nearest_node[i]} "
                            f"{list_of_nearest_layer[i]} "
                            f"{float(model['lat_rho'][np.unravel_index(list_of_nearest_node[i],np.shape(model['lon_rho']))]):.3f}  "
                            f"{float(model['lon_rho'][np.unravel_index(list_of_nearest_node[i],np.shape(model['lon_rho']))]):.3f}  "
                            f"{extract[0][i][0]}  {list_of_depths[i]:.1f}\n"
                            )
                        else:
                            logger.info("No matching model station found for "
                                        "obs station %s.", extract[0][i][0])

                if prop.ofsfiletype == 'fields':
                    with open(
                        f"{prop.control_files_path}/"
                        f"{prop.ofs}_{name_var}_model.ctl",
                        "w",
                        encoding="utf-8",
                    ) as output:
                        for i in model_ctl_file:
                            output.write(str(i))
                elif prop.ofsfiletype == 'stations':
                    with open(
                        f"{prop.control_files_path}/"
                        f"{prop.ofs}_{name_var}_model_station.ctl",
                        "w",
                        encoding="utf-8",
                    ) as output:
                        for i in model_ctl_file:
                            output.write(str(i))

                logger.info(
                    "Model Control File created successfully - Model time "
                    "series for the variable %s were created successfully\n",
                    variable,
                )
            else:
                logger.info("Observation ctl file is blank for %s. "
                            "Model ctl file will also be blank", name_var)
                if prop.ofsfiletype == 'fields':
                    with open(
                        f"{prop.control_files_path}/"
                        f"{prop.ofs}_{name_var}_model.ctl",
                        "w",
                        encoding="utf-8",
                    ) as output:
                        pass
                elif prop.ofsfiletype == 'stations':
                    with open(
                        f"{prop.control_files_path}/"
                        f"{prop.ofs}_{name_var}_model_station.ctl",
                        "w",
                        encoding="utf-8",
                    ) as output:
                        pass
        else:
            logger.info(
                "Model Control File (%s_%s_model.ctl) found in %s.If you "
                "instead want to create a new Model Control File, "
                "please change the name/delete the current "
                "%s_%s_model.ctl", prop.ofs, name_var,
                prop.control_files_path, prop.ofs, name_var)

    return model
