"""
This script is called by get_node_ofs.py in case the ofs control file is
not found
if model ctl file is not found, all the predefined function for finding
the nearest node and depth are applied to create the ofs control file
"""

import os
from pathlib import Path
import sys
import numpy as np

parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from obs_retrieval import station_ctl_file_extract
from obs_retrieval import utils
#from model_processing import concat_ofs
from model_processing import index_nearest_node
from model_processing import list_of_files
from model_processing import index_nearest_depth
from model_processing import index_nearest_station
from model_processing import intake_scisa

def write_ofs_ctlfile(prop, logger):
    """
    write_ofs_ctlfile
    """

    dir_params = utils.Utils().read_config_section("directories", logger)

    prop.model_path = os.path.join(
        dir_params["model_historical_dir"], prop.ofs, dir_params["netcdf_dir"]
    )
    prop.model_path = Path(prop.model_path).as_posix()

    dir_list = list_of_files.list_of_dir(prop, logger)
    list_files = list_of_files.list_of_files(prop, dir_list)
    logger.info('About to call intake_scisa from write_ofs_ctlfile.')
    model = intake_scisa.intake_model(list_files, prop, logger)
    logger.info('Returned from call to intake_scisa inside of write_ofs_ctlfile.')

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


        #dir_list = list_of_files.list_of_dir(prop, logger)
        #list_files = list_of_files.list_of_files(prop, dir_list)
        #logger.info('About to call intake_scisa from write_ofs_ctlfile.')
        #model = intake_scisa.intake_model(list_files, prop, logger)
        #logger.info('Returned from call to intake_scisa inside of write_ofs_ctlfile.')

        # if os.path.isfile(prop.data_model_1d_node_path +
        #                   f"/{prop.ofs}_{prop.whichcast}_"
        #                   f"{str(prop.startdate)}_"
        #                   f"{str(prop.enddate)}.nc") == True:
        #     model = xr.open_dataset(
        #         prop.data_model_1d_node_path + f"/{prop.ofs}_{prop.whichcast}_"
        #         f"{str(prop.startdate)}_"
        #         f"{str(prop.enddate)}.nc",
        #         decode_times=False,
        #     )

#    if prop.model_source=="fvcom":
#            if prop.ofsfiletype == 'fields':
#                if prop.ofs == "ngofs2":
#                model["time"] = pd.date_range(
#                    start=model.time.attrs["units"].split("since")[-1],
#                    periods=model.sizes["time"],
#                    freq="3H",
#                )
#            else:
#                model["time"] = pd.date_range(
#                    start=model.time.attrs["units"].split("since")[-1],
#                    periods=model.sizes["time"],
#                    freq="H",
#                )
#        elif prop.ofsfiletype == 'stations':
#            if prop.ofs == "ngofs2":
#                model["time"] = pd.date_range(
#                    start=model.time.attrs["units"].split("since")[-1],
#                    periods=model.sizes["time"],
#                    freq="6min",
#                )
#            else:
#                model["time"] = pd.date_range(
#                    start=model.time.attrs["units"].split("since")[-1],
#                    periods=model.sizes["time"],
#                    freq="6min",
#                )
        #     elif prop.model_source=="roms":

        #         if prop.ofsfiletype == 'fields':
        #             if prop.ofs == "gomofs" or prop.ofs == "wcofs":
        #                 model["ocean_time"] = pd.date_range(
        #                     start=model.ocean_time.attrs["units"].split("since")[-1],
        #                     periods=model.sizes["ocean_time"],
        #                     freq="3H",
        #                 )

        #             else:
        #                 model["ocean_time"] = pd.date_range(
        #                     start=model.ocean_time.attrs["units"].split("since")[-1],
        #                     periods=model.sizes["ocean_time"],
        #                     freq="H",
        #                 )
        #         elif prop.ofsfiletype == 'stations':
        #             if prop.ofs == "gomofs" or prop.ofs == "wcofs":
        #                 model["ocean_time"] = pd.date_range(
        #                     start=model.ocean_time.attrs["units"].split("since")[-1],
        #                     periods=model.sizes["ocean_time"],
        #                     freq="6min",
        #                 )
        #             else:
        #                 model["ocean_time"] = pd.date_range(
        #                     start=model.ocean_time.attrs["units"].split("since")[-1],
        #                     periods=model.sizes["ocean_time"],
        #                     freq="6min",
        #                 )

        #     logger.info(
        #         "Model Concatenated File (%s_%s_%s_%s.nc) "
        #         "found in %s",
        #         prop.ofs,
        #         prop.whichcast,
        #         str(prop.startdate),
        #         str(prop.enddate),
        #         prop.data_model_1d_node_path,
        #     )

        # else:
        #     logger.info(
        #         "Model Concatenated File (%s_%s_%s_%s.nc) not found "
        #         "in %s.Searching for hourly files",
        #         prop.ofs,
        #         prop.whichcast,
        #         prop.startdate,
        #         prop.enddate,
        #         prop.data_model_1d_node_path,
        #     )
        #if name_var == 'wl':
        #    dir_list = list_of_files.list_of_dir(prop, logger)
        #    list_files = list_of_files.list_of_files(prop, dir_list)
            ##print("dir_list: ", dir_list)
            ##print("list_files: ", list_files)
        #    logger.info('About to call intake_scisa from write_ofs_ctlfile.')
        #    print(variable)
        #    model = intake_scisa.intake_model(list_files, prop, logger)
        #model = concat_ofs.concat_ofs(prop, logger)
        # if prop.model_source == 'fvcom':
        #     model = xr.open_dataset(
        #         prop.data_model_1d_node_path + f"/{prop.ofs}_{prop.whichcast}_"
        #         f"{str(prop.startdate)}_"
        #         f"{str(prop.enddate)}.nc",
        #         decode_times=False,
        #     )
        #print('debug pause')
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

            if prop.ofsfiletype == 'fields':
                list_of_nearest_node = index_nearest_node.index_nearest_node(
                    extract[-1],
                    model,
                    prop.model_source,
                    name_var,
                    logger,
                )
                list_of_nearest_layer = index_nearest_depth.index_nearest_depth(
                    prop,
                    list_of_nearest_node,
                    model,
                    extract[-1],
                    prop.model_source,
                    name_var,
                    logger,
                )
            elif prop.ofsfiletype == 'stations':
                list_of_nearest_node, min_dist = index_nearest_station.index_nearest_station(
                    extract[-1],
                    model,
                    prop.model_source,
                    name_var,
                    logger,
                )
                list_of_nearest_layer = index_nearest_depth.index_nearest_depth(
                    prop,
                    list_of_nearest_node,
                    model,
                    extract[-1],
                    prop.model_source,
                    name_var,
                    logger,
                )
                # list_of_nearest_layer = np.zeros(len(list_of_nearest_node),
                #                                  dtype=int)


            logger.info("Extracting data found in the Model Control File")

            # This loop is used to write every line of every model ctl file
            model_ctl_file = []

            length = len(list_of_nearest_layer)
            if prop.model_source=="fvcom":
                for i in range(0, length):
                    if np.isnan(list_of_nearest_node[i]) == False:
                        if prop.ofsfiletype == 'fields':
                            model_ctl_file.append(
                                f"{list_of_nearest_node[i]} "
                                f"{list_of_nearest_layer[i]} "
                                f"{model['latc'][list_of_nearest_node[i]].data.compute():.3f}  "
                                f"{model['lonc'][list_of_nearest_node[i]].data.compute() - 360:.3f}  "
                                f"{extract[0][i][0]}  0.0\n"
                            )
                        else:
                            model_ctl_file.append(
                                f"{list_of_nearest_node[i]} "
                                f"{list_of_nearest_layer[i]} "
                                f"{model['lat'][0,list_of_nearest_node[i]].data.compute():.3f}  "
                                f"{model['lon'][0,list_of_nearest_node[i]].data.compute() - 360:.3f}  "
                                f"{extract[0][i][0]}  0.0\n"
                            )
                            #print('debug pause')
                    else:
                        model_ctl_file.append(
                        f"{-999} "
                        f"{-999} "
                        f"{-999}  "
                        f"{-999}  "
                        f"{extract[0][i][0]}  0.0\n"
                        )

            elif prop.model_source=="roms":
                for i in range(length):
                    if np.isnan(list_of_nearest_node[i]) == False:
                        model_ctl_file.append(
                        f"{list_of_nearest_node[i]} "
                        f"{list_of_nearest_layer[i]} "
                        f"{float(model['lat_rho'][np.unravel_index(list_of_nearest_node[i],np.shape(model['lon_rho']))]):.3f}  "
                        f"{float(model['lon_rho'][np.unravel_index(list_of_nearest_node[i],np.shape(model['lon_rho']))]):.3f}  "
                        f"{extract[0][i][0]}  0.0\n"
                        )
                    else:
                        model_ctl_file.append(
                        f"{-999} "
                        f"{-999} "
                        f"{-999}  "
                        f"{-999}  "
                        f"{extract[0][i][0]}  0.0\n"
                        )
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
            logger.info(
                "Model Control File (%s_%s_model.ctl) found in %s.If you "
                "instead want to create a new Model Control File, "
                "please change the name/delete the current "
                "%s_%s_model.ctl", prop.ofs, name_var,
                prop.control_files_path, prop.ofs, name_var)

    return model
