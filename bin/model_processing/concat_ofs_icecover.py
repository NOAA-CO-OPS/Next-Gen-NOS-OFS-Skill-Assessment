"""
This script takes the list of files from
list_of_files.py, opens the files using
open_netcdf.py and concatenates them.
"""
import sys
import xarray as xr
import numpy as np
from pathlib import Path
# Add parent directory to sys.path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from model_processing import list_of_files
from model_processing import open_netcdf


def concat_ofs_icecover(prop, logger):
    """
    OFS Model Concatenation
    """

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

        if prop.model_source == "fvcom":
            try:
                nc_list = []
                newarr = np.array_split(list_files, 3)
                arrlength = len(newarr)
                for i_index in range(0, arrlength):
                    if newarr[i_index] is not None and \
                            newarr[i_index].size > 0:
                        nc_item = xr.concat(
                            [open_netcdf.fvcom_netcdf(prop,i) for i in newarr[i_index]],
                            dim="time",
                            data_vars="minimal",
                        )
                        nc_list.append(nc_item)
                        filelist = ""
                        for j_index in range(0, len(newarr[i_index])):
                            filelist = filelist + str(newarr[i_index][j_index]) + "\n"
                        logger.info(filelist)
                nc_file = xr.concat(
                        nc_list,
                        dim = "time",
                        data_vars = "minimal"
                        )
                logger.info("Concatenation complete!")
            except Exception as ex:
                logger.error(f"Error happened at Concatenation: {str(ex)}")
                sys.exit(-1)

        elif prop.model_source == "roms":
            try:
                nc_list = []
                newarr = np.array_split(list_files, 3)
                arrlength = len(newarr)
                for i_index in range(0, arrlength):
                    if newarr[i_index] is not None and \
                            newarr[i_index].size > 0:
                        nc_item = xr.concat(
                            [open_netcdf.roms_netcdf(prop,i) for i in newarr[i_index]],
                            dim="ocean_time",
                            data_vars="all",
                        )
                        nc_list.append(nc_item)
                        filelist = ""
                        for j_index in range(0, len(newarr[i_index])):
                            filelist = filelist + str(newarr[i_index][j_index]) + "\n"
                        logger.info(filelist)
                nc_file = xr.concat(
                        nc_list,
                        dim="ocean_time",
                        data_vars = "all"
                        )
                logger.info("Concatenation complete!")
            except Exception as ex:
                logger.error(f"Error happened at Concatenation: {str(ex)}")
                sys.exit(-1)

        filename = \
            f"{prop.ofs}_{prop.whichcast}_{prop.startdate}_{prop.enddate}.nc"

        nc_file.to_netcdf(r"" + f"{prop.data_model_ice_path}/{filename}")

        return nc_file



    logger.error("No model files found for ice skill. Abort!")
    sys.exit(-1)


    filename = "fields.f"
    if prop.whichcast == "nowcast":
        filename = "fields.n"
