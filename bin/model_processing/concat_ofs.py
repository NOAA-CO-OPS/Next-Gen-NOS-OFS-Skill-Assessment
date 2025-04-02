"""
This script takes the list of files from
list_of_files.py, opens the files using
open_netcdf.py and concatenates them.
"""
import sys
from pathlib import Path
import errno
import numpy as np
import xarray as xr
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from model_processing import list_of_files
from model_processing import open_netcdf


def concat_ofs(prop, logger):
    """
    OFS Model Concatenation
    """
    dir_list = list_of_files.list_of_dir(prop, logger)
    list_files = list_of_files.list_of_files(prop, dir_list)
    if len(list_files) > 0:
        logger.info(
            "Model %s netcdf files for the OFS %s found for the period "
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
                            coords="minimal",
                            compat="override"
                        )
                        nc_list.append(nc_item)
                        filelist = ""
                        for j_index in range(0, len(newarr[i_index])):
                            filelist = filelist + str(newarr[i_index][j_index]) + "\n"
                        logger.info(filelist)
                nc_file = xr.concat(
                        nc_list,
                        dim = "time",
                        data_vars = "minimal",
                        coords="minimal",
                        compat="override"
                        )
                ###################### Fix for bug ################
                # Sometimes FVCOM stations forecast model output has times
                # that are not every 6 mins, they are every 5 mins & 59 secs
                # instead. This causes duplicate retention. So
                # the fix below drops all times that are not 'even', i.e.
                # 5 minutes and 59 secs.
                #
                # Check if number of data points with date problems and
                # to be dropped is >10%.
                # Continue even if >10%, but log an error.
                # This fix works fine for a small number of dropped data points.
                # If we find that the number of dropped data points is much bigger
                # for either a different OFS or date range, then we need to
                # correct or round the incorrect dates rather than drop them.
                if prop.ofsfiletype == 'stations':
                    if (len(nc_file['time'])>0 and
                        (len(np.where(nc_file['time'].time.dt.second!=0)[0])/
                         len(nc_file['time'])) <= 0.1):
                        logger.info("Proportion of incorrect times to be dropped from concat: %s",
                                    np.round(
                                        len(np.where(nc_file['time'].time.dt.second!=0)[0])/len(nc_file['time']),
                                    decimals = 3)
                                    )
                    else:
                        logger.error("Incorrect times in concat_ofs are >10% of total!")
                    # Fix the bug -- drop the incorrect times & data points
                    nc_file = nc_file.assign_coords(lat=nc_file['lat'])
                    nc_file = nc_file.assign_coords(lon=nc_file['lon'])
                    nc_file = nc_file.assign_coords(h=nc_file['h'])
                    nc_file = nc_file.assign_coords(name_station=nc_file['name_station'])
                    nc_file = nc_file.where(nc_file['time'].time.dt.second == 0, drop=True)
                    ###############################################################
                if prop.ofsfiletype == 'stations' and prop.whichcast != 'forecast_a':
                    nc_file = nc_file.drop_duplicates(dim="time",
                                                      keep="last")
                elif prop.ofsfiletype == 'stations' and prop.whichcast == 'forecast_a':
                    #Do complete forecast cycle if forecast_a
                    nc_file = nc_file.drop_duplicates(dim="time",
                                                      keep="first")

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
                if prop.ofsfiletype == 'stations' and prop.whichcast != 'forecast_a':
                    nc_file = nc_file.drop_duplicates(dim="ocean_time",
                                                      keep="last")
                elif prop.ofsfiletype == 'stations' and prop.whichcast == 'forecast_a':
                    #Do complete forecast cycle if forecast_a
                    nc_file = nc_file.drop_duplicates(dim="ocean_time",
                                                      keep="first")
                logger.info("Concatenation complete!")
            except Exception as ex:
                logger.error(f"Error happened at Concatenation: {str(ex)}")
                sys.exit(-1)

        filename = \
            f"{prop.ofs}_{prop.whichcast}_{prop.startdate}_{prop.enddate}.nc"
        logger.info(f"Writing concatenated file to {prop.data_model_1d_node_path}/{filename}")
        try:
            nc_file.to_netcdf(r"" + f"{prop.data_model_1d_node_path}/{filename}")
        except OSError as e:
            if e.errno == errno.ENOSPC:
                logger.error("Error: No space left on device. Unable to write NetCDF file.")
            else:
                logger.error(f"Unexpected OSError: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occured: {e}")

        return nc_file

    filename = "fields.f"
    if prop.whichcast == "nowcast":
        filename = "fields.n"
    logger.error(
        f"Failed to find nos.{prop.ofs}.{filename}***.nc in "
        f"{prop.model_path} "
        f"for the specific period from {prop.startdate} to {prop.enddate}."
    )
    sys.exit(-1)
