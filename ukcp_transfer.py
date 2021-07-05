import os
from pangeo_forge_recipes.recipes import XarrayZarrRecipe
from pangeo_forge_recipes.patterns import FilePattern, ConcatDim, MergeDim
import xarray as xr
import zarr
import ssl
import argparse
from datetime import timedelta
import time

import tempfile
from fsspec.implementations.local import LocalFileSystem
from pangeo_forge_recipes.storage import FSSpecTarget, CacheFSSpecTarget
from pangeo_forge_recipes.executors import PrefectPipelineExecutor
import fsspec
from adlfs import AzureBlobFileSystem

import matplotlib.pyplot as plt
import logging

from ukcp_ceda_utils import download_file
from azure_config import config

logging.basicConfig(filename="transfer_{}".format(time.strftime("%Y-%m-%d_%H-%M-%S")),
                            level=logging.WARNING,
                            format='')


def fix_attrs(ds, fname=None):
    orig_coords = list(ds.coords)
    if ds.resolution=="5km":
        ds = ds.set_coords(orig_coords+["time_bnds", "projection_y_coordinate_bnds","projection_x_coordinate_bnds"])
    elif ds.resolution == "2.2km":
        ds = ds.set_coords(orig_coords+["time_bnds", "grid_latitude_bnds","grid_longitude_bnds", "rotated_latitude_longitude"])
    else:
        raise RuntimeError("Unknown grid size {}".format(grid_size))
    return ds


def calc_date_range(overall_start_date,
                   overall_end_date,
                   increment,
                   include_day=True):
    end_date = "0"
    start_date = overall_start_date
    date_range = []
    while int(end_date) < int(overall_end_date):
        start_year = start_date[:4]
        start_month = start_date[4:6]
        start_day = "01"
        end_day = "30"
        if increment.endswith("y"):
            end_year = str(int(start_year)+int(increment[:-1]))
            end_month = str(int(start_month)-1)
            end_date = "{}{}{}".format(end_year, end_month, end_day)
            if include_day:
                date_range.append("{}-{}".format(
                    start_date, end_date
                ))
            else:
                date_range.append("{}-{}".format(
                    start_date[:6], end_date[:6]
                ))
            start_year = end_year
            start_date = start_year+start_month+start_day
        elif increment == "1m":
            end_year = start_year if start_month != "12" \
                else str(int(start_year)+1)
            end_month = str((int(start_month)-1 + int(increment[:-1]) % 12)+1)
        else:
            raise RuntimeError("Unknown increment {}".format(increment))
    return date_range


def get_date_range(grid_size, freq):
    if freq.endswith("hr"):
        increment = "1m"
        include_day = True
    elif freq == "day":
        include_day = True
        if grid_size == "2.2km":
            increment = "1y"
        else:
            increment = "10y"
    else:
        include_day = False
        increment = "20y"


    start_dates = ["19801201","20201201","20601201"]
    end_dates = ["20001130","20401130","20801130"]

    date_range = []
    for i in range(len(start_dates)):
        date_range += calc_date_range(start_dates[i],
                                      end_dates[i],
                                      increment,
                                      include_day)
    return date_range


def nc_to_zarr_direct(url_pattern, date_range, output_path, account_name, sas_token):
    """
    use this function to load xarray dataset directly and write to zarr,
    not using the xarray_zarr pangeo-forge recipe.
    Used when we don't need to concatenate along any dimension, e.g.
    for the ann-20y, where we don't have a "time" dimension.
    """
    for date in date_range:
        url = url_pattern.format(time=date)
        with tempfile.TemporaryDirectory() as cache_dir:
            tries = 0
            downloaded_ok = False
            while (not downloaded_ok) and tries < 4:
                downloaded_ok = download_file(url, output_location=cache_dir)
                tries += 1
            if downloaded_ok:
                logging.warning("NC_TO_ZARR_DIRECT - DOWNLOADED {}".format(url))
            else:
                logging.warning("NC_TO_ZARR_DIRECT - FAILED TO DOWNLOAD {}".format(url))
                continue
            filename = url.split("/")[-1]
            ds = xr.open_dataset(os.path.join(cache_dir, filename))
            ds = fix_attrs(ds)
            # add the date to the output path
            full_path = os.path.join(output_path, date)
            store = fsspec.get_mapper("abfs://{}".format(full_path),
                                      account_name=account_name,
                                      sas_token=sas_token)
            ds.to_zarr(store, mode="w", consolidated=True)
            logging.info("NC_TO_ZARR_DIRECT - UPLOADED {}".format(full_path))

    return True

def transfer_dataset(grid_size,
                     freq,
                     variable,
                     ensemble,
                     container_name,
                     target="abfs",
                     test=False):

    fs_local = LocalFileSystem()


    account_name = config["ACCOUNT_NAME"]
    sas_token= config["SAS_TOKEN"]

    abfs = AzureBlobFileSystem(account_name=account_name,
                               sas_token=sas_token)

    tag = "v20190725" if grid_size == "5km" else "v20190731"

    url_pattern = "http://dap.ceda.ac.uk/badc/ukcp18/data/land-cpm/uk/GRID_SIZE/rcp85/ENSEMBLE/VARIABLE/FREQ/TAG/VARIABLE_rcp85_land-cpm_uk_GRID_SIZE_ENSEMBLE_FREQ_{time}.nc"
    url_pattern = url_pattern.replace("GRID_SIZE", grid_size)
    url_pattern = url_pattern.replace("FREQ", freq)
    url_pattern = url_pattern.replace("TAG", tag)
    url_pattern = url_pattern.replace("VARIABLE", variable)
    url_pattern = url_pattern.replace("ENSEMBLE", ensemble)

    date_range = get_date_range(grid_size, freq)
    if test:
        date_range = date_range[:2]
    print("Date range is {}".format(date_range))

    output_path = "{}/land-cpm/uk/{}/rcp85/{}/{}/{}/{}"\
        .format(container_name,
                grid_size,
                variable,
                freq,
                tag,
                ensemble)

    if freq == "ann-20y":
        # don't use recipe - transfer files directly
        return nc_to_zarr_direct(url_pattern,
                                 date_range,
                                 output_path,
                                 account_name,
                                 sas_token)

    # otherwise, we do have a "time" dimension - use XarrayZarrRecipe

    def make_filename(time):
        return url_pattern.format(time=time)

    time_steps_per_input = {"2.2km_day": 360,
                            "5km_day": 3600,
                            "5km_ann": 20}

    pattern = FilePattern(
        make_filename,
        ConcatDim(name="time",
                  keys=date_range,
                  nitems_per_file=time_steps_per_input["{}_{}".format(grid_size,freq)]),
    )


    ### Define recipe

    print("creating recipe...")
    if grid_size == "5km":
        target_chunks = {'ensemble_member': 1,
                         'time': time_steps_per_input["{}_{}".format(grid_size,freq)],
                         'projection_y_coordinate': 244, 'projection_x_coordinate': 180, 'bnds': 2}
    elif grid_size == "2.2km":
        target_chunks = {'ensemble_member': 1,
                         'time': time_steps_per_input["{}_{}".format(grid_size,freq)],
                         'grid_longitude': 484, 'grid_latitude': 606}

    ssl_ctx = ssl.create_default_context()
    ssl_ctx.load_cert_chain("/tmp/certs/creds.pem")
    recipe = XarrayZarrRecipe(
        file_pattern=pattern,
        process_input=fix_attrs,
        target_chunks=target_chunks,
        fsspec_open_kwargs={"ssl_context": ssl_ctx}
    )

    print("creating target...")



    # ABFS target
    if target == "abfs":
        target = FSSpecTarget(abfs, output_path)
    else:
        target_dir = tempfile.TemporaryDirectory()
        target = FSSpecTarget(fs_local, target_dir.name)
    cache_dir = tempfile.TemporaryDirectory().name
    cache_target = CacheFSSpecTarget(fs_local, cache_dir)

    recipe.target = target
    recipe.input_cache = cache_target

    pipelines = recipe.to_pipelines()
    executor = PrefectPipelineExecutor()
    plan = executor.pipelines_to_plan(pipelines)
    # set retries and retry_delay
    for task in plan.tasks:
        task.max_retries=7
        task.retry_delay = timedelta(seconds=15)
    executor.execute_plan(plan)





if __name__ == "__main__":

    frequencies = ["1hr","3hr","ann","ann-20y","day","mon","mon-20y","seas","seas-20y"]
    grid_sizes = ["2.2km","5km","country","region","river"]
    ensembles = ["01","04","05","06","07","08","09","10","11","12","13","15","all"]
    variable_names = ["clt","hurs","huss","pr","prsn","psl","rls","rss","sfcWind","snw","tas","tasmax","tasmin","uas","vas","wsgmax10m","all"]

    parser = argparse.ArgumentParser("description=convert netcdf to zarr")
    parser.add_argument("--grid_size", type=str, help="grid size",
                        required=True, choices=grid_sizes)
    parser.add_argument("--freq", type=str, help="sampling frequency",
                        required=True, choices=frequencies)
    parser.add_argument("--variable", type=str, help="variable name",
                        required=True, choices=variable_names)
    parser.add_argument("--ensemble", type=str, help="ensemble id",
                        required=True, choices=ensembles)
    parser.add_argument("--container", type=str, help="output container",
                        default="testzarr")
    parser.add_argument("--test", help="only run over first two input files",
                        action="store_true")
    args = parser.parse_args()

    test = args.test if args.test else False

    if args.ensemble == "all":
        ensembles = ensembles[:-1]
    else:
        ensembles = [args.ensemble]
    if args.variable == "all":
        variable_names = variable_names[:-1]
    else:
        variable_names = [args.variable]
    for ensemble in ensembles:
        for variable in variable_names:
            print("Will transfer dataset {} {} {} {}".format(args.grid_size,
                                                             args.freq,
                                                             variable,
                                                             ensemble)
            )
            ## call the function
            transfer_dataset(
                args.grid_size,
                args.freq,
                variable,
                ensemble,
                args.container,
                test=test
            )
