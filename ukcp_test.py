import os
import xarray as xr
import zarr
import ssl
import argparse

import tempfile
import fsspec
from fsspec.implementations.local import LocalFileSystem
from pangeo_forge_recipes.storage import FSSpecTarget, CacheFSSpecTarget
from adlfs import AzureBlobFileSystem

import matplotlib.pyplot as plt

from azure_config import config

expected_shapes = {"5km_ann": (1, 60, 244, 180),
                   "2.2km_day": (1,21600, 606, 484)
                   }


def get_dataset(container_name, grid_size, variable, freq, ensemble):
    account_name = config["ACCOUNT_NAME"]
    sas_token = config["SAS_TOKEN"]

    tag = "v20190725" if grid_size == "5km" else "v20190731"
    path = "{}/land-cpm/uk/{}/rcp85/{}/{}/{}/{}"\
        .format(container_name,
                grid_size,
                variable,
                freq,
                tag,
                ensemble)
    print("looking at path: {}".format(path))
    store=fsspec.get_mapper("abfs://{}".format(path),
                            account_name="ukcpstagingtest",
                            sas_token = sas_token)

    ds = xr.open_zarr(store, consolidated=True)
    return ds


def test_dataset(container_name, grid_size, variable, freq, ensemble):

    try:
        ds = get_dataset(container_name, grid_size, variable, freq, ensemble)
        return ds[variable].shape[:2] == expected_shapes["{}_{}".format(
            grid_size, freq)][:2]
    except:
        return False


if __name__ == "__main__":

    frequencies = ["1hr","3hr","ann","ann-20y","day","mon","mon-20y","seas","seas-20y"]
    grid_sizes = ["2.2km","5km"]
    ensembles = ["01","04","05","06","07","08","09","10","11","12","13","15","all"]
    variable_names = ["clt","hurs","huss","pr","prsn","psl","rls","rss","sfcWind","snw","tas","tasmax","tasmin","uas","vas","wsgmax10m"]

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
    args = parser.parse_args()
    print("Will test dataset {} {} {} {}".format(args.grid_size,
                                                     args.freq,
                                                     args.variable,
                                                     args.ensemble
                                                     )
    )
    if args.ensemble == "all":
        ensembles = ensembles[:-1]
    else:
        ensembles = [args.ensemble]
    for ensemble in ensembles:
        ## call the function
        dataset_ok = test_dataset(
            args.container,
            args.grid_size,
            args.variable,
            args.freq,
            ensemble
        )
        print("Dataset {}/land-cpm/uk/{}/rcp85/{}/{}/{} OK? {}"\
              .format(args.container,
                      args.grid_size,
                      args.variable,
                      args.freq,
                      ensemble,
                      dataset_ok)
        )
