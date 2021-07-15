# Pangeo-UKCP-Transfer
Scripts and notes on transferring the Met Office's UKCP18 dataset from netcdf files on Jasmin, to zarr datasets on the cloud (specifically Azure).

The following notes are mainly for my benefit, but could potentially be useful to others doing similar tasks.

## UKCP18 dataset

UKCP18 is a set of climate predictions produced by the Met Office, covering the period 1980-2080.
Details on the dataset can be found at 
https://catalogue.ceda.ac.uk/uuid/c700e47ca45d4c43b213fe879863d589

## Accessing the dataset on CEDA/JASMIN



## pangeo-forge-recipes

The core bit of code that converts netcdf to zarr (via xarray) comes from the package [pangeo-forge-recipes](https://github.com/pangeo-forge/pangeo-forge-recipes).
The objective is to get the specific use of it in this repo as an official Pangeo recipe via https://github.com/pangeo-forge/staged-recipes/

## Running the scripts

### Create and activate the conda environment
```
conda env create -f env.yaml
conda activate ukcp-transfer
```
### Fill in Azure credentials
Copy the file `azure_config_template.py` to `azure_config.py` and fill in the Azure storage account name, and SAS token.

### Get SSL client certificate for CEDA/OpenDAP
```
export CEDA_USERNAME=<your CEDA username>
export CEDA_PASSWORD=<your CEDA password>
python
>>> from ukcp_ceda_utils import get_ceda_security_cert
>>> get_ceda_security_cert()
```
This will write a file `/tmp/certs/creds.pem` which the transfer script will use.

### Run the transfer script
```
python ukcp_transfer.py --grid_size <GRID_SIZE> --freq <FREQ> --variable <VAR> --ensemble <ENSEMBLE> --container <CONTAINER>
```
where the choices for all the above options can be seen with
```
python ukcp_transfer.py --help
```
Note that "variable" and "ensemble" can accept the argument "all", in which case the script will iterate through all 16 variables, and/or all 12 ensembles.

The "container" argument is the name of the blob storage container on the storage account specified in `azure_config.py`.   If it doesn't already exist, it will be created.

### Test the transfer
```
python ukcp_test.py --grid_size <GRID_SIZE> --freq <FREQ> --variable <VAR> --ensemble <ENSEMBLE> --container <CONTAINER>
```
Again, "variable" and "ensemble" can accept the value "all", in which case the script will iterate through all allowed values.  
The script is attempting to open a zarr dataset from the specified blob storage container.   At the end, it will give a count of successful and unsuccessful attempts.
## copying between Azure accounts/containers.
