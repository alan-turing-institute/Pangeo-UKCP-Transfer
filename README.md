# Pangeo-UKCP-Transfer
Scripts and notes on transferring the Met Office's UKCP18 dataset from netcdf files on Jasmin, to zarr datasets on the cloud (specifically Azure).

The following notes are mainly for my benefit, but could potentially be useful to others doing similar tasks.

## UKCP18 dataset

UKCP18 is a set of climate predictions produced by the Met Office, covering the period 1980-2080.
Details on the dataset can be found at 
https://catalogue.ceda.ac.uk/uuid/c700e47ca45d4c43b213fe879863d589

## Accessing the original dataset on CEDA

The ukcp18 dataset is currently stored on CEDA (Centre for Environmental Data Analysis).  It is free to use for researchers, but you need to sign up to get a CEDA account - this can be done following the instructions here: https://help.ceda.ac.uk/article/39-ceda-account

### JASMIN

To access the dataset via the JASMIN data analysis facility, you also need to get an account for that: 
https://accounts.jasmin.ac.uk/

Once you have those credentials, you can ssh to a login server, using an ssh key pair where you have uploaded the public key to JASMIN:
```
ssh -A -i .ssh/id_rsa login2.jasmin.ac.uk
```
Then from there, you can ssh to a transfer server
```
ssh xfer1
```
The ukcp18 dataset is located at
```
/badc/ukcp18/data/
```

### OpenDAP

As long as you have a CEDA username and password, you can use OpenDAP to access the ukcp18 netcdf files.  There are instructions and example scripts available [here](https://help.ceda.ac.uk/article/4712-reading-netcdf-with-python-opendap) but also, the script `ukcp_ceda_utils` contains a function to download a netcdf file from CEDA in this way.
```
export CEDA_USERNAME=<CEDA username>
export CEDA_PASSWORD=<CEDA password>
python
>>> from ukcp_ceda_utils import *
>>> get_ceda_security_cert()
>>> download_file(<URL>)
```
where an example URL is http://dap.ceda.ac.uk/badc/ukcp18/data/land-cpm/uk/5km/rcp85/01/tas/ann-20y/v20190725/tas_rcp85_land-cpm_uk_5km_01_ann-20y_202012-204011.nc
(This is accessing the "tas" (temperature at surface) variable for ensemble_id 01, with a 5km grid, and averaged over the 20-year period 2020-2040.)

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

The *azcopy* executable is a useful and fast way to copy data to an Azure storage account, or between accounts.  It can be installed by following the instructions [here](https://docs.microsoft.com/en-us/azure/storage/common/storage-use-azcopy-v10)

In my case, I wanted to end up with the dataset on a Microsoft-owned account, but in order to ensure that the dataset was complete and consistent, and with all paths correct, I originally wrote it onto a staging account that I created.   To then copy to the final location, I did e.g.:
```
for i in hurs huss pr tasmin tasmax clt rls rss tas wsgmax10m snw prsn psl uas vas sfcWind; do azcopy copy "https://<SRC_STORAGE_ACC>.blob.core.windows.net/<SRC_CONTAINER>/land-cpm/uk/river/rcp85/${i}/seas-20y<SRC_SAS_TOKEN>" "https://<DEST_STORAGE_ACC>.blob.core.windows.net/<DEST_CONTAINER>/zarr/land-cpm/uk/river/rcp85/${i}/<DEST_SAS_TOKEN" --recursive ; done;
```
filling in the details for source and destination storage account names, containers, and SAS tokens as required.
