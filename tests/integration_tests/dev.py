# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


# TerraKit - easy Geospatial data search and query
# Requires tokens for data connectors in .env
import numpy as np
import os
import xarray as xr

from glob import glob
from shutil import rmtree

from rasterio.crs import CRS

from terrakit import DataConnector
from terrakit.download.transformations.impute_nans_xarray import impute_nans_xarray
from terrakit.download.transformations.scale_data_xarray import scale_data_xarray
from terrakit.download.geodata_utils import save_data_array_to_file


############## CLEAN UP FUNCTIONS #################
def get_data_clean_up(save_file_dir):
    files = glob(f"{save_file_dir}/*.tif")
    print(f"Test clean up. Deleting {files}")
    for f in files:
        os.remove(f)


SAVE_FILE_DIR = "./tests/resources/intergration_test_data"
rmtree(SAVE_FILE_DIR, ignore_errors=True)
os.makedirs(SAVE_FILE_DIR, exist_ok=True)
###################################################

################ SET UP ###########################
bbox = [34.611440, -0.190887, 34.616448, -0.157678]
date_start = "2024-01-01"
date_end = "2024-01-31"
###################################################


# Example 1
data_connector = "sentinelhub"
dc = DataConnector(connector_type="sentinelhub")
dc.connector.list_collections()

collection_name = "s2_l2a"

bands = ["B04", "B03", "B02"]

unique_dates, results = dc.connector.find_data(
    data_collection_name=collection_name,
    date_start=date_start,
    date_end=date_end,
    bbox=bbox,
    bands=bands,
)

print(unique_dates)

save_file = f"{SAVE_FILE_DIR}/{data_connector}_{collection_name}.tif"

# Single day
da = dc.connector.get_data(
    data_collection_name=collection_name,
    date_start=unique_dates[0],
    date_end=unique_dates[0],
    bbox=bbox,
    bands=bands,
    save_file=save_file,
)

assert isinstance(da, xr.DataArray)
assert da.rio.crs == CRS.from_epsg(4326)
assert len(da.coords["band"]) == len(bands)
assert len(da.time) >= 1
assert os.path.exists(save_file.replace(".tif", f"_{unique_dates[0]}.tif")) is True
assert len(glob(f"{SAVE_FILE_DIR}/*.tif")) == 1
get_data_clean_up(SAVE_FILE_DIR)

dai = scale_data_xarray(da, list(np.ones(len(bands))))
dai = impute_nans_xarray(dai)
save_data_array_to_file(dai, save_file=save_file, imputed=True)

assert (
    os.path.exists(save_file.replace(".tif", f"_{unique_dates[0]}_imputed.tif")) is True
)
get_data_clean_up(SAVE_FILE_DIR)

# Multi day
da = dc.connector.get_data(
    data_collection_name=collection_name,
    date_start=unique_dates[0],
    date_end=unique_dates[-1],
    bbox=bbox,
    bands=bands,
    save_file=save_file,
)

assert isinstance(da, xr.DataArray)
assert da.rio.crs == CRS.from_epsg(4326)
assert len(da.coords["band"]) == len(bands)
assert len(da.time) >= 1
assert os.path.exists(save_file.replace(".tif", f"_{unique_dates[0]}.tif")) is True
assert len(glob(f"{SAVE_FILE_DIR}/*.tif")) == len(unique_dates)

dai = scale_data_xarray(da, list(np.ones(len(bands))))
dai = impute_nans_xarray(dai)
save_data_array_to_file(dai, save_file=save_file, imputed=True)

assert (
    os.path.exists(save_file.replace(".tif", f"_{unique_dates[0]}_imputed.tif")) is True
)
get_data_clean_up(SAVE_FILE_DIR)


#############################################################################
# Example 2                                                                 #
#############################################################################
collection_name = "s1_grd"
bands = ["VV", "VH"]

unique_dates, results = dc.connector.find_data(
    data_collection_name=collection_name,
    date_start=date_start,
    date_end=date_end,
    bbox=bbox,
    bands=bands,
)

print(unique_dates)

date = unique_dates[0]
save_file = f"{SAVE_FILE_DIR}/{data_connector}_{collection_name}.tif"

da = dc.connector.get_data(
    data_collection_name=collection_name,
    date_start=unique_dates[0],
    date_end=unique_dates[0],
    bbox=bbox,
    bands=bands,
    save_file=save_file,
)

assert isinstance(da, xr.DataArray)
assert da.rio.crs == CRS.from_epsg(4326)
assert len(da.coords["band"]) == len(bands)
assert len(da.time) >= 1
assert os.path.exists(save_file.replace(".tif", f"_{unique_dates[0]}.tif")) is True
assert len(glob(f"{SAVE_FILE_DIR}/*.tif")) == 1

dai = scale_data_xarray(da, list(np.ones(len(bands))))
dai = impute_nans_xarray(dai)
save_data_array_to_file(dai, save_file=save_file, imputed=True)

assert (
    os.path.exists(save_file.replace(".tif", f"_{unique_dates[0]}_imputed.tif")) is True
)
get_data_clean_up(SAVE_FILE_DIR)

# Multi day
da = dc.connector.get_data(
    data_collection_name=collection_name,
    date_start=unique_dates[0],
    date_end=unique_dates[-1],
    bbox=bbox,
    bands=bands,
    save_file=save_file,
)

assert isinstance(da, xr.DataArray)
assert da.rio.crs == CRS.from_epsg(4326)
assert len(da.coords["band"]) == len(bands)
assert len(da.time) >= 1
assert os.path.exists(save_file.replace(".tif", f"_{unique_dates[0]}.tif")) is True
assert len(glob(f"{SAVE_FILE_DIR}/*.tif")) == len(unique_dates)
get_data_clean_up(SAVE_FILE_DIR)


#############################################################################
# Example 3                                                                 #
#############################################################################
data_connector = "sentinel_aws"
dc = DataConnector(connector_type=data_connector)
dc.connector.list_collections()

collection_name = "sentinel-2-l2a"
bands = ["blue", "green", "red"]

unique_dates, results = dc.connector.find_data(
    data_collection_name=collection_name,
    date_start=date_start,
    date_end=date_end,
    bbox=bbox,
    bands=bands,
)

print(unique_dates)

save_file = f"{SAVE_FILE_DIR}/{data_connector}_{collection_name}.tif"

# Single Day
da = dc.connector.get_data(
    data_collection_name=collection_name,
    date_start=unique_dates[0],
    date_end=unique_dates[0],
    bbox=bbox,
    bands=bands,
    save_file=f"{save_file}",
)

assert isinstance(da, xr.DataArray)
assert da.rio.crs == CRS.from_epsg(4326)
assert len(da.coords["band"]) == len(bands)
assert len(da.time) >= 1
assert (
    os.path.exists(save_file.replace(".tif", f"_{unique_dates[0]}.tif")) is True
)  ### TODO: FAILING TO SAVE SINGLE FILE
assert len(glob(f"{SAVE_FILE_DIR}/*.tif")) == 1

dai = scale_data_xarray(da, list(np.ones(len(bands))))
dai = impute_nans_xarray(dai)
save_data_array_to_file(dai, save_file=save_file, imputed=True)

assert (
    os.path.exists(save_file.replace(".tif", f"_{unique_dates[0]}_imputed.tif")) is True
)
get_data_clean_up(SAVE_FILE_DIR)

# Multi Day
da = dc.connector.get_data(
    data_collection_name=collection_name,
    date_start=unique_dates[0],
    date_end=unique_dates[-1],
    bbox=bbox,
    bands=bands,
    save_file=f"{save_file}",
)

assert isinstance(da, xr.DataArray)
assert da.rio.crs == CRS.from_epsg(4326)
assert len(da.coords["band"]) == len(bands)
assert len(da.time) >= 1
assert os.path.exists(save_file.replace(".tif", f"_{unique_dates[0]}.tif")) is True
assert len(glob(f"{SAVE_FILE_DIR}/*.tif")) == len(unique_dates)
get_data_clean_up(SAVE_FILE_DIR)


#############################################################################
# Example 4                                                                 #
#############################################################################
data_connector = "nasa_earthdata"
dc = DataConnector(connector_type=data_connector)
dc.connector.list_collections()

collection_name = "HLSL30_2.0"
bands = ["B04", "B03", "B02"]

unique_dates, results = dc.connector.find_data(
    data_collection_name=collection_name,
    date_start=date_start,
    date_end=date_end,
    bbox=bbox,
    bands=["B04", "B03", "B02"],
)

print(unique_dates)

save_file = f"{SAVE_FILE_DIR}/{data_connector}_{collection_name}.tif"

da = dc.connector.get_data(
    data_collection_name=collection_name,
    date_start=unique_dates[0],
    date_end=unique_dates[0],
    bbox=bbox,
    bands=bands,
    save_file=save_file,
)

assert isinstance(da, xr.DataArray)
assert da.rio.crs == CRS.from_epsg(4326)
assert len(da.coords["band"]) == len(bands)
assert len(da.time) >= 1
assert os.path.exists(save_file.replace(".tif", f"_{unique_dates[0]}.tif")) is True
assert len(glob(f"{SAVE_FILE_DIR}/*{unique_dates[0]}.tif")) == 1

dai = scale_data_xarray(da, list(np.ones(len(bands))))
dai = impute_nans_xarray(dai)
save_data_array_to_file(dai, save_file=save_file, imputed=True)

assert (
    os.path.exists(save_file.replace(".tif", f"_{unique_dates[0]}_imputed.tif")) is True
)
get_data_clean_up(SAVE_FILE_DIR)


# Multi Day
da = dc.connector.get_data(
    data_collection_name=collection_name,
    date_start=unique_dates[0],
    date_end=unique_dates[-1],
    bbox=bbox,
    bands=bands,
    save_file=save_file,
)

assert isinstance(da, xr.DataArray)
assert da.rio.crs == CRS.from_epsg(4326)
assert len(da.coords["band"]) == len(bands)
assert len(da.time) >= 1
for date in unique_dates:
    assert os.path.exists(save_file.replace(".tif", f"_{date}.tif")) is True
assert len(glob(f"{SAVE_FILE_DIR}/*.tif")) == len(unique_dates)

dai = scale_data_xarray(da, list(np.ones(len(bands))))
dai = impute_nans_xarray(dai)
save_data_array_to_file(dai, save_file=save_file, imputed=True)

assert (
    os.path.exists(save_file.replace(".tif", f"_{unique_dates[0]}_imputed.tif")) is True  # type: ignore[index]
)
get_data_clean_up(SAVE_FILE_DIR)
