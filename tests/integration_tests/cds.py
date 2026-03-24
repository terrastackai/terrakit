# © Copyright IBM Corporation 2025-2026
# SPDX-License-Identifier: Apache-2.0


# TerraKit - easy Geospatial data search and query
# Requires tokens for data connectors in .env
import os
import xarray as xr

from glob import glob
from shutil import rmtree

from rasterio.crs import CRS

from terrakit import DataConnector


############## CLEAN UP FUNCTIONS #################
def get_data_clean_up(save_file_dir):
    files = glob(f"{save_file_dir}/*.nc")
    print(f"Test clean up. Deleting {files}")
    for f in files:
        os.remove(f)


SAVE_FILE_DIR = "./tests/resources/intergration_test_data"
rmtree(SAVE_FILE_DIR, ignore_errors=True)
os.makedirs(SAVE_FILE_DIR, exist_ok=True)
###################################################

################ SET UP ###########################
bbox = [-10, 40, 5, 50]
date_start = "2024-01-01"
date_end = "2024-01-03"
###################################################


# Example 1
data_connector = "climate_data_store"
dc = DataConnector(connector_type=data_connector)
dc.connector.list_collections()

collection_name = "derived-era5-single-levels-daily-statistics"

bands = [
    # Instantaneous
    "2m_temperature",
    "10m_u_component_of_wind",
    "mean_sea_level_pressure",
    # Accumulated
    "total_precipitation",
    "surface_net_solar_radiation",
    "evaporation",
    # Mean rate
    "mean_total_precipitation_rate",
    # Min/Max
    "maximum_2m_temperature_since_previous_post_processing",
    "minimum_2m_temperature_since_previous_post_processing",
]

# Additional query parameters
query_params = {
    "daily_statistic": "daily_mean",
    "time_zone": "utc+00:00",
    "frequency": "1_hourly",
}

unique_dates, results = dc.connector.find_data(
    data_collection_name=collection_name,
    date_start=date_start,
    date_end=date_end,
    bbox=bbox,
    bands=bands,
)

# print(unique_dates)

save_file = f"{SAVE_FILE_DIR}/{data_connector}_{collection_name}.nc"

# Single day
ds = dc.connector.get_data(
    data_collection_name=collection_name,
    date_start=unique_dates[0],
    date_end=unique_dates[0],
    bbox=bbox,
    bands=bands,
    save_file=save_file,
)

assert isinstance(ds, xr.Dataset)
assert len(ds.data_vars) == len(bands)
assert ds.rio.crs == CRS.from_epsg(4326)
# assert len(ds.coords["band"]) == len(bands)
assert len(ds.time) == 1
assert os.path.exists(save_file.replace(".nc", f"_{unique_dates[0]}.nc")) is True
assert len(glob(f"{SAVE_FILE_DIR}/*.nc")) == 1
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

assert isinstance(ds, xr.Dataset)
assert len(ds.data_vars) == len(bands)
assert ds.rio.crs == CRS.from_epsg(4326)
# assert len(ds.coords["band"]) == len(bands)
assert len(ds.time) == len(unique_dates)
assert os.path.exists(save_file.replace(".nc", f"_{unique_dates[0]}.nc")) is True
assert len(glob(f"{SAVE_FILE_DIR}/*.nc")) == len(unique_dates)
get_data_clean_up(SAVE_FILE_DIR)
