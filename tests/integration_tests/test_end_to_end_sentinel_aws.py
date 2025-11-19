# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


# TerraKit - easy Geospatial data search and query

import numpy as np
import pytest
import rioxarray
import xarray as xr

from unittest import mock
from rasterio.crs import CRS

from terrakit import DataConnector
from terrakit.download.transformations.impute_nans_xarray import impute_nans_xarray
from terrakit.download.transformations.scale_data_xarray import scale_data_xarray
from terrakit.download.geodata_utils import save_data_array_to_file


###### NOTE: This script is only testing the list_collection, find_data and get_data API, not the functionality of each methods.
# Example 3
def load_test_data():
    dummy_data_path = (
        "tests/resources/sentinel_aws/sentinel_aws_sentinel-2-l2a_2024-01-01.tif"
    )
    return rioxarray.open_rasterio(dummy_data_path)


# Docstrings assisted by watsonx Code Assistant)
@pytest.mark.skip(
    "Test needs updating to handle changes to get_data which now returns an DataArray with dimensions (time, bands, x, y)"
)
def test_find_data_sentinel_aws():
    """
    Test the functionality of finding and downloading Sentinel-2 L2A data from AWS using the sentinel_aws data connector.

    This test uses mocking to simulate the responses from the data connector methods.
    It checks if the returned data is an xarray DataArray, has the correct CRS, and the right number of bands.
    It also verifies the imputation of NaN values in the data.
    """
    # Initialize the DataConnector with sentinel_aws connector type
    data_connector = "sentinel_aws"
    dc = DataConnector(connector_type=data_connector)

    # List collections to ensure the connector is correctly initialized
    with mock.patch.object(
        dc.connector,
        "list_collections",
        autospec=True,
        return_value="sentinel-2-l2a",
    ):
        dc.connector.list_collections()

    # Define the collection name, bounding box, and bands for testing
    collection_name = "sentinel-2-l2a"
    bbox = [34.601440, -0.190887, 34.796448, -0.057678]
    bands = ["blue", "green", "red"]
    start_date = "2024-01-01"
    end_date = "2024-01-31"

    # Mock the find_data method to return predefined data
    with mock.patch.object(
        dc.connector,
        "find_data",
        autospec=True,
        return_value=[["2024-01-01"], 1],
    ):
        # Call find_data method and store the results
        unique_dates, results = dc.connector.find_data(
            data_collection_name=collection_name,
            date_start=start_date,
            date_end=end_date,
            bbox=bbox,
            bands=bands,
        )

    # Print the results and unique dates for debugging
    print(results)
    print(unique_dates)

    # Select the first date for further operations
    date = unique_dates[0]
    save_filestem = f"{data_connector}_{collection_name}_{date}"

    # Mock the get_data method to return predefined data
    with mock.patch.object(
        dc.connector,
        "get_data",
        autospec=True,
        return_value=load_test_data(),
    ):
        # Call get_data method to download data
        da = dc.connector.get_data(
            data_collection_name=collection_name,
            date_start=date,
            date_end=date,
            bbox=bbox,
            bands=bands,
            save_file=f"{save_filestem}.tif",
        )

    # Assertions to check the returned data
    assert isinstance(da, xr.DataArray)
    assert da.rio.crs == CRS.from_epsg(4326)
    assert len(da.coords["band"]) == 3  # for 3 bands

    # Scale and impute NaN values in the xarray DataArray
    dai = scale_data_xarray(da, list(np.ones(len(bands))))
    dai = impute_nans_xarray(dai, save_file=f"{save_filestem}_imputed.tif")
    save_data_array_to_file(dai, save_file=f"{save_filestem}.tif", imputed=True)
