# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


import pytest
import xarray as xr

from glob import glob
from rasterio.crs import CRS


from terrakit import DataConnector


class TestSentinelHub:
    connector_type = "sentinelhub"
    bands = ["B04", "B03", "B02"]

    def test_list_collections_sentinel_hub(
        self,
        **kwargs,
    ):
        expected_collections = [
            "s2_l1c",
            "dem",
            "s1_grd",
            "hls_l30",
            "s2_l2a",
            "hls_s30",
        ]
        dc = DataConnector(connector_type=self.connector_type)
        collections = dc.connector.list_collections()
        assert collections == expected_collections

    def test_list_collection_with_invalid_credentials(
        self,
        **kwargs,
    ):
        """
        Test that list_collection returns as expected, even when invalid credentials are provided.
        """
        pass

    @pytest.mark.parametrize(
        "collection", ["s2_l1c", "s1_grd", "hls_l30", "s2_l2a", "hls_s30"]
    )
    def test_find_available_data_sentinelHub(
        self,
        mock_setup_sentinelhub,
        collection,
        expected_dates_sentinelhub,
        start_date,
        end_date,
        bbox,
    ):
        dc = DataConnector(connector_type=self.connector_type)
        unique_dates, results = dc.connector.find_data(
            data_collection_name=collection,
            date_start=start_date,
            date_end=end_date,
            bbox=bbox,
            bands=self.bands,
        )
        assert unique_dates == expected_dates_sentinelhub

    # TODO: Fix test for collection = "dem"
    @pytest.mark.parametrize(
        "collection",
        [
            "s2_l1c",
            #'dem',
            "s1_grd",
            "hls_l30",
            "s2_l2a",
            "hls_s30",
        ],
    )
    def test_get_data_sentinelhub(
        self,
        mock_sentinelhub_save_data,
        collection,
        start_date,
        end_date,
        bbox,
        save_file_dir,
        get_data_clean_up,
    ):
        """
        Test the get_data method of DataConnector for SentinelHub data source.

        This test case verifies if the get_data method of DataConnector correctly
        fetches data from SentinelHub for the specified collection, date range,
        bounding box, and bands. It also checks if the returned data array has
        the expected CRS and band dimensions.

        Parameters:
            self: The instance of the test class.
            mock_sentinelhub_save_data (Mock): A mock object to verify if save_data method is called. This mock includes mocking api calls to sentinel hub.
            collection (str): The name of the data collection, e.g., 'Sentinel-2'.
            start_date (str): The start date for the data query in 'YYYY-MM-DD' format.
            end_date (str): The end date for the data query in 'YYYY-MM-DD' format.
            bbox (tuple): The bounding box coordinates (left, bottom, right, top).
            save_file_dir (str): The directory path to save the downloaded data.
            get_data_clean_up (function): A function to clean up the downloaded data.

        """
        save_file = f"{save_file_dir}/{self.connector_type}_{collection}.tif"
        dc = DataConnector(connector_type=self.connector_type)
        data = dc.connector.get_data(
            data_collection_name=collection,
            date_start=start_date,
            date_end=end_date,
            bands=self.bands,
            bbox=bbox,
            save_file=save_file,
        )
        assert isinstance(data, xr.DataArray)
        assert data.rio.crs == CRS.from_epsg(4326)
        assert len(data.coords["band"]) == len(self.bands)
        assert len(data.time) >= 1
        assert len(glob(f"{save_file_dir}/*.tif")) == 7
