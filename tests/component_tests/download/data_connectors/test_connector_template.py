# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


import pytest
import xarray as xr

from rasterio.crs import CRS
from unittest.mock import patch

from terrakit import DataConnector


@pytest.mark.skip("Template tests for new connector")
class Test_NewDataConnector_Template:
    ######### UPDATE HERE WITH NEW CONNECTOR DETAILS #########
    connector_type = "<new_connector>"
    bands: list[str] = []
    collections: list[str] = []
    ##########################################################

    def test_list_collections__new_connector(
        self,
    ):
        expected_collections = self.collections
        dc = DataConnector(connector_type=self.connector_type)
        collections = dc.connector.list_collections()
        assert collections == expected_collections

    def test_list_collection_with_invalid_credentials__new_connector(
        self,
    ):
        """
        Test that list_collection returns as expected, even when invalid credentials are provided.
        """
        pass

    @pytest.mark.parametrize("collection", [])
    def test_find_available_data__new_connector(
        self,
        collection,
        start_date,
        end_date,
        bbox,
    ):
        dc = DataConnector(connector_type=self.data_source)
        unique_dates, results = dc.connector.find_data(
            data_collection_name=collection,
            date_start=start_date,
            date_end=end_date,
            bbox=bbox,
            bands=self.bands,
        )
        assert unique_dates == []

    def find_data__new_connector(self):
        """
        Helper function to mock find_data for new_connector
        """
        pass

    @pytest.mark.parametrize(
        "collection",
        [],
    )
    def test_get_data__new_connector__new_connector(
        self,
        collection,
        start_date,
        end_date,
        bbox,
        save_file_dir,
        get_data_clean_up,
        **kwargs,
    ):
        """
        Template test to test get_data method for a new connector.

        Parameters:
            self: The instance of the test class.
            start_date (str): The start date for the data query in 'YYYY-MM-DD' format.
            end_date : The end date for the data query in 'YYYY-MM-DD' format.
            bbox (tuple): The bounding box coordinates (left, bottom, right, top).
            get_data_clean_up: Clean up test data
            **kwargs: Additional keyword arguments.
        """
        save_file = f"{save_file_dir}/{self.data_source}_{collection}.tif"

        # Initialize DataConnector
        dc = DataConnector(connector_type=self.data_source)

        patch.object(
            dc,
            "find_data",
            return_value=find_data__new_connector(),  # noqa
        )  # Mock find_data
        # Call the get_data method
        data_array = dc.connector.get_data(
            data_collection_name=collection,
            date_start=start_date,
            date_end=end_date,
            bands=self.bands,
            bbox=bbox,
            save_file=save_file,
        )

        # Assertions
        assert isinstance(data_array, xr.DataArray)
        assert data_array.rio.crs == CRS.from_epsg(4326)
        assert len(data_array.coords["band"]) == len(self.bands)
        assert len(data_array.time) >= 1
