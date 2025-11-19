# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


import pytest
import xarray as xr

from glob import glob
from rasterio.crs import CRS

from terrakit import DataConnector


class TestSentinelAWS:
    connector_type = "sentinel_aws"
    bands = ["blue", "green", "red"]

    def test_list_collections_sentinel_aws(
        self,
        **kwargs,
    ):
        expected_collections = ["sentinel-2-l2a"]
        dc = DataConnector(connector_type=self.connector_type)
        collections = dc.connector.list_collections()
        assert collections == expected_collections

    def test_list_collection_with_invalid_credentials_sentinel_aws(
        self,
        **kwargs,
    ):
        """
        Test that list_collection returns as expected, even when invalid credentials are provided.
        """
        pass

    @pytest.mark.parametrize("collection", ["sentinel-2-l2a"])
    @pytest.mark.parametrize("maxcc", [80, 30])
    def test_find_available_data_sentinel_aws(
        self, mock_aws_find_items, collection, start_date, end_date, bbox, maxcc
    ):
        dc = DataConnector(connector_type=self.connector_type)
        unique_dates, results = dc.connector.find_data(
            data_collection_name=collection,
            date_start=start_date,
            date_end=end_date,
            bbox=bbox,
            bands=self.bands,
            maxcc=maxcc,
        )
        mock_aws_find_items.assert_called_once()

        # assert maxcc filter is used
        if maxcc == 80:
            assert unique_dates == [
                "2024-01-01",
                "2024-01-06",
                "2024-01-11",
                "2024-01-16",
                "2024-01-26",
                "2024-01-31",
            ]
            assert len(results) == 10
        if maxcc == 30:
            assert unique_dates == ["2024-01-06", "2024-01-26", "2024-01-31"]
            assert len(results) == 6

        # assert return fields == only the ones we want
        assert isinstance(results, list)
        assert set(results[0].keys()) == {"id", "properties"}
        assert set(results[0]["properties"].keys()) == {"datetime", "eo:cloud_cover"}

    @pytest.mark.parametrize("collection", ["sentinel-2-l2a"])
    def test_get_data_sentinel_aws(
        self,
        mock_aws_get_data,
        collection,
        start_date,
        end_date,
        bbox,
        save_file_dir,
        get_data_clean_up,
    ):
        save_file = f"{save_file_dir}/{self.connector_type}_{collection}.tif"
        dc = DataConnector(connector_type=self.connector_type)
        data_array = dc.connector.get_data(
            data_collection_name=collection,
            date_start=start_date,
            date_end=end_date,
            bands=self.bands,
            bbox=bbox,
            save_file=save_file,
        )

        mock_aws_find_items, mock_get_sh_aws_data = mock_aws_get_data
        mock_aws_find_items.assert_called_once()
        assert mock_get_sh_aws_data.call_count == 7
        assert isinstance(data_array, xr.DataArray)
        assert data_array.rio.crs == CRS.from_epsg(4326)
        assert len(data_array.coords["band"]) == len(self.bands)
        assert len(data_array.time) >= 1
        assert len(glob(f"{save_file_dir}/*.tif")) == 7  # 7 unique dates found
