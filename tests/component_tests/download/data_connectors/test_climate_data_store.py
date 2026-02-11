# © Copyright IBM Corporation 2026
# SPDX-License-Identifier: Apache-2.0


import os
import pandas as pd
import pytest
import xarray as xr
from pathlib import Path
from rasterio.crs import CRS

from terrakit import DataConnector
from terrakit.general_utils.exceptions import (
    TerrakitValidationError,
    TerrakitValueError,
)


@pytest.fixture
def mock_cds_client(monkeypatch):
    """
    Mock CDS API client to copy test zip file instead of downloading from CDS.

    This fixture patches cdsapi.Client to return a mock that copies
    ./7fbb992ea3687a2ec12f2ba1de4cc73a.zip to the requested output path.

    # The following request was used to generate the test zip file:
    # request = {
    #     "variable": [
    #         "10m_u_component_of_wind",
    #         "10m_v_component_of_wind",
    #         "2m_temperature",
    #         "total_precipitation",
    #         "10m_wind_gust_since_previous_post_processing",
    #     ],
    #     "product_type": "reanalysis",
    #     "year": "2025",
    #     "month": ["01"],
    #     "day": ["01", "02"],
    #     "time_zone": "utc+00:00",
    #     "area": [90, -180, -90, 180],
    #     "daily_statistic": "daily_mean",
    #     "frequency": "6_hourly",
    #     "format": "netcdf",
    # }

    # #
    # cds = cdsapi.Client()
    # downloaded_filename = cds.retrieve(data_collection_name, request).download()

    Usage:
        def test_cds_download(mock_cds_client):
            # CDS API calls will use the mock
            dc = DataConnector(connector_type="climate_data_store")
            data = dc.connector.get_data(...)
    """
    import shutil
    from unittest.mock import MagicMock

    # Path to test zip file
    TEST_ZIP = Path("./7fbb992ea3687a2ec12f2ba1de4cc73a.zip")

    # Create mock client
    mock_client = MagicMock()

    def mock_retrieve(collection_name, request_params, output_path):
        """Copy test zip to output_path instead of downloading."""
        if not TEST_ZIP.exists():
            raise FileNotFoundError(
                f"Test data not found: {TEST_ZIP}\n"
                "Please ensure 7fbb992ea3687a2ec12f2ba1de4cc73a.zip exists in project root"
            )

        # Ensure output directory exists
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        # Copy test zip to requested location
        shutil.copy(TEST_ZIP, output_file)

        return str(output_file)

    # Assign mock retrieve method
    mock_client.retrieve = mock_retrieve

    # Mock the cdsapi.Client class to return our mock
    def mock_cdsapi_client(*args, **kwargs):
        return mock_client

    # Patch cdsapi.Client
    monkeypatch.setattr("cdsapi.Client", mock_cdsapi_client)

    return mock_client


class TestClimateDataStore:
    connector_type = "climate_data_store"
    # Mock data contains these 5 bands from the test zip file
    bands = ["fg10", "t2m", "tp", "u10", "v10"]

    def test_valid_data_connector(self):
        dc = DataConnector(connector_type=self.connector_type)
        assert dc.connector is not None

    def test_list_collections_climate_data_store(
        self,
        **kwargs,
    ):
        expected_collections = [
            "projections-cordex-domains-single-levels",
            "derived-era5-single-levels-daily-statistics",
        ]
        dc = DataConnector(connector_type=self.connector_type)
        collections = dc.connector.list_collections()
        assert collections == expected_collections

    def test__missing_credentials_cds(
        self,
        unset_evn_vars,
        start_date,
        bbox,
        reset_dot_env,
    ):
        """
        Test that find_data only runs if credentials are provided.
        """
        collection = "derived-era5-single-levels-daily-statistics"
        with pytest.raises(TerrakitValidationError, match="Error: Missing credentials"):
            dc = DataConnector(connector_type=self.connector_type)
            dc.connector.find_data(collection, start_date, start_date, bbox=bbox)

    def test_invalid_collection(self, start_date, bbox):
        """
        Test that an invalid collection raises a TerrakitValidationError.
        """
        collection = "invalid-collection"
        dc = DataConnector(connector_type=self.connector_type)
        with pytest.raises(TerrakitValueError, match="Invalid collection"):
            dc.connector.find_data(collection, start_date, start_date, bbox=bbox)

    @pytest.fixture
    def expected_dates_cds(self):
        dates = pd.date_range("2024-01-01", "2024-01-31").strftime("%Y-%m-%d").tolist()
        return dates

    @pytest.mark.parametrize(
        "collection",
        [
            ("derived-era5-single-levels-daily-statistics"),
            ("projections-cordex-domains-single-levels"),
        ],
    )
    def test_find_available_data_cds(
        self,
        collection,
        expected_dates_cds,
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
        assert unique_dates == expected_dates_cds

    @pytest.mark.parametrize(
        ("collection", "start_date", "end_date", "expected_dates_cds"),
        [
            (
                "derived-era5-single-levels-daily-statistics",
                "1949-01-01",
                "1949-01-02",
                ["1949-01-01, 1949-01-02"],
            ),
            (
                "projections-cordex-domains-single-levels",
                "2100-01-01",
                "2100-01-02",
                ["2100-01-01, 2100-01-02"],
            ),
        ],
    )
    def test_find_available_data_cds_start_data_given_constraints(
        self,
        collection,
        start_date,
        end_date,
        expected_dates_cds,
        bbox,
    ):
        """
        Test the find_data method with a given start date within the collection constraints.
        """
        dc = DataConnector(connector_type=self.connector_type)
        unique_dates, results = dc.connector.find_data(
            data_collection_name=collection,
            date_start=start_date,
            date_end=end_date,
            bbox=bbox,
            bands=self.bands,
        )
        assert unique_dates == [start_date, end_date]

    @pytest.mark.parametrize(
        "collection",
        [
            ("derived-era5-single-levels-daily-statistics"),
            # ("projections-cordex-domains-single-levels"),
        ],
    )
    def test_get_data_cds(
        self, mock_cds_client, collection, bbox, save_file_dir, get_data_clean_up
    ):
        """
        Test the get_data method.

        Note: The mock returns a zip file with 5 NetCDF files (one per variable),
        each containing 2 time steps (2025-01-01 and 2025-01-02).
        """
        date_start = "2025-01-01"
        date_end = "2025-01-02"
        save_file = f"{save_file_dir}/{self.connector_type}_{collection}.tif"
        dc = DataConnector(connector_type=self.connector_type)
        data = dc.connector.get_data(
            data_collection_name=collection,
            date_start=date_start,
            date_end=date_end,
            bbox=bbox,
            bands=self.bands,
            save_file=save_file,
        )
        assert data is not None
        assert len(data) > 0  # Check we got data

        assert isinstance(data, xr.DataArray)
        assert data.rio.crs == CRS.from_epsg(4326)

        # Mock data contains 5 bands (fg10, t2m, tp, u10, v10) - one per NetCDF file
        assert len(data.coords["band"]) == 5

        # Mock data contains 2 time steps (2025-01-01 and 2025-01-02)
        assert len(data.time) == 2

        # Check that files were created for the dates in the mock data
        assert os.path.exists(save_file.replace(".tif", "_2025-01-01.tif")) is True
        assert os.path.exists(save_file.replace(".tif", "_2025-01-02.tif")) is True
