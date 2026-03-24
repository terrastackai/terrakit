# © Copyright IBM Corporation 2026
# SPDX-License-Identifier: Apache-2.0


import logging
import os
import pandas as pd
import pytest
import xarray as xr
from rasterio.crs import CRS

from terrakit import DataConnector
from terrakit.general_utils.exceptions import (
    TerrakitValidationError,
    TerrakitValueError,
)


class TestClimateDataStore:
    connector_type = "climate_data_store"
    # Mock data contains these 5 bands from the test zip file
    bands = ["fg10", "t2m", "tp", "u10", "v10"]

    @pytest.fixture
    def bbox(self):
        """Override default bbox with one large enough for ERA5 (0.25° resolution)."""
        # Kenya region: ~0.5° × 0.5° bbox (meets 0.25° minimum requirement)
        return [34.5, -0.5, 35.0, 0.0]

    @pytest.fixture
    def expected_dates_cds(self):
        dates = pd.date_range("2024-01-01", "2024-01-31").strftime("%Y-%m-%d").tolist()
        return dates

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

    def test_missing_credentials_cds(
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
    def test_find_available_data_cds__start_date_given_constraints(
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

    def test_find_available_data_cds__bbox_expansion_for_small_bbox(
        self, start_date, caplog
    ):
        """
        Test that find_data expands bbox smaller than ERA5 grid resolution (0.25°) and logs warning.
        """

        collection = "derived-era5-single-levels-daily-statistics"
        # This bbox is only 0.02° x 0.02°, which is smaller than ERA5's 0.25° grid
        tiny_bbox = [-1.32, 51.06, -1.30, 51.08]
        original_bbox = tiny_bbox.copy()

        dc = DataConnector(connector_type=self.connector_type)

        # Capture logs at WARNING level
        with caplog.at_level(logging.WARNING):
            unique_dates, results = dc.connector.find_data(
                collection, start_date, start_date, bbox=tiny_bbox, bands=self.bands
            )

        # Verify bbox was expanded
        assert tiny_bbox != original_bbox, "Bbox should have been modified"

        # Verify dimensions are at least 0.25°
        lon_span = tiny_bbox[2] - tiny_bbox[0]
        lat_span = tiny_bbox[3] - tiny_bbox[1]
        assert lon_span >= 0.25, f"Longitude span {lon_span} should be >= 0.25°"
        assert lat_span >= 0.25, f"Latitude span {lat_span} should be >= 0.25°"

        # Verify warning was logged
        assert any(
            "Bounding box expanded" in record.message for record in caplog.records
        ), "Warning about bbox expansion should be logged"
        assert any("Original size" in record.message for record in caplog.records), (
            "Warning should include original size"
        )

    def test_find_available_data_cds__bbox_expansion_preserves_center_point(
        self, start_date
    ):
        """
        Test that bbox expansion preserves the center point of the original bbox.
        """
        collection = "derived-era5-single-levels-daily-statistics"
        # Small bbox centered at (0.0, 0.0)
        tiny_bbox = [-0.01, -0.01, 0.01, 0.01]

        # Calculate original center
        orig_center_lon = (tiny_bbox[0] + tiny_bbox[2]) / 2
        orig_center_lat = (tiny_bbox[1] + tiny_bbox[3]) / 2

        dc = DataConnector(connector_type=self.connector_type)
        dc.connector.find_data(
            collection, start_date, start_date, bbox=tiny_bbox, bands=self.bands
        )

        # Calculate new center
        new_center_lon = (tiny_bbox[0] + tiny_bbox[2]) / 2
        new_center_lat = (tiny_bbox[1] + tiny_bbox[3]) / 2

        # Verify center is preserved (within floating point tolerance)
        assert abs(new_center_lon - orig_center_lon) < 1e-6, (
            f"Center longitude changed from {orig_center_lon} to {new_center_lon}"
        )
        assert abs(new_center_lat - orig_center_lat) < 1e-6, (
            f"Center latitude changed from {orig_center_lat} to {new_center_lat}"
        )

    def test_find_available_data_cds__bbox_expansion_only_in_deficient_dimension(
        self, start_date
    ):
        """
        Test that bbox expansion only expands dimensions that are too small.
        """
        collection = "derived-era5-single-levels-daily-statistics"
        # Bbox with sufficient longitude but insufficient latitude
        bbox_small_lat = [34.0, -0.01, 34.5, 0.01]  # 0.5° lon, 0.02° lat
        original_lon_span = bbox_small_lat[2] - bbox_small_lat[0]

        dc = DataConnector(connector_type=self.connector_type)
        dc.connector.find_data(
            collection, start_date, start_date, bbox=bbox_small_lat, bands=self.bands
        )

        # Verify longitude span unchanged (was already sufficient)
        new_lon_span = bbox_small_lat[2] - bbox_small_lat[0]
        assert abs(new_lon_span - original_lon_span) < 1e-6, (
            "Longitude span should not change when already sufficient"
        )

        # Verify latitude span expanded to minimum
        new_lat_span = bbox_small_lat[3] - bbox_small_lat[1]
        assert new_lat_span >= 0.25, f"Latitude span {new_lat_span} should be >= 0.25°"

    def test_find_available_data_cds__bbox_no_expansion_when_sufficient(
        self, start_date, caplog
    ):
        """
        Test that bbox is not expanded when it already meets minimum requirements.
        """

        collection = "derived-era5-single-levels-daily-statistics"
        # Bbox that already meets minimum (0.5° x 0.5°)
        sufficient_bbox = [34.0, -0.25, 34.5, 0.25]
        original_bbox = sufficient_bbox.copy()

        dc = DataConnector(connector_type=self.connector_type)

        with caplog.at_level(logging.WARNING):
            dc.connector.find_data(
                collection,
                start_date,
                start_date,
                bbox=sufficient_bbox,
                bands=self.bands,
            )

        # Verify bbox was NOT modified
        assert sufficient_bbox == original_bbox, (
            "Bbox should not be modified when already sufficient"
        )

        # Verify no warning was logged about expansion
        assert not any(
            "Bounding box expanded" in record.message for record in caplog.records
        ), "No warning should be logged when bbox is already sufficient"

    def test_find_available_data_cds__bbox_expansion_not_applied_to_cordex(
        self, start_date
    ):
        """
        Test that bbox expansion is NOT applied to CORDEX collections (they use domain mapping).
        """
        collection = "projections-cordex-domains-single-levels"
        # Small bbox that would trigger expansion for ERA5
        tiny_bbox = [10.0, 45.0, 10.02, 45.02]

        dc = DataConnector(connector_type=self.connector_type)

        # CORDEX should use domain mapping, not bbox expansion
        # This should work without expanding the bbox (it maps to a domain instead)
        unique_dates, results = dc.connector.find_data(
            collection, start_date, start_date, bbox=tiny_bbox, bands=self.bands
        )

        # For CORDEX, the bbox might be modified by domain mapping, but not by the
        # expansion logic. We just verify it doesn't raise an error about being too small.
        assert unique_dates is not None

    def test_get_data_cds__bbox_expansion(
        self, mock_cds_client, start_date, save_file_dir, get_data_clean_up, caplog
    ):
        """
        Test that get_data also expands small bboxes and logs warning.
        """

        collection = "derived-era5-single-levels-daily-statistics"
        # Small bbox
        tiny_bbox = [-1.32, 51.06, -1.30, 51.08]
        original_bbox = tiny_bbox.copy()
        save_file = f"{save_file_dir}/{self.connector_type}_{collection}_small_bbox.nc"

        dc = DataConnector(connector_type=self.connector_type)

        with caplog.at_level(logging.WARNING):
            data = dc.connector.get_data(
                data_collection_name=collection,
                date_start=start_date,
                date_end=start_date,
                bbox=tiny_bbox,
                bands=self.bands,
                save_file=save_file,
            )

        # Verify bbox was expanded
        assert tiny_bbox != original_bbox, "Bbox should have been modified in get_data"

        # Verify dimensions are at least 0.25°
        lon_span = tiny_bbox[2] - tiny_bbox[0]
        lat_span = tiny_bbox[3] - tiny_bbox[1]
        assert lon_span >= 0.25, f"Longitude span {lon_span} should be >= 0.25°"
        assert lat_span >= 0.25, f"Latitude span {lat_span} should be >= 0.25°"

        # Verify warning was logged
        assert any(
            "Bounding box expanded" in record.message for record in caplog.records
        ), "Warning about bbox expansion should be logged in get_data"

        # Verify data was retrieved successfully
        assert data is not None
        assert isinstance(data, xr.Dataset)

    def test_get_data__negative_longitude_conversion(
        self, mock_cds_client, start_date, bbox, save_file_dir, get_data_clean_up
    ):
        """
        Test that negative longitudes work correctly with ERA5 data.

        ERA5 uses -180 to 180° longitude convention (not 0-360°).
        This test verifies that negative longitudes are handled correctly.
        """
        collection = "derived-era5-single-levels-daily-statistics"
        # Use bbox with negative longitude (standard -180 to 180° convention)
        # Oxford, UK: -1.32° to -1.07° should be converted to 358.68° to 358.93°
        negative_lon_bbox = [-1.32, 51.70, -1.07, 51.95]
        save_file = f"{save_file_dir}/{self.connector_type}_{collection}.nc"

        dc = DataConnector(connector_type=self.connector_type)

        # This should work - the connector should convert negative longitudes
        data = dc.connector.get_data(
            data_collection_name=collection,
            date_start=start_date,
            date_end=start_date,
            bbox=negative_lon_bbox,
            bands=self.bands,
            save_file=save_file,
        )

        assert data is not None
        assert len(data) > 0

        # Verify the data was retrieved successfully
        assert isinstance(data, xr.Dataset)

        # Verify stepType attributes are preserved
        for var in data.data_vars:
            assert "stepType" in data[var].attrs, (
                f"Variable {var} missing stepType attribute"
            )

    def test_get_data__longitude_system_no_wraparound(
        self, mock_cds_client, start_date, save_file_dir, get_data_clean_up
    ):
        """
        Test that bbox spanning negative to positive longitudes doesn't cause wraparound.

        This is a regression test for a bug where bbox [-10, 40, 5, 50] was incorrectly
        converted to [50, 350, 40, 5] in the CDS API request, causing the API to interpret
        it as wrapping around the globe and returning only a single longitude point (177.5°).

        The fix ensures ERA5 data uses -180/180° system without conversion to 0-360°,
        and uses coords='minimal' in xr.concat to handle inconsistent coordinates.
        """
        collection = "derived-era5-single-levels-daily-statistics"
        # Bbox spanning from negative to positive longitude (Western Europe)
        # This should NOT wrap around the globe
        bbox_europe = [-10, 40, 5, 50]  # Portugal to Germany
        save_file = f"{save_file_dir}/{self.connector_type}_{collection}_europe.nc"

        dc = DataConnector(connector_type=self.connector_type)

        data = dc.connector.get_data(
            data_collection_name=collection,
            date_start=start_date,
            date_end=start_date,
            bbox=bbox_europe,
            bands=self.bands,
            save_file=save_file,
        )

        assert data is not None
        assert isinstance(data, xr.Dataset)

        # Verify we got a proper 2D grid, not a single longitude point
        # The bbox spans 15° longitude, so at 0.25° resolution we should have ~60 points
        for var in data.data_vars:
            if var != "spatial_ref":  # Skip the CRS variable
                lon_dim = "longitude" if "longitude" in data[var].dims else "lon"
                lat_dim = "latitude" if "latitude" in data[var].dims else "lat"

                # Check we have multiple longitude points (not just 1)
                assert len(data[var][lon_dim]) > 1, (
                    f"Expected multiple longitude points, got {len(data[var][lon_dim])}. "
                    "This suggests the bbox caused a wraparound issue."
                )

                # Check we have multiple latitude points
                assert len(data[var][lat_dim]) > 1, (
                    f"Expected multiple latitude points, got {len(data[var][lat_dim])}"
                )

                # Verify longitude range is correct (should be close to -10 to 5)
                lon_values = data[var][lon_dim].values
                assert lon_values.min() >= -11, (
                    f"Min longitude {lon_values.min()} outside expected range"
                )
                assert lon_values.max() <= 6, (
                    f"Max longitude {lon_values.max()} outside expected range"
                )

                # Verify latitude range is correct (should be close to 40 to 50)
                # Allow some tolerance for grid alignment (±2°)
                lat_values = data[var][lat_dim].values
                assert lat_values.min() >= 38, (
                    f"Min latitude {lat_values.min()} outside expected range"
                )
                assert lat_values.max() <= 52, (
                    f"Max latitude {lat_values.max()} outside expected range"
                )

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
        save_file = f"{save_file_dir}/{self.connector_type}_{collection}.nc"
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

        # Now returns Dataset instead of DataArray
        assert isinstance(data, xr.Dataset)
        assert data.rio.crs == CRS.from_epsg(4326)

        # Verify stepType attributes are preserved
        for var in data.data_vars:
            assert "stepType" in data[var].attrs, (
                f"Variable {var} missing stepType attribute"
            )

        # Mock data contains 5 variables (fg10, t2m, tp, u10, v10) - one per NetCDF file
        # Note: Dataset has variables as data_vars, not a 'band' coordinate
        assert len(data.data_vars) == 5

        # Mock data contains 2 time steps (2025-01-01 and 2025-01-02)
        assert len(data.time) == 2

        # Check that NetCDF files were created for the dates in the mock data
        assert os.path.exists(save_file.replace(".nc", "_2025-01-01.nc")) is True
        assert os.path.exists(save_file.replace(".nc", "_2025-01-02.nc")) is True

    def test_get_data_bbox_too_small_for_era5_resolution(
        self, mock_cds_client_bbox_error, start_date, save_file_dir
    ):
        """
        Test that get_data handles MARS error for bbox smaller than ERA5 grid resolution (0.25°).

        This test uses a mock that simulates the actual MARS error response when the bbox
        is too small. The error should be caught and converted to a TerrakitValidationError
        with a helpful message.
        """
        collection = "derived-era5-single-levels-daily-statistics"
        # This bbox is only 0.02° x 0.02°, which is smaller than ERA5's 0.25° grid
        tiny_bbox = [-1.32, 51.06, -1.30, 51.08]
        save_file = f"{save_file_dir}/{self.connector_type}_{collection}.nc"

        dc = DataConnector(connector_type=self.connector_type)
        with pytest.raises(
            TerrakitValidationError, match="CLIMATE DATA STORE REQUEST FAILED"
        ):
            dc.connector.get_data(
                data_collection_name=collection,
                date_start=start_date,
                date_end=start_date,
                bbox=tiny_bbox,
                bands=self.bands,
                save_file=save_file,
            )
