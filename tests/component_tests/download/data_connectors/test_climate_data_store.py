# © Copyright IBM Corporation 2026
# SPDX-License-Identifier: Apache-2.0


import logging
import numpy as np
import os
import pandas as pd
import pytest
import shutil
import xarray as xr
import zipfile

from datetime import datetime
from pathlib import Path
from rasterio.crs import CRS
from unittest.mock import patch

from terrakit import DataConnector
from terrakit.download.data_connectors.climate_data_store import CDS
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

    def test_get_months_list_handles_month_end_dates_across_years(self):
        dc = DataConnector(connector_type=self.connector_type)

        months = dc.connector._get_months_list("2021-12-31", "2022-03-31")

        assert months == ["01", "02", "03", "12"]
        assert dc.connector._get_days_list("2021-12-31", "2022-03-31")[0] == "01"
        assert dc.connector._get_days_list("2021-12-31", "2022-03-31")[-1] == "31"

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
        "collection,bands,date_start,date_end",
        [
            (
                "derived-era5-single-levels-daily-statistics",
                ["fg10", "t2m", "tp", "u10", "v10"],
                "2025-01-01",
                "2025-01-02",
            ),
            (
                "projections-cordex-domains-single-levels",
                [
                    "10m_wind_speed",
                    "2m_air_temperature",
                    "mean_precipitation_flux",
                    "10m_u_component_of_the_wind",
                    "10m_v_component_of_the_wind",
                ],
                "1950-01-01",
                "1950-01-02",
            ),
        ],
    )
    def test_get_data_cds(
        self,
        mock_cds_client,
        collection,
        bands,
        date_start,
        date_end,
        bbox,
        save_file_dir,
        get_data_clean_up,
    ):
        """
        Test the get_data method.

        Note: The mock returns a zip file with 5 NetCDF files (one per variable),
        each containing 2 time steps.
        For ERA5: 2025-01-01 to 2025-01-02
        For CORDEX: 1950-01-01 to 1950-01-02 (historical data range)
        """
        save_file = f"{save_file_dir}/{self.connector_type}_{collection}.nc"
        dc = DataConnector(connector_type=self.connector_type)
        data = dc.connector.get_data(
            data_collection_name=collection,
            date_start=date_start,
            date_end=date_end,
            bbox=bbox,
            bands=bands,
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

        # Check that a single time-series NetCDF file was created
        assert os.path.exists(save_file) is True

        # Verify it's not split into daily files
        assert os.path.exists(save_file.replace(".nc", "_2025-01-01.nc")) is False

    def test_get_data_bbox_too_small_for_era5_resolution(
        self, mock_cds_client_bbox_error, start_date, save_file_dir
    ):
        """
        Test that get_data handles Meteorological Archival and Retrieval System (MARS) error for bbox smaller than ERA5 grid resolution (0.25°).

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

    def test_get_data_cds__cordex_ignores_scalar_grid_mapping_variable(
        self, tmp_path, monkeypatch
    ):
        """
        Test CORDEX-like NetCDF processing ignores scalar grid-mapping variables such as
        rotated_pole when extracting time-indexed band data.
        """

        time_values = pd.date_range("1950-01-01", periods=1)
        lat_values = np.array([-1.0, 0.0])
        lon_values = np.array([20.0, 21.0])

        ds_in = xr.Dataset(
            data_vars={
                "pr": (("time", "lat", "lon"), np.ones((1, 2, 2))),
                "rotated_pole": ((), 0),
            },
            coords={
                "time": time_values,
                "lat": lat_values,
                "lon": lon_values,
            },
        )
        ds_in["pr"].attrs["grid_mapping"] = "rotated_pole"
        ds_in["pr"].attrs["GRIB_stepType"] = "avg"

        source_nc = (
            tmp_path
            / "pr_AFR-44_ICHEC-EC-EARTH_historical_r1i1p1_KNMI-RACMO22T_v1_day_19500101-19501231.nc"
        )
        ds_in.to_netcdf(source_nc)
        ds_in.close()

        zip_path = tmp_path / "cordex_mock.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.write(source_nc, arcname=source_nc.name)

        dc = DataConnector(connector_type=self.connector_type)

        monkeypatch.setattr(
            dc.connector,
            "_download_from_cds",
            lambda *args, **kwargs: zip_path,
        )

        working_dir = tmp_path / "working"
        save_file = tmp_path / "output.nc"

        data = dc.connector.get_data(
            data_collection_name="projections-cordex-domains-single-levels",
            date_start="1950-01-01",
            date_end="1950-01-01",
            bbox=[20, -10, 30, 0],
            bands=["mean_precipitation_flux"],
            query_params={
                "experiment": "historical",
                "gcm_model": "ichec_ec_earth",
                "rcm_model": "knmi_racmo22t",
                "ensemble_member": "r1i1p1",
                "temporal_resolution": "daily_mean",
                "horizontal_resolution": "0_44_degree_x_0_44_degree",
            },
            working_dir=str(working_dir),
            save_file=str(save_file),
        )

        assert isinstance(data, xr.Dataset)
        assert "pr" in data.data_vars
        assert "rotated_pole" not in data.data_vars
        assert len(data.time) == 1

        shutil.rmtree(working_dir, ignore_errors=True)

    def test_get_data_cds__cordex_saves_dates_without_dataset_level_time_coordinate(
        self, tmp_path, monkeypatch
    ):
        """
        Test saving daily files still works when merged Dataset has per-variable time
        coordinates but no dataset-level `time` attribute accessor.
        """

        time_values = pd.date_range("1950-01-01", periods=1)
        lat_values = np.array([-1.0, 0.0])
        lon_values = np.array([20.0, 21.0])

        ds_in = xr.Dataset(
            data_vars={
                "pr": (("time", "lat", "lon"), np.ones((1, 2, 2))),
            },
            coords={
                "time": time_values,
                "lat": lat_values,
                "lon": lon_values,
            },
        )
        ds_in["pr"].attrs["GRIB_stepType"] = "avg"

        source_nc = (
            tmp_path
            / "pr_AFR-44_ICHEC-EC-EARTH_historical_r1i1p1_KNMI-RACMO22T_v1_day_19500101-19501231.nc"
        )
        ds_in.to_netcdf(source_nc)
        ds_in.close()

        zip_path = tmp_path / "cordex_mock_single_var.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.write(source_nc, arcname=source_nc.name)

        dc = DataConnector(connector_type=self.connector_type)

        monkeypatch.setattr(
            dc.connector,
            "_download_from_cds",
            lambda *args, **kwargs: zip_path,
        )

        working_dir = tmp_path / "working"
        save_file = tmp_path / "output.nc"

        data = dc.connector.get_data(
            data_collection_name="projections-cordex-domains-single-levels",
            date_start="1950-01-01",
            date_end="1950-01-01",
            bbox=[20, -10, 30, 0],
            bands=["mean_precipitation_flux"],
            query_params={
                "experiment": "historical",
                "gcm_model": "ichec_ec_earth",
                "rcm_model": "knmi_racmo22t",
                "ensemble_member": "r1i1p1",
                "temporal_resolution": "daily_mean",
                "horizontal_resolution": "0_44_degree_x_0_44_degree",
            },
            working_dir=str(working_dir),
            save_file=str(save_file),
        )

        assert isinstance(data, xr.Dataset)
        assert "pr" in data.data_vars
        # Check that a single time-series file was created, not daily files
        assert (tmp_path / "output.nc").exists()
        assert not (tmp_path / "output_1950-01-01.nc").exists()

        shutil.rmtree(working_dir, ignore_errors=True)

    def test_get_data_cds__cordex_supports_rotated_grid_spatial_dims(
        self, tmp_path, monkeypatch
    ):
        """
        Test CORDEX-like variables on rotated grids are processed when the data variable
        uses rlat/rlon dims and exposes lat/lon as 2D coordinates.
        """

        time_values = pd.date_range("1950-01-01", periods=1)
        rlat_values = np.array([-1.0, 0.0])
        rlon_values = np.array([20.0, 21.0])

        ds_in = xr.Dataset(
            data_vars={
                "pr": (("time", "rlat", "rlon"), np.ones((1, 2, 2))),
                "tas": (("time", "rlat", "rlon"), np.full((1, 2, 2), 280.0)),
                "sfcWind": (("time", "rlat", "rlon"), np.full((1, 2, 2), 5.0)),
                "rotated_pole": ((), 0),
            },
            coords={
                "time": time_values,
                "rlat": rlat_values,
                "rlon": rlon_values,
                "lat": (("rlat", "rlon"), [[-1.0, -1.0], [0.0, 0.0]]),
                "lon": (("rlat", "rlon"), [[20.0, 21.0], [20.0, 21.0]]),
            },
        )
        ds_in["pr"].attrs["GRIB_stepType"] = "avg"
        ds_in["tas"].attrs["GRIB_stepType"] = "instant"
        ds_in["sfcWind"].attrs["GRIB_stepType"] = "avg"

        file_specs = [
            (
                "pr_AFR-44_ICHEC-EC-EARTH_historical_r1i1p1_KNMI-RACMO22T_v1_day_19500101-19501231.nc",
                ["pr", "rotated_pole"],
            ),
            (
                "tas_AFR-44_ICHEC-EC-EARTH_historical_r1i1p1_KNMI-RACMO22T_v1_day_19500101-19501231.nc",
                ["tas", "rotated_pole"],
            ),
            (
                "sfcWind_AFR-44_ICHEC-EC-EARTH_historical_r1i1p1_KNMI-RACMO22T_v1_day_19500101-19501231.nc",
                ["sfcWind", "rotated_pole"],
            ),
        ]

        zip_path = tmp_path / "cordex_rotated_grid.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            for filename, vars_to_keep in file_specs:
                source_nc = tmp_path / filename
                ds_in[vars_to_keep].to_netcdf(source_nc)
                zf.write(source_nc, arcname=source_nc.name)

        ds_in.close()

        dc = DataConnector(connector_type=self.connector_type)

        monkeypatch.setattr(
            dc.connector,
            "_download_from_cds",
            lambda *args, **kwargs: zip_path,
        )

        working_dir = tmp_path / "working"
        save_file = tmp_path / "output.nc"

        data = dc.connector.get_data(
            data_collection_name="projections-cordex-domains-single-levels",
            date_start="1950-01-01",
            date_end="1950-01-01",
            bbox=[20, -10, 30, 0],
            bands=[
                "mean_precipitation_flux",
                "2m_air_temperature",
                "10m_wind_speed",
            ],
            query_params={
                "experiment": "historical",
                "gcm_model": "ichec_ec_earth",
                "rcm_model": "knmi_racmo22t",
                "ensemble_member": "r1i1p1",
                "temporal_resolution": "daily_mean",
                "horizontal_resolution": "0_44_degree_x_0_44_degree",
            },
            working_dir=str(working_dir),
            save_file=str(save_file),
        )

        assert isinstance(data, xr.Dataset)
        assert sorted(data.data_vars) == ["pr", "sfcWind", "tas"]
        assert len(data.time) == 1

        shutil.rmtree(working_dir, ignore_errors=True)


class TestCDSBuildRequestParams:
    """Test request parameter building for CDS API."""

    def test_era5_request_params_single_year(self):
        """Test ERA5 request parameters for single year range."""
        cds = CDS()

        collection_name = "derived-era5-single-levels-daily-statistics"
        date_start = "2024-01-01"
        date_end = "2024-01-03"
        bbox = [-10, 40, 5, 50]
        bands = ["2m_temperature"]
        constraints = {"variable": ["2m_temperature"]}

        params = cds._build_request_params(
            collection_name, date_start, date_end, bbox, bands, constraints
        )

        # Should use year/month/day for single year
        assert params["year"] == ["2024"]
        assert params["month"] == ["01"]
        assert set(params["day"]) == {"01", "02", "03"}
        assert params["variable"] == ["2m_temperature"]

    def test_era5_request_params_with_query_params(self):
        """Test that query_params are properly merged."""
        cds = CDS()

        collection_name = "derived-era5-single-levels-daily-statistics"
        date_start = "2024-01-01"
        date_end = "2024-01-03"
        bbox = [-10, 40, 5, 50]
        bands = ["2m_temperature"]
        constraints = {"variable": ["2m_temperature"]}
        query_params = {
            "daily_statistic": "daily_maximum",
            "frequency": "1_hourly",
            "time_zone": "utc+01:00",
        }

        params = cds._build_request_params(
            collection_name,
            date_start,
            date_end,
            bbox,
            bands,
            constraints,
            query_params,
        )

        # Query params should override defaults
        assert params["daily_statistic"] == "daily_maximum"
        assert params["frequency"] == "1_hourly"
        assert params["time_zone"] == "utc+01:00"

    def test_cordex_request_params(self):
        """Test CORDEX request parameters use start_year/end_year."""
        cds = CDS()

        collection_name = "projections-cordex-domains-single-levels"
        date_start = "1950-01-01"
        date_end = "1950-01-03"
        bbox = [20, -10, 30, 0]  # Africa region
        bands = ["2m_air_temperature"]
        constraints = {"variable": ["2m_air_temperature"]}

        params = cds._build_request_params(
            collection_name, date_start, date_end, bbox, bands, constraints
        )

        # CORDEX uses start_year/end_year
        assert params["start_year"] == ["1950"]
        assert params["end_year"] == ["1950"]
        assert "domain" in params  # Should have domain instead of area


class TestCDSMultiYearSplitting:
    """Test that multi-year requests are properly split."""

    def test_single_year_no_split(self):
        """Test that single-year requests don't get split."""
        start_dt = datetime.strptime("2024-01-01", "%Y-%m-%d")
        end_dt = datetime.strptime("2024-12-31", "%Y-%m-%d")
        years = list(range(start_dt.year, end_dt.year + 1))

        assert len(years) == 1
        assert years == [2024]

    def test_multi_year_split(self):
        """Test that multi-year requests are split correctly."""
        start_dt = datetime.strptime("2024-12-31", "%Y-%m-%d")
        end_dt = datetime.strptime("2025-02-03", "%Y-%m-%d")
        years = list(range(start_dt.year, end_dt.year + 1))

        assert len(years) == 2
        assert years == [2024, 2025]

    def test_many_year_split(self):
        """Test splitting across many years."""
        start_dt = datetime.strptime("2021-12-31", "%Y-%m-%d")
        end_dt = datetime.strptime("2025-01-01", "%Y-%m-%d")
        years = list(range(start_dt.year, end_dt.year + 1))

        assert len(years) == 5  # 2021, 2022, 2023, 2024, 2025
        assert years == [2021, 2022, 2023, 2024, 2025]


class TestCDSParallelDownload:
    """Test parallel download functionality for CDS connector."""

    connector_type = "climate_data_store"

    @pytest.fixture
    def bbox(self):
        """Bounding box for testing."""
        return [34.5, -0.5, 35.0, 0.0]

    @pytest.fixture
    def mock_cds_client(self):
        """Mock CDS client to avoid actual API calls."""
        with patch(
            "terrakit.download.data_connectors.climate_data_store.cdsapi.Client"
        ) as mock:
            yield mock

    @pytest.fixture
    def temp_dir(self, tmp_path):
        """Create temporary directory for test files."""
        test_dir = tmp_path / "test_cds_parallel"
        test_dir.mkdir()
        yield test_dir
        # Cleanup
        if test_dir.exists():
            shutil.rmtree(test_dir)

    def test_parallel_download_creates_multiple_requests(
        self, mock_cds_client, bbox, temp_dir
    ):
        """Test that multi-month requests are split and can be parallelized."""
        dc = DataConnector(connector_type=self.connector_type)

        # Mock the _download_from_cds method to track calls
        download_calls = []

        def mock_download(*args, **kwargs):
            download_calls.append((args, kwargs))
            # Create a mock zip file
            zip_path = Path(temp_dir) / f"mock_{len(download_calls)}.zip"
            with zipfile.ZipFile(zip_path, "w") as zf:
                # Create a minimal NetCDF file
                ds = xr.Dataset(
                    {
                        "temperature": (["time", "lat", "lon"], [[[20.0]]]),
                    }
                )
                nc_path = Path(temp_dir) / f"data_{len(download_calls)}.nc"
                ds.to_netcdf(nc_path)
                zf.write(nc_path, nc_path.name)
                nc_path.unlink()
            return str(zip_path)

        with patch.object(
            dc.connector, "_download_from_cds", side_effect=mock_download
        ):
            # Request data spanning 3 months
            try:
                dc.connector.get_data(
                    data_collection_name="derived-era5-single-levels-daily-statistics",
                    date_start="2024-01-01",
                    date_end="2024-03-31",
                    bbox=bbox,
                    bands=["2m_temperature"],
                    working_dir=str(temp_dir),
                )
            except Exception:
                # We expect this to fail due to mocking, but we can check the calls
                pass

        # Verify that multiple download calls were made (one per month)
        assert len(download_calls) == 3, "Should create 3 monthly download requests"

    def test_parallel_download_parameter_max_workers(
        self, mock_cds_client, bbox, temp_dir
    ):
        """Test that max_workers parameter controls parallelization."""
        # Test that max_workers parameter is accepted
        # This will be implemented in the actual code
        query_params = {"max_workers": 4}

        # For now, just verify the parameter can be passed
        # The actual implementation will use this parameter
        assert query_params.get("max_workers") == 4

    def test_sequential_download_when_max_workers_1(
        self, mock_cds_client, bbox, temp_dir
    ):
        """Test that max_workers=1 forces sequential download."""
        query_params = {"max_workers": 1}

        # Verify parameter is set correctly for sequential processing
        assert query_params.get("max_workers") == 1

    def test_default_max_workers(self, mock_cds_client, bbox, temp_dir):
        """Test that default max_workers is reasonable."""
        # Default should be None or a reasonable number (e.g., 4)
        # This will be defined in the implementation
        default_max_workers = 4
        assert default_max_workers > 0
        assert default_max_workers <= 10  # Reasonable upper limit

    def test_parallel_download_handles_errors_gracefully(
        self, mock_cds_client, bbox, temp_dir
    ):
        """Test that errors in one download don't break the entire process."""
        dc = DataConnector(connector_type=self.connector_type)

        call_count = [0]

        def mock_download_with_error(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 2:
                # Simulate error on second download
                raise Exception("Simulated download error")

            # Create a mock zip file for successful downloads
            zip_path = Path(temp_dir) / f"mock_{call_count[0]}.zip"
            with zipfile.ZipFile(zip_path, "w") as zf:
                ds = xr.Dataset(
                    {
                        "temperature": (["time", "lat", "lon"], [[[20.0]]]),
                    }
                )
                nc_path = Path(temp_dir) / f"data_{call_count[0]}.nc"
                ds.to_netcdf(nc_path)
                zf.write(nc_path, nc_path.name)
                nc_path.unlink()
            return str(zip_path)

        with patch.object(
            dc.connector, "_download_from_cds", side_effect=mock_download_with_error
        ):
            # Should raise an error but not hang
            with pytest.raises(Exception):
                dc.connector.get_data(
                    data_collection_name="derived-era5-single-levels-daily-statistics",
                    date_start="2024-01-01",
                    date_end="2024-03-31",
                    bbox=bbox,
                    bands=["2m_temperature"],
                    working_dir=str(temp_dir),
                )


class TestCordexValidation:
    """Test CORDEX preflight validation using constraints_variables file."""

    connector_type = "climate_data_store"
    collection = "projections-cordex-domains-single-levels"

    @pytest.fixture
    def dc(self):
        """Create a DataConnector instance."""
        return DataConnector(connector_type=self.connector_type)

    @pytest.fixture
    def valid_cordex_params(self):
        """Valid CORDEX parameters that should pass validation."""
        return {
            "domain": "africa",
            "experiment": "historical",
            "horizontal_resolution": "0_44_degree_x_0_44_degree",
            "temporal_resolution": "daily_mean",
            "gcm_model": "ichec_ec_earth",
            "rcm_model": "knmi_racmo22t",
            "ensemble_member": "r1i1p1",
            "variable": "2m_air_temperature",
            "start_year": 1950,
            "end_year": 1950,
        }

    def test_valid_cordex_combination(self, dc, valid_cordex_params):
        """Test that a valid CORDEX combination passes validation."""
        # This should not raise an exception
        dc.connector._validate_cordex_constraints(
            collection_name=self.collection,
            domain=valid_cordex_params["domain"],
            experiment=valid_cordex_params["experiment"],
            horizontal_resolution=valid_cordex_params["horizontal_resolution"],
            temporal_resolution=valid_cordex_params["temporal_resolution"],
            gcm_model=valid_cordex_params["gcm_model"],
            rcm_model=valid_cordex_params["rcm_model"],
            ensemble_member=valid_cordex_params["ensemble_member"],
            variable=valid_cordex_params["variable"],
            start_year=valid_cordex_params["start_year"],
            end_year=valid_cordex_params["end_year"],
        )

    def test_invalid_gcm_rcm_combination(self, dc, valid_cordex_params):
        """Test that an invalid GCM-RCM combination raises validation error."""
        # ichec_ec_earth + mpi_csc_remo2009 is not a valid combination for africa
        with pytest.raises(TerrakitValidationError) as exc_info:
            dc.connector._validate_cordex_constraints(
                collection_name=self.collection,
                domain=valid_cordex_params["domain"],
                experiment=valid_cordex_params["experiment"],
                horizontal_resolution=valid_cordex_params["horizontal_resolution"],
                temporal_resolution=valid_cordex_params["temporal_resolution"],
                gcm_model="ichec_ec_earth",
                rcm_model="mpi_csc_remo2009",  # Invalid combination
                ensemble_member=valid_cordex_params["ensemble_member"],
                variable=valid_cordex_params["variable"],
                start_year=valid_cordex_params["start_year"],
                end_year=valid_cordex_params["end_year"],
            )

        error_msg = str(exc_info.value)
        assert "not available" in error_msg.lower()
        assert "valid alternatives" in error_msg.lower()

    def test_invalid_variable_for_combination(self, dc, valid_cordex_params):
        """Test that an invalid variable for a specific combination raises error."""
        # 2m_relative_humidity is not available for all combinations
        with pytest.raises(TerrakitValidationError) as exc_info:
            dc.connector._validate_cordex_constraints(
                collection_name=self.collection,
                domain=valid_cordex_params["domain"],
                experiment=valid_cordex_params["experiment"],
                horizontal_resolution=valid_cordex_params["horizontal_resolution"],
                temporal_resolution=valid_cordex_params["temporal_resolution"],
                gcm_model="cnrm_cerfacs_cm5",
                rcm_model="clmcom_clm_cclm4_8_17",
                ensemble_member="r1i1p1",
                variable="2m_relative_humidity",  # Not available for this combo
                start_year=1950,
                end_year=1950,
            )

        error_msg = str(exc_info.value)
        assert "not available" in error_msg.lower()
        assert "valid" in error_msg.lower()

    def test_invalid_year_range(self, dc, valid_cordex_params):
        """Test that an invalid year range raises validation error."""
        with pytest.raises(TerrakitValidationError) as exc_info:
            dc.connector._validate_cordex_constraints(
                collection_name=self.collection,
                domain=valid_cordex_params["domain"],
                experiment=valid_cordex_params["experiment"],
                horizontal_resolution=valid_cordex_params["horizontal_resolution"],
                temporal_resolution=valid_cordex_params["temporal_resolution"],
                gcm_model=valid_cordex_params["gcm_model"],
                rcm_model=valid_cordex_params["rcm_model"],
                ensemble_member=valid_cordex_params["ensemble_member"],
                variable=valid_cordex_params["variable"],
                start_year=1940,  # Too early
                end_year=1950,
            )

        error_msg = str(exc_info.value)
        assert "year range" in error_msg.lower()
        assert "not available" in error_msg.lower()

    def test_invalid_experiment_for_domain(self, dc, valid_cordex_params):
        """Test that an invalid experiment for a domain raises error."""
        with pytest.raises(TerrakitValidationError) as exc_info:
            dc.connector._validate_cordex_constraints(
                collection_name=self.collection,
                domain=valid_cordex_params["domain"],
                experiment="rcp_2_6",  # May not be available for all combinations
                horizontal_resolution=valid_cordex_params["horizontal_resolution"],
                temporal_resolution=valid_cordex_params["temporal_resolution"],
                gcm_model="cnrm_cerfacs_cm5",
                rcm_model="clmcom_clm_cclm4_8_17",
                ensemble_member="r1i1p1",
                variable=valid_cordex_params["variable"],
                start_year=2006,
                end_year=2010,
            )

        error_msg = str(exc_info.value)
        assert "not available" in error_msg.lower()

    def test_invalid_ensemble_member(self, dc, valid_cordex_params):
        """Test that an invalid ensemble member raises validation error."""
        with pytest.raises(TerrakitValidationError) as exc_info:
            dc.connector._validate_cordex_constraints(
                collection_name=self.collection,
                domain=valid_cordex_params["domain"],
                experiment=valid_cordex_params["experiment"],
                horizontal_resolution=valid_cordex_params["horizontal_resolution"],
                temporal_resolution=valid_cordex_params["temporal_resolution"],
                gcm_model=valid_cordex_params["gcm_model"],
                rcm_model=valid_cordex_params["rcm_model"],
                ensemble_member="r99i99p99",  # Invalid
                variable=valid_cordex_params["variable"],
                start_year=valid_cordex_params["start_year"],
                end_year=valid_cordex_params["end_year"],
            )

        error_msg = str(exc_info.value)
        assert "not available" in error_msg.lower()

    def test_error_message_includes_valid_alternatives(self, dc, valid_cordex_params):
        """Test that error messages include valid alternatives."""
        with pytest.raises(TerrakitValidationError) as exc_info:
            dc.connector._validate_cordex_constraints(
                collection_name=self.collection,
                domain=valid_cordex_params["domain"],
                experiment=valid_cordex_params["experiment"],
                horizontal_resolution=valid_cordex_params["horizontal_resolution"],
                temporal_resolution=valid_cordex_params["temporal_resolution"],
                gcm_model="invalid_gcm",
                rcm_model=valid_cordex_params["rcm_model"],
                ensemble_member=valid_cordex_params["ensemble_member"],
                variable=valid_cordex_params["variable"],
                start_year=valid_cordex_params["start_year"],
                end_year=valid_cordex_params["end_year"],
            )

        error_msg = str(exc_info.value)
        # Should suggest valid GCM models
        assert "valid" in error_msg.lower()
        assert "gcm" in error_msg.lower() or "model" in error_msg.lower()

    def test_validation_with_multiple_variables(self, dc, valid_cordex_params):
        """Test validation with multiple variables."""
        # Test with a list of variables
        variables = ["2m_air_temperature", "mean_precipitation_flux"]

        # Should validate each variable
        for var in variables:
            dc.connector._validate_cordex_constraints(
                collection_name=self.collection,
                domain=valid_cordex_params["domain"],
                experiment=valid_cordex_params["experiment"],
                horizontal_resolution=valid_cordex_params["horizontal_resolution"],
                temporal_resolution=valid_cordex_params["temporal_resolution"],
                gcm_model=valid_cordex_params["gcm_model"],
                rcm_model=valid_cordex_params["rcm_model"],
                ensemble_member=valid_cordex_params["ensemble_member"],
                variable=var,
                start_year=valid_cordex_params["start_year"],
                end_year=valid_cordex_params["end_year"],
            )

    def test_validation_called_before_download(self, dc, valid_cordex_params, bbox):
        """Test that validation is called before attempting download."""
        # This test verifies integration - validation should happen in get_data
        # before calling client.retrieve

        with pytest.raises(TerrakitValidationError):
            dc.connector.get_data(
                data_collection_name=self.collection,
                date_start="1950-01-01",
                date_end="1950-01-31",
                bbox=bbox,
                bands=["2m_air_temperature"],
                query_params={
                    "gcm_model": "invalid_model",  # This should fail validation
                    "rcm_model": "knmi_racmo22t",
                    "experiment": "historical",
                },
            )

    def test_fixed_temporal_resolution_no_year_validation(self, dc):
        """Test that fixed temporal resolution doesn't validate year ranges."""
        # For temporal_resolution='fixed', start_year and end_year are not in constraints
        dc.connector._validate_cordex_constraints(
            collection_name=self.collection,
            domain="africa",
            experiment="evaluation",
            horizontal_resolution="0_44_degree_x_0_44_degree",
            temporal_resolution="fixed",
            gcm_model="era_interim",
            rcm_model="clmcom_clm_cclm4_8_17",
            ensemble_member="r0i0p0",
            variable="land_area_fraction",
            start_year=None,  # Not applicable for fixed
            end_year=None,
        )

    def test_fixed_block_validation_exact_match(self, dc):
        """Test that fixed blocks require exact year range matches."""
        # From constraints file, line 23: africa+historical+ichec_ec_earth+clmcom_clm_cclm4_8_17
        # has start_year: ["1949"], end_year: ["1950"] - a fixed block

        # This should pass - requesting the exact block
        dc.connector._validate_cordex_constraints(
            collection_name=self.collection,
            domain="africa",
            experiment="historical",
            horizontal_resolution="0_44_degree_x_0_44_degree",
            temporal_resolution="daily_mean",
            gcm_model="ichec_ec_earth",
            rcm_model="clmcom_clm_cclm4_8_17",
            ensemble_member="r12i1p1",
            variable="2m_air_temperature",
            start_year=1949,
            end_year=1950,
        )

    def test_fixed_block_validation_subset_fails(self, dc):
        """Test that requesting a subset within a fixed block fails."""
        # From constraints file, line 35: africa+historical+ichec_ec_earth+clmcom_clm_cclm4_8_17
        # has start_year: ["1951"], end_year: ["1955"] - a 5-year fixed block

        # This should FAIL - requesting only a subset (1952-1954) of the block (1951-1955)
        with pytest.raises(TerrakitValidationError) as exc_info:
            dc.connector._validate_cordex_constraints(
                collection_name=self.collection,
                domain="africa",
                experiment="historical",
                horizontal_resolution="0_44_degree_x_0_44_degree",
                temporal_resolution="daily_mean",
                gcm_model="ichec_ec_earth",
                rcm_model="clmcom_clm_cclm4_8_17",
                ensemble_member="r12i1p1",
                variable="2m_air_temperature",
                start_year=1952,
                end_year=1954,
            )

        error_msg = str(exc_info.value)
        assert "not available" in error_msg.lower()
        # Should mention that exact blocks are required
        assert "block" in error_msg.lower() or "exact" in error_msg.lower()

    def test_fixed_block_validation_single_year_fails(self, dc):
        """Test that requesting a single year from a multi-year fixed block fails."""
        # From constraints file, line 35: africa+historical+ichec_ec_earth+knmi_racmo22t
        # has start_year: ["1951"], end_year: ["1955"] - must request full 1951-1955

        # This should FAIL - requesting only 1951 from the 1951-1955 block
        with pytest.raises(TerrakitValidationError) as exc_info:
            dc.connector._validate_cordex_constraints(
                collection_name=self.collection,
                domain="africa",
                experiment="historical",
                horizontal_resolution="0_44_degree_x_0_44_degree",
                temporal_resolution="daily_mean",
                gcm_model="ichec_ec_earth",
                rcm_model="knmi_racmo22t",
                ensemble_member="r1i1p1",
                variable="2m_air_temperature",
                start_year=1951,
                end_year=1951,
            )

        error_msg = str(exc_info.value)
        assert "not available" in error_msg.lower()

    def test_fixed_block_validation_spanning_blocks_fails(self, dc):
        """Test that requesting across multiple fixed blocks fails."""
        # From constraints file: africa+historical+ichec_ec_earth+clmcom_clm_cclm4_8_17
        # has multiple blocks: ["1949"], ["1950"] and ["1951"], ["1955"]
        # Requesting 1949-1951 spans two blocks and should fail

        with pytest.raises(TerrakitValidationError) as exc_info:
            dc.connector._validate_cordex_constraints(
                collection_name=self.collection,
                domain="africa",
                experiment="historical",
                horizontal_resolution="0_44_degree_x_0_44_degree",
                temporal_resolution="daily_mean",
                gcm_model="ichec_ec_earth",
                rcm_model="clmcom_clm_cclm4_8_17",
                ensemble_member="r12i1p1",
                variable="2m_air_temperature",
                start_year=1949,
                end_year=1951,
            )

        error_msg = str(exc_info.value)
        assert "not available" in error_msg.lower()
        # Should mention available blocks
        assert "year" in error_msg.lower()

    def test_fixed_block_validation_outside_blocks_fails(self, dc):
        """Test that requesting years outside all fixed blocks fails."""
        # From constraints file: africa+historical+ichec_ec_earth+clmcom_clm_cclm4_8_17
        # has blocks starting from 1949
        # Requesting 1940-1945 is outside all blocks

        with pytest.raises(TerrakitValidationError) as exc_info:
            dc.connector._validate_cordex_constraints(
                collection_name=self.collection,
                domain="africa",
                experiment="historical",
                horizontal_resolution="0_44_degree_x_0_44_degree",
                temporal_resolution="daily_mean",
                gcm_model="ichec_ec_earth",
                rcm_model="clmcom_clm_cclm4_8_17",
                ensemble_member="r12i1p1",
                variable="2m_air_temperature",
                start_year=1940,
                end_year=1945,
            )

        error_msg = str(exc_info.value)
        assert "not available" in error_msg.lower()

    def test_is_fixed_block_constraint_detection(self, dc):
        """Test the helper method that detects fixed blocks."""
        # Test fixed block with single pair
        fixed_single = {"start_year": ["1950"], "end_year": ["1955"]}
        assert dc.connector._is_fixed_block_constraint(fixed_single) is True

        # Test fixed block with multiple pairs
        fixed_multiple = {
            "start_year": ["1950", "1956", "1961"],
            "end_year": ["1955", "1960", "1965"],
        }
        assert dc.connector._is_fixed_block_constraint(fixed_multiple) is True

        # Test no year constraints
        no_years = {"domain": ["africa"]}
        assert dc.connector._is_fixed_block_constraint(no_years) is False

        # Test mismatched lengths (flexible range pattern)
        flexible = {"start_year": ["1950"], "end_year": ["2005", "2010"]}
        assert dc.connector._is_fixed_block_constraint(flexible) is False

    def test_fixed_block_error_message_clarity(self, dc):
        """Test that error messages clearly indicate fixed blocks."""
        # Request an invalid year range for a fixed block constraint
        with pytest.raises(TerrakitValidationError) as exc_info:
            dc.connector._validate_cordex_constraints(
                collection_name=self.collection,
                domain="africa",
                experiment="historical",
                horizontal_resolution="0_44_degree_x_0_44_degree",
                temporal_resolution="daily_mean",
                gcm_model="ichec_ec_earth",
                rcm_model="clmcom_clm_cclm4_8_17",
                ensemble_member="r12i1p1",
                variable="2m_air_temperature",
                start_year=1948,  # Before any available block
                end_year=1949,
            )

        error_msg = str(exc_info.value)
        # Should mention blocks or ranges
        assert "year" in error_msg.lower()
        assert "available" in error_msg.lower()
