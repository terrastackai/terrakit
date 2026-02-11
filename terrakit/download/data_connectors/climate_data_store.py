# © Copyright IBM Corporation 2026
# SPDX-License-Identifier: Apache-2.0


import cdsapi
import json
import logging
import math
import os
import pandas as pd
import requests
import shutil
import xarray as xr
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from shapely.geometry import box
from typing import Any, Dict, Union

from terrakit.download.geodata_utils import save_cog
from ..connector import Connector
from ..geodata_utils import (
    load_and_list_collections,
    save_data_array_to_file,
)
from terrakit.general_utils.exceptions import (
    TerrakitValidationError,
    TerrakitValueError,
)
from terrakit.validate.helpers import (
    check_collection_exists,
    check_date_format,
    check_start_end_date_in_correct_order,
    basic_bbox_validation,
)
from .cds_utils.cordex_utils import (
    CORDEX_DOMAINS,
    get_domain_info,
    find_matching_domains,
)


logger = logging.getLogger(__name__)

######################################################################################################
###  Supporting functions
######################################################################################################

######################################################################################################
###  Connector class
######################################################################################################


class CDS(Connector):
    """
    Attributes:
        connector_type (str): Name of connector
        collections (list): A list of available collections.
        collections_details (list): Detailed information about the collections.
    """

    def __init__(self):
        """
        Initialize climate_data_store with collections and configuration.
        """
        self.connector_type: str = "climate_data_store"
        self.CDSAPI_URL: str = "https://cds.climate.copernicus.eu/api"
        self.stac_url: str = "https://cds.climate.copernicus.eu/api/catalogue/v1/"
        self.collections: list[Any] = load_and_list_collections(
            connector_type=self.connector_type
        )
        self.collections_details: list[Any] = load_and_list_collections(
            as_json=True, connector_type=self.connector_type
        )
        self.metadata_dir = Path(__file__).parent / "cds_utils"

        # Load CORDEX domains
        self.cordex_domains = CORDEX_DOMAINS

    def _is_cordex_collection(self, collection_name: str) -> bool:
        """Check if collection is a CORDEX dataset."""
        return "cordex" in collection_name.lower()

    def _get_cordex_domain_from_bbox(self, bbox: list) -> str:
        """
        Map user bbox to appropriate CORDEX domain code.

        Args:
            bbox: User's bounding box [min_lon, min_lat, max_lon, max_lat]

        Returns:
            str: CORDEX domain code (e.g., 'EUR-11')

        Raises:
            TerrakitValidationError: If no matching domain found
        """
        matching_domains = find_matching_domains(bbox)

        if not matching_domains:
            raise TerrakitValidationError(
                message=f"Bbox {bbox} does not intersect with any CORDEX domain. "
                f"Use list_cordex_domains() to see available domains."
            )

        if len(matching_domains) == 1:
            return matching_domains[0]

        # Multiple matches - return best match based on overlap
        return self._find_best_cordex_match(bbox, matching_domains)

    def _find_best_cordex_match(self, bbox: list, domain_codes: list) -> str:
        """
        Find CORDEX domain with maximum overlap with user bbox.

        Args:
            bbox: User's bounding box
            domain_codes: List of candidate domain codes

        Returns:
            str: Best matching domain code
        """

        user_box = box(bbox[0], bbox[1], bbox[2], bbox[3])
        best_domain: str = domain_codes[0]  # Initialize with first domain
        max_overlap = 0

        for domain_code in domain_codes:
            domain_bbox = self.cordex_domains[domain_code]["bbox"]
            domain_box = box(
                domain_bbox[0], domain_bbox[1], domain_bbox[2], domain_bbox[3]
            )

            overlap_area = user_box.intersection(domain_box).area
            if overlap_area > max_overlap:
                max_overlap = overlap_area
                best_domain = domain_code

        logger.info(
            f"Multiple CORDEX domains match bbox. Selected {best_domain} with largest overlap."
        )
        return best_domain

    def _convert_netcdf_to_geotiffs(
        self, netcdf_path: str, bbox: list, output_dir: str, collection_name: str
    ) -> list[str]:
        """Convert CDS NetCDF to individual GeoTIFF files, one per date."""

        ds = xr.open_dataset(netcdf_path)
        output_files = []

        # Determine dimension names
        lon_name = "longitude" if "longitude" in ds.dims else "lon"
        lat_name = "latitude" if "latitude" in ds.dims else "lat"
        time_name = "time" if "time" in ds.dims else "valid_time"

        # Get the main data variable
        data_vars = [
            v for v in ds.data_vars if v not in [lon_name, lat_name, time_name]
        ]
        var_name = data_vars[0]

        # Process each time step
        for time_idx in range(len(ds[time_name])):
            # Extract data for this time step
            da = ds[var_name].isel({time_name: time_idx})

            # Get the date
            time_value = ds[time_name].isel({time_name: time_idx}).values
            date_str = pd.Timestamp(time_value).strftime("%Y-%m-%d")

            # Add CRS and spatial dimensions
            da = da.rio.write_crs("EPSG:4326")
            da = da.rio.set_spatial_dims(x_dim=lon_name, y_dim=lat_name)

            # Create output filename
            output_filename = f"{collection_name}_{date_str}.tif"
            output_path = Path(output_dir) / output_filename

            # Use TerraKit's save_cog function
            save_cog(da, str(output_path))
            output_files.append(str(output_path))

            logger.info(f"Created GeoTIFF: {output_filename}")

        ds.close()
        return output_files

    def _estimate_request_size(
        self,
        collection_name: str,
        date_start: str,
        date_end: str,
        bbox: list,
        bands: list,
    ) -> dict:
        """
        Estimate the size and duration of a CDS request.

        Returns:
            dict with keys: 'num_days', 'num_variables', 'area_km2',
                        'estimated_mb', 'estimated_minutes'
        """

        # Calculate number of days
        start = datetime.strptime(date_start, "%Y-%m-%d")
        end = datetime.strptime(date_end, "%Y-%m-%d")
        num_days = (end - start).days + 1

        # Calculate area in km²
        # Approximate conversion: 1 degree ≈ 111 km at equator
        lon_range = bbox[2] - bbox[0]
        lat_range = bbox[3] - bbox[1]
        avg_lat = (bbox[1] + bbox[3]) / 2

        # Adjust longitude distance by latitude (cosine correction)
        lon_km = lon_range * 111 * math.cos(math.radians(avg_lat))
        lat_km = lat_range * 111
        area_km2 = lon_km * lat_km

        # Number of variables
        num_variables = len(bands) if bands else 1

        # Estimate file size (rough approximations based on CDS data)
        if self._is_cordex_collection(collection_name):
            # CORDEX: ~0.5 MB per day per variable for typical domain
            mb_per_day_per_var = 0.5
        else:
            # ERA5: depends on resolution and area
            # ~0.1 MB per day per variable per 10,000 km²
            mb_per_day_per_var = (area_km2 / 10000) * 0.1

        estimated_mb = num_days * num_variables * mb_per_day_per_var

        # Estimate download time
        # CDS queue time: 1-5 minutes (average 2)
        # Download speed: ~5 MB/min (conservative estimate)
        queue_time_min = 2
        download_time_min = estimated_mb / 5
        estimated_minutes = queue_time_min + download_time_min

        return {
            "num_days": num_days,
            "num_variables": num_variables,
            "area_km2": round(area_km2, 2),
            "estimated_mb": round(estimated_mb, 2),
            "estimated_minutes": round(estimated_minutes, 1),
        }

    def _download_from_cds(
        self,
        collection_name: str,
        date_start: str,
        date_end: str,
        bbox: list,
        bands: list = [],
        query_params: dict = {},
        working_dir: str = ".",
    ) -> str:
        """
        Download data from CDS API with size and time estimates.

        Args:
            collection_name: CDS dataset name
            date_start: Start date (YYYY-MM-DD)
            date_end: End date (YYYY-MM-DD)
            bbox: Bounding box [min_lon, min_lat, max_lon, max_lat]
            bands: List of variables/bands to download
            working_dir: Directory to save the downloaded zip file

        Returns:
            Path to downloaded zip file in working_dir
        """

        # Ensure working_dir exists
        Path(working_dir).mkdir(parents=True, exist_ok=True)

        # Estimate request size
        estimate = self._estimate_request_size(
            collection_name, date_start, date_end, bbox, bands
        )

        # Log detailed information
        logger.info(f"Submitting CDS request for {collection_name}")
        logger.info(
            f"Date range: {date_start} to {date_end} ({estimate['num_days']} days)"
        )
        logger.info(f"Area: {estimate['area_km2']} km²")
        logger.info(f"Variables: {estimate['num_variables']}")
        logger.info(f"Estimated size: ~{estimate['estimated_mb']} MB")
        logger.info(f"Estimated time: ~{estimate['estimated_minutes']} minutes")

        # Connect and build request
        client = self._connect_to_cds()
        request_params = self._build_request_params(
            collection_name,
            date_start,
            date_end,
            bbox,
            bands,
            self._load_constraints(collection_name),
            query_params,
        )

        # Log request parameters for debugging
        logger.debug("CDS Request Parameters:")
        logger.debug(json.dumps(request_params, indent=2))

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"cds_{collection_name}_{timestamp}.zip"
        output_zip = str(Path(working_dir) / output_filename)

        logger.info("Request submitted to CDS queue. Please wait...")

        try:
            start_time = datetime.now()
            client.retrieve(collection_name, request_params, output_zip)

            # Log success
            actual_time = (datetime.now() - start_time).total_seconds() / 60
            logger.info(f"✓ Download complete: {output_zip}")
            logger.info(f"Actual time: {actual_time:.1f} minutes")

            return output_zip

        except requests.HTTPError as e:
            # Parse CDS-specific error messages
            error_details = self._parse_cds_error(e)

            logger.error("=" * 70)
            logger.error("CLIMATE DATA STORE REQUEST FAILED")
            logger.error("=" * 70)
            logger.error(f"Collection: {collection_name}")
            logger.error(f"Error Type: {error_details['type']}")
            logger.error(f"Error Message: {error_details['message']}")
            logger.error("")
            logger.error("Request Parameters:")
            logger.error(json.dumps(request_params, indent=2))
            logger.error("")
            logger.error("Possible causes:")
            for cause in error_details["possible_causes"]:
                logger.error(f"  - {cause}")
            logger.error("=" * 70)

            raise TerrakitValidationError(
                message=f"CLIMATE DATA STORE REQUEST FAILED: {error_details['message']}\n"
                f"Collection: {collection_name}\n"
                f"Error type: {error_details['type']}\n"
                f"See logs for full request parameters and troubleshooting tips."
            )

        except Exception as e:
            logger.error("=" * 70)
            logger.error("UNEXPECTED ERROR DURING CDS DOWNLOAD")
            logger.error("=" * 70)
            logger.error(f"Collection: {collection_name}")
            logger.error(f"Error: {str(e)}")
            logger.error("")
            logger.error("Request Parameters:")
            logger.error(json.dumps(request_params, indent=2))
            logger.error("=" * 70)

            raise TerrakitValidationError(
                message=f"Failed to download from CDS: {str(e)}\n"
                f"Collection: {collection_name}\n"
                f"See logs for full request parameters."
            )

    def _parse_cds_error(self, error: requests.HTTPError) -> dict:
        """
        Parse CDS API error and provide helpful troubleshooting information.

        Returns:
            dict with keys: 'type', 'message', 'possible_causes'
        """
        error_str = str(error)

        # Common CDS error patterns
        if "ValueError" in error_str:
            return {
                "type": "ValueError",
                "message": "Invalid parameter value in request",
                "possible_causes": [
                    "Variable/band name not valid for this collection",
                    "Date outside collection temporal range",
                    "Invalid area/bbox coordinates",
                    "Missing required parameters",
                    "Check CDS documentation for valid parameter values",
                ],
            }
        elif "400" in error_str or "Bad Request" in error_str:
            return {
                "type": "Bad Request (400)",
                "message": "CDS rejected the request parameters",
                "possible_causes": [
                    "Invalid parameter format",
                    "Required parameter missing",
                    "Parameter value out of range",
                    "Check parameter names match CDS API expectations",
                ],
            }
        elif "401" in error_str or "Unauthorized" in error_str:
            return {
                "type": "Unauthorized (401)",
                "message": "Authentication failed",
                "possible_causes": [
                    "Invalid or missing CDS API key",
                    "API key not set in environment (CDSAPI_KEY)",
                    "Account not activated or suspended",
                ],
            }
        elif "403" in error_str or "Forbidden" in error_str:
            return {
                "type": "Forbidden (403)",
                "message": "Access denied to this dataset",
                "possible_causes": [
                    "Dataset license not accepted",
                    "Visit CDS website to accept terms and conditions",
                    "Account lacks permissions for this dataset",
                ],
            }
        else:
            return {
                "type": "Unknown Error",
                "message": error_str,
                "possible_causes": [
                    "Check CDS service status",
                    "Verify request parameters",
                    "Review CDS API documentation",
                ],
            }

    def _build_request_params(
        self,
        collection_name: str,
        date_start: str,
        date_end: str,
        bbox: list,
        bands: list,
        constraints: dict,
        query_params: dict = {},
    ) -> Dict[str, Any]:
        """
        Build CDS API request parameters based on collection type.

        Args:
            collection_name: CDS dataset name
            date_start: Start date (YYYY-MM-DD)
            date_end: End date (YYYY-MM-DD)
            bbox: Bounding box [min_lon, min_lat, max_lon, max_lat]
            bands: List of variables/bands to download
            constraints: Collection constraints from metadata
            query_params: Additional collection-specific parameters (e.g., daily_statistic, frequency)

        Returns:
            Dictionary of request parameters for CDS API
        """
        params: Dict[str, Any] = {}

        # Handle different collection types
        if self._is_cordex_collection(collection_name):
            # CORDEX collections need domain instead of bbox
            domain_code = self._get_cordex_domain_from_bbox(bbox)
            params["domain"] = domain_code

            # Set default parameters for CORDEX collections
            # These can be overridden by query_params
            params["experiment"] = "historical"
            params["horizontal_resolution"] = "0_44_degree_x_0_44_degree"
            params["temporal_resolution"] = "daily_mean"
            params["ensemble_member"] = "r1i1p1"
            params["format"] = "netcdf"

            # Add start_year and end_year based on date range
            start_date = datetime.strptime(date_start, "%Y-%m-%d")
            end_date = datetime.strptime(date_end, "%Y-%m-%d")
            params["start_year"] = [str(start_date.year)]
            params["end_year"] = [str(end_date.year)]

        else:
            # ERA5 and other collections use bbox directly
            params["area"] = [
                bbox[0],  # West
                bbox[1],  # South
                bbox[2],  # East
                bbox[3],  # North
            ]

            # Set default parameters for ERA5 collections
            # These can be overridden by query_params
            params["product_type"] = "reanalysis"
            params["format"] = "netcdf"
            params["daily_statistic"] = "daily_mean"
            params["frequency"] = "6_hourly"
            params["time_zone"] = "utc+00:00"

        # Add temporal parameters
        params["year"] = self._get_years_list(date_start, date_end)
        params["month"] = self._get_months_list(date_start, date_end)
        params["day"] = self._get_days_list(date_start, date_end)

        # Add variables/bands
        if bands:
            params["variable"] = bands
        elif "variable" in constraints:
            # Use first available variable if none specified
            params["variable"] = [constraints["variable"][0]]

        # Merge query_params - these override any defaults set above
        # This allows users to specify collection-specific parameters like:
        # - daily_statistic: "daily_mean", "daily_maximum", "daily_minimum", "daily_standard_deviation"
        # - frequency: "1hr", "3hr", "6hr", "day", "mon", "sem", "fx"
        # - product_type: override default "reanalysis"
        # - time_zone: override default "utc+00:00"
        params.update(query_params)

        return params

    def _get_years_list(self, date_start: str, date_end: str) -> list[str]:
        """Get list of years between start and end dates."""
        start = datetime.strptime(date_start, "%Y-%m-%d")
        end = datetime.strptime(date_end, "%Y-%m-%d")
        return [str(year) for year in range(start.year, end.year + 1)]

    def _get_months_list(self, date_start: str, date_end: str) -> list[str]:
        """Get list of months between start and end dates."""
        start = datetime.strptime(date_start, "%Y-%m-%d")
        end = datetime.strptime(date_end, "%Y-%m-%d")

        months = set()
        current = start
        while current <= end:
            months.add(f"{current.month:02d}")
            # Move to next month
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)

        return sorted(list(months))

    def _get_days_list(self, date_start: str, date_end: str) -> list[str]:
        """Get list of days between start and end dates."""
        start = datetime.strptime(date_start, "%Y-%m-%d")
        end = datetime.strptime(date_end, "%Y-%m-%d")

        days = set()
        current = start
        while current <= end:
            days.add(f"{current.day:02d}")
            current += timedelta(days=1)

        return sorted(list(days))

    def _get_constraint_value(
        self, constraints: dict, *keys: str, collection_name: str = ""
    ):
        """
        Safely extract nested values from constraints with clear error messages.

        Args:
            constraints: The constraints dictionary
            *keys: Sequence of keys to traverse (e.g., 'extent', 'temporal', 'interval')
            collection_name: Optional collection name for better error messages

        Returns:
            The value at the specified path

        Raises:
            TerrakitValidationError: If any key in the path is missing
        """
        if not constraints:
            raise TerrakitValidationError(
                message=f"No constraints metadata available{f' for {collection_name}' if collection_name else ''}"
            )

        value = constraints
        path = []

        for key in keys:
            path.append(key)
            if not isinstance(value, dict) or key not in value:
                path_str = " -> ".join(path)
                raise TerrakitValidationError(
                    message=f"Collection constraints missing required field: '{path_str}'"
                    f"{f' for {collection_name}' if collection_name else ''}"
                )
            value = value[key]

            if value is None:
                path_str = " -> ".join(path)
                raise TerrakitValidationError(
                    message=f"Collection constraints field is null: '{path_str}'"
                    f"{f' for {collection_name}' if collection_name else ''}"
                )
        return value

    def _validate_temporal(
        self,
        date_start: str,
        date_end: str,
        constraints: dict,
        collection_name: str = "",
    ):
        """Validate dates against collection constraints."""

        # Check dates are valid
        check_start_end_date_in_correct_order(date_start, date_end)
        check_date_format(date_start, start_or_end="start")
        check_date_format(date_start, start_or_end="end")

        # Get temporal interval using helper
        intervals = self._get_constraint_value(
            constraints,
            "extent",
            "temporal",
            "interval",
            collection_name=collection_name,
        )

        if not intervals or not intervals[0] or len(intervals[0]) < 2:
            raise TerrakitValidationError(
                message=f"Invalid temporal interval format in constraints"
                f"{f' for {collection_name}' if collection_name else ''}"
            )

        try:
            # Get allowed date range
            allowed_start = datetime.fromisoformat(
                intervals[0][0].replace("+00:00", "")
            )
            allowed_end = datetime.fromisoformat(intervals[0][1].replace("+00:00", ""))
            print(allowed_start, allowed_end)
            # Parse requested dates
            req_start = datetime.strptime(date_start, "%Y-%m-%d")
            req_end = datetime.strptime(date_end, "%Y-%m-%d")

            # Validate start date
            if req_start < allowed_start:
                raise TerrakitValidationError(
                    message=f"Start date {date_start} is before allowed start date {allowed_start.date()}"
                )

            # Validate end date
            if req_end > allowed_end:
                raise TerrakitValidationError(
                    message=f"End date {date_end} is after allowed end date {allowed_end.date()}"
                )

        except ValueError as e:
            raise TerrakitValidationError(message=f"Invalid date format: {e}")

    def _validate_spatial(
        self, bbox: list, constraints: dict, collection_name: str = ""
    ):
        """Validate bbox against collection constraints."""

        basic_bbox_validation(bbox, self.connector_type)

        # For CORDEX collections, map bbox to domain
        if self._is_cordex_collection(collection_name):
            try:
                domain_code = self._get_cordex_domain_from_bbox(bbox)
                logger.info(f"Mapped bbox to CORDEX domain: {domain_code}")
                # Store domain for later use in find_data
                self._selected_cordex_domain = domain_code
            except TerrakitValidationError:
                raise
        else:
            # Get spatial bbox using helper
            bbox_list = self._get_constraint_value(
                constraints,
                "extent",
                "spatial",
                "bbox",
                collection_name=collection_name,
            )

            if not bbox_list or not bbox_list[0] or len(bbox_list[0]) != 4:
                raise TerrakitValidationError(
                    message=f"Invalid spatial bbox format in constraints"
                    f"{f' for {collection_name}' if collection_name else ''}"
                )

            allowed_bbox = bbox_list[0]

            # Unpack for clarity
            min_lon, min_lat, max_lon, max_lat = bbox
            allowed_min_lon, allowed_min_lat, allowed_max_lon, allowed_max_lat = (
                allowed_bbox
            )

            # Validate each bound
            errors = []
            if min_lon < allowed_min_lon:
                errors.append(f"min_lon {min_lon} < allowed {allowed_min_lon}")
            if min_lat < allowed_min_lat:
                errors.append(f"min_lat {min_lat} < allowed {allowed_min_lat}")
            if max_lon > allowed_max_lon:
                errors.append(f"max_lon {max_lon} > allowed {allowed_max_lon}")
            if max_lat > allowed_max_lat:
                errors.append(f"max_lat {max_lat} > allowed {allowed_max_lat}")

            if errors:
                raise TerrakitValidationError(
                    message=f"Bounding box out of range: {'; '.join(errors)}"
                )

    def _load_constraints(self, collection_name: str) -> dict:
        """Load constraints metadata from local file."""
        constraints_file = self.metadata_dir / f"{collection_name}_constraints.json"

        if not constraints_file.exists():
            raise TerrakitValidationError(
                message=f"No constraints file found for collection '{collection_name}'. "
                f"Expected: {constraints_file}"
            )

        try:
            with open(constraints_file, "r") as f:
                constraints: Dict[str, Any] = json.load(f)
        except json.JSONDecodeError as e:
            raise TerrakitValidationError(
                message=f"Invalid JSON in constraints file for '{collection_name}': {e}"
            )
        except Exception as e:
            raise TerrakitValidationError(
                message=f"Error loading constraints for '{collection_name}': {e}"
            )
        return constraints

    def _connect_to_cds(self) -> cdsapi.Client:
        """
        Connect to climate data store.
        """

        try:
            client = cdsapi.Client(url=self.CDSAPI_URL, key=os.getenv("CDSAPI_KEY"))
        except Exception as err:
            error_msg = f"Unable to connect to Climate Data Store. {err}"
            logger.error(error_msg)
            raise TerrakitValidationError(error_msg)
        return client

    def list_cordex_domains(self) -> Dict[str, Any]:
        """
        List all available CORDEX domains with their information.

        Returns:
            dict: Dictionary of domain codes and their information
        """
        cordex_domains: Dict[str, Any] = self.cordex_domains
        return cordex_domains

    def get_cordex_domain_info(self, domain_code: str) -> dict:
        """
        Get information for a specific CORDEX domain.

        Args:
            domain_code: CORDEX domain code (e.g., 'EUR-11')

        Returns:
            dict: Domain information including name, bbox, and resolution

        Raises:
            TerrakitValueError: If domain code not found
        """
        return get_domain_info(domain_code)

    def list_collections(self) -> list[Any]:
        """
        Lists the available collections.

        Returns:
            list: A list of collection names.
        """
        logger.info("Listing available collections")
        return self.collections

    def list_bands(self, data_collection_name: str) -> list[dict[str, Any]]:
        """
        List available bands for a given collection.

        Parameters:
            data_collection_name (str): The name of the collection to get bands for.

        Returns:
            list[dict[str, Any]]: A list of band dictionaries containing band information.
                Each dictionary contains keys like 'band_name', 'resolution', 'description', etc.

        Raises:
            TerrakitValidationError: If the collection is not found or has no band information.

        Example:
            ```python
            from terrakit import DataConnector
            dc = DataConnector(connector_type="climate_data_store")
            dc = DataConnector(connector_type='climate_data_store')
            bands = dc.connector.list_bands(data_collection_name='derived-era5-single-levels-daily-statistics')
            print(f'\nFound {len(bands)} bands for derived-era5-single-levels-daily-statistics')
            print('\nFirst 3 bands:')
            for band in bands[:3]:
                print(f"  - {band['band_name']}: {band.get('description', 'N/A')}")
            ```
        """
        # Check if collection exists
        check_collection_exists(data_collection_name, self.collections)

        # Find the collection details
        collection_details = None
        for collection in self.collections_details:
            if collection["collection_name"] == data_collection_name:
                collection_details = collection
                break

        if collection_details is None or "bands" not in collection_details:
            raise TerrakitValidationError(
                message=f"No band information found for collection '{data_collection_name}'"
            )

        bands_list: list[dict[str, Any]] = collection_details["bands"]
        logger.info(
            f"Found {len(bands_list)} bands for collection '{data_collection_name}'"
        )
        return bands_list

    def find_data(
        self,
        data_collection_name: str,
        date_start: str,
        date_end: str,
        area_polygon=None,
        bbox=None,
        bands=[],
        maxcc=100,
        data_connector_spec=None,
    ) -> Union[tuple[list[Any], list[dict[str, Any]]], tuple[None, None]]:
        """
        This function retrieves unique dates and corresponding data results from a specified Climate Data Store data collection.

        Args:
            data_collection_name (str): The name of the Climate Data Store data collection to search.
            date_start (str): The start date for the time interval in 'YYYY-MM-DD' format.
            date_end (str): The end date for the time interval in 'YYYY-MM-DD' format.
            area_polygon (Polygon, optional): A polygon defining the area of interest.
            bbox (tuple, optional): A bounding box defining the area of interest in the format (minx, miny, maxx, maxy).
            bands (list, optional): A list of bands to retrieve. Defaults to [].
            maxcc (int, optional): The maximum cloud cover percentage for the data. Default is 100 (no cloud cover filter).
            data_connector_spec (list, optional): A dictionary containing the data connector specification.

        Returns:
            tuple: A tuple containing a sorted list of unique dates and a list of data results.
        """
        if "CDSAPI_KEY" not in os.environ:
            raise TerrakitValidationError(
                message="Error: Missing credentials 'CDSAPI_KEY'. Please update .env with correct credentials."
            )

        # Check data_collection_name exists in self.collections.
        check_collection_exists(data_collection_name, self.collections)

        # Load constraints
        constraints = self._load_constraints(data_collection_name)

        # Validate contsraint parameters using collection name for better errors
        self._validate_temporal(date_start, date_end, constraints, data_collection_name)
        self._validate_spatial(bbox, constraints, data_collection_name)

        # Generate dates

        start = datetime.strptime(date_start, "%Y-%m-%d")
        end = datetime.strptime(date_end, "%Y-%m-%d")

        unique_dates = []
        value = start
        while value <= end:
            unique_dates.append(value.strftime("%Y-%m-%d"))
            value += timedelta(days=1)

        results = [
            {
                "collection": data_collection_name,
                "date_range": f"{date_start} to {date_end}",
                "total_dates": len(unique_dates),
                "temporal_extent": constraints.get("extent", {}).get("temporal"),
                "spatial_extent": constraints.get("extent", {}).get("spatial"),
            }
        ]

        # TODO: filter by cloud cover
        return unique_dates, results

    def get_data(
        self,
        data_collection_name,
        date_start,
        date_end,
        area_polygon=None,
        bbox=None,
        bands=[],
        query_params={},
        data_connector_spec=None,
        save_file=None,
        working_dir=".",
    ) -> Union[xr.DataArray, None]:
        """
        Fetches data from Climate Data Store for the specified collection, date range, area, and bands.

        Args:
            data_collection_name (str): Name of the data collection to fetch data from.
            date_start (str): Start date for the data retrieval (inclusive), in 'YYYY-MM-DD' format.
            date_end (str): End date for the data retrieval (inclusive), in 'YYYY-MM-DD' format.
            area_polygon (list, optional): Polygon defining the area of interest. Defaults to None.
            bbox (list, optional): Bounding box defining the area of interest. Defaults to None.
            bands (list, optional): List of bands to retrieve. Defaults to all bands.
            query_params (dict, optional): Additional query parameters. Defaults to {}.
            data_connector_spec (dict, optional): Data connector specification. Defaults to None.
            save_file (str, optional): Path to save the output file. If provided, individual GeoTIFF files
                will be saved for each date with the naming pattern: {save_file}_{date}.tif
                (e.g., 'output_2025-01-01.tif', 'output_2025-01-02.tif'). Each file contains all
                requested bands for that specific date. If None, no files are saved to disk. Defaults to None.
            working_dir (str, optional): Working directory for temporary files. Defaults to '.'.

        Returns:
            xarray.DataArray: An xarray DataArray containing all fetched data with dimensions (time, band, y, x).
                All dates are stacked along the time dimension, and all bands are stacked along the band dimension.
                If save_file is provided, individual date files are also saved to disk.

        Example:
            ```python
            import terrakit
            data_connector = "climate_data_store"
            dc = terrakit.DataConnector(connector_type=data_connector)
            data = dc.connector.get_data(
                data_collection_name="derived-era5-single-levels-daily-statistics",
                date_start="2025-01-01",
                date_end="2025-01-02",
                bbox=[-1.32, 51.06, -1.30, 51.08],
                bands=["2m_temperature"],
                query_params={
                    "daily_statistic": "daily_minimum",
                    "frequency": "1hr",
                    "time_zone": "utc+03:00"
                    }
                )
                save_file="./derived-era5-single-levels-daily-statistics",
            ```
        """

        # 1. Download zip from CDS API
        zip_path = self._download_from_cds(
            data_collection_name,
            date_start,
            date_end,
            bbox,
            bands,
            query_params,
            working_dir,
        )

        # 2. Extract NetCDF from zip
        extract_dir = Path(working_dir) / "temp_netcdf"
        extract_dir.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_dir)

        # 3. Find NetCDF file(s)
        netcdf_files = list(extract_dir.glob("*.nc"))
        if not netcdf_files:
            raise TerrakitValueError(f"No NetCDF files found in {zip_path}")

        # 4. Load NetCDF and process into DataArray per date
        # CDS may return multiple NetCDF files (one per variable)
        # We need to merge them by date to avoid duplicates

        # First, collect all data organized by date
        date_data_dict: Dict[
            str, Dict[str, xr.DataArray]
        ] = {}  # {date_str: {var_name: DataArray}}

        for netcdf_file in netcdf_files:
            ds = xr.open_dataset(netcdf_file)

            # Determine dimension names
            lon_name = "longitude" if "longitude" in ds.dims else "lon"
            lat_name = "latitude" if "latitude" in ds.dims else "lat"
            time_name = "time" if "time" in ds.dims else "valid_time"

            # Get the main data variable(s) - these are our bands
            data_vars = [
                v for v in ds.data_vars if v not in [lon_name, lat_name, time_name]
            ]

            # Process each time step
            for time_idx in range(len(ds[time_name])):
                # Extract the date for this time step
                time_value = ds[time_name].isel({time_name: time_idx}).values
                date_str = pd.Timestamp(time_value).strftime("%Y-%m-%d")

                # Initialize dict for this date if not exists
                if date_str not in date_data_dict:
                    date_data_dict[date_str] = {}

                # Store each variable for this date
                for var_name in data_vars:
                    # Extract data for this specific time step
                    da_var = ds[var_name].isel({time_name: time_idx})

                    # Add CRS and spatial dimensions
                    da_var = da_var.rio.write_crs("EPSG:4326")
                    da_var = da_var.rio.set_spatial_dims(x_dim=lon_name, y_dim=lat_name)

                    # Store in dict
                    date_data_dict[date_str][var_name] = da_var

            ds.close()

        # Now process each unique date
        da_list = []
        for date_str in sorted(date_data_dict.keys()):
            data_date_datetime = datetime.strptime(date_str, "%Y-%m-%d")
            var_dict = date_data_dict[date_str]

            # Collect all variables for this date
            band_arrays = []
            band_names = []
            for var_name in sorted(var_dict.keys()):
                da_var = var_dict[var_name]
                # Expand dims to add band dimension
                da_var = da_var.expand_dims(dim="band")
                band_arrays.append(da_var)
                band_names.append(var_name)

            # Concatenate all bands for this time step
            da_time = xr.concat(band_arrays, dim="band")
            da_time = da_time.assign_coords({"band": band_names})

            # Expand time dimension and assign time coordinate
            da_time = da_time.expand_dims(dim="time")
            da_time = da_time.assign_coords({"time": [data_date_datetime]})

            # Save individual date file if save_file is provided
            if save_file is not None:
                usave_file = save_file.replace(".tif", f"_{date_str}.tif")
                save_data_array_to_file(da_time, usave_file)

            # Add this date's data to the list for final concatenation
            da_list.append(da_time)

        # 5. Concatenate all time steps into final DataArray
        da = xr.concat(da_list, dim="time", join="override", combine_attrs="override")

        # # 6. Save final concatenated file (optional, for the full time series)
        # save_data_array_to_file(da, save_file)

        # 7. Cleanup temporary files
        shutil.rmtree(extract_dir)
        Path(zip_path).unlink()

        logger.info(f"Processed {len(da_list)} time steps into DataArray")
        return da
