# © Copyright IBM Corporation 2026
# SPDX-License-Identifier: Apache-2.0


import cdsapi
import json
import logging
import os
import xarray as xr
from datetime import datetime, timedelta
from shapely.geometry import box
from typing import Any, Dict, Union
from pathlib import Path

from ..connector import Connector
from ..geodata_utils import (
    load_and_list_collections,
)
from terrakit.general_utils.exceptions import (
    TerrakitValidationError,
)
from terrakit.validate.helpers import (
    check_collection_exists,
    check_date_format,
    check_start_end_date_in_correct_order,
    basic_bbox_validation,
)
from .cds_utils.cordex_utils import CORDEX_DOMAINS, get_domain_info


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
        from .cds_utils.cordex_utils import find_matching_domains

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

    def list_collections(self) -> list[Any]:
        """
        Lists the available collections.

        Returns:
            list: A list of collection names.
        """
        logger.info("Listing available collections")
        return self.collections

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
    ):
        """
        Fetches data from <new_connector> for the specified collection, date range, area, and bands.

        Args:
            data_collection_name (str): Name of the data collection to fetch data from.
            date_start (str): Start date for the data retrieval (inclusive), in 'YYYY-MM-DD' format.
            date_end (str): End date for the data retrieval (inclusive), in 'YYYY-MM-DD' format.
            area_polygon (list, optional): Polygon defining the area of interest. Defaults to None.
            bbox (list, optional): Bounding box defining the area of interest. Defaults to None.
            bands (list, optional): List of bands to retrieve. Defaults to all bands.
            maxcc (int, optional): Maximum cloud cover threshold (0-100). Defaults to 100.
            data_connector_spec (dict, optional): Data connector specification. Defaults to None.
            save_file (str, optional): Path to save the output file. Defaults to None.
            working_dir (str, optional): Working directory for temporary files. Defaults to '.'.

        Returns:
            xarray: An xarray Datasets containing the fetched data with dimensions (time, band, y, x).
        """

        # cds_client = self._connect_to_cds()

        # da = xr.DataArray()

        # First construct the query
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
        #     "day": ["01", "02", "03", "04"],
        #     "time_zone": "utc+00:00",
        #     "area": [90, -180, -90, 180],
        #     "daily_statistic": "daily_mean",
        #     "frequency": "6_hourly",
        #     "format": "netcdf",
        # }

        # #
        # cds = cdsapi.Client()
        # downloaded_filename = cds.retrieve(data_collection_name, request).download()

        da = xr.DataArray()
        return da


#### If licenses have not been accepted.
# HTTPError: 403 Client Error: Forbidden for url: https://cds.climate.copernicus.eu/api/retrieve/v1/processes/derived-era5-single-levels-daily-statistics/execution
# required licences not accepted
# Not all the required licences have been accepted; please visit https://cds.climate.copernicus.eu/datasets/derived-era5-single-levels-daily-statistics?tab=download#manage-licences to accept the required licence(s).


# https://object-store.os-api.cci2.ecmwf.int/cci2-prod-catalogue/resources/derived-era5-single-levels-daily-statistics/constraints_a0b1ad068fce54320fa1350cc56b8692d66b395cf066d0c8bbf1579816e074b8.json
# 532 entries
# dict_keys(['daily_statistic', 'day', 'frequency', 'month', 'product_type', 'time_zone', 'variable', 'year'])
