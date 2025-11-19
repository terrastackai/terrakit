# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


# Assisted by watsonx Code Assistant

import dask.diagnostics
import logging
import numpy as np
import pystac_client
import stackstac
import xarray as xr
import json

from datetime import datetime
from rasterio.errors import RasterioIOError
from typing import Any, Union


from ..geodata_utils import (
    load_and_list_collections,
    check_bands,
    save_data_array_to_file,
)
from ..connector import Connector
from ...general_utils.exceptions import (
    TerrakitValueError,
    TerrakitBaseException,
)
from terrakit.validate.helpers import (
    check_collection_exists,
    check_start_end_date,
    check_bbox,
    check_area_polygon,
)


logger = logging.getLogger(__name__)

######################################################################################################
###  Supporting functions
######################################################################################################


def npdatetime_to_datetime(date):
    """
    Converts a numpy datetime64 object to a python datetime object

    Args:
      date (np.datetime): a np.datetime64 object

    Return:
      DATE - a python datetime object
    """
    timestamp = (date - np.datetime64("1970-01-01T00:00:00")) / np.timedelta64(1, "s")
    return datetime.utcfromtimestamp(timestamp)


def stac_get_items(
    stac_url: str,
    bbox: list,
    from_datetime: str,
    to_datetime: str,
    collections: list = ["HLSS30.v2.0", "HLSL30.v2.0"],
    fields="{}",
):
    """
    Retrieves STAC (SpatioTemporal Asset Catalog) items from a given URL within a specified bounding box and time range.

    Args:
        stac_url (str): The URL of the STAC catalog.
        bbox (list): A list representing the bounding box (left, bottom, right, top) in the format of (west, south, east, north).
        from_datetime (str): The start date and time in a format compatible with the STAC API (e.g., '2021-01-01T00:00:00Z').
        to_datetime (str): The end date and time in a format compatible with the STAC API (e.g., '2021-12-31T23:59:59Z').
        collections (list): A list of collections to search within. Default is ["HLSS30.v2.0", "HLSL30.v2.0"].
        limit (int): The maximum number of items to return. Default is 250.

    Returns:
        pystac_client ItemCollection: A list of STAC items matching the query parameters.
    """
    # Initialize the PySTAC client
    catalog = pystac_client.Client.open(stac_url)

    # Perform the search using the provided parameters
    fields = json.loads(fields)

    stac_items = catalog.search(
        bbox=bbox,
        collections=collections,
        datetime=f"{from_datetime}/{to_datetime}",
        fields=fields,
        limit=100,
    ).item_collection()

    return stac_items


def find_items(
    stac_url: str,
    bbox: list,
    from_datetime: str,
    to_datetime: str,
    bands=None,
    collections=["HLSS30.v2.0", "HLSL30.v2.0"],
    limit=250,
    maxcc: float = 100,
    data_connector_spec=None,
    fields: str = "{}",
):
    """
    Find unique dates and STAC items within a specified bounding box, date range, and collections.

    Parameters:
        stac_url (str): The URL of the STAC API.
        bbox (list): Bounding box in the format (west, south, east, north).
        from_datetime (str): Start date in 'YYYY-MM-DD' format.
        to_datetime (str): End date in 'YYYY-MM-DD' format.
        bands (list, optional): List of bands to filter by. Defaults to None.
        collections (list, optional): List of collections to filter by. Defaults to ["HLSS30.v2.0", "HLSL30.v2.0"].
        limit (int, optional): Maximum number of items to return. Defaults to 250.

    Returns:
        tuple: A tuple containing a list of unique dates and a list of STAC items.
    """

    # Find STAC items and stacked data
    stac_items, stack = find_sh_aws_stac_items(
        stac_url,
        bbox,
        from_datetime,
        to_datetime,
        bands,
        collections,
        maxcc=maxcc,
        data_connector_spec=data_connector_spec,
        fields=fields,
    )
    if len(stack.time.values) == 0:
        logger.error(f"No data found: {stack.time}")
        unique_dates: list = []
        return unique_dates, stac_items
    # Extract unique dates from stack time values
    if isinstance(stack.time.values[0], str):
        # If time values are strings, assume format 'YYYY-MM-DD'
        unique_dates = sorted(list(set([X.split("T")[0] for X in stack.time.values])))
    elif isinstance(stack.time.values[0], np.datetime64):
        # If time values are numpy datetime64, convert to datetime and extract date
        unique_dates = sorted(
            list(
                set(
                    [
                        npdatetime_to_datetime(X).date().strftime("%Y-%m-%d")
                        for X in stack.time.values
                    ]
                )
            )
        )

    return unique_dates, stac_items


def find_sh_aws_stac_items(
    stac_url: str,
    bbox: list,
    from_datetime: str,
    to_datetime: str,
    bands: list = [],
    collections: list = ["HLSS30.v2.0", "HLSL30.v2.0"],
    limit: int = 250,
    maxcc: float = 100,
    data_connector_spec=None,
    fields: str = "{}",
) -> tuple:
    """
    Fetches STAC (SpatioTemporal Asset Catalog) items from a given URL within a specified bounding box,
    date range, and collections. Stacks the items based on specified bands if available.

    Args:
        stac_url (str): The URL of the STAC server.
        bbox (list): A list representing the bounding box (minx, miny, maxx, maxy).
        from_datetime (str): Start date in 'YYYY-MM-DD' format.
        to_datetime (str): Start date in 'YYYY-MM-DD' format.
        bands (list, optional): A list of asset bands to stack. If None, no stacking is performed. Defaults to [].
        collections (list, optional): A list of collections to fetch. Defaults to ["HLSS30.v2.0", "HLSL30.v2.0"].
        limit (int, optional): The maximum number of items to fetch. Defaults to 250.

    Returns:
        tuple: A tuple containing the list of STAC items and the stacked items (if bands are specified).
    """
    stac_items = stac_get_items(
        stac_url,
        bbox,
        from_datetime,
        to_datetime,
        collections,
        fields=fields,
    )

    if len(stac_items.items) == 0:
        err_msg = f"No items found for query parameters: bbox={bbox}, start_date={from_datetime}, end_date={to_datetime}, collection={collections}, fields={fields}."
        logger.warning(err_msg)
        raise TerrakitValueError(err_msg)

    if maxcc:
        stac_items = [
            item for item in stac_items if item.properties.get("eo:cloud_cover") < maxcc
        ]
        if len(stac_items) == 0:
            err_msg = f"After filtering for cloud cover, no items were found. 'max_cc' set to {maxcc}. Consider increasing the maximum allowed cloud cover."
            logger.warning(err_msg)
            raise TerrakitValueError(err_msg)

    if bands:
        stack = stackstac.stack(
            stac_items, epsg=4326, assets=bands, bounds_latlon=bbox, sortby_date="asc"
        )
    elif "sentinel-2-l2a" in collections and len(collections) == 1:
        # when no bands are supplied use all bands for s2_l2a
        bands = [
            "coastal",
            "blue",
            "green",
            "red",
            "rededge1",
            "rededge2",
            "rededge3",
            "nir",
            "nir08",
            "nir09",
            "swir16",
            "swir22",
            "scl",
        ]
        stack = stackstac.stack(
            stac_items, epsg=4326, assets=bands, bounds_latlon=bbox, sortby_date="asc"
        )
    else:
        stack = stackstac.stack(
            stac_items, epsg=4326, bounds_latlon=bbox, sortby_date="asc"
        )
    return stac_items, stack


def get_sh_aws_data(
    stac_url,
    bbox,
    from_datetime,
    to_datetime,
    bands=["B02", "B03", "B04"],
    collections=["HLSS30.v2.0", "HLSL30.v2.0"],
    limit=250,
    maxcc=None,
    data_connector_spec=None,
    fields: str = "{}",
):
    # Fetch STAC items from the provided URL within the given bounding box and date range
    stac_items = stac_get_items(
        stac_url, bbox, from_datetime, to_datetime, collections, fields=fields
    )

    stack = stackstac.stack(
        stac_items,
        assets=bands,
        epsg=4326,
        bounds_latlon=bbox,
        rescale=False,
        sortby_date="asc",
    )
    stack = stackstac.mosaic(stack, dim="time")

    # Compute the stacked datasets
    try:
        with dask.diagnostics.ProgressBar():
            data = stack.compute()
    except RasterioIOError as e:
        err_msg = f"An error occurred opening a remote link: {e}"
        logger.error(err_msg)
        raise TerrakitBaseException(err_msg, e)

    return data


######################################################################################################
###  Connector class
######################################################################################################


class Sentinel_AWS(Connector):
    """
    Class for interacting with Sentinel AWS data via STAC API.

    Attributes:
        connector_type (str): Type of data connector, always "sentinel_aws".
        stac_url (str): Base URL for the STAC API.
        collections (list): List of available collections.
        collections_details (dict): Detailed information about collections.
    """

    def __init__(self):
        """
        Initialize Sentinel_AWS class with default attributes.
        """
        self.connector_type = "sentinel_aws"
        self.stac_url = "https://earth-search.aws.element84.com/v1/"
        self.collections: list[Any] = load_and_list_collections(
            connector_type="sentinel_aws"
        )
        self.collections_details = load_and_list_collections(
            as_json=True, connector_type="sentinel_aws"
        )

    def list_collections(self) -> list[Any]:
        """
        List available collections.

        Returns:
            list: List of available collections.
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
        Find Sentinel AWS data based on given parameters.

        Args:
            data_collection_name (str): Name of the data collection.
            date_start (str): Start date in 'YYYY-MM-DD' format.
            date_end (str): End date in 'YYYY-MM-DD' format.
            bands (list): List of bands to fetch.
            area_polygon (list, optional): Polygon defining the area of interest.
            bbox (list, optional): Bounding box defining the area of interest.
            maxcc (int, optional): Maximum cloud cover percentage.
            data_connector_spec (dict, optional): Additional data connector specifications.

        Returns:
            tuple: A tuple containing unique dates and STAC items.
        """
        logger.info("Listing Sentinel AWS data")

        check_collection_exists(data_collection_name, self.collections)

        check_start_end_date(date_start=date_start, date_end=date_end)
        check_area_polygon(
            area_polygon=area_polygon, connector_type=self.connector_type
        )
        check_bbox(bbox=bbox, connector_type=self.connector_type)

        collection_detials = self._get_collection_info(data_collection_name)
        fields = self._get_search_fields(collection_detials)

        try:
            unique_dates, stac_items = find_items(
                self.stac_url,
                bbox,
                date_start,
                date_end,
                bands=bands,
                collections=[data_collection_name],
                limit=250,
                maxcc=maxcc,
                data_connector_spec=data_connector_spec,
                fields=fields,
            )

        except ValueError as e:
            error_msg = f"Unable to find data for collection '{data_collection_name}. This could be due to the parameters set:\n\t bbox={bbox}, start_date={date_start}, end_date={date_end}, collection={data_collection_name}, fields={fields}, max_cc={maxcc}."
            logger.exception(error_msg)
            raise TerrakitValueError(error_msg) from e

        stac_items = [
            {"id": item.id, "properties": item.properties} for item in stac_items
        ]

        return unique_dates, stac_items

    def get_data(
        self,
        data_collection_name,
        date_start,
        date_end,
        area_polygon=None,
        bbox=None,
        bands=[],
        maxcc=100,
        data_connector_spec=None,
        save_file=None,
        working_dir=".",
    ) -> Union[xr.DataArray, None]:
        """
        Get Sentinel AWS data based on given parameters.

        Args:
            data_collection_name (str): Name of the data collection.
            date_start (str): Start date in 'YYYY-MM-DD' format.
            date_end (str): End date in 'YYYY-MM-DD' format.
            area_polygon (list, optional): Polygon defining the area of interest.
            bbox (list, optional): Bounding box defining the area of interest.
            bands (list, optional): List of bands to retrieve.
            maxcc (int, optional): Maximum cloud cover percentage.
            data_connector_spec (dict, optional): Additional data connector specifications.
            save_file (str, optional): Path to save the data.
            working_dir (str, optional): Working directory for saving files.

        Returns:
            xarray: An xarray Datasets containing the fetched data with dimensions (time, band, y, x).
        """
        check_collection_exists(data_collection_name, self.collections)
        # Check that the bands the user has requested exist in the data collection
        check_bands(
            connector_type=self.connector_type,
            collection_name=data_collection_name,
            bands=bands,
        )

        if data_connector_spec is None:
            data_connector_spec_list = [
                X
                for X in self.collections_details
                if X["collection_name"] == data_collection_name
            ]
            if len(data_connector_spec_list) == 0:
                error_msg = (
                    f"Unable to find collection details for '{data_collection_name}'"
                )
                logger.error(error_msg)
                raise TerrakitValueError(error_msg)
            data_connector_spec = data_connector_spec_list[0]

        try:
            unique_dates, results = self.find_data(
                data_collection_name=data_collection_name,
                date_start=date_start,
                date_end=date_end,
                bbox=bbox,
                bands=bands,
                maxcc=maxcc,
                data_connector_spec=data_connector_spec,
            )
        except TerrakitValueError as e:
            raise e

        da_list: list[Any] = []
        for date in unique_dates:  # type: ignore[union-attr]
            da: xr.DataArray = get_sh_aws_data(
                self.stac_url,
                bbox,
                date_start,
                date_end,
                bands=bands,
                collections=[data_collection_name],
                limit=250,
                maxcc=maxcc,
                data_connector_spec=data_connector_spec,
            )
            date_time_stamp = datetime.strptime(date, "%Y-%m-%d")
            da = da.assign_coords({"band": bands, "time": date_time_stamp})
            da_list.append(da)

        da = xr.concat(da_list, dim="time")
        save_data_array_to_file(da, save_file)

        return da

    def _get_collection_info(self, collection_name) -> dict[str, Any]:
        collection_info = {}
        for i, collections_details in enumerate(self.collections_details):
            if collections_details["collection_name"] == collection_name:
                collection_info = self.collections_details[i]
        return collection_info

    def _get_search_fields(self, collection_info: dict[str, Any]) -> str:
        fields = "{}"
        if "search" in collection_info:
            if "fields" in collection_info["search"]:
                fields = collection_info["search"]["fields"]
        if type(fields) is not str:
            err_msg = f"'fields' value in collections.json must be a str, not {type(fields)}: {fields}"
            raise TerrakitValueError(err_msg)
        return fields
