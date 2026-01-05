# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


import logging
import os
from pathlib import Path
from typing import Any, Union
import requests

from terrakit.general_utils.exceptions import (
    TerrakitMissingEnvironmentVariable,
    TerrakitValueError,
)

from ..raster_file_reader import NetCDFFileReader
from ..connector import Connector
from ..geodata_utils import (
    save_data_array_as_netcdf,
    save_data_array_to_file,
)
import pandas as pd
from ...general_utils.rest import post, get

logger = logging.getLogger(__name__)

######################################################################################################
###  Supporting functions
######################################################################################################


######################################################################################################
###  Connector class
######################################################################################################

IBM_RESEARCH_FMAAS_STAC_URL = (
    "https://stac-fastapi-pgstac-geospatial-be.apps.fmaas-backend.fmaas.res.ibm.com"
)
IBM_RESEARCH_CE_STAC_URL = (
    "https://oauth2.13lx5kr6jri2.us-south.codeengine.appdomain.cloud"
)


class IBMResearchSTAC(Connector):
    """
    Attributes:
        connector_type (str): Name of connector
        collections (list): A list of available collections.
        collections_details (list): Detailed information about the collections.
    """

    DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
    MEDIA_TYPES = ["application/x-netcdf", "application/netcdf"]

    def __init__(self):
        """
        Initializes the IBM Research STAC Connector.

        Attributes:
            _access_token (str): The access token for authentication.
            stac_url (str): The base URL for the STAC API.
            connector_type (str): The type of connector, which is "IBMResearchSTAC".
            collections (list[str]): A list of collection IDs available on the STAC server.
        """
        url = os.getenv("IBMRESEARCH_STAC_URL", IBM_RESEARCH_CE_STAC_URL)
        self._access_token: str | None = None
        if url.endswith("/"):
            url = url[:-1]
        self.stac_url = url
        self.connector_type: str = "IBMResearchSTAC"
        collection_details = self._get_all_collections()
        self.collections: list[str] = [coll["id"] for coll in collection_details]

    @property
    def headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}

        if self.stac_url == IBM_RESEARCH_CE_STAC_URL:
            if self.access_token is None:
                token = self._get_token()
                self.access_token = token

            headers["Authorization"] = f"Bearer {self.access_token}"

        return headers

    @property
    def access_token(self) -> str | None:
        # if token has not been obtained, get the access token

        return self._access_token

    @access_token.setter
    def access_token(self, token: str):
        if isinstance(token, str):
            self._access_token = token

    @staticmethod
    def _validate_dates(start: str, end: str):
        assert pd.Timestamp(start) < pd.Timestamp(end)

    @staticmethod
    def _validate_bbox(bbox: tuple[float, float, float, float]):
        """
        Validate the bounding box coordinates to ensure they are within valid geographic range.

        Parameters:
            bbox (tuple[float, float, float, float]): A tuple containing (west, south, east, north) coordinates.

        Raises:
            AssertionError: If the provided coordinates are not within the valid geographic range.
        """
        west, south, east, north = bbox
        assert -180 <= west < east <= 180, f"Error! invalid values: {west=} {east=}"
        assert -90 <= south < north <= 90, f"Error! invalid values: {south=} {north=}"

    def _get_token(self) -> str:
        """this function is designed to retrieve an access token for authentication using the
        IBM AppID service

        Returns:
            str: access token
        """
        for var in [
            "APPID_ISSUER",
            "CLIENT_ID",
            "CLIENT_SECRET",
            "APPID_USERNAME",
            "APPID_PASSWORD",
        ]:
            if var not in os.environ:
                link = "https://github.com/terrastackai/terrakit?tab=readme-ov-file#ibm-research-stac"
                msg = f"Error! {var} is not set. Please check {link}"
                logger.error(msg)
                raise TerrakitMissingEnvironmentVariable(message=msg)
        appid_issuer: str = os.environ["APPID_ISSUER"]
        assert isinstance(appid_issuer, str), f"Error! Invalid type: {appid_issuer=}"
        if appid_issuer.endswith("/"):
            token_url = f"{appid_issuer}token"
        else:
            token_url = f"{appid_issuer}/token"
        client_id = os.environ["CLIENT_ID"]
        client_secret = os.environ["CLIENT_SECRET"]
        username = os.environ["APPID_USERNAME"]
        password = os.environ["APPID_PASSWORD"]
        token_url = f"{appid_issuer}/token"

        payload = {
            "grant_type": "password",
            "username": username,
            "password": password,
            "client_id": client_id,
            "client_secret": client_secret,
        }
        headers = {"accept": "application/json", "content-type": "application/json"}
        response = post(
            url=token_url,
            headers=headers,
            payload=payload,
        )
        response.raise_for_status()
        token: str = response.json()["access_token"]
        if not isinstance(token, str):
            raise ValueError(f"Error! Unexpected value: {token=}")
        return token

    def _search_items(
        self,
        data_collection_name: str,
        date_start: str,
        date_end: str,
        bbox: tuple,
        bands: list[str],
    ) -> list[dict[str, Any]]:
        """
        Searches for items within a specified date range and bounding box.

        This method constructs a search query for items in a given data collection,
        within a specified bounding box and date range. It uses the client's search
        functionality to retrieve the results.

        Parameters:
            data_collection_name (str): The name of the data collection to search.
            date_start (str): The start date for the search in 'YYYY-MM-DD' format.
            date_end (str): The end date for the search in 'YYYY-MM-DD' format.
            bbox (tuple): A tuple representing the bounding box (minx, miny, maxx, maxy).
            bands (list[str]): list of band names
        Returns:
            list[dict[str, Any]]: The search results containing matching items.
        """
        logger.info(
            f"Search items: {data_collection_name=} {date_start=} {date_end=} {bbox=} {bands=}"
        )
        start_dt = pd.Timestamp(date_start).strftime(IBMResearchSTAC.DATETIME_FORMAT)
        end_dt = pd.Timestamp(date_end).strftime(IBMResearchSTAC.DATETIME_FORMAT)
        dt = f"{start_dt}/{end_dt}"

        search_url = f"{self.stac_url}/search"
        data = {
            "collections": [data_collection_name],
            "bbox": list(bbox),
            "datetime": dt,
        }
        logger.info(f"Querying STAC: {search_url} {data=}")
        resp: requests.Response = post(
            url=search_url, headers=self.headers, payload=data
        )
        feature_collection = resp.json()
        all_items: list[dict] = feature_collection["features"]
        if len(all_items) > 0:
            # filter out items that do not have specified band
            if len(bands) > 0:
                items = list()
                for i in all_items:
                    cube_variables: dict[str, Any] = i["properties"]["cube:variables"]
                    available_variables = list(cube_variables.keys())
                    if any(band in available_variables for band in bands):
                        items.append(i)

                return items
            else:
                return all_items
        else:
            msg = (
                "Error! No data has been found for the specified parameters:"
                f"{data_collection_name=} {bbox=} {date_start=} {date_end=} {bands=}"
            )
            raise TerrakitValueError(message=msg)

    def _get_all_collections(self) -> list[dict]:
        """
        Fetch all collections from the STAC API.

        Returns:
            list: A list of dictionaries, each representing a collection.
        """
        url = f"{self.stac_url}/collections"
        resp = get(url=url, headers=self.headers)
        resp.raise_for_status()
        collections_dict = resp.json()
        collections: list = collections_dict["collections"]
        if not isinstance(collections, list):
            raise ValueError(f"Error! not a list: {collections=}")
        supported_collections = [c for c in collections if self._is_supported(c["id"])]
        return supported_collections

    def _is_supported(self, collection_name: str) -> bool:
        """
        Check if the given collection supports any of the predefined media types.

        This method sends a GET request to the STAC API to fetch items from the specified collection.
        It then checks if any of the assets in the first item have a 'data' role and if the asset type
        is one of the predefined media types.

        Parameters:
            collection_name (str): The name of the collection to check.

        Returns:
            bool: 'True' if the collection supports any of the predefined media types, 'False' otherwise.
        """
        url = f"{self.stac_url}/collections/{collection_name}/items"
        resp = get(url=url, headers=self.headers, params={"limit": 1})
        resp.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)
        response = resp.json()
        features = response.get(
            "features", []
        )  # Use .get() to avoid KeyError if "features" is not present
        if (
            features and isinstance(features, list) and features
        ):  # Check if features is not empty
            item = features[0]
            assets: dict = item.get(
                "assets", {}
            )  # Use .get() to avoid KeyError if "assets" is not present
            for asset in assets.values():
                if "roles" in asset and "data" in asset["roles"]:
                    media_type: str = asset.get("type")
                    if (
                        media_type.lower() in IBMResearchSTAC.MEDIA_TYPES
                    ):  # Assuming MEDIA_TYPES is a class attribute
                        return True
        return False

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
        This function retrieves unique dates and corresponding data results from a specified Sentinel Hub data collection.

        Parameters:
            data_collection_name (str): The name of the Sentinel Hub data collection to search.
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
        # validate user's input
        IBMResearchSTAC._validate_dates(start=date_start, end=date_end)
        # convert bbox to tuple if necessary
        if bbox is not None and not isinstance(bbox, tuple):
            bbox = tuple(bbox)
            # validate bbox
            IBMResearchSTAC._validate_bbox(bbox)
        # check if collection is supported
        if data_collection_name not in [c for c in self.collections]:
            msg = f"Error! {data_collection_name=} is not supported"
            logger.error(msg)
            raise ValueError(msg)

        # search items using STAC API
        items_as_dicts = self._search_items(
            data_collection_name=data_collection_name,
            date_start=date_start,
            date_end=date_end,
            bbox=bbox,
            bands=bands,
        )
        unique_dates: set = set()
        results: list[dict[str, Any]] = list()
        item_dict: dict
        for item_dict in items_as_dicts:
            item_properties: dict = item_dict["properties"]
            if item_properties.get("datetime") is not None:
                ts = pd.Timestamp(item_properties["datetime"])
            else:
                ts = pd.Timestamp(item_properties["start_datetime"])

            date_str = ts.date().isoformat()
            unique_dates.add(date_str)
            results.append(item_dict)
        return sorted(list(unique_dates)), results

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
    ):
        """
        Fetches data from SentinelHub for the specified collection, date range, area, and bands.

        Parameters:
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
        if bbox is not None and not isinstance(bbox, tuple):
            bbox = tuple(bbox)
            IBMResearchSTAC._validate_bbox(bbox)
        logger.debug(
            f"Fetching data from {data_collection_name} {bbox=} {date_start=} {date_end=}"
        )
        items_as_dicts = self._search_items(
            data_collection_name=data_collection_name,
            date_start=date_start,
            date_end=date_end,
            bbox=bbox,
            bands=bands,
        )

        start_dt = pd.Timestamp(date_start).to_pydatetime()
        end_dt = pd.Timestamp(date_end).to_pydatetime()
        temporal_extent = (start_dt, end_dt)
        file_reader = NetCDFFileReader(
            items=items_as_dicts,
            bbox=bbox,
            temporal_extent=temporal_extent,
            bands=bands,
            properties=None,
        )
        # extract EPSG from STAC item
        # assumption: all items of this collection have the same CRS
        item_as_dict: dict = items_as_dicts[0]
        cube_dimensions = item_as_dict["properties"]["cube:dimensions"]
        epsg: int | None = None
        for cube_dim in cube_dimensions.values():
            # find spatial cube dims
            if cube_dim["type"] == "spatial":
                epsg: int = cube_dim["reference_system"]
                break

        data = file_reader.load_items()
        # persist data
        if save_file is not None:
            extension = Path(save_file).suffix
            match extension:
                case ".tif":
                    save_data_array_to_file(da=data, save_file=save_file)
                case ".nc":
                    save_data_array_as_netcdf(da=data, save_file=save_file, epsg=epsg)
                case _:  # Default case (wildcard)
                    # Code to execute if no other pattern matches
                    raise ValueError(f"Error! Invalid extension: {extension}")
        return data
