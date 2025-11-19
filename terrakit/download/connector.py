# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


import xarray as xr

from abc import ABC, abstractmethod
from typing import Any, Union


class Connector(ABC):
    """
    An abstract base class for all connectors.
    This class insists that any subclass must have a list_collections(), find_data()
    and get_data() method.

    Attributes:
        None

    Methods:
        list_collections: Returns a list of available data collections.
        find_data: Finds data within specified parameters and returns a list of unique dates and relevant metadata.
        get_data: Retrieves data based on given parameters and saves to file.
    """

    @abstractmethod
    def list_collections(self) -> list[Any]:
        """
        Returns a list of available data collections.

        Returns:
            list[Any]: List of available data collection names.
        """
        pass

    @abstractmethod
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
        Finds data within specified parameters and returns relevant metadata.

        Args:
            data_collection_name (str): The name of the data collection to search.
            date_start (str): The start date for the data retrieval.
            date_end (str): The end date for the data retrieval.
            area_polygon (Optional[Any]): Polygon defining the area of interest. Either specify area_polygon or bbox.
            bbox (Optional[Any]): Bounding box defining the area of interest. Either specify area_polygon or bbox.
            bands (list[str]): List of bands to retrieve.
            maxcc (int): Maximum cloud cover percentage.
            data_connector_spec (Optional[Any]): Additional specifications for the data connector.

        Returns:
            Union[tuple[list[Any], list[dict[str, Any]]], tuple[None, None]]: A tuple containing a list of data identifiers and a list of metadata dictionaries, or (None, None) if no data is found.
        """
        pass

    @abstractmethod
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
        Retrieves data based on given parameters and optional saving to file.

        Args:
            data_collection_name (str): The name of the data collection to retrieve.
            date_start (str): The start date for data retrieval.
            date_end (str): The end date for data retrieval.
            area_polygon (Optional[Any]): Polygon defining the area of interest.
            bbox (Optional[Any]): Bounding box defining the area of interest.
            bands (list[str]): List of bands to retrieve.
            maxcc (int): Maximum cloud cover percentage.
            data_connector_spec (Optional[Any]): Additional specifications for the data connector.
            save_file (Optional[str]): Path to save the retrieved data file.
            working_dir (str): Working directory for saving the file.

        Returns:
            Union[xr.DataArray, None]: The retrieved xarray DataArray or None if no data is found.
        """
        pass
