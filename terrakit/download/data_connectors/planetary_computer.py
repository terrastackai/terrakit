# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


import logging
import os
from pathlib import Path
from typing import Any, Union
from datetime import datetime

import pandas as pd
import xarray as xr
import numpy as np
from pystac_client import Client
import planetary_computer
import stackstac

from terrakit.general_utils.exceptions import (
    TerrakitValueError,
)

from ..connector import Connector
from ..geodata_utils import (
    load_and_list_collections,
    save_data_array_as_netcdf,
    save_data_array_to_file,
)

logger = logging.getLogger(__name__)

######################################################################################################
###  Supporting functions
######################################################################################################


def _filter_items_by_cloud_cover(items: list, maxcc: int) -> list:
    """
    Filter STAC items by cloud cover percentage.
    
    Args:
        items (list): List of STAC items
        maxcc (int): Maximum cloud cover percentage (0-100)
        
    Returns:
        list: Filtered list of items
    """
    if maxcc >= 100:
        return items
    
    filtered = []
    for item in items:
        cloud_cover = item.properties.get("eo:cloud_cover", 0)
        if cloud_cover <= maxcc:
            filtered.append(item)
    
    return filtered


def _sign_item_assets(item):
    """
    Sign all assets in a STAC item for authenticated access.
    
    Args:
        item: STAC item to sign
        
    Returns:
        Signed STAC item
    """
    return planetary_computer.sign(item)


######################################################################################################
###  Connector class
######################################################################################################

PLANETARY_COMPUTER_STAC_URL = "https://planetarycomputer.microsoft.com/api/stac/v1"


class PlanetaryComputer(Connector):
    """
    A connector for Microsoft Planetary Computer data access.
    
    This connector provides access to geospatial datasets hosted on Microsoft's
    Planetary Computer platform via their STAC API. It supports searching and
    retrieving data from various collections including Sentinel-2, Landsat, and more.
    
    Attributes:
        connector_type (str): Name of connector ("planetary_computer")
        collections (list): A list of available collections
        collections_details (list): Detailed information about the collections
        stac_url (str): The STAC API endpoint URL
        client: STAC client for API interactions
        
    Example:
        ```python
        from terrakit import DataConnector
        
        dc = DataConnector(connector_type="planetary_computer")
        collections = dc.connector.list_collections()
        
        # Find available data
        dates, results = dc.connector.find_data(
            data_collection_name="sentinel-2-l2a",
            date_start="2024-01-01",
            date_end="2024-01-31",
            bbox=(-122.5, 37.7, -122.3, 37.9),
            maxcc=20
        )
        
        # Get data
        data = dc.connector.get_data(
            data_collection_name="sentinel-2-l2a",
            date_start="2024-01-01",
            date_end="2024-01-31",
            bbox=(-122.5, 37.7, -122.3, 37.9),
            bands=["B04", "B03", "B02"],
            maxcc=20,
            save_file="output.tif"
        )
        ```
    """

    DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

    def __init__(self):
        """
        Initialize the Planetary Computer connector.
        
        Sets up the STAC client and loads available collections from the
        collections.json configuration file.
        """
        self.connector_type: str = "planetary_computer"
        self.stac_url = os.getenv(
            "PLANETARY_COMPUTER_STAC_URL", 
            PLANETARY_COMPUTER_STAC_URL
        )
        
        # Initialize STAC client
        self.client = Client.open(
            self.stac_url,
            modifier=planetary_computer.sign_inplace,
        )
        
        # Load collections from configuration
        self.collections: list[Any] = load_and_list_collections(
            connector_type=self.connector_type
        )
        self.collections_details: list[Any] = load_and_list_collections(
            as_json=True, connector_type=self.connector_type
        )
        
        logger.info(f"Initialized Planetary Computer connector with {len(self.collections)} collections")

    @staticmethod
    def _validate_dates(start: str, end: str):
        """Validate that start date is before end date."""
        if pd.Timestamp(start) >= pd.Timestamp(end):
            raise TerrakitValueError(
                f"Start date must be before end date: {start} >= {end}"
            )

    @staticmethod
    def _validate_bbox(bbox: tuple[float, float, float, float]):
        """
        Validate the bounding box coordinates.
        
        Args:
            bbox (tuple): A tuple containing (west, south, east, north) coordinates
            
        Raises:
            TerrakitValueError: If coordinates are invalid
        """
        west, south, east, north = bbox
        if not (-180 <= west < east <= 180):
            raise TerrakitValueError(
                f"Invalid longitude values: west={west}, east={east}"
            )
        if not (-90 <= south < north <= 90):
            raise TerrakitValueError(
                f"Invalid latitude values: south={south}, north={north}"
            )

    def list_collections(self) -> list[Any]:
        """
        Lists the available collections.
        
        Returns:
            list: A list of collection names available through this connector
        """
        logger.info("Listing available Planetary Computer collections")
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
        Find available data matching the specified criteria.
        
        Searches the Planetary Computer STAC catalog for items matching the
        collection, temporal range, spatial extent, and cloud cover criteria.
        
        Args:
            data_collection_name (str): The name of the data collection (e.g., "sentinel-2-l2a")
            date_start (str): Start date in 'YYYY-MM-DD' format
            date_end (str): End date in 'YYYY-MM-DD' format
            area_polygon (optional): Polygon defining the area of interest (not yet implemented)
            bbox (tuple, optional): Bounding box (west, south, east, north) in WGS84
            bands (list, optional): List of bands to filter by (currently informational)
            maxcc (int, optional): Maximum cloud cover percentage (0-100). Default is 100
            data_connector_spec (optional): Additional connector specifications
            
        Returns:
            tuple: A tuple containing:
                - list of unique dates (sorted)
                - list of STAC item dictionaries
            Returns (None, None) if no data is found
            
        Raises:
            TerrakitValueError: If parameters are invalid or no data is found
        """
        # Validate inputs
        self._validate_dates(start=date_start, end=date_end)
        
        if bbox is not None:
            if not isinstance(bbox, tuple):
                bbox = tuple(bbox)
            self._validate_bbox(bbox)
        
        if data_collection_name not in self.collections:
            raise TerrakitValueError(
                f"Collection '{data_collection_name}' is not supported. "
                f"Available collections: {self.collections}"
            )
        
        logger.info(
            f"Searching for data: collection={data_collection_name}, "
            f"dates={date_start} to {date_end}, bbox={bbox}, maxcc={maxcc}"
        )
        
        # Format datetime for STAC API
        start_dt = pd.Timestamp(date_start).strftime(self.DATETIME_FORMAT)
        end_dt = pd.Timestamp(date_end).strftime(self.DATETIME_FORMAT)
        datetime_range = f"{start_dt}/{end_dt}"
        
        # Build search parameters
        search_params = {
            "collections": [data_collection_name],
            "datetime": datetime_range,
        }
        
        if bbox is not None:
            search_params["bbox"] = bbox
        
        # Execute search
        try:
            search = self.client.search(**search_params)
            items = list(search.items())
            
            if not items:
                logger.warning(
                    f"No data found for {data_collection_name} "
                    f"between {date_start} and {date_end}"
                )
                return None, None
            
            logger.info(f"Found {len(items)} items before cloud filtering")
            
            # Filter by cloud cover
            items = _filter_items_by_cloud_cover(items, maxcc)
            
            if not items:
                logger.warning(
                    f"No data found after filtering for cloud cover <= {maxcc}%"
                )
                return None, None
            
            logger.info(f"Found {len(items)} items after cloud filtering")
            
            # Extract unique dates and convert items to dicts
            unique_dates: set = set()
            results: list[dict[str, Any]] = []
            
            for item in items:
                # Get datetime from item
                if item.datetime:
                    date_str = item.datetime.date().isoformat()
                else:
                    # Use start_datetime if datetime is not available
                    date_str = pd.Timestamp(
                        item.properties.get("start_datetime")
                    ).date().isoformat()
                
                unique_dates.add(date_str)
                results.append(item.to_dict())
            
            return sorted(list(unique_dates)), results
            
        except Exception as e:
            logger.error(f"Error searching Planetary Computer: {e}")
            raise TerrakitValueError(
                f"Failed to search Planetary Computer: {str(e)}"
            )

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
        Retrieve data from Planetary Computer.
        
        Fetches data for the specified collection, date range, area, and bands.
        Returns an xarray DataArray with dimensions (time, band, y, x).
        
        Args:
            data_collection_name (str): Name of the data collection
            date_start (str): Start date in 'YYYY-MM-DD' format
            date_end (str): End date in 'YYYY-MM-DD' format
            area_polygon (optional): Polygon defining the area of interest
            bbox (tuple, optional): Bounding box (west, south, east, north)
            bands (list, optional): List of bands to retrieve. If empty, retrieves all bands
            maxcc (int, optional): Maximum cloud cover threshold (0-100). Default is 100
            data_connector_spec (optional): Additional specifications
            save_file (str, optional): Path to save the output file (.tif or .nc)
            working_dir (str, optional): Working directory for temporary files
            
        Returns:
            xr.DataArray: An xarray DataArray containing the fetched data with
                         dimensions (time, band, y, x), or None if no data found
                         
        Raises:
            TerrakitValueError: If parameters are invalid or data retrieval fails
        """
        # Validate inputs
        self._validate_dates(start=date_start, end=date_end)
        
        if bbox is not None:
            if not isinstance(bbox, tuple):
                bbox = tuple(bbox)
            self._validate_bbox(bbox)
        
        logger.info(
            f"Fetching data from {data_collection_name}: "
            f"dates={date_start} to {date_end}, bbox={bbox}, bands={bands}"
        )
        
        # Search for items
        _, results = self.find_data(
            data_collection_name=data_collection_name,
            date_start=date_start,
            date_end=date_end,
            area_polygon=area_polygon,
            bbox=bbox,
            bands=bands,
            maxcc=maxcc,
            data_connector_spec=data_connector_spec,
        )
        
        if results is None:
            logger.warning("No data found matching criteria")
            return None
        
        # Convert dict results back to STAC items
        from pystac import Item
        items = [Item.from_dict(item_dict) for item_dict in results]
        
        # Sign items for access
        signed_items = [_sign_item_assets(item) for item in items]
        
        logger.info(f"Loading {len(signed_items)} items using stackstac")
        
        # Get collection info for band mapping
        collection_info = None
        for coll in self.collections_details:
            if coll.get("collection_name") == data_collection_name:
                collection_info = coll
                break
        
        if not collection_info:
            raise TerrakitValueError(
                f"Collection {data_collection_name} not found in available collections"
            )
        
        # Create band name mapping from alt_names
        band_mapping = {}
        if "bands" in collection_info:
            for band_info in collection_info["bands"]:
                band_name = band_info.get("band_name")
                alt_names = band_info.get("alt_names", [])
                if band_name and alt_names:
                    # Map both directions: band_name -> alt_name and alt_name -> band_name
                    for alt_name in alt_names:
                        band_mapping[band_name] = alt_name
                        band_mapping[alt_name] = band_name
        
        if band_mapping:
            logger.info(f"Band mapping: {band_mapping}")
        
        try:
            # Use stackstac to load the data
            # stackstac needs explicit resolution when it can't determine from items
            # Get resolution from the first item's GSD (Ground Sample Distance)
            resolution = None
            if len(signed_items) > 0:
                first_item = signed_items[0]
                # Try to get GSD from item properties
                if "gsd" in first_item.properties:
                    resolution = first_item.properties["gsd"]
                # For NAIP, typical resolution is 0.6m, but varies
                # For Sentinel-2, it's 10m
                # For Landsat, it's 30m
                # If we can't determine, use a reasonable default based on collection
                if resolution is None:
                    if data_collection_name == "naip":
                        resolution = 1  # 1 meter for NAIP
                    elif data_collection_name == "sentinel-2-l2a":
                        resolution = 10  # 10 meters for Sentinel-2
                    elif data_collection_name == "landsat-c2-l2":
                        resolution = 30  # 30 meters for Landsat
                    elif data_collection_name == "sentinel-1-rtc":
                        resolution = 10  # 10 meters for Sentinel-1
            
            logger.info(f"Using resolution: {resolution}m")
            
            # Check what assets are available in the first item
            assets_to_use = None
            if len(signed_items) > 0:
                available_assets = list(signed_items[0].assets.keys())
                logger.info(f"Available assets in STAC items: {available_assets}")
                
                # Filter to data assets only (exclude thumbnails, previews, etc.)
                data_assets = [
                    a for a in available_assets
                    if a not in ["thumbnail", "tilejson", "rendered_preview", "metadata"]
                ]
                logger.info(f"Data assets: {data_assets}")
                
                # For NAIP and similar collections, use "image" asset if it exists
                # For band-specific collections, use requested bands
                if bands:
                    # Map requested bands to actual STAC asset names using alt_names
                    mapped_bands = []
                    for band in bands:
                        # Check if band needs mapping
                        if band in band_mapping:
                            mapped_band = band_mapping[band]
                            if mapped_band in available_assets:
                                mapped_bands.append(mapped_band)
                                logger.info(f"Mapped band '{band}' to STAC asset '{mapped_band}'")
                            else:
                                logger.warning(f"Mapped band '{mapped_band}' not in available assets")
                        elif band in available_assets:
                            mapped_bands.append(band)
                            logger.info(f"Band '{band}' found directly in assets")
                        else:
                            logger.warning(f"Band '{band}' not found in available assets")
                    
                    # Check if any mapped bands exist in available assets
                    matching_assets = [b for b in mapped_bands if b in available_assets]
                    if matching_assets:
                        assets_to_use = matching_assets
                        logger.info(f"Using mapped bands: {assets_to_use}")
                    elif "image" in data_assets:
                        # Use "image" asset for collections like NAIP
                        assets_to_use = ["image"]
                        logger.info(f"Using 'image' asset (contains all bands)")
                    elif data_assets:
                        # Use first data asset
                        assets_to_use = [data_assets[0]]
                        logger.info(f"Using first data asset: {assets_to_use}")
                else:
                    # No bands specified, use data assets
                    if data_assets:
                        assets_to_use = data_assets
                        logger.info(f"Using all data assets: {assets_to_use}")
            
            if not assets_to_use:
                logger.warning("No suitable assets found, letting stackstac determine")
            
            # stackstac parameters
            stack_params = {
                "items": signed_items,
                "epsg": 4326,
                "resolution": resolution,
                "xy_coords": "center",
                "chunksize": 2048,  # Add chunking for better memory management
            }
            
            # Add assets if specified
            if assets_to_use:
                stack_params["assets"] = assets_to_use
            
            # Add bounds if specified - use bounds_latlon for lat/lon coordinates
            if bbox is not None:
                # bbox is in (west, south, east, north) format, which is what bounds_latlon expects
                stack_params["bounds_latlon"] = bbox
                logger.info(f"Using bounds_latlon: {bbox}")
            
            logger.info(f"stackstac parameters: {stack_params}")
            stack = stackstac.stack(**stack_params)
            
            logger.info(f"Stack created with shape: {stack.shape}, dims: {stack.dims}")
            
            # Compute the stack (load into memory)
            data = stack.compute()
            
            # Ensure proper dimension names
            if "time" not in data.dims:
                data = data.rename({"time": "time"})
            
            logger.info(f"Loaded data with shape: {data.shape}")
            
            # Save if requested
            if save_file is not None:
                extension = Path(save_file).suffix.lower()
                if extension == ".tif":
                    save_data_array_to_file(da=data, save_file=save_file)
                    logger.info(f"Saved data to {save_file}")
                elif extension == ".nc":
                    save_data_array_as_netcdf(
                        da=data, save_file=save_file, epsg=4326
                    )
                    logger.info(f"Saved data to {save_file}")
                else:
                    raise TerrakitValueError(
                        f"Unsupported file extension: {extension}. Use .tif or .nc"
                    )
            
            return data
            
        except Exception as e:
            logger.error(f"Error loading data from Planetary Computer: {e}")
            raise TerrakitValueError(
                f"Failed to load data: {str(e)}"
            )

# Made with Bob
