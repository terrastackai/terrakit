# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


# Docstings assisted by watsonx Code Assistant

# geodata_utils.py

import xarray as xr
import json
import logging
import math
import typing
import numpy as np
import os
import pandas as pd

from sentinelhub import (
    CRS,
    BBox,
    bbox_to_dimensions,
)
from shapely.geometry import shape
from typing import Any, Dict, Union


logger = logging.getLogger(__name__)


def list_data_connectors(as_json: bool = False) -> Union[list, Dict[str, Any], Any]:
    """
    List available data connectors.

    Parameters:
        as_json (bool): If True, return data connectors as a JSON object, otherwise return a list of connector names.

    Returns:
        Union[list, Dict[str, Any], Any]: List of connector names or JSON object containing all connector specifications.
    """
    location = os.path.dirname(os.path.realpath(__file__))
    file_path = os.path.join(location, "collections.json")
    with open(file_path, "r") as file:
        data_connector_spec_all = json.load(file)

    if as_json is True:
        return data_connector_spec_all
    else:
        return list(set([X["connector"] for X in data_connector_spec_all]))


def load_and_list_collections(
    as_json: bool = False, connector_type: Union[str, None] = None
) -> Union[list, Dict[str, Any]]:
    """
    Load and list collections for a given data connector type.

    Parameters:
        as_json (bool): If True, return collection details as a JSON object, otherwise return a list of collection names.
        connector_type (str): The type of data connector to filter collections by.

    Returns:
        Union[list, Dict[str, Any]]: List of collection names or JSON object containing collection specifications.
    """
    logger.info(connector_type)
    location = os.path.dirname(os.path.realpath(__file__))
    file_path = os.path.join(location, "collections.json")
    with open(file_path, "r") as file:
        data_connector_spec_all = json.load(file)
    if connector_type is not None:
        connector_collections = [
            X for X in data_connector_spec_all if X["connector"] == connector_type
        ]
    else:
        connector_collections = data_connector_spec_all
    if as_json is True:
        return connector_collections
    else:
        return [X["collection_name"] for X in connector_collections]


def check_bands(connector_type: str, collection_name: str, bands: list):
    """
    Check if the specified bands are available for a given collection and connector type.

    Parameters:
        connector_type (str): The type of data connector.
        collection_name (str): The name of the collection.
        bands (list): List of band names to check.

    Returns:
        list: Modified list of bands with any unavailable bands replaced by their alternative names if possible.
    """
    all_collections = load_and_list_collections(
        as_json=True, connector_type=connector_type
    )

    collection_details_list: list[Any] = []
    for X in all_collections:
        if X["collection_name"] == collection_name:  # type: ignore
            collection_details_list.append(X)

    if len(collection_details_list) == 0:
        error_msg = f"Unable to find collection details for '{connector_type}'"
        logger.error(error_msg)
        raise ValueError(error_msg)
    collection_details = collection_details_list[0]
    available_bands = [X["band_name"] for X in collection_details["bands"]]
    alternative_bands = [X["alt_names"] for X in collection_details["bands"]]
    alternative_bands = [item for sublist in alternative_bands for item in sublist]

    for i, b in enumerate(bands):
        if b in available_bands:
            logger.info(f"Band {b} available \u2714")
            new_band = [
                X["band_name"]
                for X in collection_details["bands"]
                if b in X["alt_names"]
            ]
            logger.info(new_band)
        else:
            logger.info(f"Band {b} unavailable \u274c")
            new_band = [
                X["band_name"]
                for X in collection_details["bands"]
                if b in X["alt_names"]
            ]
            if len(new_band) > 0:
                logger.info(f"Alterative name found and used: {new_band[0]}")
                bands[i] = new_band[0]
            else:
                logger.info(f"Bands to choose from: {available_bands}")
                break

    return bands


def polygon_to_bbox(polygon, buffer_size):
    """
    Convert a Shapely Polygon to a bounding box with a buffer zone.

    Parameters:
        polygon (shapely.geometry.Polygon): The input polygon.
        buffer_size (float): The size of the buffer zone in the same units as the polygon's CRS.

    Returns:
        list(float): Bounding box [min_lon, min_lat, max_lon, max_lat] with buffer zone.
    """
    bbox = shape(polygon).bounds
    bbox = list(bbox)
    bbox[0] = bbox[0] - buffer_size
    bbox[1] = bbox[1] - buffer_size
    bbox[2] = bbox[2] + buffer_size
    bbox[3] = bbox[3] + buffer_size
    return bbox


def calculate_resolution(meter_resolution, lat):
    """
    Calculate the spatial resolution in latitude and longitude for a given meter resolution at a specific latitude.

    Parameters:
        meter_resolution (float): The desired resolution in meters.
        lat (float): The latitude for which to calculate the resolution.

    Returns:
        tuple(float, float): Resolution in latitude and longitude.
    """
    # Get length of degrees
    lat_rad = math.radians(lat)
    # Calculate the length of one degree in latitude considering the ellipticity of the earth
    lat_degree_length = (
        111132.954 - 559.822 * math.cos(2 * lat_rad) + 1.175 * math.cos(4 * lat_rad)
    )
    # Calculate the length of one degree in longitude based on the latitude and the earth radius
    lon_degree_length = (math.pi / 180) * math.cos(lat_rad) * 6378137.0
    # Get resolution
    resolution_lat = meter_resolution / lat_degree_length
    resolution_lon = meter_resolution / lon_degree_length

    return resolution_lat, resolution_lon


def verify_input_image(image, standard_dimensions=224) -> typing.Tuple[int, str]:
    """
    Verify input dimensions for supplied image

    Args:
        image (bytes): image
        standard_dimensions (int): expected size of image

    Return:
        tuple[int, str]: [verification_status_code, verification_msg]
    """
    res = os.popen(f"gdalinfo {image} -json").read()
    res_json = json.loads(res)
    dims = res_json["size"]

    # Check if image is geotiff
    if res_json["driverShortName"] != "GTiff":
        return 1007, f"Input {image} is not a GeoTiff."

    # Check image dimensions
    image_input_dimensions = np.min(dims)

    if image_input_dimensions < standard_dimensions:
        return (
            1002,
            f"Input image too small for image {image} with dimensions {image_input_dimensions}. Both dimensions must be >= 224.",
        )
    else:
        # Log/Show dimensions of the input image
        logger.debug(f"Input image {image} has dimensions {image_input_dimensions}")
        return 200, str(image_input_dimensions)


def check_projection(file):
    """
    Check the projection is correct, if not reproject to EPSG:4326

    Parameters:
        file (str): The path to the input file.

    Returns:
        None
    """
    res = os.popen(f"gdalinfo {file} -proj4 -json").read()
    res_json = json.loads(res)
    # WGS84 is the same as EPSG:4326
    if res_json["stac"]["proj:epsg"] != 4326:
        os.system(f"gdalwarp {file} -t_srs EPSG:4326 {file}_reprojected.tif")
        os.system(f"mv {file}_reprojected.tif {file} ")


def pad_bbox(padding_degrees, bbox):
    """
    Add padding to bounding box to help with edge artifacts.
    Args:
        padding_degrees (float): number of degrees to add as border to bbox
        bbox (list(float)): original bounding box [min_lon, min_lat, max_lon, max_lat]
    Return:
        padded_bbox (list(float)): bouning box with border of padding [min_lon, min_lat, max_lon, max_lat]
    """
    return [
        bbox[0] - padding_degrees,
        bbox[1] - padding_degrees,
        bbox[2] + padding_degrees,
        bbox[3] + padding_degrees,
    ]


def tile_bbox(aoi_size, bbox, resolution, tile_size_x=2200.0, tile_size_y=2200.0):
    """
    Tile a bounding box if it exceeds 2400 pixels in any dimension.

    Parameters:
        aoi_size (tuple(float)): The size of the area of interest [width, height].
        bbox (list(float)): The original bounding box [min_lon, min_lat, max_lon, max_lat].
        resolution (float): The spatial resolution.
        tile_size_x (float): The desired width of each tile.
        tile_size_y (float): The desired height of each tile.

    Returns:
        tuple: A tuple containing lists of tiled bounding boxes and their respective sizes.
    """
    numLon = math.floor(aoi_size[0] / tile_size_x)
    numLat = math.floor(aoi_size[1] / tile_size_y)

    lonStep = (bbox[2] - bbox[0]) * (tile_size_x / aoi_size[0])
    latStep = (bbox[3] - bbox[1]) * (tile_size_y / aoi_size[1])

    lons = [bbox[0] + (lonStep * X) for X in list(range(0, numLon + 1))] + [bbox[2]]
    lats = [bbox[1] + (latStep * X) for X in list(range(0, numLat + 1))] + [bbox[3]]

    aoi_bboxes = []
    aoi_sizes = []

    for x in range(0, numLon + 1):
        for y in range(0, numLat + 1):
            aoi_bbox = BBox(
                bbox=[lons[x], lats[y], lons[x + 1], lats[y + 1]], crs=CRS.WGS84
            )
            aoi_bboxes = aoi_bboxes + [aoi_bbox]
            aoi_sizes = aoi_sizes + [
                bbox_to_dimensions(aoi_bbox, resolution=resolution)
            ]

    return aoi_bboxes, aoi_sizes


def check_and_crop_bbox(bbox, resolution):
    """
    Check and crop a bounding box to ensure it fits within Sentinel Hub's processing limits.

    Parameters:
        bbox (list(float)): The original bounding box [min_lon, min_lat, max_lon, max_lat].
        resolution (float): The spatial resolution.

    Returns:
        tuple: A tuple containing the cropped bounding box and its size.
    """
    # Check expected pixel size (Sentinel Hub is limited to 2500 pixel)
    aoi_bbox = [BBox(bbox=bbox, crs=CRS.WGS84)]
    aoi_size = [bbox_to_dimensions(aoi_bbox[0], resolution=resolution)]
    if any(s > 2400 for s in aoi_size[0]):
        aoi_bbox, aoi_size = tile_bbox(aoi_size[0], bbox, resolution)

    for i, b in enumerate(aoi_size):
        if any(s < 244 for s in b):
            logger.info(f"Dimension less than 244, will pad - {aoi_size[i]}")
            center_lon = aoi_bbox[i].middle[0]
            center_lat = aoi_bbox[i].middle[1]
            resolution_lat, resolution_lon = calculate_resolution(
                meter_resolution=resolution, lat=center_lat
            )
            padding = int(224 / 2) + 50

            new_bbox = list(aoi_bbox[i])

            if aoi_size[i][0] < 224:
                # Add padding to the image
                new_bbox[0] = center_lon - padding * resolution_lon
                new_bbox[2] = center_lon + padding * resolution_lon
            if aoi_size[i][1] < 224:
                new_bbox[1] = center_lat - padding * resolution_lat
                new_bbox[3] = center_lat + padding * resolution_lat
            aoi_bbox[i] = BBox(bbox=new_bbox, crs=CRS.WGS84)
            aoi_size[i] = bbox_to_dimensions(aoi_bbox[i], resolution=resolution)
            logger.info(f"New dimensions are {aoi_size[i]}")

    return aoi_bbox, aoi_size


def save_data_array_to_file(da, save_file, imputed=False) -> None:
    """
    Save an xarray DataArray to a GeoTIFF file.

    Parameters:
        da (xarray.DataArray): The input DataArray.
        save_file (str): The path to save the DataArray.
        imputed (bool): Whether the DataArray has been imputed.
    """
    if save_file is not None:
        if da.time is not None:
            for i, t in enumerate(da.time.values):
                date = t.astype(str)[:10]
                if save_file.find(date) == -1:
                    file_path = save_file.replace(".tif", f"_{date}.tif")
                else:
                    file_path = save_file
                if imputed is True and "imputed" not in file_path:
                    file_path = file_path.replace(".tif", "_imputed.tif")
                save_cog(da.isel(time=i), file_path)
        else:
            logger.warning(
                f"Error saving file. Missing time dimension. Dimensions are: {da.dims}"
            )


def save_cog(ds, filename="cogeo.tif") -> None:
    """
    Save an xarray Dataset as a Cloud Optimized GeoTIFF.

    Parameters:
        ds (xarray.Dataset): The input Dataset.
        filename (str): The path and filename for the output GeoTIFF.
    """
    logger.info(f"Saving cloud optimized geotiff to {filename}")
    ds.rio.to_raster(raster_path=filename, driver="COG")


def save_data_array_as_netcdf(
    da: xr.DataArray, save_file: str | bool, **kwargs
) -> None:
    """
    Save an xarray DataArray as a NetCDF file.

    Parameters:
        da (xarray.DataArray): The input DataArray.
        save_file (str): The path to save the DataArray.
        **kwargs: Additional keyword arguments for the to_netcdf method.
    """
    if not isinstance(save_file, str):
        raise TypeError("Error! save_file should be a string")
    else:
        # if epsg is passed as parameter
        if kwargs.get("epsg") is not None:
            epsg = kwargs.get("epsg")
            da.rio.write_crs(epsg, inplace=True)
        da.to_netcdf(path=save_file, **kwargs)


def validate_input_params(
    bbox: tuple | None, date_start: str | None, date_end: str | None
) -> None:
    """
    Validate input parameters for bounding box and date range.

    Parameters:
        bbox (tuple | None): The bounding box as a tuple (min_lon, min_lat, max_lon, max_lat).
        date_start (str | None): The start date in 'YYYY-MM-DD' format.
        date_end (str | None): The end date in 'YYYY-MM-DD' format.
    """
    if bbox is not None:
        if not isinstance(bbox, (tuple, list)):
            raise ValueError("Error! bbox should be either a tuple or list")
        else:
            if len(bbox) != 4:
                raise ValueError("Error! bbox must have 4 items")
            else:
                west, south, east, north = bbox
                if not (-180 <= west < east <= 180 and -90 <= south < north <= 90):
                    raise ValueError(f"Error! Invalid values in {bbox=}")
    if date_start is not None and date_end is not None:
        if not isinstance(date_start, str) or not isinstance(date_end, str):
            raise ValueError("Error! date_start and date_end should be a string")
        else:
            start = pd.Timestamp(date_start)
            end = pd.Timestamp(date_end)
            if start > end:
                raise ValueError(f"Error! {start=} cannot be after {end=}")
