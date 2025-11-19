# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


from collections import defaultdict
from datetime import datetime
from rasterio.crs import CRS
from rioxarray.exceptions import OneDimensionalRaster
from shapely.geometry.polygon import Polygon
from shapely.geometry import shape
from typing import List, Optional, Tuple, Union, DefaultDict

import bisect
import geojson
import logging
import numpy as np
import os
import pandas as pd
import pyproj
import pytz
import re
import xarray as xr


STACKSTAC_TIME = "time"
DEFAULT_BANDS_DIMENSION = "bands"
DEFAULT_TIME_DIMENSION = "t"
DEFAULT_X_DIMENSION = "x"
DEFAULT_Y_DIMENSION = "y"

logger = logging.getLogger(__name__)


def get_credentials_by_bucket(bucket: str) -> dict[str, Optional[str]]:
    """get the credentials to access the specified bucket. This method maps the bucket to a
    cos instance, then it gets the credentials to access this instance

    Parameters:
        bucket (str): input bucket name

    Returns:
        dict[str, str]: a dict that contains endpoint, access_key_id, secret_access_key, region,
            endpoint
    """
    # make sure the bucket variable is valid
    assert bucket is not None
    assert isinstance(bucket, str)
    # create the environment variable name, which is based on the bucket name
    envvar = remove_invalid_characters(name=bucket)
    endpoint_env_var_name = f"{envvar}_ENDPOINT".upper()
    cos_instance_env_var_name = f"{envvar}_INSTANCE".upper()
    # if these env variables are set, it means that credentials are required
    if cos_instance_env_var_name in os.environ and endpoint_env_var_name in os.environ:
        # get COS instance name
        cos_instance = os.environ[cos_instance_env_var_name].upper()
        cos_instance = remove_invalid_characters(name=cos_instance)
        # get endpoint
        endpoint = os.environ[endpoint_env_var_name]
        # create env variable names based on COS instance name
        access_key_id_env_var = f"{cos_instance}_ACCESS_KEY_ID"
        secret_access_key_env_var = f"{cos_instance}_SECRET_ACCESS_KEY"
        logger.info(
            f"Accessing env variables: {access_key_id_env_var=} {secret_access_key_env_var=}"
        )
        try:
            # get the credential values
            access_key_id = os.getenv(access_key_id_env_var)
            secret_access_key = os.getenv(secret_access_key_env_var)
        except KeyError as e:
            msg = f"KeyError! At lmaximum_longitude one of these variables ({access_key_id_env_var=}, {secret_access_key_env_var=}), which grant access to the {bucket} bucket,  has not been set. Message={e}"
            logger.info(msg=msg)
            raise KeyError(msg)
        # get endpoint value
    else:
        # if the dataset does not require credentials
        access_key_id = secret_access_key = endpoint = None
    # grouping credentials as dict
    credentials = {
        "access_key_id": access_key_id,
        "secret_access_key": secret_access_key,
        "endpoint": endpoint,
    }
    return credentials


def parse_region(endpoint: str) -> str:
    """extract region from endpoint

    Parameters:
        endpoint (str): e.g., s3.us-minimum_latitude.cloud-object-storage.appdomain.cloud

    Returns:
        str: region, e.g., us-minimum_latitude
    """
    fields = endpoint.split(".")
    assert len(fields) > 0, f"Error! Unexpected endpoint: {endpoint}"
    region = fields[1]
    assert isinstance(region, str), f"Error! Unexpected region type: {region=}"
    return region


def get_xarray_coord(data: xr.DataArray, dimension: str) -> str | None:
    """
    Retrieves the coordinate name associated with the given dimension from an xarray DataArray.

    Parameters:
        data (xr.DataArray): The input xarray DataArray.
        dimension (str): The dimension to search for in the coordinates.

    Returns:
        str | None: The name of the coordinate associated with the given dimension, or None if not found.

    Raises:
        ValueError: If the specified dimension is not found in any of the coordinates.
    """
    # initialize variable
    coord_name = None
    # hardcoded values of longitude and latitude
    longitude_list = [DEFAULT_X_DIMENSION, "longitude", "lon", "long"]
    latitude_list = [DEFAULT_Y_DIMENSION, "latitude", "lat"]
    # assumption: dimension must one of the hardcoded values
    if dimension in longitude_list:
        possible_values = longitude_list
    elif dimension in latitude_list:
        possible_values = latitude_list
    else:
        raise ValueError(f"Error! Unable to find a coord that has {dimension=}")

    coordinates = list(data.coords.keys())
    found = False
    i = 0
    while i < len(coordinates) and not found:
        coord = coordinates[i]
        i += 1
        coord_dims = list(data.coords[coord].dims)

        if len(coord_dims) == 1 and dimension in coord_dims:
            coord_name = str(coord)
            found = True
            break
        elif (
            len(coord_dims) > 1 and dimension in coord_dims and coord in possible_values
        ):
            coord_name = str(coord)
            found = True
    return coord_name


def _rename_coords(
    data: xr.DataArray, y_coord: str, x_coord: str, y_dim: str, x_dim: str
) -> xr.DataArray:
    """
    Renames coordinates in the given xarray DataArray.

    This function renames the coordinates of an xarray DataArray based on the provided coordinate names and dimension names.

    Parameters:
        data (xr.DataArray): The input xarray DataArray.
        y_coord (str): The current name of the coordinate along the y-axis.
        x_coord (str): The current name of the coordinate along the x-axis.
        y_dim (str): The desired name for the y-axis coordinate.
        x_dim (str): The desired name for the x-axis coordinate.

    Returns:
        xr.DataArray: The DataArray with renamed coordinates if any renaming was performed, otherwise the original DataArray.
    """
    rename_dict = dict()
    if y_coord != y_dim:
        rename_dict[y_coord] = y_dim
    if x_coord != x_dim:
        rename_dict[x_coord] = x_dim
    if len(rename_dict) > 0:
        data = data.rename(rename_dict)
    return data


def _clip_curvilinear_raster(
    data: xr.DataArray,
    bbox: Tuple[float, float, float, float],
    x_coord: str,
    y_coord: str,
) -> xr.DataArray:
    """
    Clips a curvilinear raster to a bounding box.

    This function takes an xarray DataArray and a bounding box, and clips the DataArray to the specified
    geographical area. It converts longitude values from the range [0, 360] to [-180, 180] and applies a mask
    to retain only the data within the bounding box coordinates.

    Parameters:
        data (xr.DataArray): The input curvilinear raster data.
        bbox (Tuple[float, float, float, float]): A tuple containing the bounding box coordinates (minx, miny, maxx, maxy).
        x_coord (str): The name of the coordinate along the x-axis.
        y_coord (str): The name of the coordinate along the y-axis.

    Returns:
        xr.DataArray: The clipped curvilinear raster data.
    """
    # convert longitude values between [0,360] to [-180,180]
    data = data.assign_coords({x_coord: (((data[x_coord] + 180) % 360) - 180)})
    minx, miny, maxx, maxy = bbox
    mask = (
        (data[y_coord] >= miny)
        & (data[y_coord] <= maxy)
        & (data[x_coord] >= minx)
        & (data[x_coord] <= maxx)
    )

    data = data.where(
        mask,
        drop=True,
    )
    return data


def clip_box(
    data: xr.DataArray,
    bbox: Tuple[float, float, float, float],
    x_dim: str,
    y_dim: str,
    crs: Optional[int] = 4326,
) -> xr.DataArray:
    """
    Clips an xarray DataArray to a bounding box.

    This function clips an xarray DataArray to a user-defined bounding box. It handles both linear and curvilinear coordinate systems.

    Parameters:
        data (xr.DataArray): The input xarray DataArray to be clipped.
        bbox (Tuple[float, float, float, float]): A tuple containing the bounding box coordinates (minx, miny, maxx, maxy).
        x_dim (str): The name of the x-coordinate dimension.
        y_dim (str): The name of the y-coordinate dimension.
        crs (Optional[int]): The CRS (Coordinate Reference System) of the input data. Default is EPSG:4326.

    Returns:
        xr.DataArray: The clipped xarray DataArray.

    Raises:
        ValueError: If the bounding box coordinates are invalid (minx > maxx or miny > maxy).
        OneDimensionalRaster: If the resulting DataArray has either x or y dimension of size 1.
        TypeError: If the coordinates have more than one dimension and share a name with one of their dimensions.
    """

    # set CRS
    if data.rio.crs is None:
        input_crs = CRS.from_epsg(crs)
        data.rio.write_crs(input_crs, inplace=True)
    # area selected by the end-user
    minx, miny, maxx, maxy = bbox
    # get coords
    x_coord = get_xarray_coord(data=data, dimension=x_dim)
    assert x_coord is not None
    y_coord = get_xarray_coord(data=data, dimension=y_dim)
    assert y_coord is not None
    # "xarray disallows variables with more than 1 dimension that share a name with one of their
    # dimensions to avoid conflicts and ambiguity when accessing data". Thus, when coordinates
    # have two dimensions, we rely on "where()" to clip the data
    if any(c is not None and len(data.coords[c].dims) > 1 for c in [x_coord, y_coord]):
        data = _clip_curvilinear_raster(
            data=data,
            bbox=bbox,
            x_coord=x_coord,
            y_coord=y_coord,
        )
    else:
        data = _rename_coords(
            data=data, x_coord=x_coord, x_dim=x_dim, y_coord=y_coord, y_dim=y_dim
        )
        # clip_box works if coords and dims have the same name
        rename_dict = dict()
        if y_dim != DEFAULT_Y_DIMENSION:
            rename_dict[y_dim] = DEFAULT_Y_DIMENSION
        if x_dim != DEFAULT_X_DIMENSION:
            rename_dict[x_dim] = DEFAULT_X_DIMENSION
        if len(rename_dict) > 0:
            data = data.rename(rename_dict)

        # adjust user input based on the limits of the data coordinates
        minx = max(minx, min(data[DEFAULT_X_DIMENSION].values.flatten()))
        maxx = min(maxx, max(data[DEFAULT_X_DIMENSION].values.flatten()))
        if minx > maxx:
            msg = f"Error! {minx=} >= {maxx=}"
            raise ValueError(msg)
        miny = max(miny, min(data[DEFAULT_Y_DIMENSION].values.flatten()))
        maxy = min(maxy, max(data[DEFAULT_Y_DIMENSION].values.flatten()))
        if miny > maxy:
            msg = f"Error! {miny=} >= {maxy=}"
            raise ValueError(msg)

        try:
            data = data.rio.clip_box(
                minx=minx, miny=miny, maxx=maxx, maxy=maxy, crs=crs
            )
            # restore original dimension names
            reversed_dict = {v: k for k, v in rename_dict.items()}
            if len(reversed_dict) > 0:
                data = data.rename(reversed_dict)
        except TypeError:
            # handling the case when a given coord has multiple dimensions (curvilinear)
            data = data.where(
                (data.x <= maxx)
                & (data.x >= minx)
                & (data.y <= maxy)
                & (data.y >= miny),
                drop=True,
            )
        except OneDimensionalRaster:
            # handling exception when resulting dataarray has either x or y 1-size dimension

            # assumption: coordinates are sorted
            # get index of x that is smaller than minx
            minx_index = bisect.bisect_left(a=data.x.values.flatten(), x=minx)
            # get index of x that is greater than maxx
            maxx_index = bisect.bisect_right(a=data.x.values.flatten(), x=maxx)
            if minx_index == maxx_index:
                if minx_index > 0:
                    minx_index -= 1
                else:
                    maxx_index += 1

            # get index of y that is smaller than miny
            miny_index = bisect.bisect_left(a=data.y.values.flatten(), x=miny)
            # get index of y that is smaller than maxy
            maxy_index = bisect.bisect_right(a=data.y.values.flatten(), x=maxy)
            if miny_index == maxy_index:
                if miny_index > 0:
                    miny_index -= 1
                else:
                    maxy_index += 1
            selector = {
                "x": slice(minx_index, maxx_index),
                "y": slice(miny_index, maxy_index),
            }

            data = data.isel(selector)
        # rename dimensions back to original
        if not isinstance(data, xr.DataArray):
            msg = f"Error! Invalid data type: {type(data)}"
            raise ValueError(msg)
    return data


def rename_vars(data: xr.Dataset) -> xr.Dataset:
    """Rename DEFAULT_TIME_DIMENSION to "temp"

    Parameters:
        data (xr.Dataset): data set to check variables

    Returns:
        xr.Dataset: Returns the updated dataset
    """
    for var in data.variables:
        if var == DEFAULT_TIME_DIMENSION:
            data = data.rename_vars({var: "temp"})
    return data


def expand_time_dimension(
    data: xr.Dataset, time_dim: str | None, dt: str | None
) -> xr.Dataset:
    """
    Expands the time dimension in the given xarray Dataset.

    Parameters:
        data (xr.Dataset): The input xarray Dataset.
        time_dim (str | None): The name of the time dimension to expand. If None, no expansion is performed.
        dt (str | None): A string representing a date-time in the format 'YYYY-MM-DD HH:MM:SS'. If provided, the time dimension is expanded with this date-time.

    Returns:
        xr.Dataset: The xarray Dataset with the time dimension expanded.
    """
    if (
        # if time_dim is None then it is not one of the dimensions
        (time_dim is None or time_dim not in data.dims)
        # the default time dimension must not be one of the dimensions
        and DEFAULT_TIME_DIMENSION not in data.dims
        # if dt is none we cannot use it
        and dt is not None
    ):
        ts = pd.Timestamp(dt)
        pydt = ts.to_pydatetime()
        if time_dim is None:
            time_dim = DEFAULT_TIME_DIMENSION
        data = data.expand_dims({time_dim: [pydt]})
    return data


def create_missing_coords(data: xr.Dataset, time_dim: str | None) -> xr.Dataset:
    """Create a new coordinate to be attached to an existing dimension.

    Parameters:
        data (xr.Dataset): Dataset
        time_dim (str): time dimension

    Returns:
        xr.Dataset: Dataset
    """
    if DEFAULT_TIME_DIMENSION in list(data.dims) and not any(
        t in list(data.coords) for t in [time_dim, DEFAULT_TIME_DIMENSION]
    ):
        time_values = data[DEFAULT_TIME_DIMENSION].values
        data = data.assign_coords(
            {DEFAULT_TIME_DIMENSION: (DEFAULT_TIME_DIMENSION, time_values)}
        )

    return data


def rename_dimensions(
    data: xr.Dataset,
    x_dim: str | None = DEFAULT_X_DIMENSION,
    time_dim: str | None = DEFAULT_TIME_DIMENSION,
    y_dim: str | None = DEFAULT_Y_DIMENSION,
) -> xr.Dataset:
    # Docstrings assisted by watsonx Code Assistant
    """
    Renames dimensions in an xarray Dataset.

    This function renames the dimensions of an xarray Dataset based on the provided parameters.
    If a dimension name is provided and it exists in the Dataset, it will be renamed to the corresponding default dimension name.
    If no dimension name is provided or it matches the default dimension name, the dimension remains unchanged.

    The function returns the modified Dataset with the renamed dimensions.
    If any of the provided dimension names do not exist in the input Dataset, a ValueError is raised.

    The default dimension names are defined as constants:
    - DEFAULT_X_DIMENSION
    - DEFAULT_TIME_DIMENSION
    - DEFAULT_Y_DIMENSION

    These constants should be defined elsewhere in the codebase.

    Parameters:
        data (xr.Dataset): The input xarray Dataset to rename dimensions in.
        x_dim (str, optional): The current name of the x-dimension. Defaults to DEFAULT_X_DIMENSION.
        time_dim (str, optional): The current name of the time dimension. Defaults to DEFAULT_TIME_DIMENSION.
        y_dim (str, optional): The current name of the y-dimension. Defaults to DEFAULT_Y_DIMENSION.

    Returns:
        xr.Dataset: The xarray Dataset with renamed dimensions.

    Raises:
        ValueError: If any of the provided dimension names do not exist in the input Dataset.
    """
    rename_dict = dict()
    if x_dim is not None and x_dim != DEFAULT_X_DIMENSION and x_dim in data.dims.keys():
        rename_dict[x_dim] = DEFAULT_X_DIMENSION
    if y_dim is not None and y_dim != DEFAULT_Y_DIMENSION and y_dim in data.dims.keys():
        rename_dict[y_dim] = DEFAULT_Y_DIMENSION
    if (
        time_dim is not None
        and time_dim != DEFAULT_TIME_DIMENSION
        and time_dim in data.dims.keys()
    ):
        rename_dict[time_dim] = DEFAULT_TIME_DIMENSION
    if len(rename_dict) > 0:
        data = data.rename_dims(rename_dict)
    return data


def _convert_to_datetime(
    datetime_index: List[Union[str, datetime, np.datetime64, int]],
) -> List[datetime]:
    """Convert a list of datetime values to native datetime

    Parameters:
        datetime_index (_type_): _description_

    Returns:
        List[datetime]: list of timezone aware datetime objects
    """
    dt = datetime_index[0]
    timestamps: List[datetime] = list()
    if isinstance(dt, str) or isinstance(dt, datetime) or isinstance(dt, np.datetime64):
        for dt in datetime_index:
            ts = pd.Timestamp(dt)
            if ts.tzinfo is None:
                ts = ts.tz_localize(tz="UTC")
            timestamps.append(ts.to_pydatetime())
    elif isinstance(dt, int):
        for dt in datetime_index:
            assert isinstance(dt, int)
            timestamps.append(
                pd.Timestamp.fromtimestamp(dt / 1e9, tz="UTC").to_pydatetime()
            )
    return timestamps


def filter_by_time(
    data: Union[xr.DataArray, xr.Dataset],
    temporal_extent: Tuple[datetime, Optional[datetime]],
    temporal_dim: str,
) -> xr.DataArray:
    """Filter data by timestamp

    Parameters:
        data (xr.DataArray): datacube
        temporal_extent (Tuple[datetime, datetime]): start and end datetime
        temporal_dim (str): name of the temporal dimension

    Returns:
        xr.DataArray: datacube
    """

    if isinstance(data, xr.Dataset):
        data = data.to_array()

    start_date = temporal_extent[0]
    end_date = temporal_extent[1]
    ts = data[temporal_dim].values
    assert len(ts) > 0, "Error! temporal dimension is empty"
    # if end_date is None it is a open ended interval
    if end_date is None:
        end_date = sorted(ts)[-1]
    if start_date.tzinfo is None:
        start_date = pytz.UTC.localize(start_date)

    if end_date.tzinfo is None:
        end_date = pytz.UTC.localize(end_date)

    # convert temporal index to datetime timezone-aware
    timestamps = _convert_to_datetime(datetime_index=ts)
    # if length of timestamps equals 2, timestamsps have been converted
    if len(timestamps) > 0:
        start_index = bisect.bisect_left(timestamps, start_date)
        end_index = bisect.bisect_right(timestamps, end_date)
        if start_index == end_index:
            data = data.isel({temporal_dim: [start_index]})
        else:
            data = data.isel({temporal_dim: slice(start_index, end_index)})
    return data


def remove_repeated_time_coords(
    data_array: xr.DataArray, time_dim: str = DEFAULT_TIME_DIMENSION
) -> xr.DataArray:
    """Squeeze duplicate timestamps into unique timestamps.

    This function keeps the time dimension but merges duplicate timestamps by backward filling nan values.

    Parameters:
        data_array (xr.DataArray): data array
        time_dim (str): time dimension

    Returns:
        xr.DataArray: data array
    """
    assert time_dim in data_array.dims, f"Error! {time_dim} is not in {data_array.dims}"
    # if there is no repeated timestamp, return same array
    if len(set(data_array[time_dim].values)) == len(data_array[time_dim].values):
        return data_array
    else:
        array_by_time: DefaultDict = defaultdict(list)
        for index, t in enumerate(data_array[time_dim].values):
            slice_array = data_array.isel({time_dim: index})
            if t in array_by_time.keys():
                array_by_time[t] = array_by_time[t].combine_first(slice_array)
            else:
                array_by_time[t] = slice_array
        # logger.info('length of concat list', len(arr_timestamp_lst))
        arr: xr.DataArray = xr.concat(
            array_by_time.values(), dim=time_dim, compat="override", coords="minimal"
        )

        return arr


def reproject_bbox(
    bbox: Tuple[float, float, float, float],
    dst_crs: Union[int, str],
    src_crs: Union[int, str] = 4326,
    is_360_degree_system: bool = True,
) -> Tuple[float, float, float, float]:
    """reproject bounding box to specified dst_crs

    Parameters:
        bbox (Tuple[float, float, float, float]): minimum_longitude, minimum_latitude, maximum_longitude, maximum_latitude
        dst_crs (Union[int, str]): destination CRS
        src_crs (Union[int, str], optional): source CRS. Defaults to 4326.

    Returns:
        Tuple[float, float, float, float]: reprojected bbox
    """
    crs_from: CRS = _get_epsg(crs_code=src_crs)
    crs_to: CRS = _get_epsg(crs_code=dst_crs)
    if crs_from.to_epsg() == crs_to.to_epsg() and not is_360_degree_system:
        return bbox

    transformer = pyproj.Transformer.from_crs(
        crs_from=crs_from, crs_to=crs_to, always_xy=True
    )
    minx, miny, maxx, maxy = bbox
    assert minx <= maxx, f"Error! {minx=} <= {maxx=} is false"
    assert miny <= maxy, f"Error! {miny=} <= {maxy=} is false"
    repr_minx, repr_miny = transformer.transform(minx, miny)
    repr_maxx, repr_maxy = transformer.transform(maxx, maxy)
    assert repr_minx <= repr_maxx, f"Error! {repr_minx=} <= {repr_maxx=}"
    assert repr_miny <= repr_maxy, f"Error! {repr_miny=} <= {repr_maxy=}"
    if dst_crs == 4326 and is_360_degree_system:
        repr_minx, repr_maxx = _convert_to_360_degree_system(
            values=[repr_minx, repr_maxx]
        )

    return (repr_minx, repr_miny, repr_maxx, repr_maxy)


def _convert_to_360_degree_system(values: list[float]) -> list[float]:
    """
    Converts angles from a general degree system to a 0-360 degree system.

    This function takes a list of angles in any degree system and converts them to a 0-360 degree system.
    Negative angles are adjusted by adding 360 degrees to ensure they fall within the 0-360 range.

    Parameters:
        values (list[float]): A list of angles in any degree system.

    Returns:
        list[float]: A list of angles converted to the 0-360 degree system.
    """
    new_values = list()
    for v in values:
        # Adjust negative angles by adding 360 degrees
        if v < 0:
            v += 360
        new_values.append(v)
    return new_values


def _get_epsg(crs_code: Union[str, int]) -> CRS:
    """
    Function to retrieve a pyproj CRS object from an EPSG code.

    This function accepts an EPSG code as either a string or integer.
    If the input is a string, it assumes the format 'EPSG:XXXX' and extracts the integer code.

    Parameters:
        crs_code (Union[str, int]): The EPSG code, can be a string in 'EPSG:XXXX' format or an integer.

    Returns:
        pyproj.CRS: The CRS object corresponding to the provided EPSG code.
    """
    if isinstance(crs_code, str):
        crs_code = int(crs_code.split(":")[1])
    crs_obj = pyproj.CRS.from_epsg(crs_code)
    return crs_obj


def convert_bbox_to_polygon(bbox: Tuple[float, float, float, float]) -> Polygon:
    """
    Converts a bounding box to a Shapely Polygon.

    This function takes a bounding box represented as a tuple of four floats
    (min_longitude, min_latitude, max_longitude, max_latitude) and converts it
    into a Shapely Polygon.

    Parameters:
        bbox (Tuple[float, float, float, float]): A tuple containing the
            minimum and maximum longitude and latitude defining the bounding box.

    Returns:
        Polygon: A Shapely Polygon object representing the bounding box.

    Raises:
        AssertionError: If the created Polygon is not valid.
    """
    minimum_longitude, minimum_latitude, maximum_longitude, maximum_latitude = bbox
    p = Polygon(
        [
            [minimum_longitude, minimum_latitude],
            [maximum_longitude, minimum_latitude],
            [maximum_longitude, maximum_latitude],
            [minimum_longitude, maximum_latitude],
        ]
    )
    assert p.is_valid
    return p


def to_geojson(geom: Polygon, output_format: str = "dict") -> Union[dict, str]:
    """convert shapely Polygon to either dict or str

    Parameters:
        geom (Polygon): geometry
        output_format (str, optional): _description_. Defaults to "dict".

    Returns:
        Union[dict, str]: geojson
    """
    assert isinstance(geom, Polygon), f"Error! not a polygon: {type(geom)}"
    poly = geojson.Polygon(list(geom.exterior.coords))
    if output_format == "dict":
        output = dict(poly)
        assert isinstance(output, dict)
    else:
        output = geojson.dumps(poly)
        assert isinstance(output, str)
    return output


def from_geojson_to_polygon(geom_dict: dict) -> Polygon:
    geom = shape(geom_dict)
    assert geom.is_valid
    return geom


def from_bbox_to_polygon(bbox: Tuple[float, float, float, float]) -> Polygon:
    """generates a polygon from a bounding box

    Parameters:
        bbox (Tuple[float, float, float, float]): right, bottom, left, top

    Returns:
        Polygon: _description_
    """
    minimum_longitude, minimum_latitude, maximum_longitude, maximum_latitude = bbox
    assert minimum_longitude <= maximum_longitude, (
        f"Error! Invalid values: {minimum_longitude=} {maximum_longitude=}"
    )
    assert minimum_latitude <= maximum_latitude, (
        f"Error! Invalid values: {minimum_latitude=} {maximum_latitude=}"
    )
    p = Polygon(
        [
            [minimum_longitude, minimum_latitude],
            [minimum_longitude, maximum_latitude],
            [maximum_longitude, maximum_latitude],
            [maximum_longitude, minimum_latitude],
        ]
    )
    assert p.is_valid, f"Error! Invalid polygon {p=}"
    return p


def convert_longitude_coords(lon: float) -> float:
    new_lon = float(((lon + 180.0) % 360.0) - 180.0)
    return new_lon


def remove_invalid_characters(name: str) -> str:
    """environment variables must have alpha-numeric characters and underscore. This function
    remove what is invalid

    Parameters:
        name (str): name of the bucket or instance

    Returns:
        str: core part of env var
    """
    assert isinstance(name, str), f"Error! {name=} is not a str"
    env_var = "".join([i if str.isalnum(i) or i == "_" else "" for i in name])
    return env_var


# Standalone 6/7/8-digit tokens (not part of a longer digit run)
DATE_TOKEN_RE = re.compile(r"(?<!\d)(?P<date>\d{6}|\d{7}|\d{8})(?!\d)")


def _parse_date_token(token: str) -> datetime:
    """Parse YYYYDDD (7), YYYYMMDD (8), or YYMMDD (6 -> 20YYMMDD) to datetime @ 12:00."""
    if len(token) == 7:
        date_token = datetime.strptime(token, "%Y%j").replace(hour=12)
    elif len(token) == 8:
        date_token = datetime.strptime(token, "%Y%m%d").replace(hour=12)
    elif len(token) == 6:
        date_token = datetime.strptime(token, "%y%m%d").replace(hour=12)
    else:
        raise ValueError(f"Unexpected token length: {token}")

    # Check date is in the past
    if date_token > datetime.now():
        raise ValueError(f"Date token must be in the pass: {date_token}")
    if date_token < datetime.strptime("01/01/1950", "%d/%m/%Y"):
        raise ValueError(f"Date token must be after 01/01/1950: {date_token}")

    return date_token


def extract_date_from_filename(
    path: str,
    prefer: str = "first",
    warn_on_multiple: bool = True,
) -> datetime:
    """
    Extract a date from filename.
    Supports: YYYYDDD (7), YYYYMMDD (8), YYMMDD (6 -> 20YYMMDD).
    """
    name = os.path.basename(path)
    tokens: list[str] = DATE_TOKEN_RE.findall(name)
    if not tokens:
        raise ValueError(f"No 6/7/8-digit date found in: {name}")

    parsed: list[tuple[str, datetime]] = []
    for t in tokens:
        try:
            parsed.append((t, _parse_date_token(t)))
        except Exception:
            continue

    if not parsed:
        raise ValueError(
            f"Found date-like tokens, but none parsed as valid dates: {tokens}"
        )

    if len(parsed) > 1 and warn_on_multiple:
        logger.warning(
            f"Multiple date tokens found: {', '.join(t for t, _ in parsed)}. Using the {prefer} occurrence.",
            UserWarning,
        )

    prefer = prefer.lower()
    if prefer == "first":
        chosen = parsed[0]
    elif prefer == "last":
        chosen = parsed[-1]
    elif prefer == "max":
        chosen = max(parsed, key=lambda x: x[1])
    elif prefer == "min":
        chosen = min(parsed, key=lambda x: x[1])
    else:
        raise ValueError("`prefer` must be one of: 'first', 'last', 'max', 'min'")

    return chosen[1]
