# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


from datetime import datetime
import logging
import aiohttp
import fsspec
from fsspec.implementations.http import HTTPFileSystem
import xarray as xr
from typing import Optional, Tuple, Union
import numpy as np
from shapely.geometry.polygon import Polygon


from urllib.parse import urlparse
from boto3.session import Session
from typing import Any
import pystac
import s3fs


from ..general_utils.geospatial_util import (
    DEFAULT_BANDS_DIMENSION,
    DEFAULT_X_DIMENSION,
    DEFAULT_Y_DIMENSION,
    clip_box,
    create_missing_coords,
    expand_time_dimension,
    filter_by_time,
    get_credentials_by_bucket,
    parse_region,
    reproject_bbox,
)

logger = logging.getLogger(__name__)
xr.set_options(keep_attrs=True)


class CloudStorageFileReader:
    DATA = "data"

    def __init__(
        self,
        items: list[dict],
        bands: list[str],
        bbox: Tuple[float, float, float, float],
        temporal_extent: Tuple[datetime, Optional[datetime]],
        properties: dict[str, Any] | None,
    ) -> None:
        """

        Args:
            items (list[dict[str, Any]]): items that match the criteria set by the user and grouped by the media type
            bands (list[str]): bands specified by the user
            bbox (Tuple[float, float, float, float]): bounding box specified by the user (minimum_longitude, minimum_latitude, maximum_latitude, maximum_longitude)
            temporal_extent (Tuple[datetime, datetime]): start and end.
        Returns:
            S3FileReader: S3FileReader instance
        """
        assert isinstance(items, list)
        assert len(items) > 0
        self.items = items
        # validate bbox
        assert isinstance(bbox, tuple), f"Error! {type(bbox)} is not a tuple"
        assert len(bbox) == 4, f"Error! Invalid size: {len(bbox)}"
        minimum_longitude, minimum_latitude, maximum_longitude, maximum_latitude = bbox
        assert -180 <= minimum_longitude <= maximum_longitude <= 180, (
            f"Error! {minimum_longitude=} {maximum_longitude=}"
        )
        assert -90 <= minimum_latitude <= maximum_latitude <= 90, (
            f"Error! {minimum_latitude=} {maximum_latitude=}"
        )
        self.bbox = bbox
        self.bands = bands
        if temporal_extent is not None:
            assert isinstance(temporal_extent, tuple), (
                f"Error! temporal_extent is not a tuple: {type(temporal_extent)}"
            )
            assert len(temporal_extent) == 2, (
                f"Error! tuple size is not 2: {temporal_extent=}"
            )
            # if temporal_extent is not empty tuple, then the first item cannot be None
            start = temporal_extent[0]
            end = temporal_extent[1]
            assert isinstance(start, datetime)
            # the second item can be None for open intervals
            if end is not None:
                assert isinstance(end, datetime)
                assert start <= end, f"Error! {start=} {end=}"
        self.temporal_extent = temporal_extent
        assets: dict = items[0]["assets"]
        asset_values: dict = next(iter(assets.values()))
        href = asset_values["href"]
        self.properties = properties
        self.bucket = CloudStorageFileReader._extract_bucket_name_from_url(url=href)
        credentials = get_credentials_by_bucket(bucket=self.bucket)

        self._endpoint: Optional[str] = credentials["endpoint"]
        self.access_key_id: Optional[str] = credentials["access_key_id"]
        self.secret_access_key: Optional[str] = credentials["secret_access_key"]
        if self.endpoint is not None:
            region = parse_region(endpoint=self.endpoint)
            self.region = region

    @property
    def endpoint(self) -> Optional[str]:
        if self._endpoint is not None:
            return self._endpoint.lower()
        else:
            return None

    @property
    def start_datetime(self) -> datetime:
        return self.temporal_extent[0]

    @property
    def end_datetime(self) -> Optional[datetime]:
        return self.temporal_extent[1]

    def get_polygon(self) -> Polygon:
        """convert the bbox associated with this instance of the s3reader to a polygon

        Returns:
            Polygon: a polygon that is equivalent to the bbox set by the user
        """
        xmin, ymin, xmax, ymax = self.bbox
        poly = Polygon([[xmin, ymin], [xmin, ymax], [xmax, ymax], [xmax, ymin]])
        return poly

    def _create_boto3_session(
        self,
    ) -> Session:
        session = Session(
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            region_name=self.region,
        )
        return session

    @staticmethod
    def _get_dimension_description(item: pystac.Item, axis: str) -> Optional[str]:
        item_prop = item.properties
        cube_dims: dict[str, Any] = item_prop["cube:dimensions"]
        for key, value in cube_dims.items():
            if value.get("axis") is not None and value.get("axis") == axis:
                return key
        return None

    @staticmethod
    def _extract_bucket_name_from_url(url: str) -> str:
        """parse url and get the bucket as str

        Args:
            url (str): link to file on COS

        Returns:
            str: bucket name
        """
        # the first char of the path is a slash, so we need to skip it to get the bucket name
        url_parsed = urlparse(url=url)
        if (
            url_parsed.scheme is not None
            and url_parsed.scheme.lower() == "s3"
            and isinstance(url_parsed.hostname, str)
        ):
            return url_parsed.hostname
        else:
            begin_bucket_name = 1
            end_bucket_name = url_parsed.path.find("/", begin_bucket_name)
            assert end_bucket_name > begin_bucket_name, (
                f"Error! Unable to find bucket name: {url}"
            )
            bucket = url_parsed.path[begin_bucket_name:end_bucket_name]
            return bucket

    @staticmethod
    def _get_object(url: str) -> str:
        """parse url and get the object (aka key, path) as str

        Args:
            url (str): link to file on COS

        Returns:
            str: object name
        """
        begin_bucket_name = 1
        url_parsed = urlparse(url=url)
        slash_index = url_parsed.path.find("/", begin_bucket_name) + 1
        assert slash_index > begin_bucket_name, (
            f"Error! Unable to find object name: {url}"
        )
        object_name = url_parsed.path[slash_index:]
        return object_name

    @staticmethod
    def _get_epsg(item: pystac.Item | dict[str, Any]) -> Optional[int]:
        if isinstance(item, pystac.Item):
            item = item.to_dict()
        item_prop = item["properties"]
        cube_dims: dict[str, Any] = item_prop["cube:dimensions"]
        epsg = None
        for value in cube_dims.values():
            if value.get("reference_system") is not None:
                epsg = value.get("reference_system")
        return epsg

    @staticmethod
    def _get_resolution(item: pystac.Item | dict[str, Any]) -> Optional[float]:
        if isinstance(item, pystac.Item):
            item = item.to_dict()
        item_prop = item["properties"]
        cube_dims: dict[str, Any] = item_prop["cube:dimensions"]
        resolution = None
        for value in cube_dims.values():
            if value.get("step") is not None:
                resolution = float(np.abs(value.get("step")))
        return resolution

    @staticmethod
    def _convert_https_to_s3(url: str) -> str:
        """convert a https url to s3

        Args:
            url (str): link to data on COS using https scheme

        Returns:
            str: link to data on COS using s3 scheme
        """
        assert url.lower().startswith("http")
        bucket = CloudStorageFileReader._extract_bucket_name_from_url(url=url)
        object = CloudStorageFileReader._get_object(url=url)
        url = f"s3://{bucket}/{object}"
        return url

    def create_s3filesystem(
        self,
    ) -> s3fs.S3FileSystem:
        """create a s3filesystem object

        Args:
            endpoint (str): endpoint to s3
            access_key_id (str): key
            secret (str): secret

        Returns:
            s3fs.S3FileSystem: _description_
        """

        if isinstance(self.endpoint, str) and self.endpoint.startswith("https://"):
            endpoint_url = self.endpoint
        else:
            endpoint_url = f"https://{self.endpoint}"
        fs = s3fs.S3FileSystem(
            anon=False,
            endpoint_url=endpoint_url,
            key=self.access_key_id,
            secret=self.secret_access_key,
        )
        return fs

    @staticmethod
    def _get_dimension_name(
        item: Union[dict[str, Any], pystac.Item],
        axis: Optional[str] = None,
        dim_type: Optional[str] = None,
    ) -> Optional[str]:
        """get dimension name of the specified axis or the specified dim_type. Otherwise, it throws an
        exception

        Args:
            item (dict[str, Any]): STAC item
            axis (Optional[str], optional): axis name (e.g., x, y)
            dim_type (Optional[str], optional): dimension type (e.g., temporal, spatial)

        Returns:
            str: dimension name
        """
        if isinstance(item, pystac.Item):
            item = item.to_dict()
        assert isinstance(item, dict), f"Error! item is not a dict: {item}"
        item_properties = item["properties"]
        cube_dims = item_properties["cube:dimensions"]
        assert isinstance(cube_dims, dict), f"Error! Unexpected type: {cube_dims}"
        assert axis is not None or dim_type is not None
        found = False
        i = 0
        dim_list = list(cube_dims.items())
        dimension_name = None
        # iterate over dimensions until it finds one that matches axis or type
        while i < len(dim_list) and not found:
            k, v = dim_list[i]
            i += 1
            original_axis = v.get("axis")
            if axis is not None and original_axis is not None and original_axis == axis:
                dimension_name = k
                found = True
            if (
                dim_type is not None
                and v.get("type") is not None
                and v.get("type") == dim_type
            ):
                dimension_name = k
                found = True
        return dimension_name


class RasterFileReader(CloudStorageFileReader):
    DATA = "data"

    def __init__(
        self,
        items: list[dict[str, Any]],
        bands: list[str],
        bbox: Tuple[float, float, float, float],
        temporal_extent: Tuple[datetime, Optional[datetime]],
        properties: Optional[dict[str, Any]],
    ) -> None:
        super().__init__(items, bands, bbox, temporal_extent, properties)

    def load_items(self) -> xr.DataArray:
        raise NotImplementedError


class NetCDFFileReader(RasterFileReader):
    def __init__(
        self,
        items: list[dict[str, Any]],
        bands: list[str],
        bbox: Tuple[float, float, float, float],
        temporal_extent: Tuple[datetime, Optional[datetime]],
        properties: Optional[dict[str, Any]],
    ) -> None:
        super().__init__(
            items=items,
            bands=bands,
            bbox=bbox,
            temporal_extent=temporal_extent,
            properties=properties,
        )

    @staticmethod
    def _is_360_degree_system(item: dict, x_dim: str) -> bool:
        max_lon = item["properties"]["cube:dimensions"][x_dim]["extent"][1]
        if max_lon > 180:
            return True
        else:
            return False

    def _load_xarray(self, path_or_url: str) -> xr.Dataset:
        parse_url = urlparse(path_or_url)
        if parse_url.scheme == "":
            # open local file
            ds = xr.open_dataset(path_or_url, engine="netcdf4")
        # if credentials have not been set it means that data is publicly available
        elif (
            self.endpoint is None
            and self.access_key_id is None
            and self.secret_access_key is None
        ):
            # open publicly available remote file
            fs: HTTPFileSystem = fsspec.filesystem("https")
            # chunks={} to fix this issue https://github.com/fsspec/s3fs/issues/337
            try:
                ds = xr.open_dataset(fs.open(path_or_url), chunks={}, engine="h5netcdf")
            except (
                FileNotFoundError,
                aiohttp.client_exceptions.ClientResponseError,
            ) as e:
                logger.error(e)
                raise e
        else:
            # create s3 session using credentials
            s3fs = self.create_s3filesystem()
            s3_file_obj = s3fs.open(path_or_url, mode="rb")
            # open remote file
            ds = xr.open_dataset(s3_file_obj, engine="scipy")
        return ds

    def load_items(self) -> xr.DataArray:
        """load items that are associated with netcdf files

        Returns:
            xr.DataArray: raster data cube
        """
        # initialize array and crs variables
        da = None
        crs_code = None
        data_arrays = list()
        # load each item
        for item in self.items:
            assets: dict[str, dict] = item["assets"]
            asset_value = next(iter(assets.values()))
            # href field can be either URL (a link to a file on COS) or a path to a local file
            path_or_url = asset_value["href"]
            ds = self._load_xarray(path_or_url=path_or_url)
            # add temporal dimension if it does not exist on dataarray
            time_dim = CloudStorageFileReader._get_dimension_name(
                item=item, dim_type="temporal"
            )
            dt_str: str | None = item["properties"].get("datetime")

            # get dimension names
            x_dim = CloudStorageFileReader._get_dimension_name(
                item=item, axis=DEFAULT_X_DIMENSION
            )
            y_dim = CloudStorageFileReader._get_dimension_name(
                item=item, axis=DEFAULT_Y_DIMENSION
            )

            ds = expand_time_dimension(data=ds, time_dim=time_dim, dt=dt_str)
            # ds = rename_dimensions(data=ds, y_dim=y_dim, x_dim=x_dim, time_dim=time_dim)
            ds = create_missing_coords(data=ds, time_dim=time_dim)
            # get CRS
            crs_code = CloudStorageFileReader._get_epsg(item=item)
            if ds.rio.crs is None:
                ds.rio.write_crs(f"epsg:{crs_code}", inplace=True)
            assert all(band in list(ds) for band in self.bands), (
                f"Error! not all bands={self.bands} are in ds={list(ds)}"
            )
            # drop bands that are not required by the user
            ds = ds[self.bands]

            # if bands is already one of the dimensions, use default 'variable'
            if DEFAULT_BANDS_DIMENSION in dict(ds.dims).keys():
                da = ds.to_array()
            else:
                # else export array using bands
                da = ds.to_array(dim=DEFAULT_BANDS_DIMENSION)

            data_arrays.append(da)
        if len(data_arrays) > 1:
            # concatenate all xarray.DataArray objects
            data_array = xr.concat(data_arrays, dim=time_dim)
        else:
            data_array = data_arrays.pop()
        # filter by area of interest
        assert isinstance(x_dim, str)
        assert isinstance(crs_code, int), f"Error! Invalid type: {crs_code=}"
        is_360_degree_system = NetCDFFileReader._is_360_degree_system(
            item=self.items[0], x_dim=x_dim
        )

        reprojected_bbox = reproject_bbox(
            bbox=self.bbox,
            src_crs=4326,
            dst_crs=crs_code,
            is_360_degree_system=is_360_degree_system,
        )
        assert x_dim is not None and y_dim is not None, (
            f"Error! {x_dim=} and {y_dim=} cannot be None"
        )
        da = clip_box(
            data=data_array,
            bbox=reprojected_bbox,
            crs=crs_code,
            y_dim=y_dim,
            x_dim=x_dim,
        )
        # remove timestamps that have not been selected by end-user
        if time_dim is not None:
            da = filter_by_time(
                data=da, temporal_extent=self.temporal_extent, temporal_dim=time_dim
            )

        return da
