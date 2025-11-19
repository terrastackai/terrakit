# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


# Assisted by watsonx Code Assistant
import boto3
import dotenv
import glob
import logging
import os
import rioxarray
import rasterio as rio
import subprocess
import xarray as xr
import warnings

from datetime import datetime
from joblib import Parallel, delayed
from rasterio.session import AWSSession
from rasterio.errors import RasterioIOError
from shapely import box
from tqdm import tqdm
from typing import Any, Union

from ..geodata_utils import (
    load_and_list_collections,
    save_data_array_to_file,
)
from ...general_utils.rest import get, post
from ..connector import Connector
from ...general_utils.exceptions import (
    TerrakitValidationError,
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

dotenv.load_dotenv()


warnings.simplefilter(action="ignore", category=FutureWarning)

# environment variables required for rasterio to load data directly from COG url.
os.environ["GDAL_HTTP_COOKIEFILE"] = "~/cookies.txt"
os.environ["GDAL_HTTP_COOKIEJAR"] = "~/cookies.txt"
os.environ["GDAL_DISABLE_READDIR_ON_OPEN"] = "EMPTY_DIR"
os.environ["CPL_VSIL_CURL_ALLOWED_EXTENSIONS"] = "TIF"
NASA_EARTH_BEARER_TOKEN = os.getenv("NASA_EARTH_BEARER_TOKEN", "")


def get_temp_creds():
    headers = {"Authorization": f"Bearer {NASA_EARTH_BEARER_TOKEN}"}
    temp_creds_url = "https://data.lpdaac.earthdatacloud.nasa.gov/s3credentials"
    if NASA_EARTH_BEARER_TOKEN:
        resp = get(temp_creds_url, headers=headers)
    else:
        resp = get(temp_creds_url)
    if "Content-Type" in resp.headers.keys() and "text" in resp.headers["Content-Type"]:
        if "The Earthdata Login" in resp.text:
            err_msg = "An error occurring while logging into NASA Earthdata. Please check credentials."
            logger.error(err_msg)
            raise TerrakitValueError(err_msg, resp.text)
    return resp.json()


def connect_to_stac(stac_url, subcatalog_name=None):
    stac_response = get(stac_url).json()  # Call the STAC API endpoint
    logger.info(
        f"You are now using the {stac_response['id']} API (STAC Version: {stac_response['stac_version']}). {stac_response['description']}"
    )
    # print(f"There are {len(stac_response['links'])} STAC catalogs available in CMR.")

    if subcatalog_name is not None:
        stac_lp = [
            s for s in stac_response["links"] if "LPCLOUD" in s["title"]
        ]  # Search for only LP-specific catalogs
        lp_cloud = get(
            [s for s in stac_lp if s["title"] == subcatalog_name][0]["href"]
        ).json()
        lp_links = lp_cloud["links"]
        lp_search = [link["href"] for link in lp_links if link["rel"] == "search"][0]
        lp_search = [l["href"] for l in lp_links if l["rel"] == "search"][0]  # noqa
        return lp_search
    else:
        lp_links = stac_response["links"]
        lp_search = [link["href"] for link in lp_links if link["rel"] == "search"][0]
        return lp_search


def find_items(
    lp_search,
    bbox,
    from_datetime,
    to_datetime,
    collections=["HLSS30.v2.0", "HLSL30.v2.0"],
    limit=250,
):
    check_start_end_date(date_start=from_datetime, date_end=to_datetime)
    check_bbox(bbox=bbox, connector_type="nasa_earthdata")

    params = {}
    params["bbox"] = bbox

    if limit > 250:
        logger.warning("API Limit max is 250, changing value to 250")
        limit = 250

    params["limit"] = limit
    if "T" not in from_datetime:
        from_datetime = from_datetime + "T00:00:00Z"
    if "T" not in to_datetime:
        to_datetime = to_datetime + "T23:59:59Z"

    date_time = (
        from_datetime + "/" + to_datetime
    )  # Define start time period / end time period
    params["datetime"] = date_time
    params["collections"] = collections
    params

    hls_items = post(lp_search, payload=params).json()

    logger.info("Number of items found: " + str(len(hls_items["features"])))

    return hls_items["features"]


def get_band(date_items, band, bbox, temp_creds_req, working_dir):
    """
    Downloads satellite imagery data for a given band, builds a VRT, clips the data,
    and reprojects it to EPSG:4326.

    Args:
        date_items (list): List of dictionaries containing date-specific item data.
        band (str): The band of interest (e.g., 'B01', 'B02').
        bbox (tuple): Bounding box coordinates (left, bottom, right, top) in EPSG:4326.
        temp_creds_req (dict): Temporary AWS credentials required for accessing data.
        working_dir (str): Working directory for storing temporary files.

    Returns:
        xarray.Dataset: Processed satellite imagery data clipped and reprojected.
    """

    # Extract links for the given band from date_items
    links = [X["assets"][band]["href"] for X in date_items]

    # Extract the date for the given band from date_items
    date = [X["properties"]["datetime"].split("T")[0] for X in date_items][0]

    # Open the first link to get the CRS (Coordinate Reference System)
    try:
        with rio.open(links[0]) as src:
            hls_proj = src.crs.to_string()
    except RasterioIOError as e:
        err_msg = f"An error occurred opening a remote link {links[0]}."
        logger.error(err_msg)
        raise TerrakitBaseException(err_msg, e)

    # Transform the bounding box to the CRS of the first link
    hls_bbox = list(rio.warp.transform_bounds("EPSG:4326", hls_proj, *bbox))

    # Prepare the gdalbuildvrt command based on whether a NASA_EARTH_BEARER_TOKEN is available
    if NASA_EARTH_BEARER_TOKEN:
        build_vrt = f"gdalbuildvrt {working_dir}/links_{band}_{date}.vrt -separate --config GDAL_HTTP_AUTH BEARER --config GDAL_HTTP_BEARER {NASA_EARTH_BEARER_TOKEN} --config AWS_ACCESS_KEY_ID {temp_creds_req['accessKeyId']} --config AWS_SECRET_ACCESS_KEY {temp_creds_req['secretAccessKey']} --config AWS_SESSION_TOKEN {temp_creds_req['sessionToken']} --config GDAL_DISABLE_READDIR_ON_OPEN TRUE"
    else:
        build_vrt = f"gdalbuildvrt {working_dir}/links_{band}_{date}.vrt -separate --config AWS_ACCESS_KEY_ID {temp_creds_req['accessKeyId']} --config AWS_SECRET_ACCESS_KEY {temp_creds_req['secretAccessKey']} --config AWS_SESSION_TOKEN {temp_creds_req['sessionToken']} --config GDAL_DISABLE_READDIR_ON_OPEN TRUE"

    # Combine the gdalbuildvrt command with the list of links
    build_vrt = " ".join([build_vrt] + links)

    # Execute the gdalbuildvrt command using subprocess
    subprocess.call(build_vrt, shell=True)

    # Define chunking parameters for efficient reading of the VRT
    chunks = dict(band=1, x=512, y=512)

    # Open the VRT file using rioxarray
    data = rioxarray.open_rasterio(
        f"{working_dir}/links_{band}_{date}.vrt", chunks=chunks
    )

    # Rename the 'band' dimension to 'time' and add a new 'band' dimension
    data = data.rename({"band": "time"})
    data = data.expand_dims(dim="band")
    data["band"] = [band]

    # Compute the maximum value across the 'time' dimension
    data = data.max(dim="time")

    # Clip the data to the specified bounding box
    hls_clip = box(*hls_bbox)
    data = data.rio.clip([hls_clip])

    # Reproject the data to EPSG:4326
    data = data.rio.reproject("EPSG:4326")

    return data


######################################################################################################
###  Connector class
######################################################################################################


class NASA_EarthData(Connector):
    """
    Class to interact with NASA EarthData connector for listing collections and fetching data.
    """

    def __init__(self):
        """
        List available collections.
        """
        self.connector_type = "nasa_earthdata"
        self.collections: list[Any] = load_and_list_collections(
            connector_type="nasa_earthdata"
        )
        self.collections_details = load_and_list_collections(
            as_json=True, connector_type="nasa_earthdata"
        )
        self.lp_search = connect_to_stac(
            stac_url="https://cmr.earthdata.nasa.gov/stac/", subcatalog_name="LPCLOUD"
        )

    def list_collections(self) -> list:
        """
        Returns the current list of collections for th NASA EarthData connector.

        Returns:
            list: The list of collections managed by the class.
        """
        logger.info("Listing available collections")
        return self.collections

    def find_data(
        self,
        data_collection_name,
        date_start,
        date_end,
        area_polygon=None,
        bbox=None,
        bands=[],
        maxcc=100,
        data_connector_spec=None,
    ) -> Union[tuple[list[Any], list[dict[str, Any]]], tuple[None, None]]:
        """
        Finds data items in the specified collection, date range, and area.

        Args:
            data_collection_name (str): The name of the data collection to search.
            date_start (str): The start date for the search (YYYY-MM-DD).
            date_end (str): The end date for the search (YYYY-MM-DD).
            area_polygon (list, optional): Polygon defining the area of interest. Defaults to None.
            bbox (list, optional): Bounding box defining the area of interest [west, south, east, north]. Defaults to None.
            bands (list, optional): List of bands to retrieve. Defaults to [].
            maxcc (int, optional): Maximum cloud cover percentage. Defaults to 100.
            data_connector_spec (dict, optional): Additional data connector specifications. Defaults to None.

        Returns:
            tuple: A tuple containing unique dates and the list of data items.
        """

        # Check credentials have been set correctly.
        if "NASA_EARTH_BEARER_TOKEN" not in os.environ:
            raise TerrakitValidationError(
                message="Error: Missing credentials 'NASA_EARTH_BEARER_TOKEN'. Please update .env with correct credentials."
            )

        # Check data_collection_name exists in self.collections.
        check_collection_exists(data_collection_name, self.collections)

        logger.info("Listing NASA Earthdata data")

        items = find_items(
            self.lp_search,
            bbox,
            date_start,
            date_end,
            collections=[data_collection_name],
            limit=250,
        )

        if maxcc:
            items = [
                item
                for item in items
                if item["properties"].get("eo:cloud_cover") < maxcc
            ]
        items = [
            {
                "id": item["id"],
                "properties": {
                    "datetime": item["properties"]["datetime"],
                    "eo:cloud_cover": item["properties"]["eo:cloud_cover"],
                },
            }
            for item in items
        ]
        unique_dates = sorted(
            set(([X["properties"]["datetime"].split("T")[0] for X in items]))
        )

        logger.info(f"Found {len(unique_dates)} unique dates:  {unique_dates}")

        return unique_dates, items

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
        Fetches data from NASA EarthData connector for the specified collection, date range, area, and bands.

        Args:
            data_collection_name (str): The name of the data collection to fetch.
            date_start (str): The start date for the search (YYYY-MM-DD).
            date_end (str): The end date for the search (YYYY-MM-DD).
            area_polygon (list, optional): Polygon defining the area of interest. Defaults to None.
            bbox (list, optional): Bounding box defining the area of interest [west, south, east, north]. Defaults to None.
            bands (list): List of bands to fetch. Defaults to [].
            maxcc (int, optional): Maximum cloud cover percentage. Defaults to 100.
            data_connector_spec (dict, optional): Additional data connector specifications. Defaults to None.
            save_file (str, optional): Path to save the fetched data. Defaults to None.
            working_dir (str, optional): Working directory for temporary files. Defaults to ".".

        Returns:
            xarray: An xarray Datasets containing the fetched data with dimensions (time, band, y, x).
        """
        # Check credentials have been set correctly.
        if "NASA_EARTH_BEARER_TOKEN" not in os.environ:
            raise TerrakitValidationError(
                message="Error: Missing credentials 'NASA_EARTH_BEARER_TOKEN'. Please update .env with correct credentials."
            )

        # Check data_collection_name exists in self.collections.
        if data_collection_name not in self.collections:
            raise TerrakitValueError(
                message=f"Invalid collection '{data_collection_name}'. Please choose from one of the following collection {self.collections}"
            )

        check_area_polygon(
            area_polygon=area_polygon, connector_type=self.connector_type
        )
        temp_creds_req = get_temp_creds()

        session = boto3.Session(
            aws_access_key_id=temp_creds_req["accessKeyId"],
            aws_secret_access_key=temp_creds_req["secretAccessKey"],
            aws_session_token=temp_creds_req["sessionToken"],
            region_name="us-west-2",
        )

        if NASA_EARTH_BEARER_TOKEN:
            rio_env = rio.Env(
                AWSSession(session),
                GDAL_HTTP_AUTH="BEARER",  # pragma: allowlist secret
                GDAL_HTTP_BEARER=NASA_EARTH_BEARER_TOKEN,
                GDAL_DISABLE_READDIR_ON_OPEN="TRUE",
                GDAL_HTTP_COOKIEFILE=os.path.expanduser("~/cookies.txt"),
                GDAL_HTTP_COOKIEJAR=os.path.expanduser("~/cookies.txt"),
            )
        else:
            rio_env = rio.Env(
                AWSSession(session),
                GDAL_DISABLE_READDIR_ON_OPEN="TRUE",
                GDAL_HTTP_COOKIEFILE=os.path.expanduser("~/cookies.txt"),
                GDAL_HTTP_COOKIEJAR=os.path.expanduser("~/cookies.txt"),
            )

        with rio_env:
            unique_dates: Union[list[str], None]
            results: Union[list[dict[str, Any]], None]

            results = find_items(
                self.lp_search,
                bbox,
                date_start,
                date_end,
                collections=[data_collection_name],
                limit=250,
            )
            if maxcc:
                results = [
                    item
                    for item in results
                    if item["properties"].get("eo:cloud_cover") < maxcc
                ]
            unique_dates = sorted(
                set(([X["properties"]["datetime"].split("T")[0] for X in results]))
            )
            # Check that unique dates and find_data results are not None.
            if unique_dates is None and results is None:
                logger.warning("Warning: Unique dates and find_data results are None")
                return None

            ds: xr.DataArray
            ds_list: list[Any] = []
            for udate in unique_dates:  # type: ignore[union-attr]
                date_items: list[Any] = []
                for X in results:  # type: ignore[union-attr]
                    if X["properties"]["datetime"].split("T")[0] == udate:
                        date_items.append(X)
                num_threads = len(bands)
                ans = Parallel(n_jobs=num_threads, prefer="threads")(
                    delayed(get_band)(date_items, b, bbox, temp_creds_req, working_dir)
                    for b in tqdm(bands)
                )
                da = xr.concat(ans, dim="band")

                data_date_datetime = datetime.strptime(udate, "%Y-%m-%d")
                da = da.assign_coords({"band": bands, "time": data_date_datetime})

                ds_list.append(da)
            ds = xr.concat(ds_list, dim="time")

            save_data_array_to_file(ds, save_file)
            deleteList = glob.glob(f"{working_dir}/links_*.vrt", recursive=True)
            for file_to_delete in deleteList:
                try:
                    os.remove(file_to_delete)
                except OSError as err:
                    logger.error("Error while deleting file", err)

            return ds
