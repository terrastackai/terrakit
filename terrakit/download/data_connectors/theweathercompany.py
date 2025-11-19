# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


# Docstrings assisted by watsonx Code Assistant
import asyncio
import bisect
import logging
from typing import Any
import aiohttp
import pandas as pd
import os
import numpy as np
import xarray as xr
from terrakit.download.connector import Connector
from terrakit.download.geodata_utils import (
    save_data_array_as_netcdf,
    validate_input_params,
    load_and_list_collections,
)
from copy import deepcopy
import nest_asyncio

nest_asyncio.apply()

logger = logging.getLogger(__name__)


class TheWeatherCompany(Connector):
    """
    A class for fetching weather data from The Weather Company API.

    Attributes:
        api_key (str): The API key for accessing The Weather Company API.
        connector_type (str): The type of data connector, which is "TheWeatherCompany".
        collections (list[dict[str, Any]]): A list of collections available from The Weather Company API.
    """

    SPATIAL_RESOLUTION = 0.04

    DATA_COLLECTION_NAME = "weathercompany-daily-forecast"
    FORECAST_HORIZONS = [3, 5, 7, 10, 15]  # days
    TIME_DIM = "time"
    X_DIM = "longitude"
    Y_DIM = "latitude"
    BANDS_DIM = "bands"
    VALID_TIME_UTC = "validTimeUtc"
    TEMPLATE_ITEM = {
        "type": "Feature",
        "bbox": [
            -180.0,
            -90.0,
            180.0,
            90.0,
        ],
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [-180.0, -90.0],
                    [180.0, -90.0],
                    [180.0, 90.0],
                    [-180.0, 90.0],
                    [-180.0, -90.0],
                ]
            ],
        },
        "properties": {"datetime": ""},
        "collection": "weathercompany-daily-forecast",
    }

    def __init__(self):
        """
        Initialize the TheWeatherCompany class by setting up the API key and collections.
        """
        self.api_key = os.environ["THE_WEATHER_COMPANY_API_KEY"]
        self.connector_type: str = "TheWeatherCompany"
        all_collections = load_and_list_collections(
            as_json=True, connector_type=self.connector_type
        )
        # so far, TheWeatherCompany has only one collection
        self.collections = [
            collection
            for collection in all_collections
            if collection["collection_name"] == TheWeatherCompany.DATA_COLLECTION_NAME
        ]

    @property
    def params(self) -> dict[str, Any]:
        """
        Get the parameters required for making requests to The Weather Company API.

        Returns:
            dict[str, Any]: A dictionary containing the API key, units, language, and format.
        """
        params = {
            "apiKey": self.api_key,
            "units": "m",
            "language": "en-US",
            "format": "json",
        }
        return params

    def _get_forecast_url(
        self, date_start: str, date_end: str, latitude: float, longitude: float
    ) -> str:
        """
        Generate a forecast URL for a given date range and geographical coordinates.

        This function constructs a URL for fetching daily weather forecasts from The Weather Company API.
        It validates the date range to ensure the start date is not in the past and the end date is not before the start date.

        Parameters:
            date_start (str): The start date of the forecast period in 'YYYY-MM-DD' format.
            date_end (str): The end date of the forecast period in 'YYYY-MM-DD' format.
            latitude (float): The latitude coordinate for the location.
            longitude (float): The longitude coordinate for the location.

        Returns:
            str: The constructed forecast URL.

        Raises:
            ValueError: If the start date is in the past or the end date is before the start date.
        """
        start = pd.Timestamp(date_start).date()
        end = pd.Timestamp(date_end).date()

        today = pd.Timestamp.today().date()
        if start < today:
            msg = f"Error! Start date ({start}) cannot be in the past!"
            raise ValueError(msg)
        if end < start:
            msg = f"Error! End date ({end}) cannot be before start date ({start})!"
            raise ValueError(msg)
        # forecast data is available from today until X days ahead
        delta_days = (end - today).days + 1
        index = bisect.bisect_right(TheWeatherCompany.FORECAST_HORIZONS, delta_days)
        if index == len(TheWeatherCompany.FORECAST_HORIZONS):
            index -= 1
        days_in_advance = TheWeatherCompany.FORECAST_HORIZONS[index]

        forecast_url = f"https://api.weather.com/v3/wx/forecast/daily/{days_in_advance}day?geocode={latitude},{longitude}"
        return forecast_url

    def find_data(
        self,
        data_collection_name,
        date_start,
        date_end,
        area_polygon=None,
        bbox=None,
        bands=...,
        maxcc=100,
        data_connector_spec=None,
    ):
        """
        Find data for the specified date range and area.

        This method generates a list of dates and corresponding items (features) for the given date range and area.

        Parameters:
            data_collection_name (str): The name of the data collection.
            date_start (str): The start date of the forecast period in 'YYYY-MM-DD' format.
            date_end (str): The end date of the forecast period in 'YYYY-MM-DD' format.
            area_polygon (list[list[float]], optional): A list of polygons defining the area of interest. Defaults to None.
            bbox (tuple[float], optional): Bounding box coordinates (west, south, east, north). Defaults to None.
            bands (list[str] | ..., optional): A list of bands (variables) to fetch from the weather data. Defaults to ... (all bands).
            maxcc (int, optional): Maximum cloud cover threshold. Defaults to 100.
            data_connector_spec (dict[str, Any], optional): Additional parameters for the data connector. Defaults to None.

        Returns:
            tuple[list[str], list[dict[str, Any]]]: A tuple containing a list of dates and a list of items (features).
        """
        validate_input_params(bbox=bbox, date_start=date_start, date_end=date_end)
        start_date = pd.Timestamp(date_start).date()
        end_date = pd.Timestamp(date_end).date()

        forecast_min_date = pd.Timestamp.today().date()
        # compute the max data by using the max forecast horizon - 1
        forecast_max_date = forecast_min_date + pd.Timedelta(
            days=TheWeatherCompany.FORECAST_HORIZONS[-1] - 1
        )
        if end_date < forecast_min_date or start_date > forecast_max_date:
            return list(), list()

        end = min(end_date, forecast_max_date)
        start = max(start_date, forecast_min_date)

        items = list()
        dt_index = pd.date_range(start=start, end=end, freq="D")
        for i in dt_index:
            item = deepcopy(self.TEMPLATE_ITEM)

            item["properties"]["datetime"] = i.strftime("%Y-%m-%d")
            items.append(item)
        dates = [i["properties"]["datetime"] for i in items]
        return dates, items

    def list_collections(self) -> list[dict[str, Any]]:
        """
        List all available collections from The Weather Company API.

        Returns:
            list[dict[str, Any]]: A list of dictionaries containing collection information.
        """
        return [c["collection_name"] for c in self.collections]

    @staticmethod
    async def fetch_url_async(
        session: aiohttp.ClientSession, url: str, params: dict, lat: float, lon: float
    ) -> dict[str, Any]:
        """
        Fetches data from a given URL asynchronously and adds latitude and longitude information to the response data.

        Parameters:
            session (aiohttp.ClientSession): The aiohttp ClientSession object used for making asynchronous HTTP requests.
            url (str): The URL to fetch data from.
            params (dict): Query parameters to be included in the URL.
            lat (float): The latitude coordinate to be added to the response data.
            lon (float): The longitude coordinate to be added to the response data.

        Returns:
            dict: The response data with added latitude and longitude information.

        Raises:
            aiohttp.ClientError: If there is an error in the HTTP request.
        """
        try:
            async with session.get(url, params=params) as response:
                response.raise_for_status()
                data: dict = await response.json()
                size = len(next(iter(data.values())))
                data[TheWeatherCompany.X_DIM] = [lon] * size
                data[TheWeatherCompany.Y_DIM] = [lat] * size
                return data
        except aiohttp.ClientError as e:
            raise e

    @staticmethod
    async def create_dataframe(
        urls: list[tuple[str, float, float]], bands: list[str] | None, params: dict
    ) -> pd.DataFrame:
        """
        Asynchronously fetches weather data from multiple URLs and creates a pandas DataFrame.

        Parameters:
            urls (list[tuple[str, float, float]]): A list of tuples containing URLs, latitude, and longitude.
            bands (list[str]|None): A list of bands (variables) to fetch from the weather data.
            params (dict): Additional parameters for the HTTP request.

        Returns:
            pd.DataFrame: A pandas DataFrame containing the fetched weather data.

        Raises:
            ValueError: If at least one of the specified bands is not present in the forecast response.
        """

        async with aiohttp.ClientSession() as session:
            tasks = [
                TheWeatherCompany.fetch_url_async(session, url, params, lat, lon)
                for url, lat, lon in urls
            ]
            results = await asyncio.gather(*tasks)
        df = None
        for forecast in results:
            # if no bands has been specified, all variables are included in the response
            if bands is None:
                bands = list(forecast.keys())
            if not all(band in forecast.keys() for band in bands):
                msg = f"Error! At least one of the band names is not in forecast response: {bands=}"
                raise ValueError(msg)
            # create a pandas DataFrame to store the results
            lat_index = forecast[TheWeatherCompany.Y_DIM]
            lon_index = forecast[TheWeatherCompany.X_DIM]
            time_index = forecast[TheWeatherCompany.VALID_TIME_UTC]
            tuples = list(zip(time_index, lat_index, lon_index))
            # create multi-index
            index = pd.MultiIndex.from_tuples(
                tuples,
                names=[
                    TheWeatherCompany.TIME_DIM,
                    TheWeatherCompany.Y_DIM,
                    TheWeatherCompany.X_DIM,
                ],
            )
            data = dict()
            for b in bands:
                data[b] = forecast[b]
            if df is None:
                df = pd.DataFrame(data=data, index=index)
            else:
                temp_df = pd.DataFrame(data=data, index=index)
                df = pd.concat([df, temp_df], ignore_index=False)
        return df

    def get_data(
        self,
        data_collection_name,
        date_start,
        date_end,
        area_polygon=None,
        bbox=None,
        bands: list[str] | None = None,
        maxcc=100,
        data_connector_spec=None,
        save_file=None,
        working_dir=".",
    ):
        """
        Fetch weather data for the specified date range and area.

        This method fetches weather data from The Weather Company API for the given date range and area,
        and returns a xarray DataArray.

        Parameters:
            data_collection_name (str): The name of the data collection.
            date_start (str): The start date of the forecast period in 'YYYY-MM-DD' format.
            date_end (str): The end date of the forecast period in 'YYYY-MM-DD' format.
            area_polygon (list[list[float]], optional): A list of polygons defining the area of interest. Defaults to None.
            bbox (tuple[float], optional): Bounding box coordinates (west, south, east, north). Defaults to None.
            bands (list[str] | ..., optional): A list of bands (variables) to fetch from the weather data. Defaults to ... (all bands).
            maxcc (int, optional): Maximum cloud cover threshold. Defaults to 100.
            data_connector_spec (dict[str, Any], optional): Additional parameters for the data connector. Defaults to None.
            save_file (str, optional): The path to save the resulting DataArray as a NetCDF file. Defaults to None.
            working_dir (str, optional): The working directory for saving the NetCDF file. Defaults to ".".

        Returns:
            xr.DataArray: A xarray DataArray containing the fetched weather data.

        Raises:
            ValueError: If the specified data collection name is invalid or no data is found for the given date range.
        """
        validate_input_params(date_start=date_start, date_end=date_end, bbox=bbox)
        logger.debug(f"Getting data: {date_start=} - {date_end=} {bands=}")
        if data_collection_name != self.DATA_COLLECTION_NAME:
            raise ValueError(
                f"Error! Invalid data collection name = ({data_collection_name})"
            )
        # convert dates to pandas
        start_date = pd.Timestamp(date_start)
        end_date = pd.Timestamp(date_end)
        # set time to include all minutes of the specified day
        end_date = end_date.replace(hour=23, minute=59, second=59)

        west, south, east, north = bbox
        longitudes = np.arange(west, east, TheWeatherCompany.SPATIAL_RESOLUTION)
        latitudes = np.arange(south, north, TheWeatherCompany.SPATIAL_RESOLUTION)
        # initialize dataframe
        df = None
        urls = list()
        # generate list of urls to be called asynchronously
        for lat in latitudes:
            for lon in longitudes:
                endpoint = self._get_forecast_url(
                    date_start=date_start,
                    date_end=date_end,
                    latitude=lat,
                    longitude=lon,
                )
                urls.append((endpoint, lat, lon))
        # get forecast data and store it into a pandas dataframe

        df = asyncio.run(
            TheWeatherCompany.create_dataframe(
                urls=urls, bands=bands, params=self.params
            )
        )

        # filter data by start and end dates
        start_ts = start_date.timestamp()
        end_ts = end_date.timestamp()
        query = f"{start_ts}<={TheWeatherCompany.TIME_DIM}<={end_ts}"
        if df is None:
            msg = f"Error! No data found for the specified date range ({date_start}%{date_end})."
            raise ValueError(msg)
        logger.debug(
            f"Query: {start_ts}({start_date=})-{end_ts}({end_date=}) - {df.shape=}"
        )
        df = df.query(query)
        # if no data has been found, raise an error
        if df.shape[0] == 0:
            raise ValueError(
                f"No data found for the specified date range: {date_start} - {date_end}"
            )
        # convert to dataset
        ds = xr.Dataset.from_dataframe(df)
        # convert to dataarray - variables will be moved to bands dim
        da = ds.to_array(dim=TheWeatherCompany.BANDS_DIM)
        if save_file:
            save_data_array_as_netcdf(da=da, save_file=save_file)
        return da
