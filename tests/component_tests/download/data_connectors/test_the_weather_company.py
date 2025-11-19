# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


import pytest
import xarray as xr
from unittest.mock import patch
from terrakit.download.data_connectors.theweathercompany import TheWeatherCompany


import pandas as pd
import numpy as np
import uuid


def mock_create_dataframe(
    bbox: tuple, bands: list[str], spatial_size: int = 15, num_periods: int = 15
) -> pd.DataFrame:
    """
    Generates a mock DataFrame with random data for specified bands, spatial size, and time periods.

    Args:
        bbox (tuple): A tuple containing west, south, east, and north boundaries for spatial indexing.
        bands (list[str]): A list of band names for the DataFrame columns.
        spatial_size (int, optional): The number of points along each dimension of the spatial grid. Defaults to 15.
        num_periods (int, optional): The number of time periods to generate. Defaults to 15.

    Returns:
        pd.DataFrame: A DataFrame with multi-index containing time, latitude, and longitude dimensions.
    """
    west, south, east, north = bbox
    lat_index = np.linspace(south, north, spatial_size)
    lon_index = np.linspace(west, east, spatial_size)
    start = pd.Timestamp.today().date()
    time_index = pd.date_range(start=start, periods=num_periods, freq="D")
    ts = [t.timestamp() for t in time_index]
    tuples = list()
    for lat in lat_index:
        for lon in lon_index:
            for t in ts:
                tuples.append((t, lat, lon))
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
        data[b] = np.random.uniform(0.0, 1.0, len(index))
    return pd.DataFrame(data, index=index)


def list_collections(collection_ids: list[str]) -> list[dict]:
    colls = list()
    for cid in collection_ids:
        d = {
            "id": cid,
            "type": "Collection",
        }
        colls.append(d)
    return colls


def list_items(
    collection_id: str,
    bbox: tuple[float, float, float, float],
    start_dt: str,
    end_dt: str,
    num_items: int = 10,
) -> list[dict]:
    items = list()
    dates = pd.date_range(start=start_dt, end=end_dt, periods=num_items)
    for dt in dates:
        i = {
            "id": uuid.uuid4().hex,
            "collection": collection_id,
            "bbox": list(bbox),
            "properties": {"datetime": dt.isoformat(sep="T")},
            "assets": {
                "data": {
                    "href": "s3://fake-domain.B04.tif",
                    "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                    "roles": ["data"],
                    "title": "B04",
                    "description": "",
                }
            },
        }
        items.append(i)
    return items


class MockResponse:
    def __init__(self, bands: list[str], date_start: str, date_end: str):
        self.date_start = date_start
        self.date_end = date_end
        self.bands = bands

    def raise_for_status(self):
        pass

    def get_temporal_size(self) -> int:
        # start = pd.Timestamp(self.date_start)
        # end = pd.Timestamp(self.date_end)
        # delta = end - start
        # return delta.days
        return 15

    def json(self):
        temporal_dim = TheWeatherCompany.VALID_TIME_UTC
        temporal_size = self.get_temporal_size()
        today = pd.Timestamp.today().date()
        time_index = pd.date_range(start=today, periods=temporal_size, freq="D")
        temporal_values = [t.timestamp() for t in time_index]
        data = {temporal_dim: temporal_values}
        for band in self.bands:
            data[band] = np.random.rand(len(time_index))
        return data


@pytest.fixture(scope="module")
def conn() -> TheWeatherCompany:
    conn = TheWeatherCompany()
    # set mock property to avoid request
    return conn


def test_list_collections(conn: TheWeatherCompany):
    collections = conn.list_collections()
    assert [TheWeatherCompany.DATA_COLLECTION_NAME] == collections


@pytest.mark.parametrize(
    "date_start, time_delta, expected_num_items, conn",
    [
        (pd.Timestamp.today().date().isoformat(), 15, 15, "conn"),
        (pd.Timestamp.today().date().isoformat(), 20, 15, "conn"),
        (pd.Timestamp("2020-01-01").date().isoformat(), 20, 0, "conn"),
        (
            (pd.Timestamp.today() - pd.Timedelta(1, unit="D")).date().isoformat(),
            15,
            14,
            "conn",
        ),
        (
            (pd.Timestamp.today() + pd.Timedelta(1, unit="D")).date().isoformat(),
            15,
            14,
            "conn",
        ),
    ],
    indirect=["conn"],
)
def test_find_data(
    date_start: str, time_delta: int, expected_num_items: int, conn: TheWeatherCompany
):
    date_start_ts = pd.Timestamp(date_start)
    date_end_ts = date_start_ts + pd.Timedelta(days=time_delta - 1)
    bbox = (-180, -90, 180, 90)

    dates, metadata = conn.find_data(
        data_collection_name=conn.DATA_COLLECTION_NAME,
        date_end=date_end_ts.date().isoformat(),
        date_start=date_start_ts.date().isoformat(),
        bbox=bbox,
    )
    assert isinstance(dates, list)
    if len(dates) > 0:
        assert all(isinstance(d, str) for d in dates)
        assert len(dates) == len(list(set(dates)))
    assert isinstance(metadata, list)
    assert len(metadata) == expected_num_items, (
        f"Error: {len(metadata)} != {expected_num_items}"
    )
    if len(metadata) > 0:
        assert all(isinstance(md, dict) for md in metadata)


@pytest.mark.parametrize(
    "start_date, time_delta,bbox, bands, expected_dim_sizes, conn",
    [
        (
            pd.Timestamp.today(),
            15,
            (-91, 40, -90, 41),
            ["temperatureMax"],
            {"time": 15, "latitude": 15, "longitude": 15, "bands": 1},
            "conn",
        ),
        (
            pd.Timestamp.today(),
            12,
            (-91, 40, -90, 41),
            ["temperatureMax"],
            {"time": 12, "latitude": 15, "longitude": 15, "bands": 1},
            "conn",
        ),
        (
            pd.Timestamp.today() + pd.Timedelta(1, unit="D"),
            15,
            (-91, 40, -90, 41),
            ["temperatureMax"],
            {"time": 14, "latitude": 15, "longitude": 15, "bands": 1},
            "conn",
        ),
    ],
    indirect=["conn"],
)
def test_get_data(
    start_date: pd.Timestamp,
    time_delta: int,
    bbox: tuple,
    bands: list[str],
    expected_dim_sizes: dict[str, int],
    conn: TheWeatherCompany,
):
    collection_id = TheWeatherCompany.DATA_COLLECTION_NAME
    dt = start_date + pd.Timedelta(days=time_delta - 1)
    date_end: str = dt.date().isoformat()
    date_start: str = start_date.date().isoformat()
    response = mock_create_dataframe(bbox=bbox, bands=bands)
    with patch.object(TheWeatherCompany, "create_dataframe", return_value=response):
        arr = conn.get_data(
            data_collection_name=collection_id,
            date_start=date_start,
            date_end=date_end,
            bbox=bbox,
            bands=bands,
        )
        assert isinstance(arr, xr.DataArray)
        assert arr.sizes == expected_dim_sizes, (
            f"Error! {arr.sizes} != {expected_dim_sizes}"
        )
        west, south, east, north = bbox
        for lat in arr.latitude.values:
            assert north >= lat >= south
        for lon in arr.longitude.values:
            assert east >= lon >= west
