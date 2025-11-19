# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


from terrakit.download.connector import Connector
from terrakit.download.data_connectors.theweathercompany import TheWeatherCompany
import xarray as xr
import pytest
import pandas as pd


@pytest.fixture(scope="module")
def data_conn() -> Connector:
    conn = TheWeatherCompany()
    return conn


def test_load_collections(data_conn: Connector):
    collections = data_conn.list_collections()
    assert isinstance(collections, list)

    expected_collections = ["weathercompany-daily-forecast"]
    assert len(collections) == len(expected_collections), (
        f"Error! {collections} != {expected_collections}"
    )

    for c in expected_collections:
        assert isinstance(c, str), f"Error! {c} is not a string"
        assert c in collections, f"Error! {c} not found in {collections}"


@pytest.mark.parametrize(
    "data_collection_name,bbox,date_start, time_delta,data_conn",
    [
        (
            "sentinel-5p-l3grd-ch4-wfmd",
            (-102.0, 31.0, -101.0, 32.0),
            pd.Timestamp.today().date().isoformat(),
            15,
            "data_conn",
        )
    ],
    indirect=["data_conn"],
)
def test_find_data(
    data_collection_name: str,
    bbox: tuple,
    date_start: str,
    time_delta: int,
    data_conn: Connector,
):
    dt = pd.Timestamp(date_start) + pd.Timedelta(days=time_delta)
    date_end = dt.date().isoformat()
    dates, metadata = data_conn.find_data(
        data_collection_name=data_collection_name,
        bbox=bbox,
        date_start=date_start,
        date_end=date_end,
    )
    assert isinstance(dates, list), f"Error! {dates=} is not a list"
    assert len(dates) > 0
    assert all(isinstance(d, str) for d in dates)

    assert isinstance(metadata, list), f"Error! {metadata=} is not a list"


@pytest.mark.slow
@pytest.mark.parametrize(
    "data_collection_name,bbox,date_start, days_in_advance,bands, expected_sizes, data_conn",
    [
        (
            "weathercompany-daily-forecast",
            (-102.0, 31.7, -101.7, 32.0),
            pd.Timestamp.today(),
            15,
            ["temperatureMax", "temperatureMin"],
            {"longitude": 8, "latitude": 8, "time": 15, "bands": 2},
            "data_conn",
        ),
    ],
    indirect=["data_conn"],
)
def test_get_data(
    data_collection_name: str,
    bbox: tuple,
    date_start: pd.Timestamp,
    days_in_advance: int,
    bands: list[str],
    expected_sizes: dict[str, int],
    data_conn: Connector,
):
    dt = date_start + pd.Timedelta(days=days_in_advance - 1)
    date_end = dt.date().isoformat()
    date_start = date_start.date().isoformat()
    arr = data_conn.get_data(
        data_collection_name=data_collection_name,
        date_start=date_start,
        date_end=date_end,
        bands=bands,
        bbox=bbox,
    )
    assert isinstance(arr, xr.DataArray), f"Error! {arr=} is not an xarray DataArray"
    assert arr.sizes == expected_sizes, (
        f"Error! {arr.sizes=} not equal to {expected_sizes=}"
    )
    for band_name in arr.bands:
        assert band_name in bands
