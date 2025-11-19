# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


from terrakit.download.connector import Connector
from terrakit.download.data_connectors.ibmresearch_stac import IBMResearchSTAC
import xarray as xr
import pytest


@pytest.fixture(scope="module")
def data_conn() -> Connector:
    conn = IBMResearchSTAC()
    return conn


@pytest.mark.slow
def test_load_collections(data_conn: Connector):
    collections = data_conn.list_collections()
    assert isinstance(collections, list)
    assert len(collections) > 0
    assert all(isinstance(c, str) for c in collections)
    expected_collections = [
        "ukcp18-land-cpm-uk-2.2km",
        "ch4",
        "sentinel-5p-l3grd-ch4-wfmd",
        "nex-gddp-cmip6-historical",
    ]
    assert len(collections) == len(expected_collections), (
        f"Error! different sizes: {collections} != {expected_collections}"
    )
    for c in collections:
        assert c in expected_collections, f"Error! {c} not in {expected_collections}"


@pytest.mark.slow
@pytest.mark.parametrize(
    "data_collection_name,bbox,date_start,date_end,data_conn",
    [
        (
            "sentinel-5p-l3grd-ch4-wfmd",
            (-102.0, 31.0, -101.0, 32.0),
            "2024-01-01",
            "2024-02-01",
            "data_conn",
        ),
        (
            "nex-gddp-cmip6-historical",
            (-102.0, 31.0, -101.0, 32.0),
            "2009-01-01",
            "2009-02-01",
            "data_conn",
        ),
    ],
    indirect=["data_conn"],
)
def test_find_data(
    data_collection_name: str,
    bbox: tuple,
    date_start: str,
    date_end: str,
    data_conn: Connector,
):
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
    "data_collection_name,bbox,date_start,date_end,data_conn",
    [
        (
            "HLS_S30",
            (-102.0, 31.0, -101.0, 32.0),
            "2024-01-01",
            "2024-02-01",
            "data_conn",
        )
    ],
    indirect=["data_conn"],
)
def test_find_data_invalid(
    data_collection_name: str,
    bbox: tuple,
    date_start: str,
    date_end: str,
    data_conn: Connector,
):
    with pytest.raises(ValueError) as excinfo:
        data_conn.find_data(
            data_collection_name=data_collection_name,
            bbox=bbox,
            date_start=date_start,
            date_end=date_end,
        )
    assert "is not supported" in str(excinfo.value)


@pytest.mark.slow
@pytest.mark.parametrize(
    "data_collection_name,bbox,date_start,date_end,bands, expected_sizes, data_conn",
    [
        (
            "sentinel-5p-l3grd-ch4-wfmd",
            (-102.0, 31.9, -101.9, 32.0),
            "2024-01-01",
            "2024-01-03",
            ["CH4_column_volume_mixing_ratio"],
            {"longitude": 4, "latitude": 4, "time": 3, "bands": 1},
            "data_conn",
        ),
        (
            "ch4",
            (-102.0, 31.9, -101.9, 32.0),
            "2024-12-31",
            "2025-01-02",
            ["CH4_column_volume_mixing_ratio_dry_air"],
            {"longitude": 4, "latitude": 4, "time": 2, "bands": 1},
            "data_conn",
        ),
        (
            "nex-gddp-cmip6-historical",
            (-102.0, 31.0, -101.0, 32.0),
            "2000-01-01",
            "2001-01-02",
            ["pr"],
            {"lon": 4, "lat": 4, "time": 731, "bands": 1},
            "data_conn",
        ),
    ],
    indirect=["data_conn"],
)
def test_get_data(
    data_collection_name: str,
    bbox: tuple,
    date_start: str,
    date_end: str,
    bands: list[str],
    expected_sizes: dict[str, int],
    data_conn: Connector,
):
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
