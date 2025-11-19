# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


import pytest
import xarray as xr
from unittest.mock import patch
from terrakit.download.data_connectors.ibmresearch_stac import IBMResearchSTAC
from terrakit.download.data_connectors import ibmresearch_stac

from terrakit.download.raster_file_reader import NetCDFFileReader

import pandas as pd
import uuid

from tests.component_tests.component_tests_util import (
    convert_angle_to_0_360,
    create_xarray,
)


X_DIM = "x"
Y_DIM = "y"
TIME_DIM = "time"


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
    west, south, east, north = bbox
    dates = pd.date_range(start=start_dt, end=end_dt, periods=num_items)
    start = dates[0].date().isoformat()
    end = dates[-1].date().isoformat()
    for dt in dates:
        i = {
            "id": uuid.uuid4().hex,
            "collection": collection_id,
            "bbox": list(bbox),
            "properties": {
                "datetime": dt.isoformat(sep="T"),
                "cube:dimensions": {
                    X_DIM: {
                        "type": "spatial",
                        "axis": "x",
                        "extent": [west, east],
                        "reference_system": 4326,
                    },
                    Y_DIM: {
                        "type": "spatial",
                        "axis": "y",
                        "extent": [south, north],
                        "reference_system": 4326,
                    },
                    TIME_DIM: {
                        "type": "temporal",
                        "extent": [start, end],
                    },
                },
            },
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
    def __init__(
        self,
        collection_id: str,
        bbox: tuple,
        start_dt: str,
        end_dt: str,
        num_items: int = 10,
    ):
        items = list_items(
            collection_id=collection_id,
            bbox=bbox,
            num_items=num_items,
            start_dt=start_dt,
            end_dt=end_dt,
        )
        self.items = {"features": items}

    def raise_for_status(self):
        pass

    def json(self):
        return self.items


@pytest.fixture(scope="module")
def conn() -> IBMResearchSTAC:
    with patch.object(
        IBMResearchSTAC,
        "_get_all_collections",
        return_value=list_collections(["fake-collection-id"]),
    ):
        conn = IBMResearchSTAC()
        # set mock property to avoid request
        conn.access_token = "fake-access-token"
        return conn


def test_list_collections(conn: IBMResearchSTAC):
    collections = conn.list_collections()
    assert ["fake-collection-id"] == collections


def test_find_data(conn: IBMResearchSTAC):
    date_end = "2020-01-01"
    date_start = "2000-01-01"
    bbox = (-180, -90, 180, 90)
    num_items = 10
    for data_collection_name in conn.collections:
        with patch.object(
            ibmresearch_stac,
            "post",
            return_value=MockResponse(
                collection_id=data_collection_name,
                start_dt=date_start,
                end_dt=date_end,
                bbox=bbox,
                num_items=num_items,
            ),
        ):
            dates, metadata = conn.find_data(
                data_collection_name=data_collection_name,
                date_end=date_end,
                date_start=date_start,
                bbox=bbox,
            )
            assert isinstance(dates, list)
            assert all(isinstance(d, str) for d in dates)
            assert len(dates) == len(list(set(dates)))
            assert isinstance(metadata, list)
            assert len(metadata) == num_items
            assert all(isinstance(md, dict) for md in metadata)

            for item in metadata:
                assert item["collection"] == data_collection_name


@pytest.mark.parametrize(
    "start_dt, end_dt, periods, bbox, bands, is_360_degree_system, conn",
    [
        (
            "2000-01-01",
            "2001-01-01",
            1,
            (-91, 40, -90, 41),
            ["temperature"],
            False,
            "conn",
        )
    ],
    indirect=["conn"],
)
def test_get_data(
    start_dt: str,
    end_dt: str,
    periods: int,
    bbox: tuple,
    bands: list[str],
    is_360_degree_system: bool,
    conn: IBMResearchSTAC,
):
    collection_id = "fake-coll"
    items = list_items(
        collection_id=collection_id,
        start_dt=start_dt,
        end_dt=end_dt,
        bbox=bbox,
        num_items=periods,
    )
    min_x, min_y, max_x, max_y = bbox
    delta_space = 1.0
    size_x = 1000
    size_y = 1000
    x_res = (max_x - min_x) / size_x
    y_res = (max_y - min_y) / size_y
    with patch.object(IBMResearchSTAC, "_search_items", return_value=items):
        ds = create_xarray(
            start_date=start_dt,
            periods=periods,
            end_date=end_dt,
            min_x=min_x - delta_space,
            min_y=min_y - delta_space,
            max_x=max_x + delta_space,
            max_y=max_y + delta_space,
            size_x=size_x,
            size_y=size_y,
            bands=bands,
            is_360_degree_system=is_360_degree_system,
            is_dataarray=False,
        )
        with patch.object(NetCDFFileReader, "_load_xarray", return_value=ds):
            for data_collection_name in conn.collections:
                data = conn.get_data(
                    data_collection_name=data_collection_name,
                    date_end=end_dt,
                    date_start=start_dt,
                    bbox=bbox,
                    bands=bands,
                )
                assert isinstance(data, xr.DataArray)
                for c in data[Y_DIM].values:
                    assert min_y - y_res <= c <= max_y + y_res, (
                        f"Error! {min_y} <= {c} <= {max_y} is false"
                    )
                for c in data[X_DIM].values:
                    if is_360_degree_system:
                        new_min_x = convert_angle_to_0_360(min_x)
                        new_max_x = convert_angle_to_0_360(max_x)
                        assert new_min_x <= c <= new_max_x
                    else:
                        assert min_x - x_res <= c <= max_x + x_res, (
                            f"Error! {min_x} <= {c} <= {max_x} is false"
                        )
