# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


import dotenv
import json
import numpy as np
import os
import pystac
import pytest
import rioxarray
import shutil
import stackstac
import xarray as xr

from glob import glob
from pathlib import Path
from sentinelhub import SentinelHubRequest
from unittest.mock import MagicMock


############################# Test Parameters ############################

SAVE_FILE_DIR = "./tests/resources/component_test_data"
DEFAULT_WORKING_DIR = "./tmp"


@pytest.fixture
def start_date():
    return "2024-01-01"


@pytest.fixture
def end_date():
    return "2024-01-31"


@pytest.fixture
def bbox():
    return [34.671440, -0.090887, 34.706448, -0.087678]


@pytest.fixture
def save_file_dir():
    return SAVE_FILE_DIR


###################################################################################################


############################# TEST SETUP AND TEARDOWN ############################


@pytest.fixture
def unset_evn_vars():
    if (
        os.getenv("SH_CLIENT_ID")
        and os.getenv("SH_CLIENT_SECRET")
        and os.getenv("NASA_EARTH_BEARER_TOKEN")
    ):
        del os.environ["SH_CLIENT_ID"]
        del os.environ["SH_CLIENT_SECRET"]
        del os.environ["NASA_EARTH_BEARER_TOKEN"]


@pytest.fixture
def invalid_evn_vars():
    os.environ["SH_CLIENT_ID"] = "<invalid>"
    os.environ["SH_CLIENT_SECRET"] = "<invalid>"
    os.environ["NASA_EARTH_BEARER_TOKEN"] = "<invalid>"


@pytest.fixture
def reset_dot_env():
    yield
    dotenv.load_dotenv()


@pytest.fixture
def get_data_clean_up():
    yield
    files = glob(f"{SAVE_FILE_DIR}/*.tif")
    print(f"Test clean up. Deleting {files}")
    for f in files:
        os.remove(f)


@pytest.fixture
def download_data_setup():
    print("Starting test...")
    # Ensure dir does not exist before starting
    if os.path.exists(DEFAULT_WORKING_DIR):
        print(f"{DEFAULT_WORKING_DIR} being deleted before starting the test...")
        shutil.rmtree(DEFAULT_WORKING_DIR)
    print(f"Creating {DEFAULT_WORKING_DIR}..")
    Path(DEFAULT_WORKING_DIR).mkdir(parents=True, exist_ok=True)
    shp_files = glob(
        "./tests/resources/component_test_data/download/terrakit_curated_dataset_all_bbox*"
    )
    for shp_file in shp_files:
        shutil.copy(shp_file, DEFAULT_WORKING_DIR)
    shp_files = glob(
        "./tests/resources/component_test_data/download/terrakit_curated_dataset_labels*"
    )
    for shp_file in shp_files:
        shutil.copy(shp_file, DEFAULT_WORKING_DIR)


@pytest.fixture
def download_data_setup_classes():
    """Setup fixture for multi-class label tests using pre-generated shapefiles."""
    print("Starting multi-class label test...")
    # Ensure dir does not exist before starting
    if os.path.exists(DEFAULT_WORKING_DIR):
        print(f"{DEFAULT_WORKING_DIR} being deleted before starting the test...")
        shutil.rmtree(DEFAULT_WORKING_DIR)
    print(f"Creating {DEFAULT_WORKING_DIR}..")
    Path(DEFAULT_WORKING_DIR).mkdir(parents=True, exist_ok=True)
    
    # Copy pre-generated class label shapefiles
    shp_files = glob(
        "./tests/resources/component_test_data/download/terrakit_curated_dataset_classes_all_bbox*"
    )
    for shp_file in shp_files:
        shutil.copy(shp_file, DEFAULT_WORKING_DIR)
    shp_files = glob(
        "./tests/resources/component_test_data/download/terrakit_curated_dataset_classes_labels*"
    )
    for shp_file in shp_files:
        shutil.copy(shp_file, DEFAULT_WORKING_DIR)


###################################################################################################


############################# NASA EARTHDATA helper functions and fixtures ############################
def find_data_nasa_earthdata(*args, **kwargs):
    with open("./tests/resources/nasa_earthdata/find_items.json", "r") as f:
        items = json.load(f)
    return items


def mock_save_nasa_earthdata(*args, **kwargs):
    raster_file = kwargs["raster_path"].split("/")[-1]
    shutil.copy(
        "tests/resources/component_test_data/download/dummy.tif", f"./tmp/{raster_file}"
    )


def stac_earthdata_response():
    with open("./tests/resources/nasa_earthdata/connect_to_stac.json", "r") as f:
        response = json.load(f)
    return response


def stac_earthdata_lp_response():
    with open(
        "tests/resources/nasa_earthdata/connect_to_stac_search_lp_specific.json", "r"
    ) as f:
        response = json.load(f)
    return response


def lp_search_earthdata_post_response():
    with open("./tests/resources/nasa_earthdata/land_process_post.json", "r") as f:
        response = json.load(f)
    return response


def s3_cred_earthdata_url_response():
    with open("tests/resources/nasa_earthdata/get_s3credentials.json", "r") as f:
        response = json.load(f)
    return response


@pytest.fixture
def mock_setup_nasa(requests_mock):
    requests_mock.get(
        "https://cmr.earthdata.nasa.gov/stac/",
        json=stac_earthdata_response(),
        status_code=200,
    )
    requests_mock.get(
        "https://cmr.earthdata.nasa.gov/stac/LPCLOUD",
        json=stac_earthdata_lp_response(),
        status_code=200,
    )
    requests_mock.post(
        "https://cmr.earthdata.nasa.gov/stac/LPCLOUD/search",
        json=lp_search_earthdata_post_response(),
        status_code=200,
    )
    requests_mock.get(
        "https://data.lpdaac.earthdatacloud.nasa.gov/s3credentials",
        json=s3_cred_earthdata_url_response(),
        status_code=200,
    )


@pytest.fixture()
def mock_nasa_find_datasets(mocker, mock_setup_nasa):
    mock_find_items_nasa = mocker.patch(
        "terrakit.download.data_connectors.nasa_earthdata.find_items",
        side_effect=find_data_nasa_earthdata,
    )
    return mock_find_items_nasa


@pytest.fixture()
def mock_nasa_download_datasets(mocker, mock_setup_nasa, mock_nasa_find_datasets):
    mock_get_band = mocker.patch(
        "terrakit.download.data_connectors.nasa_earthdata.get_band",
        return_value=xr.DataArray(
            np.random.rand(1, 3660, 3660),
            coords=[[1], np.arange(3660), np.arange(3660)],
            dims=["band", "y", "x"],
            attrs={"_FillValue": -9999, "scale_factor": 0.0001, "add_offset": 0.0},
        ),
    )
    mock_to_raster = mocker.patch(
        "rioxarray.raster_array.RasterArray.to_raster",
        side_effect=mock_save_nasa_earthdata,
    )
    return mock_nasa_find_datasets, mock_get_band, mock_to_raster


###################################################################################################


############################# SENTINELHUB helper functions and fixtures ############################
@pytest.fixture
def expected_dates_sentinelhub() -> list:
    return [
        "2024-01-01",
        "2024-01-06",
        "2024-01-11",
        "2024-01-16",
        "2024-01-21",
        "2024-01-26",
        "2024-01-31",
    ]


def catalog_search_response():
    with open("./tests/resources/sentinelhub/find_data_results.json", "r") as f:
        features = json.load(f)
    return {
        "type": "FeatureCollection",
        "features": features,
        "links": [
            {
                "href": "https://services.sentinel-hub.com/api/v1/catalog/1.0.0/search",
                "rel": "self",
                "type": "application/geo+json",
            }
        ],
        "context": {"limit": 100, "returned": 12},
    }


@pytest.fixture
def mock_setup_sentinelhub(requests_mock):
    requests_mock.get(
        "https://services.sentinel-hub.com/api/v1/catalog/1.0.0/search",
        json={},
        status_code=200,
    )
    requests_mock.post(
        "https://services.sentinel-hub.com/auth/realms/main/protocol/openid-connect/token",
        json={"access_token": "", "expires_at": 3601},
        status_code=200,
    )
    res = catalog_search_response()
    requests_mock.post(
        "https://services-uswest2.sentinel-hub.com/api/v1/catalog/1.0.0/search",
        json=res,
        status_code=200,
    )
    requests_mock.post(
        "https://services.sentinel-hub.com/api/v1/catalog/1.0.0/search",
        json=res,
        status_code=200,
    )
    return requests_mock


@pytest.fixture
def mock_sentinelhub_save_data(mocker, mock_setup_sentinelhub):
    mock_save_data = mocker.patch.object(
        SentinelHubRequest, "save_data", new_callable=sh_request_mock
    )
    return mock_setup_sentinelhub, mock_save_data


# Docstrings assisted by watsonx Code Assistant
class sh_request_mock(MagicMock):
    """
    A mock class for simulating Sentinel Hub request behavior.

    This class extends MagicMock to provide a custom behavior when an instance is called.
    Upon calling, it saves test data to a specified directory.
    """

    def __call__(self):
        """
        Simulate the execution of a Sentinel Hub request by saving test data.

        This method is called when an instance of sh_request_mock is invoked like a function.
        It saves 'response.tiff' and 'request.json' files from the source directory to the destination directory.
        """
        self.save_data()

    def save_data(self):
        """
        Save test data files to the specified directory.

        This method creates the destination directory if it doesn't exist and copies
        'response.tiff' and 'request.json' files from the source directory to the destination.
        """
        dst_dir = "./sh_data/test_data"
        # Create the destination directory if it doesn't exist
        Path(dst_dir).mkdir(parents=True, exist_ok=True)

        # Define source file paths
        src_dir = "./tests/resources/sentinelhub/sh_data/test_data"

        # Define source file paths
        src_tiff = f"{src_dir}/response.tiff"
        src_json = f"{src_dir}/request.json"

        # Copy 'response.tiff' file
        shutil.copyfile(src_tiff, f"{dst_dir}/response.tiff")

        # Copy 'request.json' file
        shutil.copyfile(src_json, f"{dst_dir}/request.json")


###################################################################################################


############################# SENTINEL AWS helper functions and fixtures ############################


def mock_stac_aws_get_items(*args, **kwargs):
    catalog = pystac.Catalog(
        id="catalog-with-collection",
        catalog_type="FeatureCollection",
        description="Test catalog",
    )
    with open("./tests/resources/sentinel_aws/items.json", "r") as f:
        find_data_items = json.load(f)
    stac_items = pystac.ItemCollection.from_dict(find_data_items, root=catalog)
    return stac_items


@pytest.fixture
def mock_stackstac(mocker):
    # The mock stac catalogue does not match the bbox for labels.shp so lets mock the stackstac call.
    stac_items = mock_stac_aws_get_items()
    mock_stackstac_stack = mocker.patch(
        "stackstac.stack",
        return_value=stackstac.stack(stac_items, epsg=4326, sortby_date="asc"),
    )
    print("Running mock for stack stac here")
    mock_stackstac_stack


@pytest.fixture
def mock_aws_find_items(mocker):
    mock_get_stac_items = mocker.patch(
        "terrakit.download.data_connectors.sentinel_aws.stac_get_items",
        side_effect=mock_stac_aws_get_items,
    )
    return mock_get_stac_items


@pytest.fixture
def mock_aws_get_data(mocker, mock_aws_find_items):
    mock_get_sh_aws_data = mocker.patch(
        "terrakit.download.data_connectors.sentinel_aws.get_sh_aws_data",
        return_value=rioxarray.open_rasterio(
            "tests/resources/sentinel_aws/aws_test_data.tif"
        ),
    )
    return mock_aws_find_items, mock_get_sh_aws_data


###################################################################################################
