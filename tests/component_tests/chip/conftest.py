# © Copyright IBM Corporation 2025-2026
# SPDX-License-Identifier: Apache-2.0


import numpy as np
import pytest
import rasterio
import shutil

from pathlib import Path
from rasterio.transform import from_bounds

WORKING_DIR = "tests/resources/component_test_data/chip"
DEFAULT_DATA_SUFFIX = ".tif"
DEFAULT_LABEL_SUFFIX = "_label.tif"
NAMED_BAND_NAMES = ["red", "green", "blue"]


def create_tif_with_band_descriptions(path: Path, band_names: list[str]) -> None:
    """Write a minimal single-window GeoTIFF whose bands carry rasterio descriptions."""
    size = 10
    profile = {
        "driver": "GTiff",
        "dtype": "uint16",
        "width": size,
        "height": size,
        "count": len(band_names),
        "crs": "EPSG:4326",
        "transform": from_bounds(0, 0, 1, 1, size, size),
    }
    rng = np.random.default_rng(0)
    with rasterio.open(path, "w", **profile) as dst:
        for i, name in enumerate(band_names, start=1):
            dst.write(rng.integers(100, 1000, (size, size), dtype=np.uint16), i)
            dst.set_band_description(i, name)


@pytest.fixture
def chip_and_label_setup():
    """
    Set up test copying a dummy tif file into the working directory.
    One file is called `dummy_imputed.tif`, while the other is called
    `dummy_imputed_label.tif`.
    """
    Path(WORKING_DIR).mkdir(parents=True, exist_ok=True)
    shutil.copy(
        "tests/resources/component_test_data/download/dummy.tif",
        f"{WORKING_DIR}/dummy_imputed{DEFAULT_DATA_SUFFIX}",
    )
    shutil.copy(
        "tests/resources/component_test_data/download/dummy.tif",
        f"{WORKING_DIR}/dummy_imputed{DEFAULT_LABEL_SUFFIX}",
    )


@pytest.fixture
def chip_and_label_setup_label_suffix():
    Path(WORKING_DIR).mkdir(parents=True, exist_ok=True)
    shutil.copy(
        "tests/resources/component_test_data/download/dummy.tif",
        f"{WORKING_DIR}/dummy_imputed{DEFAULT_DATA_SUFFIX}",
    )
    shutil.copy(
        "tests/resources/component_test_data/download/dummy.tif",
        f"{WORKING_DIR}/dummy_imputed.this_is_a_label_suffix.tif",
    )


@pytest.fixture
def chip_and_label_setup_data_suffix():
    Path(WORKING_DIR).mkdir(parents=True, exist_ok=True)
    shutil.copy(
        "tests/resources/component_test_data/download/dummy.tif",
        f"{WORKING_DIR}/dummy_imputed.this_is_a_data_suffix.tif",
    )
    shutil.copy(
        "tests/resources/component_test_data/download/dummy.tif",
        f"{WORKING_DIR}/dummy_imputed{DEFAULT_LABEL_SUFFIX}",
    )


@pytest.fixture
def chip_and_label_setup_file_extension():
    Path(WORKING_DIR).mkdir(parents=True, exist_ok=True)
    shutil.copy(
        "tests/resources/component_test_data/download/dummy.tif",
        f"{WORKING_DIR}/dummy_imputed.tiff",
    )
    shutil.copy(
        "tests/resources/component_test_data/download/dummy.tif",
        f"{WORKING_DIR}/dummy_imputed_label.tif",
    )


@pytest.fixture
def chip_and_label_setup_named_bands():
    """Set up test with a TIF whose bands have named descriptions (red/green/blue)."""
    Path(WORKING_DIR).mkdir(parents=True, exist_ok=True)
    data_path = Path(f"{WORKING_DIR}/dummy_named{DEFAULT_DATA_SUFFIX}")
    label_path = Path(f"{WORKING_DIR}/dummy_named{DEFAULT_LABEL_SUFFIX}")
    create_tif_with_band_descriptions(data_path, NAMED_BAND_NAMES)
    create_tif_with_band_descriptions(label_path, ["label"])


@pytest.fixture
def chip_and_label_cleanup():
    yield
    shutil.rmtree(WORKING_DIR)
