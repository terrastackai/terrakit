# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


import pytest
import shutil

from pathlib import Path

DUMMY_DATA_DIR = "./tests/resources/component_test_data/store"
WORKING_DIR = f"{DUMMY_DATA_DIR}/tmp"


@pytest.fixture
def taco_setup():
    """
    Set up test copying a dummy tif file into the working directory.
    One file is called `dummy_imputed.tif`, while the other is called
    `dummy_imputed_labels.tif`.
    """
    print("Setting up test data")
    Path(WORKING_DIR).mkdir(parents=True, exist_ok=True)
    for i in range(1, 10):
        shutil.copy(
            f"{DUMMY_DATA_DIR}/dummy_2025-01-01_imputed_0.data.tif",
            f"{WORKING_DIR}/dummy_2025-01-01_imputed_{i}.data.tif",
        )
        shutil.copy(
            f"{DUMMY_DATA_DIR}/dummy_2025-01-01_imputed_0.label.tif",
            f"{WORKING_DIR}/dummy_2025-01-01_imputed_{i}.label.tif",
        )


@pytest.fixture
def create_dummy_shpfile():
    Path(f"{WORKING_DIR}/test_dataset_labels.prj").touch()
    Path(f"{WORKING_DIR}/test_dataset_labels.dbf").touch()
    Path(f"{WORKING_DIR}/test_dataset_labels.shp").touch()
    Path(f"{WORKING_DIR}/test_dataset_labels.shx").touch()
    Path(f"{WORKING_DIR}/test_dataset_all_bboxes.cpg").touch()
    Path(f"{WORKING_DIR}/test_dataset_all_bboxes.prj").touch()
    Path(f"{WORKING_DIR}/test_dataset_all_bboxes.dbf").touch()
    Path(f"{WORKING_DIR}/test_dataset_all_bboxes.shp").touch()
    Path(f"{WORKING_DIR}/test_dataset_all_bboxes.shx").touch()


@pytest.fixture
def store_cleanup():
    yield
    shutil.rmtree(WORKING_DIR)
