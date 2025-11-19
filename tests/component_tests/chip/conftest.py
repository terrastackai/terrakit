# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


import pytest
import shutil

from pathlib import Path

WORKING_DIR = "tests/resources/component_test_data/chip"
DEFAULT_DATA_SUFFIX = ".tif"
DEFAULT_LABEL_SUFFIX = "_labels.tif"


@pytest.fixture
def chip_and_label_setup():
    """
    Set up test copying a dummy tif file into the working directory.
    One file is called `dummy_imputed.tif`, while the other is called
    `dummy_imputed_labels.tif`.
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
        f"{WORKING_DIR}/dummy_imputed_labels.tiff",
    )


@pytest.fixture
def chip_and_label_cleanup():
    yield
    shutil.rmtree(WORKING_DIR)
