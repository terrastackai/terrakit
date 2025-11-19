# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


import os
import pytest
import shutil

DEFAULT_WORKING_DIR = "./tmp"


@pytest.fixture
def default_dir_clean_up():
    yield
    print(f"Test clean up. Deleting {DEFAULT_WORKING_DIR}")
    if os.path.exists(DEFAULT_WORKING_DIR):
        shutil.rmtree(DEFAULT_WORKING_DIR)
