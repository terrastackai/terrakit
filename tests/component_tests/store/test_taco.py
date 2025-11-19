# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


import pytest
import os

from terrakit.store.taco import taco_store_data
from tests.component_tests.store.conftest import WORKING_DIR


class TestStore:
    def test_taco_store_data(self, taco_setup, store_cleanup):
        taco_store_data(
            dataset_name="test",
            working_dir=WORKING_DIR,
            tortilla_name="test.tortilla",
            save_dir=WORKING_DIR,
        )
        assert "test.tortilla" in os.listdir(WORKING_DIR)

    @pytest.mark.skip(
        "WiP: Expected this test to fail as there appears to be an issue when running labels_to_data.py which is resolved if the shp files are removed from the working dir."
    )
    def test_taco_store_data_shp_files(
        self, taco_setup, create_dummy_shpfile, store_cleanup
    ):
        """
        Test that the store data function works even if shapefiles exist in the working directory
        """

        taco_store_data(
            dataset_name="test",
            working_dir=WORKING_DIR,
            tortilla_name="test.tortilla",
            save_dir=WORKING_DIR,
        )
        assert "test.tortilla" in os.listdir(WORKING_DIR)
