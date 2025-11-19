# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


from pathlib import Path
import shutil
import pytest
import os

from terrakit.chip.tiling import chip_and_label_data
from tests.component_tests.chip.conftest import WORKING_DIR
from tests.component_tests import component_tests_util


class TestChipAndLabel:
    @pytest.mark.parametrize(
        "working_dir, dataset_name,num_x, num_y, sample_dim",
        [
            (
                "tests/resources/component_test_data/download/netcdf/test_0",
                "TEST",
                2,
                3,
                256,
            ),
            (
                "tests/resources/component_test_data/download/netcdf/test_0",
                "TEST",
                1,
                1,
                256,
            ),
        ],
    )
    def test_chip_and_label_netcdf(
        self, working_dir, dataset_name, num_x, num_y, sample_dim
    ):
        """
        Test the chip_and_label_data function by creating a temporary directory,
        generating dummy netCDF files, and verifying the output.

        Args:
            working_dir (str): The path to the temporary working directory.
            dataset_name (str): The name of the dataset.
            num_x (int): Number of chips along the x-axis.
            num_y (int): Number of chips along the y-axis.
            sample_dim (int): Dimension of each chip.

        Returns:
            None
        """
        data_suffix = ".nc"
        label_suffix = "_labels.nc"
        working_dir_path = Path(working_dir)

        # Delete the directory if it already exists
        if working_dir_path.exists():
            shutil.rmtree(working_dir_path)

        # Create an empty directory
        working_dir_path.mkdir(parents=True, exist_ok=True)

        # Copy files to the working directory
        size_x = sample_dim * num_x
        size_y = sample_dim * num_y
        dummy_data_path = component_tests_util.create_netcdf_file(
            working_dir=working_dir_path, size_x=size_x, size_y=size_y
        )
        stem = dummy_data_path.stem
        shutil.copy(dummy_data_path, f"{working_dir}/{stem}{label_suffix}")

        try:
            # Call the chip_and_label_data function
            res = chip_and_label_data(
                data_suffix=data_suffix,
                label_suffix=label_suffix,
                chip_suffix=".data.nc",
                chip_label_suffix=".label.nc",
                dataset_name=dataset_name,
                working_dir=working_dir,
                sample_dim=sample_dim,
                stats=False,
            )

            # Verify the number of generated files
            num_files = (
                (num_x * num_y) * 2 + 2 + 1
            )  # 2 files per chip (data and labels) + 2 original files + 1 _metadata.json file
            assert len(os.listdir(working_dir)) == num_files

            # Check if the expected files have been created
            for f in res:
                generated_file = Path(f)
                assert generated_file.exists(), f"Error: {f} does not exist"

        finally:
            # Clean up the temporary directory
            shutil.rmtree(working_dir)

    def test_chip_and_label_default(self, chip_and_label_setup, chip_and_label_cleanup):
        res = chip_and_label_data(
            dataset_name="TEST",
            working_dir=WORKING_DIR,
        )

        assert os.listdir(WORKING_DIR) == [
            "dummy_imputed.tif",
            "dummy_imputed_labels.tif",
            "TEST_metadata.json",
            "dummy_imputed_0.data.tif",
            "dummy_imputed_0.label.tif",
        ]
        assert res == [
            f"{WORKING_DIR}/dummy_imputed_0.data.tif",
            f"{WORKING_DIR}/dummy_imputed_0.label.tif",
        ]

    def test_chip_and_label_queried_data(
        self, chip_and_label_setup, chip_and_label_cleanup
    ):
        res = chip_and_label_data(
            dataset_name="TEST",
            working_dir=WORKING_DIR,
            queried_data=[f"{WORKING_DIR}/dummy_imputed.tif"],
        )
        assert os.listdir(WORKING_DIR) == [
            "dummy_imputed.tif",
            "dummy_imputed_labels.tif",
            "TEST_metadata.json",
            "dummy_imputed_0.data.tif",
            "dummy_imputed_0.label.tif",
        ]

        assert res == [
            f"{WORKING_DIR}/dummy_imputed_0.data.tif",
            f"{WORKING_DIR}/dummy_imputed_0.label.tif",
        ]

    def test_chip_and_label__label_suffix(
        self, chip_and_label_setup_label_suffix, chip_and_label_cleanup
    ):
        label_suffix = ".this_is_a_label_suffix.tif"
        res = chip_and_label_data(
            dataset_name="TEST", working_dir=WORKING_DIR, label_suffix=label_suffix
        )

        assert res == [
            f"{WORKING_DIR}/dummy_imputed_0.data.tif",
            f"{WORKING_DIR}/dummy_imputed_0.label.tif",
        ]

    def test_chip_and_label__data_suffix(
        self, chip_and_label_setup_data_suffix, chip_and_label_cleanup
    ):
        data_suffix = ".this_is_a_data_suffix.tif"

        res = chip_and_label_data(
            dataset_name="TEST",
            working_dir=WORKING_DIR,
            queried_data=[f"{WORKING_DIR}/dummy_imputed{data_suffix}"],
            data_suffix=data_suffix,
        )
        assert res == [
            f"{WORKING_DIR}/dummy_imputed_0.data.tif",
            f"{WORKING_DIR}/dummy_imputed_0.label.tif",
        ]

        res = chip_and_label_data(
            dataset_name="TEST", working_dir=WORKING_DIR, data_suffix=data_suffix
        )
        assert res == [
            f"{WORKING_DIR}/dummy_imputed_0.data.tif",
            f"{WORKING_DIR}/dummy_imputed_0.label.tif",
        ]

    def test_chip_and_label__active(
        self,
    ):
        res = chip_and_label_data(
            dataset_name="TEST", working_dir=WORKING_DIR, active=False
        )
        assert res == []

    def test_chip_and_label__keep_files(
        self, chip_and_label_setup, chip_and_label_cleanup
    ):
        res = chip_and_label_data(
            dataset_name="TEST", working_dir=WORKING_DIR, keep_files=False
        )

        assert os.listdir(WORKING_DIR) == [
            "TEST_metadata.json",
            "dummy_imputed_0.data.tif",
            "dummy_imputed_0.label.tif",
        ]
        assert res == [
            f"{WORKING_DIR}/dummy_imputed_0.data.tif",
            f"{WORKING_DIR}/dummy_imputed_0.label.tif",
        ]

    def test_chip_and_label__chip_suffix(
        self, chip_and_label_setup, chip_and_label_cleanup
    ):
        chip_suffix = "_train.tif"
        res = chip_and_label_data(
            dataset_name="TEST", working_dir=WORKING_DIR, chip_suffix=chip_suffix
        )

        assert res == [
            f"{WORKING_DIR}/dummy_imputed_0{chip_suffix}",
            f"{WORKING_DIR}/dummy_imputed_0.label.tif",
        ]

    def test_chip_and_label__chip_label_suffix(
        self, chip_and_label_setup, chip_and_label_cleanup
    ):
        chip_label_suffix = "_labels.tiff"
        res = chip_and_label_data(
            dataset_name="TEST",
            working_dir=WORKING_DIR,
            chip_label_suffix=chip_label_suffix,
        )

        assert res == [
            f"{WORKING_DIR}/dummy_imputed_0.data.tif",
            f"{WORKING_DIR}/dummy_imputed_0{chip_label_suffix}",
        ]
