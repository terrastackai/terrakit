# © Copyright IBM Corporation 2025-2026
# SPDX-License-Identifier: Apache-2.0


from pathlib import Path
import shutil
import pytest
import os
import json
import numpy as np

from terrakit.chip.tiling import chip_and_label_data, format_dataset_stats
from tests.component_tests.chip.conftest import WORKING_DIR, NAMED_BAND_NAMES
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
            "dummy_imputed_label.tif",
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
            "dummy_imputed_label.tif",
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
        chip_label_suffix = "_label.tiff"
        res = chip_and_label_data(
            dataset_name="TEST",
            working_dir=WORKING_DIR,
            chip_label_suffix=chip_label_suffix,
        )

        assert res == [
            f"{WORKING_DIR}/dummy_imputed_0.data.tif",
            f"{WORKING_DIR}/dummy_imputed_0{chip_label_suffix}",
        ]

    def test_chip_and_label__stats_per_band_dict(
        self, chip_and_label_setup, chip_and_label_cleanup
    ):
        """Statistics norm_means and norm_stds are dicts keyed by band name."""
        chip_and_label_data(
            dataset_name="TEST",
            working_dir=WORKING_DIR,
            stats=True,
        )

        props_file = f"{WORKING_DIR}/TEST_metadata.json"
        with open(props_file) as f:
            metadata = json.load(f)

        stats = metadata["lineage"][0]["dataset_statistics"]

        # Both fields must be dicts
        assert isinstance(stats["norm_means"], dict)
        assert isinstance(stats["norm_stds"], dict)

        # Keys match band count (dummy.tif has 3 bands, no descriptions → band_1/2/3)
        assert set(stats["norm_means"].keys()) == {"band_1", "band_2", "band_3"}
        assert set(stats["norm_stds"].keys()) == {"band_1", "band_2", "band_3"}

        # Values are finite floats
        for val in stats["norm_means"].values():
            assert isinstance(val, float)
            assert np.isfinite(val)
        for val in stats["norm_stds"].values():
            assert isinstance(val, float)
            assert np.isfinite(val)

    def test_chip_and_label__stats_band_names_from_tif_descriptions(
        self, chip_and_label_setup_named_bands, chip_and_label_cleanup
    ):
        """Band names read from rasterio descriptions are used as dict keys."""
        chip_and_label_data(
            dataset_name="TEST",
            working_dir=WORKING_DIR,
            queried_data=[f"{WORKING_DIR}/dummy_named.tif"],
            stats=True,
        )

        props_file = f"{WORKING_DIR}/TEST_metadata.json"
        with open(props_file) as f:
            metadata = json.load(f)

        stats = metadata["lineage"][0]["dataset_statistics"]

        assert set(stats["norm_means"].keys()) == set(NAMED_BAND_NAMES)
        assert set(stats["norm_stds"].keys()) == set(NAMED_BAND_NAMES)


class TestFormatDatasetStats:
    def test_band_names_used_as_keys(self):
        mean = np.array([1.0, 2.0, 3.0])
        std = np.array([0.1, 0.2, 0.3])
        result = format_dataset_stats(
            "ds", ".tif", mean, std, ["red", "green", "blue"], []
        )
        assert result["norm_means"] == {"red": 1.0, "green": 2.0, "blue": 3.0}
        assert result["norm_stds"] == {"red": 0.1, "green": 0.2, "blue": 0.3}
        assert result["bands"] == 3

    def test_fallback_band_names(self):
        mean = np.array([10.0, 20.0])
        std = np.array([1.0, 2.0])
        result = format_dataset_stats("ds", ".tif", mean, std, ["band_1", "band_2"], [])
        assert list(result["norm_means"].keys()) == ["band_1", "band_2"]
        assert result["norm_means"]["band_1"] == pytest.approx(10.0)
        assert result["norm_stds"]["band_2"] == pytest.approx(2.0)
