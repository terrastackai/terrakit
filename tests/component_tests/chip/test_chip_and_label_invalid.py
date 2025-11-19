# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


import pytest

from terrakit.chip.tiling import chip_and_label_data
from terrakit.general_utils.exceptions import (
    TerrakitValidationError,
)
from tests.component_tests.chip.conftest import WORKING_DIR


class TestChipAndLabel_FailureTests:
    def test_chip_and_label__invalid_suffix(
        self, chip_and_label_setup, chip_and_label_cleanup
    ):
        with pytest.raises(
            TerrakitValidationError,
            match="Chipping is not currently supported for the file type provided",
        ):
            chip_and_label_data(
                dataset_name="TEST", working_dir=WORKING_DIR, data_suffix=".zarr"
            )

        with pytest.raises(
            TerrakitValidationError,
            match="Chipping is not currently supported for the file type provided",
        ):
            chip_and_label_data(
                dataset_name="TEST",
                working_dir=WORKING_DIR,
                label_suffix=".unsupported_file_suffix",
            )

        with pytest.raises(
            TerrakitValidationError,
            match="Chipping is not currently supported for the file type provided",
        ):
            chip_and_label_data(
                dataset_name="TEST", working_dir=WORKING_DIR, chip_suffix=".tifff"
            )

        with pytest.raises(
            TerrakitValidationError,
            match="Chipping is not currently supported for the file type provided",
        ):
            chip_and_label_data(
                dataset_name="TEST",
                working_dir=WORKING_DIR,
                chip_label_suffix=".unsupported_file_suffix",
            )
