# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


import pytest

from terrakit.download.download_data import download_data
from terrakit.general_utils.exceptions import TerrakitValueError


@pytest.mark.skip("WiP: test plan")
class TestDownloadData_FailureTests:
    def test_download_data__(self):
        config = {
            "dataset_name": "test",
            "working_dir": ".",
            "download": {
                "datetime_bbox": "./all_bboxes.shp",
                "data_sources": [
                    {
                        "data_connector": "sentinel_aws",
                        "collection_name": "sentinel-2-l2a",
                        "bands": ["blue", "green", "red"],
                        "save_file": "",
                    },
                ],
                "date_allowance": {"pre_days": 0, "post_days": 21},
                "max_cloud_cover": 10,
                "transform": {
                    "scale_data_xarray": True,
                    "impute_nans": True,
                    "reproject": True,
                },
            },
        }

        queried_data = download_data(
            dataset_name=config["dataset_name"],
            root_working_dir=config["working_dir"],
            datetime_bbox=config["download"]["datetime_bbox"],
            data_sources=config["download"]["data_sources"],
            date_allowance=config["download"]["date_allowance"],
            transform=config["download"]["transform"],
        )

        assert queried_data is not None



class TestDownloadData_InvalidParams:
    """Test invalid parameter combinations for download_data"""

    def test_download_data__class_zero_conflict_raises_error(
        self,
        download_data_setup_classes,
        default_dir_clean_up,
        mock_aws_get_data,
        mock_stackstac,
    ):
        """Test that using class 0 with set_no_data=False raises TerrakitValueError.

        This test verifies that when labels contain class 0 and set_no_data=False,
        a TerrakitValueError is raised because class 0 conflicts with the background
        class (which is also 0 when set_no_data=False).
        """
        data_source = [
            {
                "data_connector": "sentinel_aws",
                "collection_name": "sentinel-2-l2a",
                "bands": ["blue", "green", "red"],
                "save_file": "",
            },
        ]

        # This should raise TerrakitValueError because labels use class 0
        # but set_no_data=False (background is also 0, causing conflict)
        with pytest.raises(TerrakitValueError) as exc_info:
            download_data(
                dataset_name="terrakit_curated_dataset_classes",
                data_sources=data_source,
                date_allowance={"pre_days": 0, "post_days": 21},
                set_no_data=False,  # This conflicts with class 0
                transform={
                    "scale_data_xarray": True,
                    "impute_nans": True,
                    "reproject": True,
                },
            )

        # Verify the error message is correct
        assert "class 0 which conflicts with the background class" in str(
            exc_info.value
        )
        assert "set_no_data=True" in str(exc_info.value)
