# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


import pytest

from terrakit.download.download_data import download_data


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
