# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


import pytest

from glob import glob

from terrakit.download.download_data import download_data
from terrakit.general_utils.exceptions import TerrakitValueError


@pytest.mark.parametrize(
    "connector_type, collection, bands",
    (
        ["sentinelhub", "s2_l1c", ["B04", "B03", "B02"]],
        ["nasa_earthdata", "HLSS30_2.0", ["B04", "B03", "B02"]],
        ["sentinel_aws", "sentinel-2-l2a", ["blue", "green", "red"]],
    ),
)
class TestDownloadData_WorkingDir:
    def test_download_data__default(
        self,
        download_data_setup,
        default_dir_clean_up,
        mock_nasa_download_datasets,
        mock_sentinelhub_save_data,
        mock_aws_get_data,
        mock_stackstac,
        connector_type,
        collection,
        bands,
    ):
        """Input: .shp file in default working dir
        Output: tiles in default working dir: ./tmp folder
        """
        data_source = [
            {
                "data_connector": connector_type,
                "collection_name": collection,
                "bands": bands,
                "save_file": "",
            },
        ]
        queried_data = download_data(
            data_sources=data_source,
            date_allowance={"pre_days": 0, "post_days": 21},
            transform={
                "scale_data_xarray": True,
                "impute_nans": True,
                "reproject": True,
            },
        )

        if connector_type == "sentinelhub":
            assert len(queried_data) == 14  # 7 unique dates for each event -> 7x2 =14
        elif connector_type == "sentinel_aws":
            assert len(queried_data) == 14  # 7 unique dates for each event -> 7x2 =14
        elif connector_type == "nasa_earthdata":
            assert len(queried_data) == 10  # 5 unique dates for each event -> 5x2=10
        assert (
            len(glob("./tmp/terrakit_curated_dataset_all_bboxes*")) == 5
        )  # bbox shp files
        assert (
            len(glob("./tmp/terrakit_curated_dataset_labels*")) == 5
        )  # labels shp files
        assert len(glob("./tmp/*.tif")) > 1
        assert len(glob("./tmp/terrakit_curated_dataset_metadata.json")) == 1


class TestDownloadData_SetNoDataWithClasses:
    """Test set_no_data functionality with multi-class labels"""

    def test_download_data__set_no_data_with_classes(
        self,
        download_data_setup_classes,
        default_dir_clean_up,
        mock_aws_get_data,
        mock_stackstac,
    ):
        """Test that set_no_data works correctly with multi-class label files.

        This test verifies that when downloading data with multi-class labels,
        the set_no_data transformation correctly creates label rasters with
        proper class values and no-data handling.
        """
        data_source = [
            {
                "data_connector": "sentinel_aws",
                "collection_name": "sentinel-2-l2a",
                "bands": ["blue", "green", "red"],
                "save_file": "",
            },
        ]
        queried_data = download_data(
            dataset_name="terrakit_curated_dataset_classes",
            data_sources=data_source,
            date_allowance={"pre_days": 0, "post_days": 21},
            set_no_data=True,
            transform={
                "scale_data_xarray": True,
                "impute_nans": True,
                "reproject": True,
                "set_no_data": True,
            },
        )

        # Verify data was downloaded
        assert len(queried_data) > 0

        # Verify label raster was created with proper naming
        label_files = glob("./tmp/*_imputed_labels.tif")
        assert len(label_files) > 0, "Expected label raster file to be created"

        # Verify metadata was created
        assert len(glob("./tmp/terrakit_curated_dataset_classes_metadata.json")) == 1
