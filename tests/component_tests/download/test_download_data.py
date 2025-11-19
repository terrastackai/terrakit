# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


import pytest

from glob import glob

from terrakit.download.download_data import download_data


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
