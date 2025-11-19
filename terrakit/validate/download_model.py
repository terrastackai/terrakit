# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


import logging

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
)


logger = logging.getLogger(__name__)


class DateAllowance(BaseModel):
    """
    Model for specifying date allowance around the target date.

    Attributes:
        pre_days (int): Number of days before the target date to include. Default is 0.
        post_days (int): Number of days after the target date to include. Default is 7.

    Example:
        ```python
        from terrakit.validate.download_model import DateAllowance

        date_allowance = DateAllowance(
            pre_days = 0, post_days = 21
        )
        ```
    """

    pre_days: int = 0
    post_days: int = 7


class Transform(BaseModel):
    """
    Model for specifying data transformation options.

    Attributes:
        scale_data_xarray (bool): Whether to scale the data using xarray. Default is True.
        impute_nans (bool): Whether to impute NaN values. Default is True.
        reproject (bool): Whether to reproject the data. Default is True.

    Example:
        ```python
        from terrakit.validate.download_model import Transfrom

        transform = Transform(
            scale_data_xarray=True,
            impute_nans=True,
            reproject=True,
        )
        ```
    """

    scale_data_xarray: bool = True
    impute_nans: bool = True
    reproject: bool = True
    """ >>> INCLUDE NEW TRANSFORMATIONS HERE <<< 
    <new_transformation_option>: bool = False
    """


class DataSource(BaseModel):
    """
    Model for specifying data source configuration.

    Attributes:
        data_connector (str): The data connector to use. Default is "sentinel_aws".
        collection_name (str): The collection name to download. Default is "sentinel-2-l2a".
        bands (list[str]): The bands to download. Default is ["blue", "green", "red"].
        save_file (str | None): The file path to save the downloaded data. Default is None.

    Example:
        ```python
        from terrakit.validate.download_model import DataSource

        data_source = DataSource(
            data_connector = "sentinel_aws",
            collection_name = "sentinel-2-l2a",
            bands = ["blue", "green", "red"],
            save_file = "",
        )
        ```
    """

    data_connector: str = "sentinel_aws"
    collection_name: str = "sentinel-2-l2a"
    bands: list[str] = ["blue", "green", "red"]
    save_file: str | None = None


class DownloadModel(BaseModel):
    """
    Model for configuring the download process.

    Attributes:
        model_config (ConfigDict): Configuration dictionary.
        transform (Transform): Transformation options.
        date_allowance (DateAllowance): Date allowance around the target date.
        active (bool): Whether the download step is active. Default is True.
        max_cloud_cover (int): Maximum cloud cover allowed. Default is 80.
        keep_files (bool): Whether to keep redundent shapefiles. Default is False.
        datetime_bbox_shp_file (str): File path for datetime bounding box shapefile. Default is "./terrakit_curated_dataset_all_bboxes.shp".
        labels_shp_file (str): File path for labels shapefile. Default is "./tmp/terrakit_curated_dataset_labels.shp".
        data_sources (list[DataSource]): List of data sources to download. Default is an empty list.
    """

    model_config = ConfigDict(from_attributes=True)

    transform: Transform
    date_allowance: DateAllowance
    active: bool = True
    max_cloud_cover: int = 80
    keep_files: bool = False
    datetime_bbox_shp_file: str = "./tmp/terrakit_curated_dataset_all_bboxes.shp"
    labels_shp_file: str = "./tmp/terrakit_curated_dataset_labels.shp"
    data_sources: list[DataSource] = Field(default_factory=list)
