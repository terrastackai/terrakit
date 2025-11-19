# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


import geopandas as gpd
import json
import logging
import numpy as np
import os
import rasterio

from datetime import datetime, timedelta
from typing import Any, Literal, Union


from terrakit import DataConnector
from terrakit.general_utils.exceptions import (
    TerrakitValidationError,
    TerrakitValueError,
    TerrakitBaseException,
)
from terrakit.general_utils.curation_metadata import dataset_metdata
from terrakit.validate.pipeline_model import PipelineModel, pipeline_model_validation
from terrakit.validate.download_model import (
    Transform,
    DataSource,
    DateAllowance,
    DownloadModel,
)
from .geodata_utils import save_data_array_to_file
from .transformations.impute_nans_xarray import impute_nans_xarray
from .transformations.scale_data_xarray import scale_data_xarray

""" >>> IMPORT NEW TRANSFORMATIONS HERE <<< 
from .transformations.<new_transformation> import <new_transformation>
"""


logger = logging.getLogger(__name__)


class DownloadCls:
    """
    Class to handle the download and preprocessing of geospatial data.

    Attributes:
        dataset_name (str): Name of the dataset.
        working_dir (str): Working directory for shapefiles and downloaded tiles.
        transform (Transform): Transformation parameters for data.
        date_allowance (DateAllowance): Date range allowance for data query.
        data_sources (list[DataSource]): List of data sources to query.
        active (bool): Flag to activate/deactivate data download.
        max_cloud_cover (int): Maximum cloud cover percentage for data selection.
        keep_files (bool): Flag to keep shapefiles once they have been used. Downloaded files will not be removed.
        datetime_bbox_shp_file (str): Path to shapefile containing datetime and bounding boxes to be downloaded.
        labels_shp_file (str): Path to shapefile containing labels.

    Example:
        To instantiate and validate DownloadCls:
        ```python
        from terrakit.download.download_data import DownloadCls
        from terrakit.validate.download_model import (
            Transform,
            DataSource,
            DateAllowance,
        )

        data_source = DataSource(
            data_connector = "sentinel_aws",
            collection_name = "sentinel-2-l2a",
            bands = ["blue", "green", "red"],
            save_file = "",
        )
        date_allowance = DateAllowance(
            pre_days = 0, post_days = 21
        )
        transform = Transform(
            scale_data_xarray=True,
            impute_nans=True,
            reproject=True,
        )
        download = DownloadCls(
            dataset_name="terrakit_curated_dataset",
            working_dir="./tmp",
            active=True,
            max_cloud_cover=80,
            datetime_bbox_shp_file="./tmp/terrakit_curated_dataset_all_bboxes.shp",
            keep_files=False,
            data_sources=[data_source],
            date_allowance=date_allowance,
            labels_shp_file= "./tmp/terrakit_curated_dataset_labels.shp",
            transform=transform,
        )

        ```

    """

    def __init__(
        self,
        *,
        transform: Transform = json.loads(Transform().model_dump_json()),
        date_allowance: DateAllowance = json.loads(DateAllowance().model_dump_json()),
        data_sources: list[DataSource] = [json.loads(DataSource().model_dump_json())],
        dataset_name: str = "terrakit_curated_dataset",
        working_dir: str = "./tmp",
        active: bool = True,
        max_cloud_cover: int = 80,
        keep_files: bool = False,
        datetime_bbox_shp_file: str = "./tmp/terrakit_curated_dataset_all_bboxes.shp",
        labels_shp_file: str = "./tmp/terrakit_curated_dataset_labels.shp",
    ):
        """
        Initialize DownloadCls with specified parameters.

        Parameters:
            transform (Union[Transform, dict[str, Any]]): Transformation parameters for data.
            date_allowance (Union[DateAllowance, dict[str, Any]]): Date range allowance for data query.
            data_sources (Union[list[DataSource], list[dict[str, Any]]]): List of data sources to query.
            dataset_name (str): Name of the dataset.
            working_dir (str): Working directory for temporary files.
            active (bool): Flag to activate/deactivate data download.
            max_cloud_cover (int): Maximum cloud cover percentage for data selection.
            keep_files (bool): Flag to keep shapefiles once they have been used. Downloaded files will not be removed.
            datetime_bbox_shp_file (str): Path to shapefile containing datetime bounding boxes.
            labels_shp_file (str): Path to shapefile containing labels.
        """
        self.dataset_name = dataset_name
        self.working_dir = working_dir
        self.transform = transform
        self.date_allowance = date_allowance
        self.active = active
        self.max_cloud_cover = max_cloud_cover
        self.keep_files = keep_files
        self.datetime_bbox_shp_file = datetime_bbox_shp_file
        self.labels_shp_file = labels_shp_file
        self.data_sources = data_sources

    """Supported shapefile types"""
    SHP_FILE_TYPES = Literal["labels", "bbox"]

    def _find_shp_file(self, shp_file_type: SHP_FILE_TYPES, shp_file_path: str) -> str:
        """
        Find and return the path to the specified shapefile.

        Args:
            shp_file_type (SHP_FILE_TYPES): Type of shapefile ('labels' or 'bbox').
            shp_file_path (str): Path to the shapefile.

        Returns:
            str: Path to the shapefile.

        Raises:
            TerrakitValidationError: If the specified shapefile does not exist.
        """
        # Check if shp_file_path is passed in as a parameter
        if os.path.isfile(shp_file_path) is False:
            # Otherwise, check the working directory for the default shp file: {working_dir}/{dataset_name}_{shp_file_suffix}.shp
            if shp_file_type == "labels":
                shp_file_suffix = "labels"
            else:
                shp_file_suffix = "all_bboxes"

            shp_filename = f"{self.dataset_name}_{shp_file_suffix}.shp"

            # If the dataset_name is empty, then the default shp files will just be called "all_bboxes.shp" and "labels.shp"
            if self.dataset_name == "":
                shp_filename = f"{shp_file_suffix}.shp"
            else:
                shp_filename = f"{self.dataset_name}_{shp_file_suffix}.shp"

            shp_file_path = f"{self.working_dir}/{shp_filename}"

        # Now we have the shp file path we are looking for, check that it exists.
        if os.path.isfile(shp_file_path) is False:
            msg = f"The specified shp file '{shp_file_path}' does not exist. Please make sure the file exists."
            logger.warning(msg)
            raise TerrakitValidationError(msg)
        return shp_file_path

    def _read_shp_file(self, shp_file_path) -> gpd.GeoDataFrame:
        """
        Read the specified shapefile into a GeoDataFrame.

        Args:
            shp_file_path (str): Path to the shapefile.

        Returns:
            gpd.GeoDataFrame: GeoDataFrame containing the shapefile data.

        Raises:
            TerrakitValueError: If the shapefile cannot be read.
            TerrakitValidationError: If the shapefile does not contain 'geometry' and 'datetime' columns.
        """
        try:
            shp_file_gdf = gpd.read_file(shp_file_path)
        except TypeError as e:
            err_msg = f"Error reading shp file: {shp_file_path}. {e}"
            logger.warning(err_msg)
            raise TerrakitValueError(err_msg)
        except ValueError as e:
            err_msg = f"Error reading shp file: {shp_file_path}. {e}"
            logger.warning(err_msg)
            raise TerrakitValueError(err_msg)
        except Exception as e:
            err_msg = f"Error reading shp file: {shp_file_path}. {e}"
            logger.warning(err_msg)
            raise TerrakitValueError(err_msg)
        if "geometry" not in shp_file_gdf or "datetime" not in shp_file_gdf:
            msg = "Input data must contain both 'geometry' and 'datetime' columns."
            logger.warning(msg)
            raise TerrakitValidationError(msg)
        return shp_file_gdf

    def find_and_query_data_for_matching_dates(
        self,
    ) -> list:
        """
        Find and query data for matching dates from the specified data sources.

        Returns:
            list: List of queried data file paths.
        """
        bbox_shp_file = self._find_shp_file(
            shp_file_type="bbox", shp_file_path=self.datetime_bbox_shp_file
        )
        grouped_bbox_gdf = self._read_shp_file(bbox_shp_file)

        queried_data = []
        for li in range(0, len(grouped_bbox_gdf)):
            l = grouped_bbox_gdf.loc[li]  # noqa

            from_date = (
                datetime.strptime(l.datetime, "%Y-%m-%d")
                - timedelta(days=self.date_allowance.pre_days)
            ).strftime("%Y-%m-%d")
            to_date = (
                datetime.strptime(l.datetime, "%Y-%m-%d")
                + timedelta(days=self.date_allowance.post_days)
            ).strftime("%Y-%m-%d")

            for source in self.data_sources:
                dc = DataConnector(connector_type=source.data_connector)

                logger.info(source.collection_name)
                logger.info(from_date)
                logger.info(to_date)
                logger.info(list(l.geometry.bounds))
                unique_dates, results = dc.connector.find_data(  # type: ignore[attr-defined]
                    data_collection_name=source.collection_name,
                    date_start=from_date,
                    date_end=to_date,
                    bbox=list(l.geometry.bounds),
                    bands=source.bands,
                )

                if len(unique_dates) == 0:  # type: ignore[arg-type]
                    logger.warning(
                        f"No data found for given request: {source}, {from_date}, {to_date}, {list(l.geometry.bounds)}."
                    )
                    return []

                logger.info(unique_dates)

                # Now find the closest date from the search
                time_diffs_abs = [
                    abs(
                        datetime.strptime(X, "%Y-%m-%d")
                        - datetime.strptime(l.datetime, "%Y-%m-%d")
                    )
                    for X in unique_dates  # type: ignore[union-attr]
                ]
                closest_index = time_diffs_abs.index(min(time_diffs_abs))

                closest_date = unique_dates[closest_index]  # type: ignore[index]

                save_file = f"{self.working_dir}/{source.data_connector}_{source.collection_name}.tif"

                da = dc.connector.get_data(  # type: ignore[attr-defined]
                    data_collection_name=source.collection_name,
                    date_start=closest_date,
                    date_end=closest_date,
                    bbox=list(l.geometry.bounds),
                    bands=source.bands,
                    save_file=save_file,
                    maxcc=self.max_cloud_cover,
                )

                try:
                    if self.transform.scale_data_xarray:
                        dai = scale_data_xarray(da, list(np.ones(len(source.bands))))  # type: ignore[arg-type]
                    if self.transform.impute_nans:
                        dai = impute_nans_xarray(dai)
                    """ >>> INCLUDE NEW TRANSFORMATIONS HERE <<< 
                    if self.transform.<new_transformation_func>:
                        dai = <new_tranformation_fnc(da)>
                    """
                    save_data_array_to_file(dai, save_file, imputed=True)
                except TerrakitBaseException as e:
                    raise TerrakitBaseException(
                        f"Error while transforming data... {e}"
                    ) from e

                for t in da.time.values:  # type: ignore[union-attr]
                    date = t.astype(str)[:10]

                for i, t in enumerate(da.time.values):  # type: ignore[union-attr]
                    date = t.astype(str)[:10]
                    queried_data.append(
                        save_file.replace(".tif", f"_{date}_imputed.tif")
                    )

                if self.keep_files is False:
                    logger.info(f"Deleting {save_file.replace('.tif', f'_{date}.tif')}")
                    os.remove(save_file.replace(".tif", f"_{date}.tif"))
            logging.info(f"Queried data: {queried_data}")
        return queried_data

    def rasterize_vectors_to_the_queried_data(self, queried_data: list) -> int:
        """
        Rasterize vector data to the queried raster data.

        Args:
            queried_data (list): List of queried raster file paths.

        Returns:
            int: Number of files rasterized.
        """
        labels_shp_file = self._find_shp_file(
            shp_file_type="labels", shp_file_path=self.labels_shp_file
        )
        label_gdf = self._read_shp_file(labels_shp_file)

        logging.info("Rasterizing vectors to the queried data")
        file_save_count = 0
        for q in queried_data:
            with rasterio.open(q, "r") as src:
                out_meta = src.meta
                out_meta.update({"count": 1})
                image = rasterio.features.rasterize(
                    ((g, 1) for g in label_gdf.geometry),
                    out_shape=src.shape,
                    transform=src.transform,
                )
                # Write the burned image to geotiff
                logging.info(f"Writing to {q.replace('.tif', '')}_labels.tif")
                with rasterio.open(
                    f"{q.replace('.tif', '')}_labels.tif", "w", **out_meta
                ) as dst:
                    dst.write(image, indexes=1)
                    file_save_count = +1
        return file_save_count


def download_validation(
    pipeline_model: PipelineModel,
    date_allowance: Union[DateAllowance, dict[str, Any]],
    transform: Union[Transform, dict[str, Any]],
    data_sources: Union[list[DataSource], list[dict[str, Any]]],
    active: bool = True,
    max_cloud_cover: int = 80,
    datetime_bbox_shp_file: str = "./tmp/terrakit_curated_dataset_all_bboxes.shp",
    labels_shp_file: str = "./tmp/terrakit_curated_dataset_labels.shp",
    keep_files: bool = False,
) -> tuple[DownloadCls, DownloadModel]:
    """
    Validate and initialize the download process.

    Args:
        pipeline_model (PipelineModel): Pipeline model containing dataset and working directory information.
        date_allowance (Union[DateAllowance, dict[str, Any]]): Date range allowance for data query.
        transform (Union[Transform, dict[str, Any]]): Transformation parameters for data.
        data_sources (Union[list[DataSource], list[dict[str, Any]]]): List of data sources to query.
        active (bool): Flag to activate/deactivate data download.
        max_cloud_cover (int): Maximum cloud cover percentage for data selection.
        datetime_bbox_shp_file (str): Path to shapefile containing datetime bounding boxes.
        labels_shp_file (str): Path to shapefile containing labels.
        keep_files (bool): Flag to keep shapefiles once they have been used. Downloaded files will not be removed.

    Returns:
        DownloadCls: Initialized DownloadCls object.
        DownloadModel: Validated DownloadModel instance.

    Examples:
        ```python
        from terrakit.validate.download_model import DownloadModel
        download_model = DownloadModel.model_validate(download)
        ```
    """
    logging.info(f"Processing download_data with arguments: {locals()}")
    data_source_list: list = []
    for source in data_sources:
        if isinstance(source, dict):
            if "data_connector" not in source:
                msg = "Dict in data_source list did not contain 'data_connector'"
                raise TerrakitValidationError(msg)
            if "collection_name" not in source:
                msg = "Dict in data_source list did not contain 'collection_name'"
                raise TerrakitValidationError(msg)
            if "bands" not in source:
                msg = "Dict in data_source list did not contain 'bands'"
                raise TerrakitValidationError(msg)
            if "save_file" not in source:
                logger.info(
                    "save_file not explicitly set. This will be set dynamically by the data connector instead."
                )
                source["save_file"] = None
    for source in data_sources:
        data_source_list.append(
            DataSource(
                data_connector=source["data_connector"],  # type: ignore[index]
                collection_name=source["collection_name"],  # type: ignore[index]
                bands=source["bands"],  # type: ignore[index]
                save_file=source["save_file"],  # type: ignore[index]
            )
        )

    if isinstance(date_allowance, dict):
        if "pre_days" not in date_allowance:
            msg = "Dict in date_allowance list did not contain 'pre_days'"
            raise TerrakitValidationError(msg)
        if "post_days" not in date_allowance:
            msg = "Dict in date_allowance list did not contain 'post_days'"
    date_allowance = DateAllowance(
        pre_days=date_allowance["pre_days"],
        post_days=date_allowance["post_days"],  # type: ignore[index]
    )

    if isinstance(transform, dict):
        if "scale_data_xarray" not in transform:
            raise TerrakitValidationError(msg)
        if "impute_nans" not in transform:
            msg = "Dict in transform list did not contain 'impute_nans'"
        if "reproject" not in transform:
            msg = "Dict in transform list did not contain 'reproject'"
    transform = Transform(
        scale_data_xarray=transform["scale_data_xarray"],  # type: ignore[index]
        impute_nans=transform["impute_nans"],  # type: ignore[index]
        reproject=transform["reproject"],  # type: ignore[index]
    )
    download = DownloadCls(
        dataset_name=pipeline_model.dataset_name,
        working_dir=pipeline_model.working_dir,  # type: ignore[arg-type]
        active=active,
        max_cloud_cover=max_cloud_cover,
        datetime_bbox_shp_file=datetime_bbox_shp_file,
        keep_files=keep_files,
        data_sources=data_source_list,
        date_allowance=date_allowance,
        labels_shp_file=labels_shp_file,
        transform=transform,
    )

    download_model = DownloadModel.model_validate(download)
    logging.info(f"Downloading data with arguments: {download_model}")
    return download, download_model


def download_data(
    data_sources: Union[list[DataSource], list[dict[str, Any]]] = [
        json.loads(DataSource().model_dump_json())
    ],
    date_allowance: Union[DateAllowance, dict[str, Any]] = json.loads(
        DateAllowance().model_dump_json()
    ),
    transform: Union[Transform, dict[str, Any]] = json.loads(
        Transform().model_dump_json()
    ),
    dataset_name: str = "terrakit_curated_dataset",
    working_dir: str = "./tmp",
    active: bool = True,
    max_cloud_cover: int = 80,
    datetime_bbox_shp_file: str = "./tmp/terrakit_curated_dataset_all_bboxes.shp",
    labels_shp_file: str = "./tmp/terrakit_curated_dataset_labels.shp",
    keep_files: bool = False,
) -> list:
    """
    Download and preprocess geospatial data.

    Args:
        data_sources (Union[list[DataSource], list[dict[str, Any]]]): List of data sources to query.
        date_allowance (Union[DateAllowance, dict[str, Any]]): Date range allowance for data query.
        transform (Union[Transform, dict[str, Any]]): Transformation parameters for data.
        dataset_name (str): Name of the dataset.
        working_dir (str): Working directory for temporary files.
        active (bool): Flag to activate/deactivate data download.
        max_cloud_cover (int): Maximum cloud cover percentage for data selection.
        datetime_bbox_shp_file (str): Path to shapefile containing datetime bounding boxes.
        labels_shp_file (str): Path to shapefile containing labels.
        keep_files (bool): Flag to keep shapefiles once they have been used. Downloaded files will not be removed.

    Returns:
        list: List of queried data file paths.

    Raises:
        TerrakitBaseException: If a RuntimeError occurs while finding or querying data
        TerrakitValueError: If a TerrakitValueError occurs while finding or reading shp files
        TerrakitValidationError: If a TerrakitValidationError occurs while finding or reading shp files
    Example:
        ```python
        import terrakit

        config = {
            "download": {
                "data_sources": [
                    {
                        "data_connector": "sentinel_aws",
                        "collection_name": "sentinel-2-l2a",
                        "bands": ["blue", "green", "red"],
                        "save_file": "",
                    },
                ],
                "date_allowance": {"pre_days": 0, "post_days": 21},
                "transform": {
                    "scale_data_xarray": True,
                    "impute_nans": True,
                    "reproject": True,
                },
                "max_cloud_cover": 80,
            },
        }

        queried_data = terrakit.download_data(
            data_sources=config["download"]["data_sources"],
            date_allowance=config["download"]["date_allowance"],
            transform=config["download"]["transform"],
            max_cloud_cover=config["download"]["max_cloud_cover"],
            dataset_name="test_dataset",
            working_dir="./tmp",
            keep_files=False,
        )
        ```
    """
    if not active:
        logging.warning(
            "IMPORTANT: Download is not active. Skipping download data step. Set download.active = True to activate download."
        )
        return []

    logging.info(f"Processing download_data with arguments: {locals()}")
    pipeline_model = pipeline_model_validation(
        dataset_name=dataset_name, working_dir=working_dir
    )
    download, download_model = download_validation(
        pipeline_model=pipeline_model,
        date_allowance=date_allowance,
        transform=transform,
        data_sources=data_sources,
        active=active,
        max_cloud_cover=max_cloud_cover,
        datetime_bbox_shp_file=datetime_bbox_shp_file,
        labels_shp_file=labels_shp_file,
        keep_files=keep_files,
    )

    logging.info("Listing collections..")
    for source in download.data_sources:
        dc = DataConnector(connector_type=source.data_connector)
        logging.info(dc.connector.list_collections())

    # Find and query data
    try:
        queried_data: list = download.find_and_query_data_for_matching_dates()
    except RuntimeError as e:
        logger.error("-----> ERROR <------")
        logger.error(e)
        logger.error("-----> ERROR <------")
        raise TerrakitBaseException("Error while finding data...") from e
    except TerrakitValueError as e:
        raise e
    except TerrakitValidationError as e:
        raise e

    # Rasterize
    file_save_count = download.rasterize_vectors_to_the_queried_data(
        queried_data=queried_data
    )

    if file_save_count > 0:
        logging.info(f"Successfully rasterized {file_save_count} files")

    download_metadata = {
        "step_id": "download",
        "activity": "Extract datetime and bounding boxes from labels. Download data for a given date and bbox according to parameters.",
        "method": "terrakit.download.download_data",
        "working_dir": str(working_dir),
        "parameters": json.loads(download_model.model_dump_json()),
    }

    dataset_metdata(pipeline_model, download_metadata)

    return queried_data
