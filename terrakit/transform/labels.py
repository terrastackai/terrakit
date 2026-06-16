# © Copyright IBM Corporation 2025-2026
# SPDX-License-Identifier: Apache-2.0


import geopandas as gpd
import json
import logging
import os
import pandas as pd
import pyogrio
import rasterio
import re
import warnings
import csv

from glob import glob
from pathlib import Path
from pydantic import ValidationError
from shapely.geometry import box, shape
from shapely.ops import unary_union
from rasterio.features import shapes
from typing import Any, Literal

from terrakit.general_utils.exceptions import (
    TerrakitValidationError,
    TerrakitBaseException,
    TerrakitValueError,
)
from terrakit.general_utils.geospatial_util import extract_date_from_filename
from terrakit.validate.pipeline_model import PipelineModel, pipeline_model_validation
from terrakit.validate.labels_model import LabelsModel
from terrakit.general_utils.curation_metadata import dataset_metdata


logger = logging.getLogger(__name__)

CRS = "epsg:4326"
"""EPSG coordinate referecen system"""


class LabelsCls:
    """
    Class to handle loading and processing of label data.

    Attributes:
        dataset_name (str): Name of the dataset.
        working_dir (str): Working directory where tile and label shapefiles will be saved.
        active (bool): Indicates if labels are active.
        labels_folder (str): Path to the folder containing label files.
        label_type (str): Type of labels, currently supports 'vector' or 'raster'.
        datetime_info (str): Specifies how to extract datetime information, either 'filename' or 'csv'.

    Example:
        To instantiate LabelsCls:
        ```python
        from terrakit.transform.labels import LabelsCls

        labels = LabelsCls(dataset_name="my_dataset",
            working_dir="./tmp",
            active=True,
            labels_folder="./docs/examples/test_wildfire_vector",
            label_type="vector",
            datetime_info="filename"
        )
        ```
    """

    def __init__(
        self,
        *,
        labels_folder: str,
        dataset_name: str = "terrakit_curated_dataset",
        working_dir: str = "./tmp",
        active: bool = True,
        label_type: str = "vector",
        datetime_info: str = "filename",
    ):
        """
        Initialize LabelsCls with specified parameters.

        Parameters:
            labels_folder (str): Path to the folder containing label files.
            dataset_name (str, optional): Name of the dataset. Defaults to "terrakit_curated_dataset".
            working_dir (str, optional): Working directory for temporary files. Defaults to "./tmp".
            active (bool, optional): Indicates if labels are active. Defaults to True.
            label_type (str, optional): Type of labels, currently supports 'vector'. Defaults to "vector".
            datetime_info (str, optional): Specifies how to extract datetime information, either 'filename' or 'csv'. Defaults to "filename".
        """
        self.dataset_name = dataset_name
        self.working_dir = working_dir
        self.active = active
        self.labels_folder = labels_folder
        self.label_type = label_type
        self.datetime_info = datetime_info

    def save_shp_file(self, shp_file_name, gdf):
        """
        Save a GeoDataFrame to a shapefile.

        Parameters:
            shp_file_name (str): Name of the shapefile to be saved.
            gdf (geopandas.GeoDataFrame): GeoDataFrame to be saved.

        Returns:
            gdf (geopandas.GeoDataFrame): Saved GeoDataFrame.

        Raises:
            TerrakitBaseException: If there is an error saving the shapefile.
        """
        if self.dataset_name == "":
            shp_filename = shp_file_name
        else:
            shp_filename = f"{self.dataset_name}_{shp_file_name}"

        save_file: str = f"{self.working_dir}/{shp_filename}"
        if os.path.exists(save_file):
            logging.warning(
                f"File '{save_file}' already exists and will not be overwritten."
            )
            return gdf

        try:
            logging.info(f"Saving {save_file}")
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                gdf.to_file(save_file)
        except ValueError as e:
            err_msg = f"There was an issue saving to {save_file}."
            logger.error(err_msg)
            raise TerrakitBaseException(err_msg, e)  # type: ignore[arg-type]

    def get_metadata_csv(self) -> str:
        """
        Get the path to the metadata CSV file for the labels folder.

        Returns:
            str: Full path string to the metadata CSV file found in the labels folder

        Raises:
            TerrakitBaseException: If more than one metadata.csv file is found.
        """
        metadata_csv = glob(f"{self.labels_folder}/metadata.csv")
        if len(metadata_csv) != 1:
            raise TerrakitBaseException(
                "There should only be one 'metadata.csv' in the labels directory."
            )
        return metadata_csv[0]

    def load_metadata_csv(self) -> dict:
        """Load metadata from metadata.csv and return as a dictionary.

        Returns:
            dict: Dictionary containing filenames as keys and dates as values.

        Raises:
            TerrakitBaseException: If an exception is raised opening or reading the metadata CSV file.
        """
        metadata_csv = self.get_metadata_csv()
        metadata_dict = {}
        with open(metadata_csv, mode="r", newline="") as metadata:
            reader = csv.DictReader(metadata, fieldnames=["filename", "date"])
            for row in reader:
                metadata_dict[row["filename"]] = row["date"]
        return metadata_dict

    def raster_to_gdf(
        self,
        tif_path,
        band: int = 1,
        label_value: int = 1,
    ):
        """
        Convert a raster file to a GeoDataFrame.

        Parameters:
            tif_path (str): Path to the raster file.
            band (int, optional): Band number to read. Defaults to 1.
            label_value (int, optional): Value in the raster to consider as labels. Defaults to 1.

        Returns:
            gpd.GeoDataFrame: GeoDataFrame containing the raster features with the specified label value.
        """
        with rasterio.open(tif_path) as src:
            area = src.read(band)
            crs = src.crs

            results = (
                {"value": v, "geometry": shape(g)}
                for g, v in shapes(area, transform=src.transform)
            )

            gdf = gpd.GeoDataFrame(list(results), crs=crs).to_crs(CRS)
            gdf = gdf[gdf["value"] == label_value].copy()

        return gdf

    def get_label_date(self, file_path, label_type, metadata_csv=None):
        """
        Extract datetime information from a label file.

        Parameters:
            file_path (str): Path to the label file.
            label_type (str): Type of label, either 'vector' or 'raster'.
            metadata_csv (dict, optional): Dictionary containing metadata if datetime info is from a CSV. Defaults to None.

        Returns:
            tuple: A tuple containing a boolean indicating success and the extracted datetime string.

        Raises:
            TerrakitBaseException: If there is an error extracting datetime information.
        """
        pattern = r"\d{4}-\d{2}-\d{2}"  # YYYY-MM-DD
        filename = Path(file_path).name
        if self.datetime_info == "filename":
            if label_type == "vector":
                # Check filename for YYYY-MM-DD pattern
                match = re.search(pattern, filename)
                if match:
                    # Extract the matched part
                    label_date_string = match.group()
                else:
                    logger.error(
                        f"No datetime found in {file_path}. Please update the filename to include datetime information using the format YYYY-MM-DD."
                    )
                    return False, ""

            elif label_type == "raster":
                label_date_string = extract_date_from_filename(
                    filename, prefer="first"
                ).strftime("%Y-%m-%d")
        elif self.datetime_info == "csv":
            # Look up date for filename in metadata_csv
            if filename not in metadata_csv:
                logger.error(
                    f"No matching entry found in {metadata_csv} for {filename}"
                )
                return False, ""
            elif not re.search(pattern, metadata_csv[filename]):
                logger.error(
                    f"Date string '{metadata_csv[filename]}' for '{filename}' does not match expected format: {pattern}. Please update the 'metadata.csv' to include datetime information using the format YYYY-MM-DD."
                )
                return False, ""
            else:
                label_date_string = metadata_csv[filename].strip()
        else:
            logger.error(
                "No datetime info specified. Please update 'datetime_info' to one of {'filename', 'csv'}."
            )
            return False, ""
        return True, label_date_string

    def load_label_files(self, label_type: Literal["vector", "raster"]) -> pd.DataFrame:
        """
        Load label files from the labels folder and return as a GeoDataFrame.

        Parameters:
            label_type (Literal["vector", "raster"]): Either load vector or raster label files

        Returns:
            pd.DataFrame: GeoDataFrame containing all successfully loaded GeoJSON files.

        Raises:
            TerrakitValueError: After processing each label file, if an error occurred during processing, then an exception is raised.
        """

        # Initialize an empty list to store GeoDataFrames
        gdf_list = []

        # Prepare list of counting any files that have failed to be processed correctly.
        if label_type == "vector":
            file_list = glob(f"{self.labels_folder}/*json")
        elif label_type == "raster":
            file_list = glob(f"{self.labels_folder}/*tif")
        file_count = len(file_list)
        failed_files = []

        if self.datetime_info == "csv":
            # check for metadata.csv
            metadata_csv = self.load_metadata_csv()

        # Iterate over files in the specified folder
        for file_path in file_list:
            filename = Path(file_path).name
            # Reset label_date_string for each file in the labels folder.
            label_date_string = None

            # Check get the datetime info.
            try:
                if self.datetime_info == "csv":
                    success, label_date_string = self.get_label_date(
                        file_path, label_type, metadata_csv
                    )
                else:
                    success, label_date_string = self.get_label_date(
                        file_path, label_type
                    )
                if success is False:
                    gdf = None
                    failed_files.append(file_path)
            except Exception as e:
                gdf = None
                logger.error(
                    f"Error: An error occurred extract a data from '{file_path}'. Check this is a valid geojson or raster file: {e}"
                )
                failed_files.append(file_path)

            # read the label file
            try:
                if label_type == "vector":
                    gdf = gpd.read_file(file_path)
                elif label_type == "raster":
                    gdf = self.raster_to_gdf(file_path)
            except pyogrio.errors.DataSourceError:
                gdf = None
                logger.error(
                    f"Error: An error occurred while reading '{file_path}'. Check this is a valid geojson or raster file."
                )
                failed_files.append(file_path)

            # Check for label class
            label_class = 1
            class_pattern = r"_CLASS_(\d+)_"
            match = re.search(class_pattern, filename)
            if match:
                label_class = int(match.group(1))

            # Append the datetime to the GeoDataFrame
            if gdf is not None:
                if "geometry" not in gdf:
                    gdf = None
                    logger.error(
                        "No 'geometry' field found in {file_path}. Please update to validate geojson."
                    )
                    failed_files.append(file_path)
                elif label_date_string:
                    logging.info(
                        f"Setting datetime to {label_date_string} and label class to {label_class} for {filename}."
                    )
                    gdf["datetime"] = label_date_string
                    gdf["filename"] = filename
                    gdf["labelclass"] = label_class
                    gdf_list.append(gdf)
                    logger.info(f"Successfully processed {file_path}")

        logger.info(
            f"{len(gdf_list)}/{file_count} label files were successfully processed."
        )
        # Concatenate all GeoDataFrames into one
        if len(gdf_list) > 0:
            final_gdf = pd.concat(gdf_list, ignore_index=True)
            if len(failed_files) > 0:
                logger.warning(
                    f"There was an issue processing the following label files: {failed_files}. Please check the logs for issues raised."
                )
        else:
            err_msg = "Warning: There was an issue loading labels. Please check label data and retry."
            logging.warning(err_msg)
            raise TerrakitValueError(err_msg)
        self.save_shp_file("labels.shp", final_gdf)
        return final_gdf

    def get_grouped_bbox_gdf(self, label_gdf: pd.DataFrame) -> pd.DataFrame:
        """Group bounding boxes by date and overlapping source file extents.

        For each unique datetime, source raster files are grouped based on whether
        their geographic extents overlap or are nearby. All labels from overlapping
        source files are grouped together, ensuring that labels from tiles with
        overlapping footprints are in the same group.

        Parameters:
            label_gdf (pd.DataFrame): GeoDataFrame containing label data.

        Returns:
            pd.DataFrame: GeoDataFrame with grouped bounding boxes and tilesuffix column.
        """
        logging.info(
            f"Getting grouped bounding boxes for {self.dataset_name} at {self.working_dir}"
        )
        label_bbox_grouped_bbox_list: list[pd.DataFrame] = []

        group_idx = 0
        for d in list(label_gdf.datetime.unique()):
            label_date_gdf = label_gdf[label_gdf.datetime == d]

            # Step 1: Get source file extents for each unique filename
            file_extents = {}
            for filename in label_date_gdf.filename.unique():
                # Only use filename to determine extent since opening each file may be an expensive operation to perform
                file_labels = label_date_gdf[label_date_gdf.filename == filename]
                file_extents[filename] = box(
                    *unary_union(file_labels.geometry.tolist()).bounds
                )

            filenames = list(file_extents.keys())
            extent_geoms = [file_extents[fn] for fn in filenames]

            logging.info(f"Date {d}: Processing {len(filenames)} source file(s)")

            # Step 2: Group files by overlapping/nearby extents
            file_groups = self._group_overlapping_geometries(extent_geoms)

            logging.info(
                f"Date {d}: Found {len(file_groups)} non-overlapping file group(s)"
            )

            # Step 3: Process each group of files
            for subgroup_idx, file_indices in enumerate(file_groups):
                # Get all filenames in this group
                group_filenames = [filenames[i] for i in file_indices]

                # Get all labels from these files
                group_labels = label_date_gdf[
                    label_date_gdf.filename.isin(group_filenames)
                ]

                # Create combined bbox for all labels in this group
                all_group_geoms = group_labels.geometry.tolist()
                combined_bounds = unary_union(all_group_geoms).bounds
                combined_bbox = box(*combined_bounds)

                # Create tile suffix for this group
                tile_suffix = f"_tile_{group_idx}_{subgroup_idx + 1}"

                logging.info(
                    f"  Group {group_idx}.{subgroup_idx + 1}: {len(group_filenames)} file(s), "
                    f"bbox bounds = {combined_bounds}, tilesuffix = {tile_suffix}"
                )
                logging.info(f"    Files: {group_filenames}")

                # Create one row per class in this group
                for lc in list(group_labels.labelclass.unique()):
                    label_bbox_class_row = (
                        group_labels[group_labels.labelclass == lc].iloc[[0]].copy()
                    )
                    label_bbox_class_row["geometry"] = [combined_bbox]
                    label_bbox_class_row["tilesuffix"] = tile_suffix
                    logging.info(
                        f"    Adding bbox for class {lc}: {combined_bbox.bounds}"
                    )
                    label_bbox_grouped_bbox_list.append(label_bbox_class_row)

            group_idx += 1

        label_bbox_grouped_bbox_gdf = pd.concat(
            label_bbox_grouped_bbox_list, ignore_index=True
        )

        self.save_shp_file("all_bboxes.shp", label_bbox_grouped_bbox_gdf)
        return label_bbox_grouped_bbox_gdf

    def _group_overlapping_geometries(
        self, geometries: list, proximity_threshold: float = 0.01
    ) -> list[list[int]]:
        """Group geometries based on spatial proximity.

        Uses a union-find algorithm to group geometries that overlap, touch, or are
        within a specified distance of each other. This allows grouping of nearby
        geometries even if they don't directly intersect.

        Parameters:
            geometries (list): List of shapely geometry objects.
            proximity_threshold (float, optional): Distance threshold in degrees for
                grouping nearby geometries. Geometries within this distance will be
                grouped together. Default is 0.01 degrees (~1.1 km at equator).
                Set to 0 to only group intersecting geometries.

        Returns:
            list[list[int]]: List of groups, where each group is a list of indices
                           of geometries that are spatially proximate to each other.
        """
        n = len(geometries)
        if n == 0:
            return []

        # Initialize union-find structure
        parent = list(range(n))

        def find(x):
            if parent[x] != x:
                parent[x] = find(parent[x])
            return parent[x]

        def union(x, y):
            root_x = find(x)
            root_y = find(y)
            if root_x != root_y:
                parent[root_x] = root_y

        # Check all pairs for proximity
        for i in range(n):
            for j in range(i + 1, n):
                # Check if geometries intersect or are within proximity threshold
                if geometries[i].intersects(geometries[j]):
                    union(i, j)
                elif proximity_threshold > 0:
                    # Check distance between geometries
                    distance = geometries[i].distance(geometries[j])
                    if distance <= proximity_threshold:
                        union(i, j)

        # Group indices by their root parent
        groups_dict: dict[Any, Any] = {}
        for i in range(n):
            root = find(i)
            if root not in groups_dict:
                groups_dict[root] = []
            groups_dict[root].append(i)

        return list(groups_dict.values())


def labels_model_validation(
    pipeline_model: PipelineModel,
    active: bool,
    labels_folder: str,
    label_type: str,
    datetime_info: str,
) -> tuple[LabelsCls, LabelsModel]:
    """
    Validate and initialize LabelsCls with provided parameters.

    Parameters:
        pipeline_model (PipelineModel): Pipeline model containing dataset_name and working_dir.
        active (bool): Indicates if labels are active.
        labels_folder (str): Path to the folder containing label files.
        label_type (str): Type of labels, currently supports 'vector'.
        datetime_info (str): Specifies how to extract datetime information, either 'filename' or 'csv'.

    Returns:
        LabelsCls: Initialized LabelsCls instance.
        LabelsModel: Validated LabelsModel instance.

    Raises:
        TerrakitValidationError: If validation of labels parameters fails.

    Examples:
        ```python
        from terrakit.validate.labels_model import LabelsModel
        from terrakit.validate.pipeline_model import PipelineModel

        pipeline_model = PipelineModel(dataset_name = "terrakit_curated_dataset", working_dir = "./tmp")
        labels, labels_model = labels_model_validation(
            pipeline_model = pipeline_model,
            active = True,
            labels_folder = "./docs/examples/test_wildfire_vector",
            label_type = "vector",
            datetime_info = "filename",
        )
        ```
    """
    try:
        labels = LabelsCls(
            dataset_name=pipeline_model.dataset_name,
            working_dir=pipeline_model.working_dir,  # type: ignore[arg-type]
            active=active,
            labels_folder=labels_folder,  # type: ignore[arg-type]
            label_type=label_type,
            datetime_info=datetime_info,
        )
        labels_model = LabelsModel.model_validate(labels)
    except ValidationError as e:
        for error in e.errors():
            logging.error(
                f"Invalid label arguments: {error['msg']}. \n\t'{error['loc'][0]}' currently set to '{error['input']}. Please update to a valid entry."
            )
        raise TerrakitValidationError(
            "Invalid label arguments", details=e.errors()
        ) from e
    logging.info(f"Processing labels with arguments: {labels_model}")
    return labels, labels_model


def process_labels(
    labels_folder: str,
    dataset_name: str = "terrakit_curated_dataset",
    working_dir: str = "./tmp",
    active: bool = True,
    label_type: str = "vector",
    datetime_info: str = "filename",
) -> pd.DataFrame:
    """
    Entry point function for processing labels.

    The function validates and initalizes the LabelsCls, then loads, groups, and saves bounding boxes for labels and tiles.

    Parameters:
        labels_folder (str): Path to the folder containing label files.
        dataset_name (str, optional): Name of the dataset. Defaults to "terrakit_curated_dataset".
        working_dir (str, optional): Working directory for temporary files. Defaults to "./tmp".
        active (bool, optional): Indicates if labels are active. Defaults to True.
        label_type (str, optional): Type of labels, currently supports 'vector'. Defaults to "vector".
        datetime_info (str, optional): Specifies how to extract datetime information, either 'filename' or 'csv'. Defaults to "filename".

    Returns:
        pd.DataFrame: GeoDataFrame containing all successfully loaded and grouped label data.

    Raises:
        TerrakitValidationError: If there are issues with label data or validation.

    Example:
        ```python
        import terrakit

        label_args = {
            "dataset_name": "test_dataset",
            "labels": {
                "labels_folder": labels_folder,
            },
        }

        labels_gdf, grouped_bbox_gdf = terrakit.process_labels(
            dataset_name=label_args["dataset_name"],
            labels_folder=label_args["labels"]["labels_folder"],
        )
        ```
    """
    logging.info(f"Processing labels with arguments: {locals()}")
    pipeline_model = pipeline_model_validation(
        dataset_name=dataset_name, working_dir=working_dir
    )
    labels, labels_model = labels_model_validation(
        pipeline_model=pipeline_model,
        active=active,
        labels_folder=labels_folder,
        label_type=label_type,
        datetime_info=datetime_info,
    )

    if not active:
        logging.warning(
            "IMPORTANT: Labels are not active. Set labels.active = True to activate labels."
        )
        return pd.DataFrame(), pd.DataFrame()

    if datetime_info == "csv":
        metadata_csv = glob(f"{labels_folder}/metadata.csv")
        # If using csv, check metadata.csv exists
        if len(metadata_csv) == 0:
            raise TerrakitValidationError(
                "No metadata.csv file provided in labels directory. Please include 'metadata.csv' in labels dir with headers 'filename,date' and date information for each labels file. Alternatively set 'datetime_info' to 'filename'."
            )
        # Check headers are as expected
        excepted_headers = ["filename", "date"]
        with open(f"{labels_folder}/metadata.csv", "r") as f:
            reader = csv.reader(f)
            headers = next(reader)
            if headers != excepted_headers:
                raise TerrakitValidationError(
                    "metadata.csv missing 'filename,date' headers. Please update 'metadata.csv' to include these headers."
                )
    try:
        labels_gdf = labels.load_label_files(label_type)  # type: ignore [arg-type]
    except TerrakitValidationError as e:
        raise e
    except TerrakitBaseException as e:
        raise e
    except Exception as e:
        raise e

    try:
        grouped_boxes_gdf = labels.get_grouped_bbox_gdf(labels_gdf)
    except TerrakitValidationError as e:
        raise e
    except TerrakitBaseException as e:
        raise e
    except Exception as e:
        raise e

    # Save dataset metadata to file
    if dataset_name == "":
        output_files = ["all_bboxes.shp", "labels.shp"]
    else:
        output_files = [f"{dataset_name}_all_bboxes.shp", f"{dataset_name}_labels.shp"]
    labels_metadata = {
        "step_id": "labels",
        "activity": "Process label files to bound box and label shp files",
        "method": "terrakit.transform.labels.process_labels",
        "working_dir": str(working_dir),
        "parameters": json.loads(labels_model.model_dump_json()),
        "input_files": list(labels_gdf["filename"].unique()),
        "output_label_dates": list(labels_gdf["datetime"].unique()),
        "output_files": output_files,
    }

    dataset_metdata(pipeline_model, labels_metadata)

    return labels_gdf, grouped_boxes_gdf
