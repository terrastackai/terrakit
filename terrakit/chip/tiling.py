# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


# Docstrings assisted by watsonx Code Assistant

import logging
import json
import numpy as np
import os
import rasterio
import xarray as xr

from glob import glob
from pydantic import ValidationError
from rasterio.windows import Window
from tqdm import tqdm

from terrakit.general_utils.exceptions import (
    TerrakitBaseException,
    TerrakitValidationError,
)
from terrakit.general_utils.curation_metadata import dataset_metdata
from terrakit.validate.pipeline_model import PipelineModel
from terrakit.validate.pipeline_model import pipeline_model_validation
from terrakit.validate.tiling_model import ChipAndLabelModel


logger = logging.getLogger(__name__)


def format_dataset_stats(
    dataset_name, chip_suffix, total_mean, total_std, bands, tile_stats
):
    """Organises dataset stats in a dictorary"""
    norm_stds = total_std.tolist()
    norm_means = total_mean.tolist()
    dataset_stats = {
        "dataset_name": dataset_name,
        "norm_stds": norm_stds,
        "norm_means": norm_means,
        "bands": bands,
        "file_suffix": chip_suffix,
        "tile_stats": tile_stats,
    }
    return dataset_stats


def save_dataset_properties(working_dir, stats) -> None:
    """Function to save dataset properties to file"""
    logger.info("Save calculated properties of the dataset")
    try:
        with open(f"{working_dir}/dataset_properties.json", "w") as f:
            json.dump(stats, f, indent=4)
    except Exception as e:
        raise TerrakitBaseException(f"Error saving stats: {e}")


def load_dataset_properties(working_dir) -> dict:
    """Function to load dataset properties from file"""
    properties_filename = f"{working_dir}/dataset_properties.json"
    try:
        with open(properties_filename, "r") as f:
            stats: dict = json.load(f)
    except Exception as e:
        raise TerrakitBaseException(f"Error saving stats: {e}")

    # Remove stats file after reading
    try:
        os.remove(properties_filename)
    except Exception as e:
        raise TerrakitBaseException(f"Error saving stats: {e}")

    return stats


class ChipAndLabelCls:
    """
    Class for chipping and labeling raster data.

    Attributes:
        dataset_name (str): Name of the dataset.
        working_dir (str): Working directory for input and output files.
        active (bool): Flag to activate or deactivate chipping.
        data_suffix (str): Suffix of the input data files.
        label_suffix (str): Suffix of the input label files.
        chip_suffix (str): Suffix for chipped data files.
        chip_label_suffix (str): Suffix for chipped label files.
        sample_dim (int): Dimension for chipping.
        queried_data (list): List of files to be queried.
        keep_files (bool): Flag to keep original files after chipping.
        match_suffix (bool): Flag to match suffixes of original and chipped data.

    Example:
        To instantiate LabelsCls:
        ```python
        from terrakit.chip.tiling import ChipAndLabelCls

        ChipAndLabelCls(
            dataset_name="my_dataset",
            working_dir="./tmp",
            active=True,
            data_suffix=".tif",
            label_suffix="_labels.tif",
            chip_suffix=".data.tif",
            chip_label_suffix=".label.tif",
            sample_dim=256,
            queried_data=[],
            keep_files=True,
            match_suffix=True,
        )
        ```
    """

    def __init__(
        self,
        *,
        dataset_name: str = "terrakit_curated_dataset",
        working_dir: str = "./tmp",
        active: bool = True,
        data_suffix: str = ".tif",
        label_suffix: str = "_labels.tif",
        chip_suffix: str = ".data.tif",
        chip_label_suffix: str = ".label.tif",
        sample_dim: int = 256,
        queried_data: list = [],
        keep_files: bool = True,
        match_suffix: bool = True,
        stats: bool = True,
    ):
        """
        Initialize LabelsCls with specified parameters.

        Parameters:
            dataset_name (str): Name of the dataset.
            working_dir (str): Working directory for input and output files.
            active (bool): Flag to activate or deactivate chipping.
            data_suffix (str): Suffix of the input data files.
            label_suffix (str): Suffix of the input label files.
            chip_suffix (str): Suffix for chipped data files.
            chip_label_suffix (str): Suffix for chipped label files.
            sample_dim (int): Dimension for chipping.
            queried_data (list): List of files to be queried.
            keep_files (bool): Flag to keep original files after chipping.
            match_suffix (bool): Flag to match suffixes of original and chipped data.
        """
        self.dataset_name = dataset_name
        self.working_dir = working_dir
        self.active = active
        self.data_suffix = data_suffix
        self.label_suffix = label_suffix
        self.chip_suffix = chip_suffix
        self.chip_label_suffix = chip_label_suffix
        self.sample_dim = sample_dim
        self.queried_data = queried_data
        self.keep_files = keep_files
        self.match_suffix = match_suffix
        self.stats = stats

    def get_windows(self, data_meta: dict):
        """
        Function to get windows of the patch.

        Parameters:
            data_meta (dict): data metadata

        Returns:
            list: List of windows of the patch.
        """
        x_coords = [X for X in range(0, data_meta["width"], self.sample_dim)]
        y_coords = [X for X in range(0, data_meta["height"], self.sample_dim)]

        # If tile will extend beyond bounds of the data, move start of window back
        x_coords = [
            (
                data_meta["width"] - self.sample_dim
                if X + self.sample_dim > data_meta["width"]
                else X
            )
            for X in x_coords
        ]
        y_coords = [
            (
                data_meta["height"] - self.sample_dim
                if Y + self.sample_dim > data_meta["height"]
                else Y
            )
            for Y in y_coords
        ]
        windows = []
        for x in x_coords:
            for y in y_coords:
                windows.append(Window(x, y, self.sample_dim, self.sample_dim))
        return windows

    def create_patch(self, src, win):
        """
        Function to create patches of the read tif file.

        Parameters:
            src (Rasterio): Rasterio object of the opened file.
            win (int): Window index.

        Returns:
            numpy, dict: Array of the patched tile and dictionary of patched tile metadata.
        """

        win_transform = src.window_transform(win)
        kwargs = src.meta.copy()
        kwargs.update(
            {
                "height": win.height,
                "width": win.width,
                "transform": win_transform,
            }
        )

        band_data = []
        for i in range(1, kwargs["count"] + 1):
            band_data.append(np.expand_dims(src.read(i, window=win), 0))
        data = np.concatenate(band_data, axis=0)
        return data, kwargs

    def files_to_chip(
        self,
        working_dir: str,
    ):
        """
        Function to chip files.

        Parameters:
            working_dir (str): Directory of both the input and destination where chipped files will be generated to.

        Returns:
            list: List of all queried data.
        """
        if len(self.queried_data) == 0:
            logger.info("looking for files to chip in working dir..\n")
            candidate_list = glob(f"{working_dir}/*{self.data_suffix}")
        else:
            candidate_list = self.queried_data
        logger.info(f"Found candidate files to chip: {candidate_list}")
        queried_data = []
        for file in candidate_list:
            if self.label_suffix not in file:
                logger.info(
                    f"Adding {file} as it does not match the following 'label_suffix': '{self.label_suffix}'"
                )
                queried_data.append(file)
        return queried_data

    def chip_and_label(
        self,
    ) -> list:
        """
        Function to Chip the label and data rasters.

        1. Load the input and label images.
        2. Based on the requested dimension, create list of start and finish indices (in future add options to overlap etc).
        3. Loop through and subset for each.
        4. Calculate updated spatial extent metadata.
        5. Write to file.

        Returns:
            list: List of all chipped data and label files.
        """
        data_files_to_chip = self.files_to_chip(self.working_dir)
        logging.info(f"Chipping data: {data_files_to_chip}")

        # Initalize stats:
        data_file_count = len(data_files_to_chip)
        sums = [None] * data_file_count
        sums_sqs = [None] * data_file_count
        count = 0
        tile_stats = []
        dataset_stats = []

        chip_and_label_list: list = []

        for query in tqdm(data_files_to_chip):
            windows: list
            query_stem = query.replace(self.data_suffix, "")
            query_label = query.replace(self.data_suffix, self.label_suffix)
            query_label_stem = query_label.replace(self.label_suffix, "")
            logger.info(f"{self.data_suffix}")
            logger.info(f"{query}")
            logger.info(f"{query_stem}")
            logger.info(f"{query_label}")
            logger.info(f"{query_label_stem}")

            with rasterio.open(query) as src:
                # Per tile operations
                data_meta = src.meta
                windows = self.get_windows(data_meta)

                logging.info(f"Chipping data: {query}")

                for win_index, win in enumerate(windows):
                    data, kwargs = self.create_patch(src, win)

                    data_file_name = f"{query_stem}_{win_index}{self.chip_suffix}"
                    if self.chip_suffix.endswith(".tif"):
                        with rasterio.open(data_file_name, "w", **kwargs) as dst:
                            dst.write(data)
                    else:
                        # Wrap in xarray DataArray if needed
                        da = xr.DataArray(data)
                        da.to_netcdf(data_file_name)

                    chip_and_label_list.append(data_file_name)
                    win_index += 1

                # gather stats for each tile
                if self.stats:
                    bands = src.count
                    image = src.read()[range(bands), :, :]
                    sums[count] = image.sum(axis=(1, 2))
                    sums_sqs[count] = (image**2).sum(axis=(1, 2))
                    count += 1

                    tile_sums = sum([x for x in sums if x is not None])  # type: ignore[misc]
                    tile_stats.append(
                        {"tile": query, "sum": tile_sums.tolist(), "bands": bands}  # type: ignore[attr-defined]
                    )

            with rasterio.open(query_label) as src:
                logging.info(f"Chipping label data: {query_label}")
                for win_index, win in enumerate(windows):
                    data, kwargs = self.create_patch(src, win)

                    label_file_name = (
                        f"{query_label_stem}_{win_index}{self.chip_label_suffix}"
                    )
                    if self.chip_suffix.endswith(".tif"):
                        with rasterio.open(
                            label_file_name,
                            "w",
                            **kwargs,
                        ) as dst:
                            dst.write(data)
                    else:
                        # Wrap in xarray DataArray if needed
                        da = xr.DataArray(data)
                        da.to_netcdf(label_file_name)
                    chip_and_label_list.append(label_file_name)

            if self.keep_files is False:
                logging.info(f"Cleaning up files...{query}")
                os.remove(query)
                os.remove(query_label)
        logging.info(f"finished chipping {chip_and_label_list}")

        # Calculate dataset stats
        if self.stats:
            sums = [x for x in sums if x is not None]
            sums_sqs = [x for x in sums_sqs if x is not None]
            total_sum = sum(sums)  # type: ignore[arg-type]
            total_sum_sqs = sum(sums_sqs)  # type: ignore[arg-type]
            pixel_count = count * image.shape[1] * image.shape[2]
            total_mean = np.float64(total_sum / pixel_count)
            total_var = (total_sum_sqs / pixel_count) - (total_mean**2)
            total_std = np.float64(np.sqrt(total_var))
            dataset_stats = format_dataset_stats(
                self.dataset_name,
                self.chip_suffix,
                total_mean,
                total_std,
                bands,
                tile_stats,
            )
            save_dataset_properties(self.working_dir, dataset_stats)
        logging.info(f"Returning chip_and_label_list: {chip_and_label_list}")
        return chip_and_label_list


def chip_model_validation(
    pipeline_model: PipelineModel,
    active: bool,
    data_suffix: str,
    label_suffix: str,
    chip_suffix: str,
    chip_label_suffix: str,
    sample_dim: int,
    queried_data: list,
    keep_files: bool,
    match_suffix: bool,
    stats: bool,
) -> tuple[ChipAndLabelCls, ChipAndLabelModel]:
    """
    Function to validate the chip model.

    Parameters:
        pipeline_model (PipelineModel): Pipeline model instance.
        active (bool): Flag to activate or deactivate chipping.
        data_suffix (str): Suffix of the input data files.
        label_suffix (str): Suffix of the input label files.
        chip_suffix (str): Suffix for chipped data files.
        chip_label_suffix (str): Suffix for chipped label files.
        sample_dim (int): Dimension for chipping.
        queried_data (list): List of files to be queried.
        keep_files (bool): Flag to keep original files after chipping.
        match_suffix (bool): Flag to match suffixes of original and chipped data.

    Returns:
        ChipAndLabelCls: Initialized ChipAndLabelCls instance.
        ChipAndLabelModel: Validated ChipModel instance.

    Raises:
        TerrakitValidationError: If validation of the chip model fails.
    """
    try:
        chip = ChipAndLabelCls(
            dataset_name=pipeline_model.dataset_name,
            working_dir=pipeline_model.working_dir,  # type: ignore[arg-type]
            active=active,
            data_suffix=data_suffix,
            label_suffix=label_suffix,
            chip_suffix=chip_suffix,
            chip_label_suffix=chip_label_suffix,
            sample_dim=sample_dim,
            queried_data=queried_data,
            keep_files=keep_files,
            match_suffix=match_suffix,
            stats=stats,
        )  # Initialize class with chip specific args
        chip_model = ChipAndLabelModel.model_validate(
            chip
        )  # validate chip model - do this in the chip class
    except ValidationError as e:
        for error in e.errors():
            logging.error(
                f"Invalid label arguments: {error['msg']}. \n\t'{error['loc'][0]}' currently set to '{error['input']}. Please update to a valid entry."
            )
        raise TerrakitValidationError(
            "Invalid label arguments", details=e.errors()
        ) from e
    logging.info(f"Chipping data with arguments: {chip_model}")
    return chip, chip_model


def chip_and_label_data(
    dataset_name: str,
    working_dir: str = "./tmp",
    active: bool = True,
    data_suffix: str = ".tif",
    label_suffix: str = "_labels.tif",
    chip_suffix: str = ".data.tif",
    chip_label_suffix: str = ".label.tif",
    sample_dim: int = 256,
    queried_data: list = [],
    keep_files: bool = True,
    match_suffix: bool = True,  # TODO: If set  to true, then chip_suffix = data_suffix and chip_label_suffix = label_suffix.
    stats: bool = True,
) -> list[str]:
    """
    Entry point function to the class for Chipping.

    Parameters:
        dataset_name (str): Name of the dataset.
        working_dir (str): Working directory for input and output files.
        active (bool): Flag to activate or deactivate chipping.
        data_suffix (str): Suffix of the input data files.
        label_suffix (str): Suffix of the input label files.
        chip_suffix (str): Suffix for chipped data files.
        chip_label_suffix (str): Suffix for chipped label files.
        sample_dim (int): Dimension for chipping.
        queried_data (list): List of files to be queried.
        keep_files (bool): Flag to keep original files after chipping.
        match_suffix (bool): Flag to match suffixes of original and chipped data.
        stats (bool, optional): Bool to choose to calculate dataset stats or not, by default True

    Returns:
        list[str]: List of Chipped files.

    Example:
        ```python
        import terrakit

        chip_args = {
            "dataset_name": "test_dataset",
            "chip": {"sample_dim": 256},
        }

        res = terrakit.chip_and_label_data(
            dataset_name=chip_args["dataset_name"],
            sample_dim=chip_args["chip"]["sample_dim"],
            keep_files=True,
        )
        ```
    """
    logging.info(f"Processing labels with arguments: {locals()}")
    pipeline_model = pipeline_model_validation(
        dataset_name=dataset_name, working_dir=working_dir
    )

    chip, chip_model = chip_model_validation(
        pipeline_model=pipeline_model,
        active=active,
        data_suffix=data_suffix,
        label_suffix=label_suffix,
        chip_suffix=chip_suffix,
        chip_label_suffix=chip_label_suffix,
        sample_dim=sample_dim,
        queried_data=queried_data,
        keep_files=keep_files,
        match_suffix=match_suffix,
        stats=stats,
    )

    if not active:
        logging.warning(
            "IMPORTANT: Chip_and_label_data is not active. Skipping chip and label data step. Set chip.active = True to activate this step."
        )
        return []

    try:
        chip_and_label_list: list[str] = chip.chip_and_label()
    except rasterio.errors.RasterioIOError as e:
        logger.error(f"RasterioIoError while chipping data: {e}")
        raise TerrakitBaseException("Error while chipping data...") from e
    except Exception as e:
        logger.error(f"Error while chipping data: {e}")
        raise TerrakitBaseException("Error while chipping data...") from e

    # Save dataset metadata to file
    chip_metadata = {
        "step_id": "chip",
        "activity": "Chip tiles and labels.",
        "method": "terrakit.chip.tiling.chip_and_label_data",
        "working_dir": str(working_dir),
        "parameters": json.loads(chip_model.model_dump_json()),
    }
    if stats:
        dataset_properties = load_dataset_properties(working_dir)
        chip_metadata["dataset_statistics"] = dataset_properties

    dataset_metdata(pipeline_model, chip_metadata)

    return chip_and_label_list
