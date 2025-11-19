# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


import logging
import os

from pathlib import Path
from pydantic import (
    BaseModel,
    ConfigDict,
    field_validator,
    ValidationInfo,
)
from typing import Literal


logger = logging.getLogger(__name__)


class LabelsModel(BaseModel):
    """
    Model for configuration of the process labels TerraKit pipeline step.

    Attributes:
        model_config (ConfigDict): Configuration dictionary for the model.
        labels_folder (Path): Path to the folder containing label files.
        active (bool): Indicates if the labels step is active. Default is True.
        label_type (Literal["vector"]): Type of labels, currently only 'vector' is supported. Default is 'vector'.
        datetime_info (Literal["filename", "csv"]): Specifies how datetime information is stored, either by 'filename' or 'csv'. Default is 'filename'.
    """

    model_config = ConfigDict(from_attributes=True)

    labels_folder: Path
    active: bool = True
    label_type: Literal["vector", "raster"] = "vector"
    datetime_info: Literal["filename", "csv"] = "filename"

    @field_validator("labels_folder", mode="after")
    def check_labels_folder(cls, v):
        """
        Validates that the labels_folder exists, is not empty, and contains at least one supported file.

        Raises:
            ValueError: If the labels_folder does not exist, is empty, or does not contain any supported files.
        """
        if os.path.exists(v) is False:
            raise ValueError(
                f"Labels folder '{v}' does not exist. Please provide a valid labels folder"
            )

        if os.listdir(v) == 0:
            raise ValueError(
                f"Labels folder '{v}' does not contain any files. Please provide a valid labels folder with at least one labels file"
            )

        return v

    @field_validator("label_type", mode="after")
    def check_labels_type(cls, v, info: ValidationInfo):
        labels_folder = info.data.get("labels_folder")

        check_for_valid_type = False
        valid_file_type = ""
        if v == "vector":
            valid_file_type = "json"
            for filename in os.listdir(labels_folder):
                if filename.endswith(valid_file_type):
                    check_for_valid_type = True

        if v == "raster":
            valid_file_type = "tif"
            for filename in os.listdir(labels_folder):
                if filename.endswith(valid_file_type):
                    check_for_valid_type = True

        if check_for_valid_type is False:
            raise ValueError(
                f"Labels folder '{labels_folder}' does not contain any supported files. Please provide a valid labels folder with at least one valid .{valid_file_type} file."
            )
        return v

    @field_validator("datetime_info", mode="before")
    def check_datetime_info(cls, v):
        """
        Placeholder for future validation of datetime_info.

        Currently, no specific checks are implemented for datetime_info.
        """

        return v
