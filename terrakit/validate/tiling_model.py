# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


import logging

from pydantic import (
    BaseModel,
    ConfigDict,
    field_validator,
)

logger = logging.getLogger(__name__)


class ChipAndLabelModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    active: bool = True
    data_suffix: str = ".tif"
    label_suffix: str = "_labels.tif"
    chip_suffix: str = ".data.tif"
    chip_label_suffix: str = ".label.tif"
    sample_dim: int = 256
    queried_data: list = []
    keep_files: bool = True
    match_suffix: bool = True

    @field_validator(
        "data_suffix", "label_suffix", "chip_suffix", "chip_label_suffix", mode="after"
    )
    def check_data_suffix(cls, v):
        supported_file_extensions = [".tif", ".tiff", ".nc"]
        ext = f".{v.split('.')[-1]}"
        if ext not in supported_file_extensions:
            raise ValueError(
                f"Please choose a supported file extension from one of {supported_file_extensions}. Chipping is not currently supported for the file type provided: {ext}"
            )
        return v
