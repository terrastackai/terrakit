# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


import logging
import os

from pathlib import Path
from pydantic import (
    BaseModel,
    ValidationError,
    field_validator,
)

from terrakit.general_utils.exceptions import (
    TerrakitValidationError,
)

logger = logging.getLogger(__name__)


class PipelineModel(BaseModel):
    """
    A model for configuring the TerraKit Pipeline. This class defines the attributes common
    across all pipeline steps.

    Attributes:
        dataset_name (str): Name of the dataset. Default is "terrakit_curated_dataset".
        working_dir (Path): Working directory for the pipeline. Default is "./tmp". The directory is created if it does not already exist.
    """

    dataset_name: str = "terrakit_curated_dataset"
    working_dir: Path = Path("./tmp")

    @field_validator("dataset_name", mode="before")
    def check_dataset_name(cls, v):
        """
        Validate that the dataset_name does not contain special characters.

        Args:
            v (str): The dataset name to validate.

        Returns:
            str: The validated dataset name.
        """

        return v

    @field_validator("working_dir", mode="before")
    def check_working_dir(cls, v) -> Path:
        """
        Validate and create the working directory if it does not exist.

        Args:
            v (Path): The working directory path.

        Returns:
            Path: The validated and existing working directory path.

        Raises:
            ValueError: If the provided path is not a directory.
        """
        if v is None:
            v = "./tmp"
        pathname = Path(v)
        logging.debug(f"Working directory set to: {pathname}")
        if not pathname.exists():
            logging.info(f"Creating working directory: {pathname}")
            pathname.mkdir(parents=True, exist_ok=True)
        elif os.path.isdir(pathname) is False:
            raise ValueError(
                f"Working directory must be a path, not a file: 'working_dir' set to {v}"
            )
        return pathname


def pipeline_model_validation(dataset_name: str, working_dir: str):
    """
    Validate the TerraKit Pipeline model configuration.

    Args:
        dataset_name (str): Name of the dataset.
        working_dir (str): Working directory for the pipeline.

    Returns:
        PipelineModel: The validated PipelineModel instance.

    Raises:
        TerrakitValidationError: If the provided arguments are invalid.
    """
    try:
        parent_params = PipelineModel(
            dataset_name=dataset_name, working_dir=working_dir
        )
        pipeline_model = PipelineModel.model_validate(parent_params)
    except ValidationError as e:
        for error in e.errors():
            logging.error(
                f"Invalid parent arguments: {error['msg']}. \n\t'{error['loc'][0]}' currently set to '{error['input']}'. Please update to a valid entry."
            )
        raise TerrakitValidationError(
            "Invalid parent arguments", details=e.errors()
        ) from e
    logging.info(f"Processing with parent arguments: {pipeline_model}")
    return pipeline_model
