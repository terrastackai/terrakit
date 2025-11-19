# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


import json
import logging
import numpy as np
import os
import pandas as pd
import rasterio
import re
import tacoreader
import tacotoolbox

from glob import glob
from pydantic import BaseModel, ConfigDict
from sklearn.model_selection import train_test_split
from tqdm import tqdm

from terrakit.general_utils.curation_metadata import dataset_metdata
from terrakit.validate.pipeline_model import pipeline_model_validation

logger = logging.getLogger(__name__)


class Taco(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    active: bool = True
    format: str = "taco"
    dataset_save_dir: str = "."
    save_dir: str = "./tmp"
    tortilla_name: str = ""
    statistics: bool = True
    include_config: bool = True
    check_dataset: bool = True


class TacoCls:
    def __init__(
        self,
        *,
        active: bool,
        format: str = "taco",
        dataset_save_dir: str = ".",
        save_dir: str = "./tmp",
        tortilla_name: str = "",
        statistics: bool = True,
        include_config: bool = True,
        check_dataset: bool = True,
    ):
        """
        Args:
            active: Set the set to active or inactive
            type: Either "taco" or "local"
            dataset_save_dir: Directory containing the GeoTIFF images and labels
            save_dir: Directory to save the tortilla files
            tortilla_name: Name of the final tortilla file
        """
        self.active = active
        self.format = format
        self.dataset_save_dir = dataset_save_dir
        self.save_dir = save_dir
        self.tortilla_name = tortilla_name
        self.statistics = statistics
        self.include_config = include_config
        self.check_dataset = check_dataset

    def create_tortilla(
        self,
        dataset_name: str,
        working_dir: str = "./tmp",
        chip_suffix: str = ".data.tif",
    ) -> str:
        """Create a tortilla version of an object detection dataset."""

        tortilla_dir = os.path.join(self.save_dir, "tortilla")
        os.makedirs(tortilla_dir, exist_ok=True)

        # Collect all .tif files in the demo folder
        all_files = sorted(glob(f"{working_dir}/*{chip_suffix}"))

        logger.info(f"{all_files}")
        # Split into train, val, and test
        train_files, test_files = train_test_split(
            all_files, test_size=0.2, random_state=42
        )
        train_files, val_files = train_test_split(
            train_files, test_size=0.2, random_state=42
        )

        train_labels = [X.replace(".data.", ".label.") for X in train_files]
        test_labels = [X.replace(".data.", ".label.") for X in test_files]
        val_labels = [X.replace(".data.", ".label.") for X in val_files]

        train_df = pd.DataFrame(
            {
                "file_path": train_files,
                "data_split": "train",
                "label_file": train_labels,
            }
        )
        val_df = pd.DataFrame(
            {
                "file_path": val_files,
                "data_split": "validation",
                "label_file": val_labels,
            }
        )
        test_df = pd.DataFrame(
            {"file_path": test_files, "data_split": "test", "label_file": test_labels}
        )
        metadata_df = pd.concat([train_df, val_df, test_df], ignore_index=True)

        pattern = r"\d{4}-\d{2}-\d{2}"
        metadata_df["date"] = [
            re.search(pattern, X).group()  # type: ignore[union-attr]
            for X in metadata_df["file_path"]
        ]  # type: ignore[union-attr]

        for idx, geotiff_path in enumerate(
            tqdm(metadata_df["file_path"].values, desc="Creating tortillas")
        ):
            with rasterio.open(geotiff_path) as src:
                profile = src.profile
                height, width = profile["height"], profile["width"]
                crs = str(profile["crs"])

                transform = (
                    profile["transform"].to_gdal() if profile["transform"] else None
                )

            # create image
            image_sample = tacotoolbox.tortilla.datamodel.Sample(
                id="image",
                path=geotiff_path,
                file_format="GTiff",
                data_split=metadata_df["data_split"].values[idx],
                stac_data={
                    "crs": str(crs),
                    "geotransform": transform,
                    "raster_shape": (height, width),
                    "time_start": metadata_df["date"].values[idx],
                },
            )

            with rasterio.open(metadata_df["label_file"].values[idx]) as src:
                mask = src.read(1)
                mask_sum = np.sum(mask)

            # Create annotation part
            label_sample = tacotoolbox.tortilla.datamodel.Sample(
                id="label",
                path=metadata_df["label_file"].values[idx],
                file_format="GTiff",
                data_split=metadata_df["data_split"].values[idx],
                stac_data={
                    "crs": str(crs),
                    "geotransform": transform,
                    "raster_shape": (height, width),
                    "time_start": metadata_df["date"].values[idx],
                },
                burn_scar=mask_sum,
            )

            taco_samples = tacotoolbox.tortilla.datamodel.Samples(
                samples=[image_sample, label_sample]
            )

            sample_path = os.path.join(tortilla_dir, f"sample_{idx}.tortilla")
            tacotoolbox.tortilla.create(taco_samples, sample_path, quiet=True)

        # Merge all individual tortillas into one dataset
        all_tortilla_files = sorted(glob(os.path.join(tortilla_dir, "*.tortilla")))

        samples = []
        for tortilla_file in tqdm(all_tortilla_files, desc="Building final tortilla"):
            sample_data = tacoreader.load(tortilla_file).iloc[0]

            sample = tacotoolbox.tortilla.datamodel.Sample(
                id=os.path.basename(tortilla_file).split(".")[0],
                path=tortilla_file,
                file_format="TORTILLA",
                stac_data={
                    "crs": sample_data.get("stac:crs"),
                    "geotransform": sample_data.get("stac:geotransform"),
                    "raster_shape": sample_data.get("stac:raster_shape"),
                    "time_start": "2016",
                },
                data_split=sample_data["tortilla:data_split"],
                lon=sample_data.get("lon"),
                lat=sample_data.get("lat"),
            )
            samples.append(sample)

        if self.tortilla_name == "":
            self.tortilla_name = dataset_name
        final_samples = tacotoolbox.tortilla.datamodel.Samples(samples=samples)
        final_path = os.path.join(self.save_dir, self.tortilla_name)
        tacotoolbox.tortilla.create(final_samples, final_path, quiet=False, nworkers=1)
        return final_path


def taco_store_data(
    dataset_name: str,
    working_dir: str = "./tmp",
    active: bool = True,
    format: str = "taco",
    dataset_save_dir: str = ".",
    save_dir: str = "./tmp",
    tortilla_name: str = "",
    statistics: bool = True,
    include_config: bool = True,
    check_dataset: bool = True,
) -> str:
    """
    Main function to store Taco dataset data.

    This function initializes the Taco class with chip-specific arguments, validates the Taco model,
    and creates a tortilla (stores data) using the provided dataset name and working directory.

    """
    pipeline_model = pipeline_model_validation(
        dataset_name=dataset_name, working_dir=working_dir
    )

    taco = TacoCls(
        active=active,
        format=format,
        dataset_save_dir=dataset_save_dir,
        save_dir=save_dir,
        tortilla_name=tortilla_name,
        statistics=statistics,
        include_config=include_config,
        check_dataset=check_dataset,
    )  # Initialize class with chip specific args
    taco_model = Taco.model_validate(
        taco
    )  # validate store model - do this in the store class
    logging.info(f"Storing data with arguments: {taco_model}")
    store_data = taco.create_tortilla(dataset_name, working_dir)

    # Save dataset metadata to file
    store_metadata = {
        "step_id": "store",
        "activity": "Package dataset in taco format",
        "method": "terrakit.store.taco.taco_store_data",
        "working_dir": str(working_dir),
        "parameters": json.loads(taco_model.model_dump_json()),
    }
    dataset_metdata(pipeline_model, store_metadata)

    return store_data


def load_tortilla(tortilla_name) -> None:
    tt = tacoreader.load(tortilla_name)
    logger.info(tt)
