# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


import json
import logging
import os

from datetime import datetime, timezone

from terrakit._version import VERSION, PACKAGE_NAME
from terrakit.general_utils.exceptions import (
    TerrakitBaseException,
)

logger = logging.getLogger(__name__)


def dataset_metdata(pipeline_model, step_metadata):
    # create dataset metadata file / Check if once exitst
    dataset_metadata_filename = (
        f"{pipeline_model.working_dir}/{pipeline_model.dataset_name}_metadata.json"
    )
    if os.path.isfile(dataset_metadata_filename) is False:
        dataset_metadata_filename = create_dataset_metadata(pipeline_model)
    update_lineage(dataset_metadata_filename, step_metadata)


def create_dataset_metadata(pipeline_model):
    dataset_metadata_filename = (
        f"{pipeline_model.working_dir}/{pipeline_model.dataset_name}_metadata.json"
    )
    metadata = {
        "dataset_name": pipeline_model.dataset_name,
        "creation_date": datetime.now(timezone.utc).isoformat(),
        "dataset_version": "1.0",
        "description": "A geospatial dataset curated using TerraKit.",
        "package": f"{PACKAGE_NAME} v{VERSION}",
        "lineage": [],
    }
    with open(dataset_metadata_filename, "w") as f:
        json.dump(metadata, f, indent=4)
    return dataset_metadata_filename


def update_lineage(dataset_metadata_filename, step_metadata):
    now = datetime.now(timezone.utc).isoformat()
    step_metadata["timestamp"] = now

    try:
        with open(dataset_metadata_filename, "r") as f:
            metadata = json.load(f)
    except Exception as e:
        err_msg = f"Error reading {dataset_metadata_filename}: {e}"
        logger.error(err_msg)
        raise TerrakitBaseException(err_msg)

    if all(key not in metadata.keys() for key in ("lineage", "package")):
        raise TerrakitBaseException(
            f"Error updating metadata. Check 'lineage' exists in {dataset_metadata_filename}"
        )
    else:
        lineage = metadata["lineage"]

        # Check terrakit version and update lineage if this has changed.
        package = metadata["package"]
        if package != f"{PACKAGE_NAME} v{VERSION}":
            step_metadata["package"] = f"{PACKAGE_NAME} v{VERSION}"

        # Note the order that this step has taken place at
        step_metadata["step_order"] = len(lineage)

        # Update linage
        lineage.append(step_metadata)

        # Update metadata
        metadata["lineage"] = lineage
        metadata["last_update"] = now

    try:
        with open(
            f"{dataset_metadata_filename.replace('.json', '_tmp.json')}", "w"
        ) as f:
            json.dump(metadata, f, indent=4)
    except Exception as e:
        err_msg = f"Error writting to {dataset_metadata_filename.replace('.json', '_tmp.json')}: {e}"
        logger.error(err_msg)
        logger.error(f"{metadata=}")
        raise TerrakitBaseException(err_msg)

    try:
        os.rename(
            f"{dataset_metadata_filename.replace('.json', '_tmp.json')}",
            f"{dataset_metadata_filename}",
        )
    except Exception as e:
        err_msg = f"Error renaming {dataset_metadata_filename.replace('.json', '_tmp.json')}: {e}"
        logger.error(err_msg)
        raise TerrakitBaseException(err_msg)
