# © Copyright IBM Corporation 2026
# SPDX-License-Identifier: Apache-2.0


"""Tests for curation metadata functions."""

import json
import os
import pytest
import shutil
from unittest.mock import patch

from terrakit.general_utils.curation_metadata import (
    create_dataset_metadata,
    update_lineage,
    dataset_metdata,
)
from terrakit.general_utils.exceptions import TerrakitBaseException
from terrakit.validate.pipeline_model import PipelineModel
from terrakit._version import VERSION, PACKAGE_NAME


@pytest.fixture
def test_working_dir(tmp_path):
    """Create a temporary working directory for tests."""
    working_dir = tmp_path / "test_metadata"
    working_dir.mkdir(exist_ok=True)
    yield working_dir
    # Cleanup
    if working_dir.exists():
        shutil.rmtree(working_dir)


@pytest.fixture
def pipeline_model(test_working_dir):
    """Create a PipelineModel instance for testing."""
    return PipelineModel(dataset_name="test_dataset", working_dir=test_working_dir)


@pytest.fixture
def sample_step_metadata():
    """Create sample step metadata for testing."""
    return {
        "step_id": "download",
        "activity": "Download data from source",
        "method": "terrakit.download.download_data",
        "working_dir": "./tmp",
        "parameters": {"param1": "value1", "param2": "value2"},
    }


class TestCreateDatasetMetadata:
    """Tests for create_dataset_metadata function."""

    def test_create_metadata_file(self, pipeline_model, test_working_dir):
        """Test that metadata file is created with correct structure."""
        metadata_file = create_dataset_metadata(pipeline_model)

        # Verify file was created
        assert os.path.exists(metadata_file)
        assert metadata_file == str(test_working_dir / "test_dataset_metadata.json")

        # Verify file contents
        with open(metadata_file, "r") as f:
            metadata = json.load(f)

        assert metadata["dataset_name"] == "test_dataset"
        assert "creation_date" in metadata
        assert metadata["dataset_version"] == "1.0"
        assert metadata["description"] == "A geospatial dataset curated using TerraKit."
        assert metadata["package"] == f"{PACKAGE_NAME} v{VERSION}"
        assert metadata["lineage"] == []

    def test_create_metadata_with_different_dataset_name(self, test_working_dir):
        """Test metadata creation with different dataset name."""
        pipeline_model = PipelineModel(
            dataset_name="custom_dataset", working_dir=test_working_dir
        )

        metadata_file = create_dataset_metadata(pipeline_model)

        with open(metadata_file, "r") as f:
            metadata = json.load(f)

        assert metadata["dataset_name"] == "custom_dataset"
        assert "custom_dataset_metadata.json" in metadata_file

    def test_metadata_file_is_valid_json(self, pipeline_model):
        """Test that created metadata file is valid JSON."""
        metadata_file = create_dataset_metadata(pipeline_model)

        # Should not raise exception
        with open(metadata_file, "r") as f:
            json.load(f)

    def test_creation_date_format(self, pipeline_model):
        """Test that creation_date is in ISO format."""
        metadata_file = create_dataset_metadata(pipeline_model)

        with open(metadata_file, "r") as f:
            metadata = json.load(f)

        # Verify ISO format (should contain 'T' and timezone info)
        assert "T" in metadata["creation_date"]
        assert "+" in metadata["creation_date"] or "Z" in metadata["creation_date"]


class TestUpdateLineage:
    """Tests for update_lineage function."""

    def test_update_lineage_adds_step(self, pipeline_model, sample_step_metadata):
        """Test that update_lineage adds a step to the lineage."""
        metadata_file = create_dataset_metadata(pipeline_model)

        update_lineage(metadata_file, sample_step_metadata)

        with open(metadata_file, "r") as f:
            metadata = json.load(f)

        assert len(metadata["lineage"]) == 1
        assert metadata["lineage"][0]["step_id"] == "download"
        assert metadata["lineage"][0]["activity"] == "Download data from source"
        assert "timestamp" in metadata["lineage"][0]
        assert metadata["lineage"][0]["step_order"] == 0

    def test_update_lineage_multiple_steps(self, pipeline_model):
        """Test that multiple steps are added in correct order."""
        metadata_file = create_dataset_metadata(pipeline_model)

        # Add multiple steps
        steps = [
            {"step_id": "download", "activity": "Download data"},
            {"step_id": "labels", "activity": "Process labels"},
            {"step_id": "chip", "activity": "Chip tiles"},
            {"step_id": "store", "activity": "Store data"},
        ]

        for step in steps:
            update_lineage(metadata_file, step)

        with open(metadata_file, "r") as f:
            metadata = json.load(f)

        assert len(metadata["lineage"]) == 4
        for i, step in enumerate(steps):
            assert metadata["lineage"][i]["step_id"] == step["step_id"]
            assert metadata["lineage"][i]["step_order"] == i

    def test_update_lineage_adds_timestamp(self, pipeline_model, sample_step_metadata):
        """Test that timestamp is added to step metadata."""
        metadata_file = create_dataset_metadata(pipeline_model)

        update_lineage(metadata_file, sample_step_metadata)

        with open(metadata_file, "r") as f:
            metadata = json.load(f)

        assert "timestamp" in metadata["lineage"][0]
        # Verify ISO format
        assert "T" in metadata["lineage"][0]["timestamp"]

    def test_update_lineage_adds_last_update(
        self, pipeline_model, sample_step_metadata
    ):
        """Test that last_update field is added to metadata."""
        metadata_file = create_dataset_metadata(pipeline_model)

        update_lineage(metadata_file, sample_step_metadata)

        with open(metadata_file, "r") as f:
            metadata = json.load(f)

        assert "last_update" in metadata
        assert "T" in metadata["last_update"]

    def test_update_lineage_preserves_existing_steps(self, pipeline_model):
        """Test that existing lineage steps are preserved."""
        metadata_file = create_dataset_metadata(pipeline_model)

        step1 = {"step_id": "download", "activity": "Download"}
        step2 = {"step_id": "labels", "activity": "Labels"}

        update_lineage(metadata_file, step1)
        update_lineage(metadata_file, step2)

        with open(metadata_file, "r") as f:
            metadata = json.load(f)

        assert len(metadata["lineage"]) == 2
        assert metadata["lineage"][0]["step_id"] == "download"
        assert metadata["lineage"][1]["step_id"] == "labels"

    def test_update_lineage_tracks_version_changes(
        self, pipeline_model, sample_step_metadata
    ):
        """Test that version changes are tracked in step metadata."""
        metadata_file = create_dataset_metadata(pipeline_model)

        # Manually modify the package version in metadata to simulate version change
        with open(metadata_file, "r") as f:
            metadata = json.load(f)
        metadata["package"] = "terrakit v0.1.0"
        with open(metadata_file, "w") as f:
            json.dump(metadata, f)

        update_lineage(metadata_file, sample_step_metadata)

        with open(metadata_file, "r") as f:
            metadata = json.load(f)

        # Should add package version to step metadata when it differs
        assert "package" in metadata["lineage"][0]
        assert metadata["lineage"][0]["package"] == f"{PACKAGE_NAME} v{VERSION}"

    def test_update_lineage_no_version_change(
        self, pipeline_model, sample_step_metadata
    ):
        """Test that package version is not added when unchanged."""
        metadata_file = create_dataset_metadata(pipeline_model)

        update_lineage(metadata_file, sample_step_metadata)

        with open(metadata_file, "r") as f:
            metadata = json.load(f)

        # Should not add package to step metadata when version matches
        assert "package" not in metadata["lineage"][0]

    def test_update_lineage_uses_atomic_write(
        self, pipeline_model, sample_step_metadata
    ):
        """Test that update_lineage uses atomic write (tmp file then rename)."""
        metadata_file = create_dataset_metadata(pipeline_model)

        update_lineage(metadata_file, sample_step_metadata)

        # Verify tmp file was cleaned up
        tmp_file = metadata_file.replace(".json", "_tmp.json")
        assert not os.path.exists(tmp_file)

        # Verify final file exists
        assert os.path.exists(metadata_file)

    def test_update_lineage_missing_file_raises_error(
        self, test_working_dir, sample_step_metadata
    ):
        """Test that update_lineage raises error if metadata file doesn't exist."""
        non_existent_file = str(test_working_dir / "nonexistent_metadata.json")

        with pytest.raises(TerrakitBaseException) as exc_info:
            update_lineage(non_existent_file, sample_step_metadata)

        assert "Error reading" in str(exc_info.value)

    def test_update_lineage_invalid_json_raises_error(
        self, test_working_dir, sample_step_metadata
    ):
        """Test that update_lineage raises error for invalid JSON."""
        invalid_file = test_working_dir / "invalid_metadata.json"
        with open(invalid_file, "w") as f:
            f.write("{ invalid json }")

        with pytest.raises(TerrakitBaseException) as exc_info:
            update_lineage(str(invalid_file), sample_step_metadata)

        assert "Error reading" in str(exc_info.value)

    def test_update_lineage_missing_lineage_key_raises_error(
        self, test_working_dir, sample_step_metadata
    ):
        """Test that update_lineage raises error if lineage key is missing."""
        metadata_file = test_working_dir / "bad_metadata.json"
        with open(metadata_file, "w") as f:
            json.dump({"dataset_name": "test"}, f)

        with pytest.raises(TerrakitBaseException) as exc_info:
            update_lineage(str(metadata_file), sample_step_metadata)

        assert "Check 'lineage' exists" in str(exc_info.value)

    def test_update_lineage_write_error_raises_exception(
        self, pipeline_model, sample_step_metadata
    ):
        """Test that write errors are properly handled."""
        metadata_file = create_dataset_metadata(pipeline_model)

        # Mock open to raise an exception only during write (not read)
        original_open = open

        def mock_open_func(file, mode="r", *args, **kwargs):
            if "w" in mode and "_tmp.json" in str(file):
                raise PermissionError("Permission denied")
            return original_open(file, mode, *args, **kwargs)

        with patch("builtins.open", side_effect=mock_open_func):
            with pytest.raises(TerrakitBaseException) as exc_info:
                update_lineage(metadata_file, sample_step_metadata)

            assert "Error writting to" in str(exc_info.value)

    def test_update_lineage_rename_error_raises_exception(
        self, pipeline_model, sample_step_metadata
    ):
        """Test that rename errors are properly handled."""
        metadata_file = create_dataset_metadata(pipeline_model)

        # Mock os.rename to raise an exception
        with patch("os.rename", side_effect=OSError("Rename failed")):
            with pytest.raises(TerrakitBaseException) as exc_info:
                update_lineage(metadata_file, sample_step_metadata)

            assert "Error renaming" in str(exc_info.value)


class TestDatasetMetdata:
    """Tests for dataset_metdata function (main entry point)."""

    def test_dataset_metdata_creates_file_if_not_exists(
        self, pipeline_model, sample_step_metadata
    ):
        """Test that dataset_metdata creates metadata file if it doesn't exist."""
        metadata_file = (
            f"{pipeline_model.working_dir}/{pipeline_model.dataset_name}_metadata.json"
        )

        # Ensure file doesn't exist
        if os.path.exists(metadata_file):
            os.remove(metadata_file)

        dataset_metdata(pipeline_model, sample_step_metadata)

        # Verify file was created
        assert os.path.exists(metadata_file)

    def test_dataset_metdata_updates_existing_file(
        self, pipeline_model, sample_step_metadata
    ):
        """Test that dataset_metdata updates existing metadata file."""
        # Create initial metadata
        metadata_file = create_dataset_metadata(pipeline_model)

        # Add first step
        dataset_metdata(pipeline_model, sample_step_metadata)

        with open(metadata_file, "r") as f:
            metadata = json.load(f)

        assert len(metadata["lineage"]) == 1

    def test_dataset_metdata_multiple_pipeline_steps(self, pipeline_model):
        """Test complete pipeline with multiple steps."""
        # Simulate complete pipeline execution
        download_metadata = {
            "step_id": "download",
            "activity": "Extract datetime and bounding boxes from labels. Download data.",
            "method": "terrakit.download.download_data",
            "working_dir": str(pipeline_model.working_dir),
            "parameters": {"connector": "test_connector"},
        }

        labels_metadata = {
            "step_id": "labels",
            "activity": "Process label files to bound box and label shp files",
            "method": "terrakit.transform.labels.process_labels",
            "working_dir": str(pipeline_model.working_dir),
            "parameters": {"labels_dir": "./labels"},
            "input_files": ["label1.json", "label2.json"],
            "output_files": ["all_bboxes.shp", "labels.shp"],
        }

        chip_metadata = {
            "step_id": "chip",
            "activity": "Chip tiles and labels.",
            "method": "terrakit.chip.tiling.chip_and_label_data",
            "working_dir": str(pipeline_model.working_dir),
            "parameters": {"tile_size": 256},
        }

        store_metadata = {
            "step_id": "store",
            "activity": "Package dataset in taco format",
            "method": "terrakit.store.taco.taco_store_data",
            "working_dir": str(pipeline_model.working_dir),
            "parameters": {"format": "taco"},
        }

        # Execute pipeline steps
        dataset_metdata(pipeline_model, download_metadata)
        dataset_metdata(pipeline_model, labels_metadata)
        dataset_metdata(pipeline_model, chip_metadata)
        dataset_metdata(pipeline_model, store_metadata)

        # Verify all steps are recorded
        metadata_file = (
            f"{pipeline_model.working_dir}/{pipeline_model.dataset_name}_metadata.json"
        )
        with open(metadata_file, "r") as f:
            metadata = json.load(f)

        assert len(metadata["lineage"]) == 4
        assert metadata["lineage"][0]["step_id"] == "download"
        assert metadata["lineage"][1]["step_id"] == "labels"
        assert metadata["lineage"][2]["step_id"] == "chip"
        assert metadata["lineage"][3]["step_id"] == "store"

        # Verify step order
        for i in range(4):
            assert metadata["lineage"][i]["step_order"] == i

    def test_dataset_metdata_preserves_step_parameters(self, pipeline_model):
        """Test that step parameters are preserved in metadata."""
        step_metadata = {
            "step_id": "download",
            "activity": "Download data",
            "method": "terrakit.download.download_data",
            "working_dir": str(pipeline_model.working_dir),
            "parameters": {
                "connector": "climate_data_store",
                "bbox": [-10, 30, 10, 50],
                "start_date": "2020-01-01",
                "end_date": "2020-12-31",
            },
        }

        dataset_metdata(pipeline_model, step_metadata)

        metadata_file = (
            f"{pipeline_model.working_dir}/{pipeline_model.dataset_name}_metadata.json"
        )
        with open(metadata_file, "r") as f:
            metadata = json.load(f)

        assert metadata["lineage"][0]["parameters"]["connector"] == "climate_data_store"
        assert metadata["lineage"][0]["parameters"]["bbox"] == [-10, 30, 10, 50]

    def test_dataset_metdata_with_optional_fields(self, pipeline_model):
        """Test that optional fields in step metadata are preserved."""
        step_metadata = {
            "step_id": "chip",
            "activity": "Chip tiles and labels.",
            "method": "terrakit.chip.tiling.chip_and_label_data",
            "working_dir": str(pipeline_model.working_dir),
            "parameters": {"tile_size": 256},
            "dataset_statistics": {
                "total_tiles": 1000,
                "mean_value": 0.5,
                "std_value": 0.2,
            },
        }

        dataset_metdata(pipeline_model, step_metadata)

        metadata_file = (
            f"{pipeline_model.working_dir}/{pipeline_model.dataset_name}_metadata.json"
        )
        with open(metadata_file, "r") as f:
            metadata = json.load(f)

        assert "dataset_statistics" in metadata["lineage"][0]
        assert metadata["lineage"][0]["dataset_statistics"]["total_tiles"] == 1000

    def test_dataset_metdata_idempotent_creation(
        self, pipeline_model, sample_step_metadata
    ):
        """Test that calling dataset_metdata multiple times doesn't recreate the file."""
        # First call creates the file
        dataset_metdata(pipeline_model, sample_step_metadata)

        metadata_file = (
            f"{pipeline_model.working_dir}/{pipeline_model.dataset_name}_metadata.json"
        )
        with open(metadata_file, "r") as f:
            original_metadata = json.load(f)

        creation_date = original_metadata["creation_date"]

        # Second call should update, not recreate
        step2 = {"step_id": "labels", "activity": "Process labels"}
        dataset_metdata(pipeline_model, step2)

        with open(metadata_file, "r") as f:
            updated_metadata = json.load(f)

        # Creation date should remain the same
        assert updated_metadata["creation_date"] == creation_date
        # But lineage should have both steps
        assert len(updated_metadata["lineage"]) == 2


class TestMetadataIntegration:
    """Integration tests for metadata tracking across pipeline."""

    def test_metadata_tracks_complete_pipeline_execution(self, pipeline_model):
        """Test that metadata correctly tracks a complete pipeline execution."""
        # Simulate a realistic pipeline execution
        steps = [
            {
                "step_id": "download",
                "activity": "Download satellite imagery",
                "method": "terrakit.download.download_data",
                "working_dir": str(pipeline_model.working_dir),
                "parameters": {"connector": "sentinel_aws", "bands": ["B04", "B08"]},
            },
            {
                "step_id": "labels",
                "activity": "Process wildfire labels",
                "method": "terrakit.transform.labels.process_labels",
                "working_dir": str(pipeline_model.working_dir),
                "parameters": {"labels_dir": "./labels"},
                "input_files": ["fire_2020.json"],
                "output_files": ["all_bboxes.shp", "labels.shp"],
            },
            {
                "step_id": "chip",
                "activity": "Create 256x256 tiles",
                "method": "terrakit.chip.tiling.chip_and_label_data",
                "working_dir": str(pipeline_model.working_dir),
                "parameters": {"tile_size": 256, "overlap": 0},
            },
            {
                "step_id": "store",
                "activity": "Package in TACO format",
                "method": "terrakit.store.taco.taco_store_data",
                "working_dir": str(pipeline_model.working_dir),
                "parameters": {"format": "taco"},
            },
        ]

        for step in steps:
            dataset_metdata(pipeline_model, step)

        # Verify complete metadata
        metadata_file = (
            f"{pipeline_model.working_dir}/{pipeline_model.dataset_name}_metadata.json"
        )
        with open(metadata_file, "r") as f:
            metadata = json.load(f)

        # Check basic structure
        assert metadata["dataset_name"] == "test_dataset"
        assert metadata["dataset_version"] == "1.0"
        assert metadata["package"] == f"{PACKAGE_NAME} v{VERSION}"
        assert "creation_date" in metadata
        assert "last_update" in metadata

        # Check lineage
        assert len(metadata["lineage"]) == 4

        # Verify each step
        for i, step in enumerate(steps):
            assert metadata["lineage"][i]["step_id"] == step["step_id"]
            assert metadata["lineage"][i]["step_order"] == i
            assert "timestamp" in metadata["lineage"][i]
            assert metadata["lineage"][i]["method"] == step["method"]

    def test_metadata_file_naming_convention(self, test_working_dir):
        """Test that metadata file follows naming convention."""
        dataset_names = ["my_dataset", "test_123", "wildfire_data"]

        for name in dataset_names:
            pipeline_model = PipelineModel(
                dataset_name=name, working_dir=test_working_dir
            )

            step = {"step_id": "test", "activity": "Test step"}
            dataset_metdata(pipeline_model, step)

            expected_file = test_working_dir / f"{name}_metadata.json"
            assert expected_file.exists()


# Made with Bob


class TestPipelineStepsMetadataIntegration:
    """Tests that validate all TerraKit pipeline steps correctly append to metadata."""

    def test_download_step_metadata_structure(self, pipeline_model):
        """Test that download step creates correct metadata structure."""
        download_metadata = {
            "step_id": "download",
            "activity": "Extract datetime and bounding boxes from labels. Download data for a given date and bbox according to parameters.",
            "method": "terrakit.download.download_data",
            "working_dir": str(pipeline_model.working_dir),
            "parameters": {
                "connector": "climate_data_store",
                "bbox": [-10, 30, 10, 50],
                "start_date": "2020-01-01",
                "end_date": "2020-12-31",
                "bands": ["temperature", "precipitation"],
            },
        }

        dataset_metdata(pipeline_model, download_metadata)

        metadata_file = (
            f"{pipeline_model.working_dir}/{pipeline_model.dataset_name}_metadata.json"
        )
        with open(metadata_file, "r") as f:
            metadata = json.load(f)

        assert len(metadata["lineage"]) == 1
        step = metadata["lineage"][0]
        assert step["step_id"] == "download"
        assert step["method"] == "terrakit.download.download_data"
        assert "timestamp" in step
        assert step["step_order"] == 0
        assert "parameters" in step
        assert step["parameters"]["connector"] == "climate_data_store"

    def test_labels_step_metadata_structure(self, pipeline_model):
        """Test that labels step creates correct metadata structure."""
        # First add download step
        download_metadata = {
            "step_id": "download",
            "activity": "Download data",
            "method": "terrakit.download.download_data",
            "working_dir": str(pipeline_model.working_dir),
            "parameters": {},
        }
        dataset_metdata(pipeline_model, download_metadata)

        # Then add labels step
        labels_metadata = {
            "step_id": "labels",
            "activity": "Process label files to bound box and label shp files",
            "method": "terrakit.transform.labels.process_labels",
            "working_dir": str(pipeline_model.working_dir),
            "parameters": {
                "labels_folder": "./labels",
                "labels_format": "geojson",
                "active": True,
            },
            "input_files": ["fire_2020_01.json", "fire_2020_02.json"],
            "output_label_dates": ["2020-01-15", "2020-02-20"],
            "output_files": ["all_bboxes.shp", "labels.shp"],
        }

        dataset_metdata(pipeline_model, labels_metadata)

        metadata_file = (
            f"{pipeline_model.working_dir}/{pipeline_model.dataset_name}_metadata.json"
        )
        with open(metadata_file, "r") as f:
            metadata = json.load(f)

        assert len(metadata["lineage"]) == 2
        step = metadata["lineage"][1]
        assert step["step_id"] == "labels"
        assert step["method"] == "terrakit.transform.labels.process_labels"
        assert step["step_order"] == 1
        assert "input_files" in step
        assert "output_files" in step
        assert len(step["input_files"]) == 2
        assert len(step["output_files"]) == 2

    def test_chip_step_metadata_structure(self, pipeline_model):
        """Test that chip step creates correct metadata structure."""
        # Add previous steps
        download_metadata = {
            "step_id": "download",
            "activity": "Download",
            "method": "terrakit.download.download_data",
            "working_dir": str(pipeline_model.working_dir),
            "parameters": {},
        }
        labels_metadata = {
            "step_id": "labels",
            "activity": "Labels",
            "method": "terrakit.transform.labels.process_labels",
            "working_dir": str(pipeline_model.working_dir),
            "parameters": {},
        }
        dataset_metdata(pipeline_model, download_metadata)
        dataset_metdata(pipeline_model, labels_metadata)

        # Add chip step
        chip_metadata = {
            "step_id": "chip",
            "activity": "Chip tiles and labels.",
            "method": "terrakit.chip.tiling.chip_and_label_data",
            "working_dir": str(pipeline_model.working_dir),
            "parameters": {
                "tile_size": 256,
                "overlap": 0,
                "bands": ["B04", "B08"],
                "chip_suffix": "_chip.tif",
            },
            "dataset_statistics": {
                "total_tiles": 1500,
                "norm_means": [0.3, 0.4],
                "norm_stds": [0.1, 0.15],
                "bands": ["B04", "B08"],
            },
        }

        dataset_metdata(pipeline_model, chip_metadata)

        metadata_file = (
            f"{pipeline_model.working_dir}/{pipeline_model.dataset_name}_metadata.json"
        )
        with open(metadata_file, "r") as f:
            metadata = json.load(f)

        assert len(metadata["lineage"]) == 3
        step = metadata["lineage"][2]
        assert step["step_id"] == "chip"
        assert step["method"] == "terrakit.chip.tiling.chip_and_label_data"
        assert step["step_order"] == 2
        assert "dataset_statistics" in step
        assert step["dataset_statistics"]["total_tiles"] == 1500

    def test_store_step_metadata_structure(self, pipeline_model):
        """Test that store step creates correct metadata structure."""
        # Add all previous steps
        steps = [
            {
                "step_id": "download",
                "activity": "Download",
                "method": "terrakit.download.download_data",
            },
            {
                "step_id": "labels",
                "activity": "Labels",
                "method": "terrakit.transform.labels.process_labels",
            },
            {
                "step_id": "chip",
                "activity": "Chip",
                "method": "terrakit.chip.tiling.chip_and_label_data",
            },
        ]
        for step in steps:
            step["working_dir"] = str(pipeline_model.working_dir)
            step["parameters"] = {}
            dataset_metdata(pipeline_model, step)

        # Add store step
        store_metadata = {
            "step_id": "store",
            "activity": "Package dataset in taco format",
            "method": "terrakit.store.taco.taco_store_data",
            "working_dir": str(pipeline_model.working_dir),
            "parameters": {
                "format": "taco",
                "dataset_save_dir": "./output",
                "statistics": True,
                "check_dataset": True,
            },
        }

        dataset_metdata(pipeline_model, store_metadata)

        metadata_file = (
            f"{pipeline_model.working_dir}/{pipeline_model.dataset_name}_metadata.json"
        )
        with open(metadata_file, "r") as f:
            metadata = json.load(f)

        assert len(metadata["lineage"]) == 4
        step = metadata["lineage"][3]
        assert step["step_id"] == "store"
        assert step["method"] == "terrakit.store.taco.taco_store_data"
        assert step["step_order"] == 3
        assert step["parameters"]["format"] == "taco"

    def test_complete_pipeline_execution_order(self, pipeline_model):
        """Test that complete pipeline execution maintains correct order."""
        # Execute complete pipeline in order
        pipeline_steps = [
            {
                "step_id": "download",
                "activity": "Extract datetime and bounding boxes from labels. Download data.",
                "method": "terrakit.download.download_data",
                "working_dir": str(pipeline_model.working_dir),
                "parameters": {"connector": "sentinel_aws", "bbox": [-10, 30, 10, 50]},
            },
            {
                "step_id": "labels",
                "activity": "Process label files to bound box and label shp files",
                "method": "terrakit.transform.labels.process_labels",
                "working_dir": str(pipeline_model.working_dir),
                "parameters": {"labels_folder": "./labels"},
                "input_files": ["label1.json"],
                "output_files": ["all_bboxes.shp", "labels.shp"],
            },
            {
                "step_id": "chip",
                "activity": "Chip tiles and labels.",
                "method": "terrakit.chip.tiling.chip_and_label_data",
                "working_dir": str(pipeline_model.working_dir),
                "parameters": {"tile_size": 256},
            },
            {
                "step_id": "store",
                "activity": "Package dataset in taco format",
                "method": "terrakit.store.taco.taco_store_data",
                "working_dir": str(pipeline_model.working_dir),
                "parameters": {"format": "taco"},
            },
        ]

        for step in pipeline_steps:
            dataset_metdata(pipeline_model, step)

        metadata_file = (
            f"{pipeline_model.working_dir}/{pipeline_model.dataset_name}_metadata.json"
        )
        with open(metadata_file, "r") as f:
            metadata = json.load(f)

        # Verify all steps are present
        assert len(metadata["lineage"]) == 4

        # Verify correct order
        expected_order = ["download", "labels", "chip", "store"]
        for i, expected_step_id in enumerate(expected_order):
            assert metadata["lineage"][i]["step_id"] == expected_step_id
            assert metadata["lineage"][i]["step_order"] == i

        # Verify timestamps are in chronological order
        timestamps = [step["timestamp"] for step in metadata["lineage"]]
        assert timestamps == sorted(timestamps)

    def test_pipeline_steps_preserve_all_fields(self, pipeline_model):
        """Test that all fields from each pipeline step are preserved."""
        # Download step with all typical fields
        download_metadata = {
            "step_id": "download",
            "activity": "Download satellite imagery",
            "method": "terrakit.download.download_data",
            "working_dir": str(pipeline_model.working_dir),
            "parameters": {
                "connector": "climate_data_store",
                "bbox": [-10, 30, 10, 50],
                "start_date": "2020-01-01",
                "end_date": "2020-12-31",
                "bands": ["temperature", "precipitation"],
                "transform": {"impute_nans": True, "scale_data": True},
            },
        }
        dataset_metdata(pipeline_model, download_metadata)

        # Labels step with all typical fields
        labels_metadata = {
            "step_id": "labels",
            "activity": "Process wildfire labels",
            "method": "terrakit.transform.labels.process_labels",
            "working_dir": str(pipeline_model.working_dir),
            "parameters": {
                "labels_folder": "./labels",
                "labels_format": "geojson",
                "active": True,
                "rasterize": True,
            },
            "input_files": [
                "fire_2020_01.json",
                "fire_2020_02.json",
                "fire_2020_03.json",
            ],
            "output_label_dates": ["2020-01-15", "2020-02-20", "2020-03-10"],
            "output_files": ["all_bboxes.shp", "labels.shp"],
        }
        dataset_metdata(pipeline_model, labels_metadata)

        # Chip step with statistics
        chip_metadata = {
            "step_id": "chip",
            "activity": "Create 256x256 tiles with statistics",
            "method": "terrakit.chip.tiling.chip_and_label_data",
            "working_dir": str(pipeline_model.working_dir),
            "parameters": {
                "tile_size": 256,
                "overlap": 0,
                "bands": ["temperature", "precipitation"],
                "chip_suffix": "_chip.tif",
                "stats": True,
            },
            "dataset_statistics": {
                "dataset_name": "test_dataset",
                "total_tiles": 2000,
                "norm_means": [0.35, 0.42],
                "norm_stds": [0.12, 0.18],
                "bands": ["temperature", "precipitation"],
                "file_suffix": "_chip.tif",
            },
        }
        dataset_metdata(pipeline_model, chip_metadata)

        # Store step
        store_metadata = {
            "step_id": "store",
            "activity": "Package in TACO format with validation",
            "method": "terrakit.store.taco.taco_store_data",
            "working_dir": str(pipeline_model.working_dir),
            "parameters": {
                "format": "taco",
                "dataset_save_dir": "./output",
                "statistics": True,
                "include_config": True,
                "check_dataset": True,
            },
        }
        dataset_metdata(pipeline_model, store_metadata)

        # Verify all fields are preserved
        metadata_file = (
            f"{pipeline_model.working_dir}/{pipeline_model.dataset_name}_metadata.json"
        )
        with open(metadata_file, "r") as f:
            metadata = json.load(f)

        # Check download step
        download_step = metadata["lineage"][0]
        assert download_step["parameters"]["connector"] == "climate_data_store"
        assert len(download_step["parameters"]["bands"]) == 2
        assert "transform" in download_step["parameters"]

        # Check labels step
        labels_step = metadata["lineage"][1]
        assert len(labels_step["input_files"]) == 3
        assert len(labels_step["output_label_dates"]) == 3
        assert labels_step["parameters"]["rasterize"] is True

        # Check chip step
        chip_step = metadata["lineage"][2]
        assert "dataset_statistics" in chip_step
        assert chip_step["dataset_statistics"]["total_tiles"] == 2000
        assert len(chip_step["dataset_statistics"]["norm_means"]) == 2

        # Check store step
        store_step = metadata["lineage"][3]
        assert store_step["parameters"]["check_dataset"] is True
        assert store_step["parameters"]["include_config"] is True

    def test_pipeline_steps_with_different_dataset_names(self, test_working_dir):
        """Test that different datasets maintain separate metadata files."""
        datasets = ["wildfire_dataset", "flood_dataset", "drought_dataset"]

        for dataset_name in datasets:
            pipeline_model = PipelineModel(
                dataset_name=dataset_name, working_dir=test_working_dir
            )

            # Add a download step for each dataset
            download_metadata = {
                "step_id": "download",
                "activity": f"Download data for {dataset_name}",
                "method": "terrakit.download.download_data",
                "working_dir": str(pipeline_model.working_dir),
                "parameters": {"connector": "test_connector"},
            }
            dataset_metdata(pipeline_model, download_metadata)

            # Verify separate metadata file exists
            metadata_file = test_working_dir / f"{dataset_name}_metadata.json"
            assert metadata_file.exists()

            with open(metadata_file, "r") as f:
                metadata = json.load(f)

            assert metadata["dataset_name"] == dataset_name
            assert len(metadata["lineage"]) == 1

    def test_pipeline_steps_timestamps_are_sequential(self, pipeline_model):
        """Test that timestamps for sequential steps are in order."""
        import time

        steps = [
            {
                "step_id": "download",
                "activity": "Download",
                "method": "terrakit.download.download_data",
            },
            {
                "step_id": "labels",
                "activity": "Labels",
                "method": "terrakit.transform.labels.process_labels",
            },
            {
                "step_id": "chip",
                "activity": "Chip",
                "method": "terrakit.chip.tiling.chip_and_label_data",
            },
            {
                "step_id": "store",
                "activity": "Store",
                "method": "terrakit.store.taco.taco_store_data",
            },
        ]

        for step in steps:
            step["working_dir"] = str(pipeline_model.working_dir)
            step["parameters"] = {}
            dataset_metdata(pipeline_model, step)
            time.sleep(0.01)  # Small delay to ensure different timestamps

        metadata_file = (
            f"{pipeline_model.working_dir}/{pipeline_model.dataset_name}_metadata.json"
        )
        with open(metadata_file, "r") as f:
            metadata = json.load(f)

        # Extract timestamps and verify they're sequential
        timestamps = [step["timestamp"] for step in metadata["lineage"]]

        # Convert to datetime for comparison
        from datetime import datetime

        dt_timestamps = [datetime.fromisoformat(ts) for ts in timestamps]

        # Verify each timestamp is greater than or equal to the previous
        for i in range(1, len(dt_timestamps)):
            assert dt_timestamps[i] >= dt_timestamps[i - 1]

    def test_all_pipeline_steps_have_required_fields(self, pipeline_model):
        """Test that all pipeline steps include required metadata fields."""
        required_fields = ["step_id", "activity", "method", "working_dir", "parameters"]

        steps = [
            {
                "step_id": "download",
                "activity": "Download data",
                "method": "terrakit.download.download_data",
                "working_dir": str(pipeline_model.working_dir),
                "parameters": {"connector": "test"},
            },
            {
                "step_id": "labels",
                "activity": "Process labels",
                "method": "terrakit.transform.labels.process_labels",
                "working_dir": str(pipeline_model.working_dir),
                "parameters": {"labels_folder": "./labels"},
            },
            {
                "step_id": "chip",
                "activity": "Chip tiles",
                "method": "terrakit.chip.tiling.chip_and_label_data",
                "working_dir": str(pipeline_model.working_dir),
                "parameters": {"tile_size": 256},
            },
            {
                "step_id": "store",
                "activity": "Store data",
                "method": "terrakit.store.taco.taco_store_data",
                "working_dir": str(pipeline_model.working_dir),
                "parameters": {"format": "taco"},
            },
        ]

        for step in steps:
            dataset_metdata(pipeline_model, step)

        metadata_file = (
            f"{pipeline_model.working_dir}/{pipeline_model.dataset_name}_metadata.json"
        )
        with open(metadata_file, "r") as f:
            metadata = json.load(f)

        # Verify each step has all required fields
        for step in metadata["lineage"]:
            for field in required_fields:
                assert field in step, (
                    f"Missing required field '{field}' in step {step['step_id']}"
                )

            # Also verify added fields
            assert "timestamp" in step
            assert "step_order" in step


# Made with Bob
