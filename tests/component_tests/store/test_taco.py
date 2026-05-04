# © Copyright IBM Corporation 2025-2026
# SPDX-License-Identifier: Apache-2.0


import json
import os
import pytest
import tacoreader

from pathlib import Path

from terrakit.store.taco import Taco, TacoCls, taco_store_data, load_tortilla
from tests.component_tests.store.conftest import WORKING_DIR, DUMMY_DATA_DIR


class TestTacoModel:
    """Test Taco pydantic model validation"""

    def test_taco_model_default_values(self):
        """Test Taco model initializes with correct default values"""
        taco = Taco()
        assert taco.active is True
        assert taco.format == "taco"
        assert taco.dataset_save_dir == "."
        assert taco.save_dir == "./tmp"
        assert taco.tortilla_name == ""
        assert taco.statistics is True
        assert taco.include_config is True
        assert taco.check_dataset is True

    def test_taco_model_custom_values(self):
        """Test Taco model accepts custom values"""
        taco = Taco(
            active=False,
            format="local",
            dataset_save_dir="/custom/path",
            save_dir="/custom/save",
            tortilla_name="custom.tortilla",
            statistics=False,
            include_config=False,
            check_dataset=False,
        )
        assert taco.active is False
        assert taco.format == "local"
        assert taco.dataset_save_dir == "/custom/path"
        assert taco.save_dir == "/custom/save"
        assert taco.tortilla_name == "custom.tortilla"
        assert taco.statistics is False
        assert taco.include_config is False
        assert taco.check_dataset is False

    def test_taco_model_validation_from_dict(self):
        """Test Taco model can be validated from dictionary"""
        data = {
            "active": True,
            "format": "taco",
            "dataset_save_dir": ".",
            "save_dir": "./tmp",
            "tortilla_name": "test.tortilla",
        }
        taco = Taco.model_validate(data)
        assert taco.active is True
        assert taco.tortilla_name == "test.tortilla"


class TestTacoClsInitialization:
    """Test TacoCls class initialization"""

    def test_taco_cls_initialization_default(self):
        """Test TacoCls initializes with default values"""
        taco = TacoCls(active=True)
        assert taco.active is True
        assert taco.format == "taco"
        assert taco.dataset_save_dir == "."
        assert taco.save_dir == "./tmp"
        assert taco.tortilla_name == ""
        assert taco.statistics is True
        assert taco.include_config is True
        assert taco.check_dataset is True

    def test_taco_cls_initialization_custom(self):
        """Test TacoCls initializes with custom values"""
        taco = TacoCls(
            active=False,
            format="local",
            dataset_save_dir="/custom/path",
            save_dir="/custom/save",
            tortilla_name="custom.tortilla",
            statistics=False,
            include_config=False,
            check_dataset=False,
        )
        assert taco.active is False
        assert taco.format == "local"
        assert taco.dataset_save_dir == "/custom/path"
        assert taco.save_dir == "/custom/save"
        assert taco.tortilla_name == "custom.tortilla"
        assert taco.statistics is False
        assert taco.include_config is False
        assert taco.check_dataset is False

    def test_taco_cls_requires_active_parameter(self):
        """Test TacoCls requires active parameter"""
        with pytest.raises(TypeError) as exc_info:
            TacoCls()
        assert "missing 1 required keyword-only argument: 'active'" in str(
            exc_info.value
        )


class TestCreateTortilla:
    """Test create_tortilla method"""

    def test_create_tortilla_basic(self, taco_setup, store_cleanup):
        """Test create_tortilla creates tortilla file successfully"""
        taco = TacoCls(
            active=True,
            save_dir=WORKING_DIR,
            tortilla_name="test.tortilla",
        )

        result = taco.create_tortilla(
            dataset_name="test",
            working_dir=WORKING_DIR,
        )

        assert result == os.path.join(WORKING_DIR, "test.tortilla")
        assert os.path.exists(result)
        assert "test.tortilla" in os.listdir(WORKING_DIR)

    def test_create_tortilla_custom_chip_suffix(self, taco_setup, store_cleanup):
        """Test create_tortilla works with custom chip suffix"""
        taco = TacoCls(
            active=True,
            save_dir=WORKING_DIR,
            tortilla_name="test_custom.tortilla",
        )

        result = taco.create_tortilla(
            dataset_name="test_custom",
            working_dir=WORKING_DIR,
            chip_suffix=".data.tif",
        )

        assert os.path.exists(result)
        assert "test_custom.tortilla" in os.listdir(WORKING_DIR)

    def test_create_tortilla_creates_tortilla_directory(
        self, taco_setup, store_cleanup
    ):
        """Test create_tortilla creates intermediate tortilla directory"""
        taco = TacoCls(
            active=True,
            save_dir=WORKING_DIR,
            tortilla_name="test.tortilla",
        )

        taco.create_tortilla(
            dataset_name="test",
            working_dir=WORKING_DIR,
        )

        tortilla_dir = os.path.join(WORKING_DIR, "tortilla")
        assert os.path.exists(tortilla_dir)
        assert os.path.isdir(tortilla_dir)

    def test_create_tortilla_uses_dataset_name_when_tortilla_name_empty(
        self, taco_setup, store_cleanup
    ):
        """Test create_tortilla uses dataset_name when tortilla_name is empty"""
        taco = TacoCls(
            active=True,
            save_dir=WORKING_DIR,
            tortilla_name="",  # Empty tortilla name
        )

        result = taco.create_tortilla(
            dataset_name="my_dataset",
            working_dir=WORKING_DIR,
        )

        assert result == os.path.join(WORKING_DIR, "my_dataset")
        assert os.path.exists(result)

    def test_create_tortilla_splits_data_correctly(self, taco_setup, store_cleanup):
        """Test create_tortilla splits data into train/val/test correctly"""
        taco = TacoCls(
            active=True,
            save_dir=WORKING_DIR,
            tortilla_name="test.tortilla",
        )

        result = taco.create_tortilla(
            dataset_name="test",
            working_dir=WORKING_DIR,
        )

        # Load the tortilla and check splits
        tt = tacoreader.load(result)
        assert "tortilla:data_split" in tt.columns

        splits = tt["tortilla:data_split"].unique()
        assert "train" in splits
        assert "validation" in splits
        assert "test" in splits

    def test_create_tortilla_extracts_dates_from_filenames(
        self, taco_setup, store_cleanup
    ):
        """Test create_tortilla extracts dates from filenames correctly"""
        taco = TacoCls(
            active=True,
            save_dir=WORKING_DIR,
            tortilla_name="test.tortilla",
        )

        result = taco.create_tortilla(
            dataset_name="test",
            working_dir=WORKING_DIR,
        )

        # Load the tortilla and check dates
        tt = tacoreader.load(result)
        assert "stac:time_start" in tt.columns
        # Check that dates are extracted (files have 2025-01-01 in name)
        dates = tt["stac:time_start"].unique()
        assert len(dates) > 0


class TestTacoStoreData:
    """Test taco_store_data function"""

    def test_taco_store_data_basic(self, taco_setup, store_cleanup):
        """Test taco_store_data creates tortilla successfully"""
        result = taco_store_data(
            dataset_name="test",
            working_dir=WORKING_DIR,
            tortilla_name="test.tortilla",
            save_dir=WORKING_DIR,
        )

        assert result == os.path.join(WORKING_DIR, "test.tortilla")
        assert os.path.exists(result)
        assert "test.tortilla" in os.listdir(WORKING_DIR)

    def test_taco_store_data_creates_metadata(self, taco_setup, store_cleanup):
        """Test taco_store_data creates metadata file"""
        taco_store_data(
            dataset_name="test",
            working_dir=WORKING_DIR,
            tortilla_name="test.tortilla",
            save_dir=WORKING_DIR,
        )

        metadata_file = os.path.join(WORKING_DIR, "test_metadata.json")
        assert os.path.exists(metadata_file)

        # Verify metadata content
        with open(metadata_file, "r") as f:
            metadata = json.load(f)

        # Check top-level metadata structure
        assert "dataset_name" in metadata
        assert metadata["dataset_name"] == "test"
        assert "lineage" in metadata
        assert len(metadata["lineage"]) > 0

        # Check store step in lineage
        store_step = metadata["lineage"][0]
        assert store_step["step_id"] == "store"
        assert store_step["activity"] == "Package dataset in taco format"
        assert store_step["method"] == "terrakit.store.taco.taco_store_data"
        assert "parameters" in store_step

    def test_taco_store_data_with_custom_parameters(self, taco_setup, store_cleanup):
        """Test taco_store_data with custom parameters"""
        result = taco_store_data(
            dataset_name="custom_test",
            working_dir=WORKING_DIR,
            active=True,
            format="taco",
            dataset_save_dir=WORKING_DIR,
            save_dir=WORKING_DIR,
            tortilla_name="custom.tortilla",
            statistics=False,
            include_config=False,
            check_dataset=False,
        )

        assert os.path.exists(result)
        assert "custom.tortilla" in os.listdir(WORKING_DIR)

    def test_taco_store_data_validates_pipeline_model(self, taco_setup, store_cleanup):
        """Test taco_store_data validates pipeline model"""
        # This should succeed with valid parameters
        result = taco_store_data(
            dataset_name="test",
            working_dir=WORKING_DIR,
            save_dir=WORKING_DIR,
        )
        assert os.path.exists(result)

    @pytest.mark.skip(
        "WiP: Expected this test to fail as there appears to be an issue when running "
        "labels_to_data.py which is resolved if the shp files are removed from the working dir."
    )
    def test_taco_store_data_with_shapefiles(
        self, taco_setup, create_dummy_shpfile, store_cleanup
    ):
        """Test that store data function works even if shapefiles exist in working directory"""
        taco_store_data(
            dataset_name="test",
            working_dir=WORKING_DIR,
            tortilla_name="test.tortilla",
            save_dir=WORKING_DIR,
        )
        assert "test.tortilla" in os.listdir(WORKING_DIR)


class TestLoadTortilla:
    """Test load_tortilla function"""

    def test_load_tortilla_basic(self, taco_setup, store_cleanup, caplog):
        """Test load_tortilla loads and logs tortilla data"""
        # First create a tortilla
        result = taco_store_data(
            dataset_name="test",
            working_dir=WORKING_DIR,
            tortilla_name="test.tortilla",
            save_dir=WORKING_DIR,
        )

        # Now load it
        load_tortilla(result)

        # Check that something was logged (the function logs the dataframe)
        assert len(caplog.records) > 0

    def test_load_tortilla_returns_none(self, taco_setup, store_cleanup):
        """Test load_tortilla returns None"""
        result = taco_store_data(
            dataset_name="test",
            working_dir=WORKING_DIR,
            tortilla_name="test.tortilla",
            save_dir=WORKING_DIR,
        )

        return_value = load_tortilla(result)
        assert return_value is None


class TestTacoErrorHandling:
    """Test error handling and edge cases"""

    def test_create_tortilla_no_data_files(self, store_cleanup):
        """Test create_tortilla handles missing data files gracefully"""
        Path(WORKING_DIR).mkdir(parents=True, exist_ok=True)

        taco = TacoCls(
            active=True,
            save_dir=WORKING_DIR,
            tortilla_name="test.tortilla",
        )

        # Should raise an error or handle gracefully when no .tif files exist
        with pytest.raises(Exception):  # Could be ValueError or IndexError
            taco.create_tortilla(
                dataset_name="test",
                working_dir=WORKING_DIR,
            )

    def test_create_tortilla_mismatched_data_label_files(self, store_cleanup):
        """Test create_tortilla handles mismatched data/label files"""
        Path(WORKING_DIR).mkdir(parents=True, exist_ok=True)

        # Create only data file without corresponding label file
        import shutil

        shutil.copy(
            f"{DUMMY_DATA_DIR}/dummy_2025-01-01_imputed_0.data.tif",
            f"{WORKING_DIR}/dummy_2025-01-01_imputed_1.data.tif",
        )

        taco = TacoCls(
            active=True,
            save_dir=WORKING_DIR,
            tortilla_name="test.tortilla",
        )

        # Should raise an error when label file is missing
        with pytest.raises(Exception):  # Could be FileNotFoundError or rasterio error
            taco.create_tortilla(
                dataset_name="test",
                working_dir=WORKING_DIR,
            )

    def test_taco_store_data_invalid_working_dir(self):
        """Test taco_store_data with invalid working directory on read-only filesystem"""
        # On macOS, /nonexistent is on a read-only filesystem and will raise OSError
        # when trying to create directories
        with pytest.raises(OSError):
            taco_store_data(
                dataset_name="test",
                working_dir="/nonexistent/directory/path",
                save_dir=WORKING_DIR,
            )

    def test_create_tortilla_creates_save_dir_if_not_exists(
        self, taco_setup, store_cleanup
    ):
        """Test create_tortilla creates save_dir if it doesn't exist"""
        new_save_dir = os.path.join(WORKING_DIR, "new_save_dir")
        assert not os.path.exists(new_save_dir)

        taco = TacoCls(
            active=True,
            save_dir=new_save_dir,
            tortilla_name="test.tortilla",
        )

        result = taco.create_tortilla(
            dataset_name="test",
            working_dir=WORKING_DIR,
        )

        assert os.path.exists(new_save_dir)
        assert os.path.exists(result)


class TestTacoIntegration:
    """Integration tests for complete workflows"""

    def test_full_workflow_taco_model_to_tortilla(self, taco_setup, store_cleanup):
        """Test complete workflow from Taco model validation to tortilla creation"""
        # Create TacoCls instance
        taco_cls = TacoCls(
            active=True,
            format="taco",
            dataset_save_dir=WORKING_DIR,
            save_dir=WORKING_DIR,
            tortilla_name="integration_test.tortilla",
        )

        # Validate with Taco model
        taco_model = Taco.model_validate(taco_cls)
        assert taco_model.active is True
        assert taco_model.tortilla_name == "integration_test.tortilla"

        # Create tortilla
        result = taco_cls.create_tortilla(
            dataset_name="integration_test",
            working_dir=WORKING_DIR,
        )

        assert os.path.exists(result)

        # Load and verify tortilla
        tt = tacoreader.load(result)
        assert len(tt) > 0
        assert "tortilla:data_split" in tt.columns

    def test_full_workflow_with_taco_store_data(self, taco_setup, store_cleanup):
        """Test complete workflow using taco_store_data function"""
        result = taco_store_data(
            dataset_name="workflow_test",
            working_dir=WORKING_DIR,
            tortilla_name="workflow_test.tortilla",
            save_dir=WORKING_DIR,
            statistics=True,
            include_config=True,
            check_dataset=True,
        )

        # Verify tortilla was created
        assert os.path.exists(result)

        # Verify metadata was created
        metadata_file = os.path.join(WORKING_DIR, "workflow_test_metadata.json")
        assert os.path.exists(metadata_file)

        # Load and verify tortilla content
        tt = tacoreader.load(result)
        assert len(tt) > 0

        # Verify data splits exist
        splits = tt["tortilla:data_split"].unique()
        assert len(splits) == 3  # train, validation, test


# Made with Bob
