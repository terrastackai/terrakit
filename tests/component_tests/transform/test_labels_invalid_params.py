# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


import os
import pandas as pd
import pytest
import shutil

from glob import glob
from pathlib import Path

from terrakit.transform.labels import process_labels
from terrakit.general_utils.exceptions import (
    TerrakitValidationError,
    TerrakitValueError,
)
from tests.component_tests.transform.conftest import (
    DEFAULT_DATASET_NAME,
    EMPTY_LABELS_FOLDER,
    LABELS_FOLDER,
    LABELS_FOLDER_CSV_DATETIME,
    WORKING_DIR,
    DEFAULT_WORKING_DIR,
)


class TestLabels_WorkingDir_FailureTests:
    def test_process_labels__working_dir_invalid_not_directory(
        self, process_labels_clean_up_working_dir
    ):
        """Test an error is thrown if a file is provided as the working directory"""
        # Ensure working dir exist before starting
        Path(WORKING_DIR).mkdir(parents=True, exist_ok=True)
        # Create a file to use instead of a valid directory
        Path(f"{WORKING_DIR}/not_a_directory.txt").touch()

        with pytest.raises(TerrakitValidationError) as e:
            process_labels(
                working_dir=f"{WORKING_DIR}/not_a_directory.txt",
                labels_folder=LABELS_FOLDER,
            )
        assert "Invalid parent arguments" in str(e.value)

    @pytest.mark.skip("WiP")
    def test_process_labels__working_dir_invalid_file(
        self,
    ):
        """Test a file is not created instead of a directory if a file as provided as the working directory"""
        # Ensure working dir exist before starting
        assert os.path.exists(WORKING_DIR) is True

        with pytest.raises(TerrakitValidationError) as e:
            process_labels(
                working_dir=f"{WORKING_DIR}/not_a_directory.txt",
                labels_folder=LABELS_FOLDER,
            )
        assert "Invalid root arguments" in str(e.value)


class TestLabels_LabelsFolder_FailureTests:
    def test_process_labels__labels_folder_arg_missing(
        self,
    ):
        """Test label_folder argument is required"""
        with pytest.raises(TypeError) as e:
            process_labels()

        assert "missing 1 required positional argument: 'labels_folder'" in str(e.value)

    def test_process_labels__labels_folder_does_not_exist(self, caplog):
        """Test an exception is raise if a labels folder"""
        # Ensure labels dir does not exist before starting"""
        assert os.path.exists(EMPTY_LABELS_FOLDER) is False

        with pytest.raises(TerrakitValidationError) as e:
            process_labels(labels_folder=EMPTY_LABELS_FOLDER)
        assert "Invalid label arguments" in str(e.value)

    def test_process_labels__labels_folder_empty(
        self, process_labels_clean_up_labels_dir, caplog
    ):
        """Test labels folder must have at least one file in it"""
        # Ensure labels dir does not exist before starting
        assert os.path.exists(EMPTY_LABELS_FOLDER) is False
        # Create empty label files
        Path(EMPTY_LABELS_FOLDER).mkdir(parents=True, exist_ok=True)

        with pytest.raises(TerrakitValidationError) as e:
            process_labels(
                labels_folder=EMPTY_LABELS_FOLDER,
            )

        assert (
            "Labels folder 'tests/tmp/labels' does not contain any supported files"
            in str(e.value)
        )

    def test_process_labels__labels_not_json(
        self, process_labels_clean_up_labels_dir, caplog
    ):
        """Test files in labels folder must be .json"""
        # Ensure labels dir does not exist before starting
        assert os.path.exists(EMPTY_LABELS_FOLDER) is False
        # Create empty label files with one file that is not a .json file
        Path(EMPTY_LABELS_FOLDER).mkdir(parents=True, exist_ok=True)
        Path(f"{EMPTY_LABELS_FOLDER}/not_a_geojson_2024-01-01.txt").touch()

        with pytest.raises(TerrakitValidationError) as e:
            process_labels(
                labels_folder=EMPTY_LABELS_FOLDER,
            )

        assert (
            "Labels folder 'tests/tmp/labels' does not contain any supported files."
            in str(e.value)
        )

    def test_process_labels__labels_not_geojson(
        self,
        process_labels_clean_up_labels_dir,
        process_labels_clean_up_default_working_dir,
        caplog,
    ):
        """Test files in labels dir must be valid geojson"""
        # Ensure labels dir does not exist before starting
        assert os.path.exists(EMPTY_LABELS_FOLDER) is False
        # Create invalid label files
        labels_dir = EMPTY_LABELS_FOLDER
        Path(labels_dir).mkdir(parents=True, exist_ok=True)

        Path(f"{labels_dir}/not_a_geojson_2024-01-01.json").touch()

        with pytest.raises(
            TerrakitValueError, match="There was an issue loading labels."
        ):
            process_labels(
                labels_folder=labels_dir,
            )
        # Check for warning messages in the logs:
        assert "0/1 label files were successfully processed." in caplog.text

        # Check that a valid label file will still be processed
        label_file = glob(f"{LABELS_FOLDER}/*")
        shutil.copyfile(label_file[0], f"{labels_dir}/{label_file[0].split('/')[-1]}")

        labels_gdf, grouped_boxes_gdf = process_labels(
            labels_folder=labels_dir,
        )
        # Check that the valid geojson file has been processed
        assert "1/2 label files were successfully processed." in caplog.text
        assert len(grouped_boxes_gdf) > 0
        assert len(labels_gdf) > 0


class TestLabels_DatatimeInfo_FailureTests:
    """Test process_labels fails as expected when datetime_info is set incorrectly"""

    def test_process_labels__datetime_info__filename__no_date_in_filename(
        self,
        caplog,
        process_labels_clean_up_default_working_dir,
        process_labels_clean_up_labels_dir,
    ):
        """If date info is missing from filename, test file is not processed and that no exceptions are raised so that processing can continue for other files in the directory."""
        # Ensure labels dir does not exist before starting
        assert os.path.exists(EMPTY_LABELS_FOLDER) is False
        # create label files with missing YYYY-MM-DD info
        Path(EMPTY_LABELS_FOLDER).mkdir(parents=True, exist_ok=True)
        label_file = glob(f"{LABELS_FOLDER}/*")
        shutil.copyfile(
            label_file[0],
            f"{EMPTY_LABELS_FOLDER}/label_file_without_datetime_info.json",
        )
        # If no label files are successfully processed, then raise an exception
        with pytest.raises(
            TerrakitValueError, match="There was an issue loading labels"
        ):
            labels_gdf, grouped_boxes_gdf = process_labels(
                labels_folder=EMPTY_LABELS_FOLDER,
                datetime_info="filename",
            )
        assert " 0/1 label files were successfully processed" in caplog.text
        # Check that no shp files have been written.
        assert f"{DEFAULT_DATASET_NAME}_all_bboxes.shp" not in os.listdir(
            DEFAULT_WORKING_DIR
        )

        # Now check that if processing continues as expected if a valid file also exists in the labels folder.
        shutil.copyfile(
            label_file[1],
            f"{EMPTY_LABELS_FOLDER}/f{label_file[1].split('/')[-1]}",
        )

        labels_gdf, grouped_boxes_gdf = process_labels(
            labels_folder=EMPTY_LABELS_FOLDER,
            datetime_info="filename",
        )
        assert isinstance(grouped_boxes_gdf, pd.DataFrame)
        assert len(grouped_boxes_gdf) == 1
        assert isinstance(labels_gdf, pd.DataFrame)
        assert len(labels_gdf) == 12
        assert " 1/2 label files were successfully processed" in caplog.text
        # Check that no shp files have been written.
        assert f"{DEFAULT_DATASET_NAME}_all_bboxes.shp" in os.listdir(
            DEFAULT_WORKING_DIR
        )
        assert f"{DEFAULT_DATASET_NAME}_labels.shp" in os.listdir(DEFAULT_WORKING_DIR)

    def test_process_labels__datetime_info__arg_invalid(
        self,
        process_labels_clean_up_default_working_dir,
    ):
        """Test an exception is raised if datetime_info is set incorrectly"""
        with pytest.raises(TerrakitValidationError) as e:
            process_labels(
                labels_folder=LABELS_FOLDER,
                datetime_info="invalid",
            )
        assert "Invalid label arguments" in str(e.value)
        # Check that no shp files have been written.
        assert f"{DEFAULT_DATASET_NAME}_all_bboxes.shp" not in os.listdir(
            DEFAULT_WORKING_DIR
        )

    def test_process_labels__csv_missing(
        self,
    ):
        with pytest.raises(
            TerrakitValidationError,
            match="No metadata.csv file provided in labels directory.",
        ):
            process_labels(
                labels_folder=LABELS_FOLDER,  # No metadata.csv exists in this directory
                datetime_info="csv",
            )

    def test_process_labels__csv_missing_headers(
        self,
        process_labels_setup_csv_datetime_missing_headers,
        process_labels_clean_up_csv_labels_dir,
    ):
        """Test an exception is raised if no headers are include in csv"""

        with pytest.raises(
            TerrakitValidationError,
            match="metadata.csv missing 'filename,date' headers",
        ):
            process_labels(
                labels_folder=LABELS_FOLDER_CSV_DATETIME,  # No metadata.csv exists in this directory
                datetime_info="csv",
            )

    def test_process_labels__csv_entry_missingv(
        self,
        process_labels_setup_csv_datetime,
        process_labels_clean_up_csv_labels_dir,
        process_labels_clean_up_default_working_dir,
        caplog,
    ):
        """Test that a file will only be processed if it is included in the metadata"""
        Path(f"{LABELS_FOLDER_CSV_DATETIME}/extra_label_file.json").touch()
        process_labels(
            labels_folder=LABELS_FOLDER_CSV_DATETIME,  # A file exists in the directory that is not included in the metadata.csv
            datetime_info="csv",
        )
        assert " 2/3 label files were successfully processed" in caplog.text

    def test_process_labels__csv_invalid_date_format(
        self,
        process_labels_setup_csv_datetime_invalid_date,
        process_labels_clean_up_csv_labels_dir,
        process_labels_clean_up_default_working_dir,
        caplog,
    ):
        """Test that a file will only be processed if the date is in the correct format"""
        process_labels(
            labels_folder=LABELS_FOLDER_CSV_DATETIME,  # A file exists in the directory that is not included in the metadata.csv
            datetime_info="csv",
        )
        assert "1/2 label files were successfully processed" in caplog.text


class TestLabels_LabelType_FailureTests:
    def test_process_labels__label_type__arg_invalid(
        self, process_labels_clean_up_default_working_dir
    ):
        """Test an exception is raised if active is set incorrectly"""
        with pytest.raises(TerrakitValidationError) as e:
            process_labels(
                labels_folder=LABELS_FOLDER,
                label_type="invalid_label_type",
            )
        assert "Invalid label arguments" in str(e.value)
        # Check that no shp files have been written.
        assert f"{DEFAULT_DATASET_NAME}_all_bboxes.shp" not in os.listdir(
            DEFAULT_WORKING_DIR
        )
