#!/usr/bin/env python
"""
Generate test data for multi-class label tests.

This script creates shapefile test resources from multi-class label files
so that tests can run completely offline.
"""

import json
import shutil
from pathlib import Path

from terrakit.general_utils.labels_downloader import rapid_mapping_geojson_downloader
from terrakit.transform.labels import process_labels

# Paths
LABELS_FOLDER_CLASSES = "./tmp_class_labels"
OUTPUT_DIR = "./tmp_class_output"
TEST_RESOURCES_DIR = "./tests/resources/component_test_data/download"

# Expected class label files
EXAMPLE_CLASS_LABEL_FILES = [
    "EMSR801_AOI01_DEL_MONIT02_CLASS_0_observedEventA_v1_2025-04-23.json",
    "EMSR801_AOI01_DEL_MONIT02_CLASS_1_observedEventA_v1_2025-04-23.json",
]


def create_class_label_files():
    """Download and split label file into class-specific files."""
    print("Creating class label files...")
    
    # Clean up and create labels folder
    if Path(LABELS_FOLDER_CLASSES).exists():
        shutil.rmtree(LABELS_FOLDER_CLASSES)
    Path(LABELS_FOLDER_CLASSES).mkdir(parents=True, exist_ok=True)

    # Download MONIT02 from EMSR801 (contains 2 spatial features from same date)
    downloaded_file = rapid_mapping_geojson_downloader(
        event_id="801",
        aoi="01",
        monitoring_number="02",
        version="v1",
        dest=LABELS_FOLDER_CLASSES,
    )
    print(f"Downloaded: {downloaded_file}")

    # Split the downloaded file into separate CLASS files based on features
    with open(downloaded_file, "r") as f:
        data = json.load(f)

    # Create CLASS_0 file with first feature (large burnt area)
    class_0_data = data.copy()
    class_0_data["features"] = [data["features"][1]]  # Larger area feature
    class_0_file = downloaded_file.replace(
        "_observedEventA_v1_", "_CLASS_0_observedEventA_v1_"
    )
    with open(class_0_file, "w") as f:
        json.dump(class_0_data, f)
    print(f"Created: {class_0_file}")

    # Create CLASS_1 file with second feature (small burnt area)
    class_1_data = data.copy()
    class_1_data["features"] = [data["features"][0]]  # Smaller area feature
    class_1_file = downloaded_file.replace(
        "_observedEventA_v1_", "_CLASS_1_observedEventA_v1_"
    )
    with open(class_1_file, "w") as f:
        json.dump(class_1_data, f)
    print(f"Created: {class_1_file}")

    # Remove the original combined file
    Path(downloaded_file).unlink()
    print(f"Removed: {downloaded_file}")


def generate_shapefiles():
    """Run process_labels to generate shapefiles."""
    print("\nGenerating shapefiles...")
    
    # Clean up output directory
    if Path(OUTPUT_DIR).exists():
        shutil.rmtree(OUTPUT_DIR)
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

    # Process labels to generate shapefiles
    labels_gdf, grouped_boxes_gdf = process_labels(
        dataset_name="terrakit_curated_dataset_classes",
        working_dir=OUTPUT_DIR,
        labels_folder=LABELS_FOLDER_CLASSES,
    )
    
    print(f"Generated shapefiles in: {OUTPUT_DIR}")
    print(f"  - Labels: {len(labels_gdf)} records")
    print(f"  - Bounding boxes: {len(grouped_boxes_gdf)} records")


def copy_to_test_resources():
    """Copy generated shapefiles to test resources directory."""
    print("\nCopying shapefiles to test resources...")
    
    # Ensure test resources directory exists
    Path(TEST_RESOURCES_DIR).mkdir(parents=True, exist_ok=True)
    
    # Copy all shapefile components
    for file in Path(OUTPUT_DIR).glob("terrakit_curated_dataset_classes*"):
        dest = Path(TEST_RESOURCES_DIR) / file.name
        shutil.copy(file, dest)
        print(f"  Copied: {file.name}")


def cleanup():
    """Clean up temporary directories."""
    print("\nCleaning up temporary directories...")
    if Path(LABELS_FOLDER_CLASSES).exists():
        shutil.rmtree(LABELS_FOLDER_CLASSES)
        print(f"  Removed: {LABELS_FOLDER_CLASSES}")
    if Path(OUTPUT_DIR).exists():
        shutil.rmtree(OUTPUT_DIR)
        print(f"  Removed: {OUTPUT_DIR}")


def main():
    """Main function to generate test data."""
    print("=" * 60)
    print("Generating multi-class label test data")
    print("=" * 60)
    
    try:
        create_class_label_files()
        generate_shapefiles()
        copy_to_test_resources()
        cleanup()
        
        print("\n" + "=" * 60)
        print("SUCCESS: Test data generated successfully!")
        print("=" * 60)
        print(f"\nTest resources saved to: {TEST_RESOURCES_DIR}")
        print("Files created:")
        for file in sorted(Path(TEST_RESOURCES_DIR).glob("terrakit_curated_dataset_classes*")):
            print(f"  - {file.name}")
        
    except Exception as e:
        print(f"\nERROR: {e}")
        cleanup()
        raise


if __name__ == "__main__":
    main()

# Made with Bob
