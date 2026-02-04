# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


# Docstings assisted by watsonx Code Assistant

import contextily as cx
import folium
from matplotlib.colors import to_hex
import matplotlib.pyplot as plt
import numpy as np
import random
import rasterio

from pathlib import Path
from pandas import DataFrame

from terrakit.general_utils.exceptions import TerrakitBaseException


def normalize_band(band) -> np.ndarray:
    """
    Normalize bands to a range of 0 to 1.

    Parameters:
        band (numpy.ndarray): A single band from a raster dataset.

    Returns:
        numpy.ndarray: Normalized band values.
    """
    band_float = band.astype(np.float32)
    band_min = np.min(band_float)
    band_max = np.max(band_float)
    return (band_float - band_min) / (band_max - band_min)


def plot_label_dataframes(labels_gdf: DataFrame, grouped_bbox_gdf: DataFrame) -> None:
    """Plot label DataFrames for each unique label file.

    Parameters:
        label_gdf (DataFrame): labels DataFrame
        grouped_bbox_gdf (DataFrame): grouped bbox DataFrame

    """
    dates = grouped_bbox_gdf["datetime"].unique()
    classes = grouped_bbox_gdf["labelclass"].unique()

    fig, axs = plt.subplots(1, len(dates), figsize=(15, 4))
    if len(dates) == 1:
        axs = [axs]
    cmap = plt.cm.get_cmap("tab10", len(classes))

    for date_id in range(0, len(dates)):
        filename = "\n".join(
            labels_gdf.loc[labels_gdf["datetime"] == dates[date_id]][
                "filename"
            ].unique()
        )

        axs[date_id].set_title(filename)
        axs[date_id].set_xlabel("lng")
        axs[date_id].set_ylabel("lat")

        for class_id in range(0, len(classes)):
            cls = classes[class_id]
            grouped_bbox_gdf.loc[
                (grouped_bbox_gdf["datetime"] == dates[date_id])
                & (grouped_bbox_gdf["labelclass"] == classes[class_id])
            ].boundary.plot(ax=axs[date_id], color=cmap(class_id), label="bbox")
            labels_gdf.loc[
                (labels_gdf["datetime"] == dates[date_id])
                & (labels_gdf["labelclass"] == classes[class_id])
            ].boundary.plot(
                ax=axs[date_id], color=cmap(class_id), label=f"label class {cls}"
            )

        if len(classes) > 1:
            axs[date_id].legend(
                loc="upper left",
                bbox_to_anchor=(1.02, 1.0),
                borderaxespad=0.0,
                title="Classes",
            )


def plot_labels_on_map(
    labels_gdf: DataFrame, grouped_bbox_gdf: DataFrame
) -> tuple[list, list]:
    """Plot label DataFrames for each unique label file.

    Parameters:
        label_gdf (DataFrame): labels DataFrame
        grouped_bbox_gdf (DataFrame): grouped bbox DataFrame

    Returns:
        list: a list of maps with bbox and labels ready to be displayed.
        list: a list of title strings to accompany each map.
    """
    map_collection = []
    title_list = []

    dates = grouped_bbox_gdf["datetime"].unique()
    classes = grouped_bbox_gdf["labelclass"].unique()
    cmap = plt.cm.get_cmap("tab10", len(classes))

    for date_id in range(0, len(dates)):
        filenames = ",".join(
            labels_gdf.loc[labels_gdf["datetime"] == dates[date_id]][
                "filename"
            ].unique()
        )

        bbox = grouped_bbox_gdf.loc[grouped_bbox_gdf["datetime"] == dates[date_id]]

        center_lat = (bbox.bounds.miny.mean() + bbox.bounds.maxy.mean()) / 2
        center_lon = (bbox.bounds.minx.mean() + bbox.bounds.maxx.mean()) / 2

        m = folium.Map(
            location=[center_lat, center_lon], zoom_start=5, tiles="OpenStreetMap"
        )

        m.fit_bounds(
            [
                [bbox.bounds.miny.mean(), bbox.bounds.minx.mean()],
                [bbox.bounds.maxy.mean(), bbox.bounds.maxx.mean()],
            ]
        )
        title_list.append(
            f"\n\nDownload tile bounding box and labels for: {filenames}\n"
        )

        for class_id in range(0, len(classes)):
            filename = ",".join(
                labels_gdf.loc[
                    (labels_gdf["datetime"] == dates[date_id])
                    & (labels_gdf["labelclass"] == classes[class_id])
                ]["filename"].unique()
            )
            class_bbox = grouped_bbox_gdf.loc[
                (grouped_bbox_gdf["datetime"] == dates[date_id])
                & (grouped_bbox_gdf["labelclass"] == classes[class_id])
            ]
            labels = labels_gdf.loc[
                (labels_gdf["datetime"] == dates[date_id])
                & (labels_gdf["labelclass"] == classes[class_id])
            ]
            color_hex = to_hex(cmap(class_id))
            folium.GeoJson(
                class_bbox.to_json(),
                name=f"Download tile bounding box {filename}",
                style_function=lambda feat, c=color_hex: {
                    "color": c,
                    "weight": 2,
                    "fillOpacity": 0,
                },
            ).add_to(m)

            folium.GeoJson(
                labels.to_json(),
                name=f"Shapefile Data {filename}",
                style_function=lambda feat, c=color_hex: {
                    "color": c,
                    "fillColor": c,
                    "weight": 2,
                    "fillOpacity": 0.5,
                },
            ).add_to(m)

        folium.LayerControl().add_to(m)

        map_collection.append(m)

    return map_collection, title_list


def plot_tiles_and_label_pair(
    image_list,
    bands,
    scale=1,
    alpha: float = 0.7,
    samples: int = 2,
    no_data_value: int = 0,
) -> None:
    """
    Plot tiles and labels side by side.

    Parameters:
        image_list (list): List of downloaded tile paths.

        bands (list): Image bands to plot.

        scale (int): Scaling factor used during normalization. Default is 1. Increase to 10 for images with high reflectance.

        alpha (float): Transparency between 0 and 1 of image and label.

        samples (int): Number of tile/label pairs to plot. Default is 2. Max is 10.

        no_data_value (int): Value to treat as no-data/background and set to NaN for visualization. Default is 0. Set to -1 when using set_no_data=True in download_data.

    Raises
        TerrakitBaseException: If an error occurs during plotting.
    """

    max_samples = 4
    if samples > max_samples:
        samples = max_samples

    image_list = random.sample(image_list, min([len(image_list), samples]))

    image: list = []
    count = 1
    try:
        image_path = image_list
        for i in range(0, len(image_list)):
            fig = plt.figure(i, figsize=(15, 4), layout="constrained")
            image = []
            with rasterio.open(image_path[i]) as src:
                for band_index in range(1, len(bands) + 1):
                    image.append(normalize_band(src.read(band_index)) * scale)
                # Get the key geospatial metadata
                crs = src.crs
                bounds = (
                    src.bounds
                )  # Returns (left, bottom, right, top) in the native CRS

            extent = [bounds.left, bounds.right, bounds.bottom, bounds.top]
            suffix = Path(image_path[i]).suffix
            label_path = image_path[i].replace(suffix, f"_labels{suffix}")

            # Check if label file exists
            if not Path(label_path).exists():
                labels = np.zeros_like(image[0])
            else:
                with rasterio.open(label_path) as src:
                    labels = src.read(1).astype(np.float32)
                    labels[labels == no_data_value] = np.nan

            image_stack = np.dstack(image)

            axs = fig.add_subplot(2, len(image_list), count)
            axs.imshow(image_stack, extent=extent, alpha=alpha, zorder=2)
            axs.axis("off")
            axs.set_title(f"image_{i}")
            cx.add_basemap(
                axs,
                crs=crs,
                source=cx.providers.OpenStreetMap.Mapnik,
            )
            count = count + 1

            axs_labels = fig.add_subplot(2, len(image_list), count)

            # Get unique label classes (excluding NaN values)
            unique_classes = np.unique(labels[~np.isnan(labels)])

            # Create a colormap for the label classes
            cmap = (
                plt.cm.get_cmap("tab10", len(unique_classes))
                if len(unique_classes) > 0
                else None
            )

            # Display labels with colormap
            im = axs_labels.imshow(
                labels, alpha=alpha, extent=extent, zorder=2, cmap=cmap
            )

            # Add colorbar with class labels if there are multiple classes
            if len(unique_classes) > 1:
                cbar = plt.colorbar(im, ax=axs_labels, fraction=0.046, pad=0.04)
                cbar.set_label("Label Classes", rotation=270, labelpad=15)
                cbar.set_ticks(unique_classes)
                cbar.set_ticklabels([f"{int(c)}" for c in unique_classes])

            axs_labels.axis("off")
            axs_labels.set_title(f"label_{i}")
            cx.add_basemap(
                axs_labels,
                crs=crs,
                source=cx.providers.OpenStreetMap.Mapnik,
            )
            count = count + 1
            plt.show()
            print("\033[1m" + "Legend" + "\033[0m")
            print(
                f"image_{i}: {image_list[i].split('/')[-1]}, label_{i}: {label_path.split('/')[-1]}"
            )
            print("---")

    except Exception as e:
        raise TerrakitBaseException(
            f"Something went wrong plotting image label pairs: {e}"
        )


def plot_chip_and_label_pairs(
    chip_list: list,
    bands: list,
    scale: int = 1,
    chip_suffix: str = ".data.tif",
    chip_label_suffix: str = ".label.tif",
    samples: int = 10,
    no_data_value: int = 0,
) -> None:
    """
    Plot chip and label pairs with different colors for each label class.

    Parameters:
        chip_list (list): List of paths to chips.
        bands (list): Image bands to plot.
        scale (int): Scaling factor used during normalization. Default is 1. Increase to 10 for images with high reflectance.
        chip_suffix (str): Chip suffix.
        chip_label_suffix (str): Chipped label suffix.
        samples (int): Number of sample pairs to plot. Default 10. Max is 10.
        no_data_value (int): Value to treat as no-data/background and set to NaN for visualization. Default is 0. Set to -1 when using set_no_data=True in download_data.

    Raises
        TerrakitBaseException: If an error occurs during plotting.
    """

    max_samples = 10
    if samples > max_samples:
        samples = max_samples

    chip_list = [chip for chip in chip_list if chip_suffix in chip]
    chip_list = random.sample(chip_list, min([len(chip_list), samples]))

    fig, axs = plt.subplots(1, len(chip_list), figsize=(15, 4))
    label_fig, label_axs = plt.subplots(1, len(chip_list), figsize=(15, 4))

    # Handle single chip case where axs is not an array
    if len(chip_list) == 1:
        axs = [axs]
        label_axs = [label_axs]

    # First pass: collect all unique classes across all chips
    all_labels = []
    for chip_path in chip_list:
        with rasterio.open(chip_path.replace(chip_suffix, chip_label_suffix)) as src:
            label_data = src.read(1)
            all_labels.append(label_data)

    # Get all unique classes across all chips (excluding no_data_value)
    all_unique_classes = np.unique(
        np.concatenate([label[label != no_data_value] for label in all_labels])
    )

    # Create a single colormap for all chips based on all classes
    cmap = (
        plt.cm.get_cmap("tab10", len(all_unique_classes))
        if len(all_unique_classes) > 0
        else None
    )
    vmin = all_unique_classes.min() if len(all_unique_classes) > 0 else 0
    vmax = all_unique_classes.max() if len(all_unique_classes) > 0 else 1

    # Second pass: plot all chips with consistent colormap
    for plot_id in range(0, len(chip_list)):
        image: list = []
        with rasterio.open(chip_list[plot_id]) as src:
            for band_index in range(1, len(bands) + 1):
                image.append(normalize_band(src.read(band_index)) * scale)
        image_stack = np.dstack(image)

        # Use the pre-loaded label data
        label = all_labels[plot_id].astype(np.float32)
        label[label == no_data_value] = np.nan

        axs[plot_id].imshow(image_stack)
        axs[plot_id].axis("off")
        axs[plot_id].set_title(f"image_{plot_id}")

        # Use consistent colormap across all chips
        im = label_axs[plot_id].imshow(label, cmap=cmap, vmin=vmin, vmax=vmax)

        # Add colorbar with class labels if there are multiple classes
        if len(all_unique_classes) > 1:
            cbar = plt.colorbar(im, ax=label_axs[plot_id], fraction=0.046, pad=0.04)
            cbar.set_label("Classes", rotation=270, labelpad=15)
            cbar.set_ticks(all_unique_classes)
            cbar.set_ticklabels([f"{int(c)}" for c in all_unique_classes])

        label_axs[plot_id].axis("off")
        label_axs[plot_id].set_title(f"label_{plot_id}")

    plt.show()
    print("\033[1m" + "Legend" + "\033[0m")
    for i in range(0, len(chip_list)):
        print(f"image_{i}: {chip_list[i].split('/')[-1]}")
    print("---")
    for i in range(0, len(chip_list)):
        print(
            f"label_{i}: {chip_list[i].replace(chip_suffix, chip_label_suffix).split('/')[-1]}"
        )
