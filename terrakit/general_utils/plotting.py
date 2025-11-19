# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


# Docstings assisted by watsonx Code Assistant

import contextily as cx
import folium
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

    fig, axs = plt.subplots(1, len(dates), figsize=(15, 4))

    for plot_id in range(0, len(dates)):
        filename = labels_gdf.loc[labels_gdf["datetime"] == dates[plot_id]][
            "filename"
        ].unique()[0]

        grouped_bbox_gdf.loc[
            grouped_bbox_gdf["datetime"] == dates[plot_id]
        ].boundary.plot(ax=axs[plot_id], label="bbox")
        labels_gdf.loc[labels_gdf["datetime"] == dates[plot_id]].boundary.plot(
            ax=axs[plot_id], color="red", label="label"
        )
        axs[plot_id].set_title(filename)
        axs[plot_id].set_xlabel("lng")
        axs[plot_id].set_ylabel("lat")


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

    for plot_id in range(0, len(dates)):
        filename = labels_gdf.loc[labels_gdf["datetime"] == "2024-08-26"][
            "filename"
        ].unique()[0]

        bbox = grouped_bbox_gdf.loc[grouped_bbox_gdf["datetime"] == dates[plot_id]]
        labels = labels_gdf.loc[labels_gdf["datetime"] == dates[plot_id]]

        center_lat = (bbox.bounds.miny.mean() + bbox.bounds.maxy.mean()) / 2
        center_lon = (bbox.bounds.minx.mean() + bbox.bounds.maxx.mean()) / 2

        m = folium.Map(
            location=[center_lat, center_lon], zoom_start=5, tiles="OpenStreetMap"
        )

        folium.GeoJson(
            bbox.to_json(),
            name=f"Download tile bounding box {filename}",
        ).add_to(m)

        folium.GeoJson(labels.to_json(), name="Shapefile Data", colors="red").add_to(m)

        m.fit_bounds(
            [
                [bbox.bounds.miny.mean(), bbox.bounds.minx.mean()],
                [bbox.bounds.maxy.mean(), bbox.bounds.maxx.mean()],
            ]
        )
        folium.LayerControl().add_to(m)
        title_list.append(
            f"\n\nDownload tile bounding box and labels for: {filename}\n"
        )
        map_collection.append(m)

    return map_collection, title_list


def plot_tiles_and_label_pair(
    image_list,
    bands,
    scale=1,
    alpha: float = 0.7,
    samples: int = 2,
) -> None:
    """
    Plot tiles and labels side by side.

    Parameters:
        image_list (list): List of downloaded tile paths.

        bands (list): Image bands to plot.

        scale (int): Scaling factor used during normalization. Default is 1. Increase to 10 for images with high reflectance.

        alpha (float): Transparency between 0 and 1 of image and label.

        samples (int): Number of tile/label pairs to plot. Default is 2. Max is 10.

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
            with rasterio.open(label_path) as src:
                labels = src.read(1).astype(np.float32)
                labels[labels == 0] = np.nan

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
            axs_labels.imshow(labels, alpha=alpha, extent=extent, zorder=2)
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
) -> None:
    """
    Plot chip and label pairs.

    Parameters:
        chip_list (list): List of paths to chips.
        bands (list): Image bands to plot.
        scale (int): Scaling factor used during normalization. Default is 1. Increase to 10 for images with high reflectance.
        chip_suffix (str): Chip suffix.
        chip_label_suffix (str): Chipped label suffix.
        samples (int): Number of sample pairs to plot. Default 10. Max is 10.

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

    for plot_id in range(0, len(chip_list)):
        image: list = []
        with rasterio.open(chip_list[plot_id]) as src:
            for band_index in range(1, len(bands) + 1):
                image.append(normalize_band(src.read(band_index)) * scale)
        image_stack = np.dstack(image)
        with rasterio.open(
            chip_list[plot_id].replace(chip_suffix, chip_label_suffix)
        ) as src:
            label = src.read(1)
        axs[plot_id].imshow(image_stack)
        axs[plot_id].axis("off")
        axs[plot_id].set_title(f"image_{plot_id}")

        label_axs[plot_id].imshow(label)
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
