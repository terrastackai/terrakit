# © Copyright IBM Corporation 2025-2026
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


def plot_era5_variable(dataset, variable_name, time_index=0):
    """
    Plot a single variable from the dataset with spatial map and optional profile.
    Handles both datasets with time dimension and single-date datasets without time dimension.

    Parameters:
        dataset: xarray Dataset with variables as data variables
        variable_name: Name of the variable to plot
        time_index: Time index to plot (default: 0, first day only) - ignored if no time dimension
    """
    if variable_name not in dataset.data_vars:
        raise ValueError(
            f"Variable '{variable_name}' not found in dataset. Available: {list(dataset.data_vars)}"
        )

    # Get the variable data
    var_data = dataset[variable_name]
    step_type = var_data.attrs.get("GRIB_stepType", "unknown")

    # Determine coordinate names
    lat_name = "latitude" if "latitude" in var_data.dims else "lat"
    lon_name = "longitude" if "longitude" in var_data.dims else "lon"
    time_name = (
        "time"
        if "time" in var_data.dims
        else ("valid_time" if "valid_time" in var_data.dims else None)
    )

    # Get spatial dimensions
    n_lat = len(var_data[lat_name])
    n_lon = len(var_data[lon_name])

    # Select data for this time step (if time dimension exists)
    if time_name and time_name in var_data.dims:
        spatial_slice = var_data.isel({time_name: time_index})
        time_str = str(var_data[time_name].values[time_index])[:10]
    else:
        # No time dimension - use data as is
        spatial_slice = var_data
        # Try to get time from coordinates
        if "time" in var_data.coords:
            time_str = str(var_data.coords["time"].values)[:10]
        elif "valid_time" in var_data.coords:
            time_str = str(var_data.coords["valid_time"].values)[:10]
        else:
            time_str = "unknown date"

    # Check if we have enough spatial data for meaningful plots
    if n_lat > 1 and n_lon > 1:
        # Full 2D spatial data - create profile and map
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 5))

        # Plot 1: Longitude profile at middle latitude
        lat_idx = n_lat // 2
        spatial_profile = spatial_slice.isel({lat_name: lat_idx})
        spatial_profile.plot(ax=ax1, marker="o", markersize=6)
        ax1.set_title(
            f"stepType: {step_type} | Variable: {variable_name}\nLongitude Profile at lat={float(var_data[lat_name].values[lat_idx]):.2f}"
        )
        ax1.set_xlabel("Longitude")
        ax1.set_ylabel(f"{var_data.attrs.get('units', 'Value')}")
        ax1.grid(True, alpha=0.3)

        # Plot 2: Spatial map
        im = spatial_slice.plot.pcolormesh(ax=ax2, cmap="viridis", add_colorbar=True)  # noqa: F841
        ax2.set_title(
            f"stepType: {step_type} | Variable: {variable_name}\nSpatial Map at {time_str}"
        )
        ax2.set_xlabel("Longitude")
        ax2.set_ylabel("Latitude")

    elif n_lat > 1:
        # Only latitude varies - plot latitude profile
        fig, ax = plt.subplots(1, 1, figsize=(10, 6))
        spatial_slice.plot.pcolormesh(ax=ax, cmap="viridis", add_colorbar=True)

        ax.set_title(
            f"stepType: {step_type} | Variable: {variable_name}\nLatitude Profile at {time_str}"
        )
        ax.set_xlabel("Latitude")
        ax.set_ylabel(f"{var_data.attrs.get('units', 'Value')}")
        ax.grid(True, alpha=0.3)

    elif n_lon > 1:
        # Only longitude varies - plot longitude profile
        fig, ax = plt.subplots(1, 1, figsize=(10, 6))
        spatial_slice.plot.pcolormesh(ax=ax, cmap="viridis", add_colorbar=True)

        ax.set_title(
            f"stepType: {step_type} | Variable: {variable_name}\nLongitude Profile at {time_str}"
        )
        ax.set_xlabel("Longitude")
        ax.set_ylabel(f"{var_data.attrs.get('units', 'Value')}")
        ax.grid(True, alpha=0.3)

    else:
        # Single point - just display the value
        fig, ax = plt.subplots(1, 1, figsize=(8, 6))
        value = float(spatial_slice.values)
        ax.text(
            0.5,
            0.5,
            f"{variable_name}\n{value:.4f} {var_data.attrs.get('units', '')}",
            ha="center",
            va="center",
            fontsize=16,
            transform=ax.transAxes,
        )
        ax.set_title(f"stepType: {step_type} | Single Point Value at {time_str}")
        ax.axis("off")

    plt.tight_layout()
    return fig


def plot_era5_variables_comparison(dataset, time_index=0):
    """
    Plot all variables side by side with spatial maps for the first day.
    Handles both datasets with time dimension and single-date datasets without time dimension.

    Parameters:
        dataset: xarray Dataset with variables as data variables
        time_index: Time index to plot (default: 0, first day only) - ignored if no time dimension
    """
    variables = list(dataset.data_vars)
    n_vars = len(variables)

    # Determine coordinate names from first variable
    first_var = dataset[variables[0]]
    time_name = (
        "time"
        if "time" in first_var.dims
        else ("valid_time" if "valid_time" in first_var.dims else None)
    )
    lat_name = "latitude" if "latitude" in first_var.dims else "lat"
    lon_name = "longitude" if "longitude" in first_var.dims else "lon"

    # Check spatial dimensions
    n_lat = len(first_var[lat_name])
    n_lon = len(first_var[lon_name])

    # Get time string
    if time_name and time_name in first_var.dims:
        time_str = str(first_var[time_name].values[time_index])[:10]
    elif "time" in first_var.coords:
        time_str = str(first_var.coords["time"].values)[:10]
    elif "valid_time" in first_var.coords:
        time_str = str(first_var.coords["valid_time"].values)[:10]
    else:
        time_str = "unknown date"

    # Determine grid layout
    n_cols = min(3, n_vars)  # Max 3 columns
    n_rows = (n_vars + n_cols - 1) // n_cols

    fig, axes = plt.subplots(n_rows, n_cols, figsize=(6 * n_cols, 5 * n_rows))

    if n_vars == 1:
        axes = [axes]
    else:
        axes = axes.flatten()

    for v_idx, var_name in enumerate(variables):
        ax = axes[v_idx]
        var_data = dataset[var_name]

        # Select data for this time step (if time dimension exists)
        if time_name and time_name in var_data.dims:
            data = var_data.isel({time_name: time_index})
        else:
            data = var_data

        step_type = var_data.attrs.get("GRIB_stepType", "unknown")

        # Plot based on spatial dimensions
        if n_lat > 1 and n_lon > 1:
            # 2D spatial map
            data.plot.pcolormesh(ax=ax, cmap="viridis", add_colorbar=True)
            ax.set_xlabel("Longitude")
            ax.set_ylabel("Latitude")
        elif n_lat > 1 or n_lon > 1:
            # 1D profile
            data.plot(ax=ax, marker="o")
            ax.set_ylabel(f"{var_data.attrs.get('units', 'Value')}")
            ax.grid(True, alpha=0.3)
        else:
            # Single point
            value = float(data.values)
            ax.text(
                0.5,
                0.5,
                f"{value:.4f}\n{var_data.attrs.get('units', '')}",
                ha="center",
                va="center",
                fontsize=12,
                transform=ax.transAxes,
            )
            ax.axis("off")

        ax.set_title(
            f"stepType: {step_type} | {var_name}", fontsize=10, fontweight="bold"
        )

    # Hide extra subplots
    for idx in range(n_vars, len(axes)):
        axes[idx].axis("off")

    plt.suptitle(f"All Variables - {time_str}", fontsize=14, fontweight="bold", y=1.02)
    plt.tight_layout()
    return fig
