# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


import numpy as np


def compute_stats(dataset):
    """
    Compute descriptive statistics for a given dataset.

    Parameters:
        dataset (numpy.ndarray): The dataset for which to compute statistics.

    Returns:
        tuple: A tuple containing mean, median, minimum, maximum, standard deviation, and count of the dataset.
    """
    mean_val = np.mean(dataset)
    median_val = np.median(dataset)
    min_val = np.min(dataset)
    max_val = np.max(dataset)
    std_dev = np.std(dataset)
    count = dataset.size
    print(f"Mean pixel value: {mean_val}")
    print(f"Median pixel value: {median_val}")
    print(f"Minimum pixel value: {min_val}")
    print(f"Maximum pixel value: {max_val}")
    print(f"Standard deviation: {std_dev}")
    print(f"Number of masked pixels: {count}\n--------")
    return mean_val, median_val, min_val, max_val, std_dev, count


def compute_stats_for_masked_pixels(image, mask):
    """
    Compute descriptive statistics for masked pixels in the given image.

    Parameters:
        image (numpy.ndarray): The image data.
        mask (numpy.ndarray): The mask to filter the image data.

    Returns:
        tuple: A tuple containing mean, median, minimum, maximum, standard deviation, and count of the masked pixels.
    """
    masked_data = image[mask > 0]
    return compute_stats(masked_data)


def load_verified_stats():
    """
    Load precomputed statistics for verified data calculated from target_tif = "sentinel_aws_sentinel-2-l2a_2024-08-30_imputed_20" generated using EMSR748

    Returns:
        tuple: A tuple containing verified label statistics, verified data statistics, and verified masked statistics.
    """
    verified_label_stats = (
        np.float64(0.532928466796875),
        np.float64(1.0),
        np.float64(0.0),
        np.float64(1.0),
        np.float64(0.49891453784632),
        65536,
    )

    verified_data_stats = (
        np.float64(6038.482086181641),
        np.float64(6510.0),
        np.float64(507.0),
        np.float64(10984.0),
        np.float64(1818.4264044731774),
        65536,
    )

    verified_mask_stats = (
        np.float64(6435.794422493272),
        np.float64(6784.0),
        np.float64(664.0),
        np.float64(9424.0),
        np.float64(1584.2594606115797),
        34926,
    )
    return verified_label_stats, verified_data_stats, verified_mask_stats
