# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


# Docstings assisted by watsonx Code Assistant

import logging

from xarray import DataArray

from terrakit.general_utils.exceptions import TerrakitBaseException

logger = logging.getLogger(__name__)


def scale_data_xarray(da: DataArray, scaling_factors: list) -> DataArray:
    """
    Scale the values in an xarray DataArray by given scaling factors.

    Parameters:
        da (xarray.DataArray): The input DataArray.
        scaling_factors (list): A list of scaling factors corresponding to each band.

    Raises:
        TerrakitBaseException: If an error occurs during transformation.

    Returns:
        xarray.DataArray: The scaled DataArray.
    """
    try:
        for b in range(0, len(scaling_factors)):
            da[:, b] = da[:, b] * scaling_factors[b]
    except Exception as e:
        err_msg = f"An error occuring running 'scale_data_xarray' with '{scaling_factors=}': {e}"
        logger.error(err_msg)
        raise TerrakitBaseException(err_msg)
    return da
