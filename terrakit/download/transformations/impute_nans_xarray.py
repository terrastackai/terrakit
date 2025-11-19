# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


# Docstings assisted by watsonx Code Assistant

import logging
import numpy as np

from xarray import DataArray

from terrakit.general_utils.exceptions import TerrakitBaseException

logger = logging.getLogger(__name__)


def impute_nans_xarray(da: DataArray, nodata_value=-9999) -> DataArray:
    """
    Impute NaN values in an xarray DataArray using nearest neighbor interpolation.

    Parameters:
        da (xarray.DataArray): The input DataArray.
        nodata_value (int): The value representing missing data.

    Raises:
        TerrakitBaseException: If an error occurs during transformation.

    Returns:
        xarray.DataArray: The imputed DataArray.
    """
    logger.info("Imputing NaNs in xarray data array.")
    try:
        total_nodata_pixels = np.count_nonzero(da.isin([nodata_value]))
        if total_nodata_pixels > 0:
            for d in range(0, da.shape[0]):
                for i in range(0, len(da["band"])):
                    slice_dims = da[d, i] if da.ndim == 4 else da[i]
                    interpolated = slice_dims.rio.interpolate_na("nearest")
                    if da.ndim == 4:
                        da[d, i, :, :] = interpolated
                    else:
                        da[i, :, :] = interpolated
        else:
            logger.info("Skipping imputation as no nodata pixels found")
    except Exception as e:
        err_msg = f"An error occurred running 'impute_nans_xarray' with '{nodata_value=}': {e}"
        logger.error(err_msg)
        raise TerrakitBaseException(err_msg)

    return da
