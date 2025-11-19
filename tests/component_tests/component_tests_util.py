# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


from pathlib import Path
import xarray as xr
import numpy as np
import pandas as pd


def convert_angle_to_0_360(angle):
    """
    Convert an angle to a 0-360 degree system.

    Parameters:
    angle (float): The angle to convert.

    Returns:
    float: The angle in the 0-360 degree system.
    """
    if angle < 0:
        return angle + 360
    else:
        return angle


def create_xarray(
    min_x: float,
    max_x: float,
    size_x: int,
    min_y: float,
    max_y: float,
    size_y: int,
    start_date: str,
    end_date: str,
    periods: int,
    bands: list[str],
    is_360_degree_system: bool = False,
    is_dataarray: bool = False,
    x_dim: str = "x",
    y_dim: str = "y",
    time_dim: str = "time",
) -> xr.DataArray | xr.Dataset:
    # Define the dimensions and their corresponding coordinates
    time_coords = pd.date_range(start=start_date, periods=periods, end=end_date)
    y_coords = np.linspace(min_y, max_y, size_y)
    if is_360_degree_system:
        new_min_x = convert_angle_to_0_360(min_x)
        new_max_x = convert_angle_to_0_360(max_x)
        x_coords = np.linspace(new_min_x, new_max_x, size_x)
    else:
        x_coords = np.linspace(min_x, max_x, size_x)

    # Create some sample data with the desired shape
    data = np.random.rand(len(time_coords), len(x_coords), len(y_coords))

    # Create the xarray DataArray
    ds = xr.Dataset(
        data_vars={bands[0]: ([time_dim, x_dim, y_dim], data)},
        coords={time_dim: time_coords, x_dim: x_coords, y_dim: y_coords},
    )
    if is_dataarray:
        da = ds.to_array(dim="bands")
        return da
    else:
        return ds


def create_netcdf_file(
    working_dir: Path, size_x: int = 256, size_y: int = 256, size_t: int = 10
) -> Path:
    """
    Create a NetCDF file with a specified structure in the given working directory.

    This function generates an xarray Dataset with dimensions for longitude (x), latitude (y), and time,
    and then saves it as a NetCDF file.

    Parameters:
    working_dir (Path): The directory where the NetCDF file will be created.

    Returns:
    Path: The path to the created NetCDF file.
    """

    # Create an xarray Dataset with specified dimensions and attributes
    ds = create_xarray(
        max_x=180,  # Maximum longitude value
        min_x=-180,  # Minimum longitude value
        max_y=90,  # Maximum latitude value
        min_y=-90,  # Minimum latitude value
        size_x=size_x,  # Number of points along the x-axis
        size_y=size_y,  # Number of points along the y-axis
        start_date="2000-01-01",  # Start date of the time series
        end_date="2001-01-01",  # End date of the time series
        periods=size_t,  # Number of time periods
        bands=["temperature"],  # Variable bands (in this case, only 'temp')
        is_360_degree_system=True,  # Indicates if it's a 360-degree (as opposed to 0-360) longitude system
        is_dataarray=False,  # Indicates if the output should be a DataArray (False) or Dataset (True)
        x_dim="x",  # Dimension name for longitude
        y_dim="y",  # Dimension name for latitude
        time_dim="time",  # Dimension name for time
    )

    # Define the path for the NetCDF file
    path = working_dir / "test.nc"

    # Save the xarray Dataset to a NetCDF file
    ds.to_netcdf(path=path)

    # Assert that the NetCDF file has been created
    assert path.exists()

    return path
