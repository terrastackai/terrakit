# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


from terrakit.general_utils.geospatial_util import clip_box, reproject_bbox
import pytest

from tests.component_tests.component_tests_util import create_xarray


@pytest.mark.parametrize(
    "min_x, min_y, max_x, max_y, size_x, size_y, is_360_degree_system",
    [(-91, 40, -90, 41, 10, 10, False), (-91, 40, -90, 41, 10, 10, True)],
)
def test_clip_box(min_x, min_y, max_x, max_y, size_x, size_y, is_360_degree_system):
    """
    Test function to verify the clipping of an xarray DataArray based on a bounding box.

    Parameters:
    min_x (float): Minimum x-coordinate of the original DataArray.
    min_y (float): Minimum y-coordinate of the original DataArray.
    max_x (float): Maximum x-coordinate of the original DataArray.
    max_y (float): Maximum y-coordinate of the original DataArray.
    size_x (int): Size of the DataArray along the x-dimension.
    size_y (int): Size of the DataArray along the y-dimension.
    is_360_degree_system (bool): True, the x-coordinates are in a 0-360 degree system.
    Returns:
    None
    """

    x_dim = "x"
    y_dim = "y"
    delta_space = 0.2
    # create a random xarray
    da = create_xarray(
        min_x=min_x,
        min_y=min_y,
        max_x=max_x,
        max_y=max_y,
        size_x=size_x,
        size_y=size_y,
        is_dataarray=True,
        bands=["temp"],
        x_dim=x_dim,
        y_dim=y_dim,
        start_date="2000-01-01",
        end_date="2001-01-01",
        periods=10,
        is_360_degree_system=is_360_degree_system,
    )
    # set a bbox to clip the xarray
    bbox = (
        min_x + delta_space,
        min_y + delta_space,
        max_x - delta_space,
        max_y - delta_space,
    )
    # reproject bbox based on 360 degree system
    reprojected_bbox = reproject_bbox(
        bbox=bbox,
        src_crs=4326,
        dst_crs=4326,
        is_360_degree_system=is_360_degree_system,
    )
    clipped_da = clip_box(data=da, bbox=reprojected_bbox, x_dim=x_dim, y_dim=y_dim)
    # validate if coords are correct
    for x in clipped_da[x_dim].values:
        assert x >= reprojected_bbox[0] - delta_space
        assert x <= reprojected_bbox[2] + delta_space
    for y in clipped_da[y_dim].values:
        assert y >= reprojected_bbox[1] - delta_space
        assert y <= reprojected_bbox[3] + delta_space
