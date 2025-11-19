# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


import logging

from datetime import datetime, date

from terrakit.general_utils.exceptions import TerrakitValueError

logger = logging.getLogger(__name__)


def check_collection_exists(data_collection_name: str, collections: list):
    """
    Check if the provided data_collection_name exists in the collections list.

    Parameters:
        data_collection_name (str): The name of the collection to check.
        collections (list): A list of available collections.

    Raises:
        TerrakitValueError: If the collection does not exist.
    """
    if data_collection_name not in collections:
        error_msg = f"Invalid collection '{data_collection_name}'. Please choose from one of the following collection {collections}"
        logger.error(error_msg)
        raise TerrakitValueError(error_msg)


def check_start_end_date(date_start: str, date_end: str) -> None:
    """
    Validate the start and end dates ensuring the end date is after the start date.

    Parameters:
        date_start (str): The start date in ISO format (YYYY-MM-DD).
        date_end (str): The end date in ISO format (YYYY-MM-DD).

    Raises:
        TerrakitValueError: If the date range is invalid.
    """
    check_datetime(start=True, date_str=date_start)
    check_datetime(start=False, date_str=date_end)

    start = date.fromisoformat(date_start)
    end = date.fromisoformat(date_end)
    delta = end - start
    if delta.days < 0:
        err_msg = f"Invalid date range: {date_start} to {date_end}. End date must be greater than start date."
        logger.error(err_msg)
        raise TerrakitValueError(err_msg)


def check_datetime(start: bool, date_str: str) -> None:
    """
    Validate a date string ensuring it's in ISO format and not in the future.

    Parameters:
        start (bool): True if validating the start date, False for end date.
        date_str (str): The date string to validate.

    Raises:
        TerrakitValueError: If the date format is incorrect or the date is in the future.
    """
    if start:
        start_or_end = "start"
    else:
        start_or_end = "end"
    try:
        query_date = date.fromisoformat(date_str)
    except ValueError as e:
        err_msg = f"Invalid {start_or_end} date format: {date_str}. Please use ISO format (YYYY-MM-DD)."
        logger.error(err_msg)
        raise TerrakitValueError(err_msg, e)  # type: ignore [arg-type]

    if query_date > datetime.date(datetime.now()):
        err_msg = f"Invalid {start_or_end} date: {date_str}. Date must be in the past."
        logger.error(err_msg)
        raise TerrakitValueError(
            err_msg,
        )
    if query_date < datetime.strptime("01/01/1950", "%d/%m/%Y").date():
        err_msg = (
            f"Invalid {start_or_end} date: {date_str}. Date must be after 01/01/1950."
        )
        logger.error(err_msg)
        raise TerrakitValueError(
            err_msg,
        )


def check_area_polygon(area_polygon, connector_type: str) -> None:
    """
    For connector_types that do not yet support 'area_polygon', this function provides a check to use 'bbox' instead.

    Parameters:
        area_polygon: The area polygon to check.
        connector_type (str): The type of connector.

    Raises:
        TerrakitValueError: If 'area_polygon' is provided instead of 'bbox'.
    """
    if area_polygon is not None:
        err_msg = f"Error: Issue finding data from {connector_type}. Please use 'bbox' instead of 'area_polygon'"
        logger.error(err_msg)
        raise TerrakitValueError(err_msg)


def check_bbox(bbox: list, connector_type: str) -> None:
    """
    Validate the bounding box ensuring it's a list of four floats and not a degenerate rectangle.

    Parameters:
        bbox (list): The bounding box to check.
        connector_type (str): The type of connector.

    Raises:
        TerrakitValueError: If the bounding box is invalid.
    """
    if bbox is None:
        error_msg = f"Error: Issue finding data from {connector_type}. Please specify at least one of 'bbox' and 'area_polygon'"
        logger.error(error_msg)
        raise TerrakitValueError(error_msg)
    if isinstance(bbox, list) is False:
        err_msg = f"Error: Issue finding data from {connector_type} with bbox '{bbox}'. Please specify 'bbox' as a list of floats."
        logger.error(err_msg)
        raise TerrakitValueError(err_msg)
    if len(bbox) != 4:
        err_msg = f"Error: Issue finding data from {connector_type} with bbox '{bbox}'. Please specify 'bbox' as a list of length 4."
        logger.error(err_msg)
        raise TerrakitValueError(err_msg)
    for item in bbox:
        try:
            float(item)
        except ValueError:
            err_msg = f"Error: Issue finding data from {connector_type} with bbox '{bbox}'. Please specify 'bbox' as a list of floats. The entry '{item}' is not a float."
            logger.error(err_msg)
            raise TerrakitValueError(err_msg)
    if len(set(bbox)) == 1:
        err_msg = f"Error: Issue finding data from {connector_type} with bbox '{bbox}'. Cannot determine area from 'bbox'. Please specify a valid area."
        logger.error(err_msg)
        raise TerrakitValueError(err_msg)
    west, south, east, north = bbox
    if not (-180 <= west < east <= 180 and -90 <= south < north <= 90):
        raise TerrakitValueError(
            f"Error: Issue finding data from {connector_type} with bbox '{bbox}'. Bbox is expected as 'west, south, east, north' or 'minx, miny, maxx, maxy' using EPSG: 4326 coordinate system."
        )
