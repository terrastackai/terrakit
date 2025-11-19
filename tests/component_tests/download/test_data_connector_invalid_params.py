# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


import pytest
import re

from datetime import datetime, timedelta

from terrakit import DataConnector
from terrakit.general_utils.exceptions import (
    TerrakitValidationError,
    TerrakitValueError,
)


class TestDataConnector_InvalidConnectorType:
    def test_invalid_data_source(self):
        with pytest.raises(
            TerrakitValidationError,
            match="Invalid connector type: 'invalid_data_source'",
        ):
            DataConnector(connector_type="invalid_data_source")


@pytest.mark.parametrize(
    "connector_type, collection",
    (
        ["sentinelhub", "s2_l1c"],
        ["nasa_earthdata", "HLSS30_2.0"],
        ["sentinel_aws", "sentinel-2-l2a"],
    ),
)
class TestFindData_InvalidParams:
    def test_find_data__missing_area_polygon_or_bbox(
        self,
        mock_setup_nasa,
        connector_type,
        collection,
        start_date,
    ):
        with pytest.raises(
            TerrakitValueError,
            match=f"Error: Issue finding data from {connector_type}. Please specify at least one of 'bbox' and 'area_polygon'",
        ):
            dc = DataConnector(connector_type=connector_type)
            dc.connector.find_data(collection, start_date, start_date)

    def test_find_data__missing_credentials(
        self,
        mock_setup_nasa,
        unset_evn_vars,
        connector_type,
        collection,
        start_date,
        bbox,
        reset_dot_env,
    ):
        """
        Test that find_data only runs if credentials are provided.
        """
        if connector_type == "sentinel_aws":
            pytest.skip("sentinel_aws does not require credentials")

        with pytest.raises(TerrakitValidationError, match="Error: Missing credentials"):
            dc = DataConnector(connector_type=connector_type)
            dc.connector.find_data(collection, start_date, start_date, bbox=bbox)

    def test_find_data__invalid_collection(
        self, mock_setup_nasa, connector_type, collection, start_date, bbox
    ):
        with pytest.raises(TerrakitValueError, match="Invalid collection"):
            dc = DataConnector(connector_type=connector_type)
            dc.connector.find_data(
                "collection_invalid", start_date, start_date, bbox=bbox
            )

    def test_find_data__invalid_start_date(
        self, mock_setup_nasa, connector_type, collection, end_date, bbox
    ):
        invalid_start_date = "not a date"
        with pytest.raises(
            TerrakitValueError, match=f"Invalid start date format: {invalid_start_date}"
        ):
            dc = DataConnector(connector_type=connector_type)
            dc.connector.find_data(collection, invalid_start_date, end_date, bbox=bbox)

    def test_find_data__invalid_end_date(
        self, mock_setup_nasa, connector_type, collection, start_date, bbox
    ):
        invalid_end_date = "not a date"
        with pytest.raises(
            TerrakitValueError, match=f"Invalid end date format: {invalid_end_date}"
        ):
            dc = DataConnector(connector_type=connector_type)
            dc.connector.find_data(collection, start_date, invalid_end_date, bbox=bbox)

    def test_find_data__end_date_before_start(
        self, mock_setup_nasa, connector_type, collection, start_date, end_date, bbox
    ):
        with pytest.raises(
            TerrakitValueError,
            match=f"Invalid date range: {end_date} to {start_date}. End date must be greater than start date.",
        ):
            dc = DataConnector(connector_type=connector_type)
            dc.connector.find_data(
                collection, date_start=end_date, date_end=start_date, bbox=bbox
            )

    def test_find_data__future_start_date(
        self, mock_setup_nasa, connector_type, collection, end_date, bbox
    ):
        start_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        with pytest.raises(TerrakitValueError, match="Date must be in the past."):
            dc = DataConnector(connector_type=connector_type)
            dc.connector.find_data(
                collection, date_start=start_date, date_end=end_date, bbox=bbox
            )

    def test_find_data__future_end_date(
        self, mock_setup_nasa, connector_type, collection, start_date, bbox
    ):
        end_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        with pytest.raises(TerrakitValueError, match="Date must be in the past."):
            dc = DataConnector(connector_type=connector_type)
            dc.connector.find_data(
                collection, date_start=start_date, date_end=end_date, bbox=bbox
            )

    def test_find_data__invalid_bbox(
        self, mock_setup_nasa, connector_type, collection, start_date, end_date
    ):
        invalid_bbox = "not a bbox"
        with pytest.raises(
            TerrakitValueError,
            match=f"Error: Issue finding data from {connector_type} with bbox '{invalid_bbox}'. Please specify 'bbox' as a list of floats.",
        ):
            dc = DataConnector(connector_type=connector_type)
            dc.connector.find_data(collection, start_date, end_date, bbox=invalid_bbox)

        invalid_bbox = [1, 2, 3]
        with pytest.raises(
            TerrakitValueError,
            match=re.escape(
                f"Error: Issue finding data from {connector_type} with bbox '{invalid_bbox}'. Please specify 'bbox' as a list of length 4."
            ),
        ):
            dc = DataConnector(connector_type=connector_type)
            dc.connector.find_data(collection, start_date, end_date, bbox=invalid_bbox)

        invalid_bbox = ["test", 0, 0, 0]
        with pytest.raises(
            TerrakitValueError,
            match=re.escape(
                f"Error: Issue finding data from {connector_type} with bbox '{invalid_bbox}'. Please specify 'bbox' as a list of floats. The entry 'test' is not a float."
            ),
        ):
            dc = DataConnector(connector_type=connector_type)
            dc.connector.find_data(collection, start_date, end_date, bbox=invalid_bbox)

        invalid_bbox = [["test", 0, 0, 0]]
        with pytest.raises(
            TerrakitValueError,
            match=f"Error: Issue finding data from {connector_type} with bbox",
        ):
            dc = DataConnector(connector_type=connector_type)
            dc.connector.find_data(collection, start_date, end_date, bbox=invalid_bbox)

    def test_find_data__invalid_bbox_out_of_bounds(
        self, mock_setup_nasa, connector_type, collection, start_date, end_date
    ):
        invalid_bbox = [0, 0, 0, 0]
        with pytest.raises(
            TerrakitValueError,
            match=re.escape(
                f"Error: Issue finding data from {connector_type} with bbox '{invalid_bbox}'. Cannot determine area from 'bbox'. Please specify a valid area."
            ),
        ):
            dc = DataConnector(connector_type=connector_type)
            dc.connector.find_data(collection, start_date, end_date, bbox=invalid_bbox)

    @pytest.mark.skip("WiP - Test plan")
    def test_find_data__invalid_maxcc(
        self, mock_setup_nasa, connector_type, collection, start_date, end_date, bbox
    ):
        invalid_maxcc = 101
        with pytest.raises(
            TerrakitValidationError,
            match="Invalid max cloud cover: maxcc={invalid_maxcc}",
        ):
            dc = DataConnector(connector_type=connector_type)
            dc.connector.find_data(
                collection,
                start_date,
                end_date,
                bbox=bbox,
                maxcc=invalid_maxcc,
            )

    @pytest.mark.skip("WiP - Test plan")
    def test_find_data__maxcc_no_results_found(
        self, mock_setup_nasa, connector_type, collection, start_date, end_date, bbox
    ):
        maxcc_no_results_found = 1  # Validate expected result when no items are found
        with pytest.raises(
            TerrakitValueError, match="Invalid bounding box: {invalid_bbox}"
        ):
            dc = DataConnector(connector_type=connector_type)
            dc.connector.find_data(
                collection,
                start_date,
                end_date,
                bbox=bbox,
                maxcc=maxcc_no_results_found,
            )


@pytest.mark.parametrize(
    "connector_type, collection",
    (
        ["sentinelhub", "s2_l1c"],
        ["nasa_earthdata", "HLSS30_2.0"],
        ["sentinel_aws", "sentinel-2-l2a"],
    ),
)
class TestDownloadDataInvalidInput:
    def test_get_data__missing_area_polygon_or_bbox(
        self,
        mock_setup_nasa,
        connector_type,
        collection,
        start_date,
    ):
        with pytest.raises(
            TerrakitValueError,
            match=f"Error: Issue finding data from {connector_type}. Please specify at least one of 'bbox' and 'area_polygon'",
        ):
            dc = DataConnector(connector_type=connector_type)
            dc.connector.get_data(collection, start_date, start_date)

    def test_get_data__missing_credentials(
        self,
        mock_setup_nasa,
        unset_evn_vars,
        connector_type,
        collection,
        start_date,
        bbox,
        reset_dot_env,
    ):
        """
        Test that get_data only runs if credentials are provided.
        """
        # sentinel_aws does not require credentials
        if connector_type == "sentinel_aws":
            pytest.skip("sentinel_aws does not require credentials")
        with pytest.raises(TerrakitValidationError, match="Error: Missing credentials"):
            dc = DataConnector(connector_type=connector_type)
            dc.connector.get_data(collection, start_date, start_date, bbox)

    @pytest.mark.skip("WiP - Test plan")
    def test_get_data_with_invalid_credentials(
        self,
        mock_setup_nasa,
        caplog,
        invalid_evn_vars,
        connector_type,
        collection,
        start_date,
        reset_dot_env,
    ):
        """
        Test that get_data gives a clear error message when credentials are invalid
        """
        # sentinel_aws does not require credentials
        if connector_type == "sentinel_aws":
            pytest.skip("sentinel_aws does not require credentials")
        with pytest.raises(TerrakitValidationError, match="Error: Invalid credentials"):
            dc = DataConnector(connector_type=connector_type)
            dc.connector.get_data(
                data_collection_name=collection,
                date_start=start_date,
                date_end=start_date,
            )

    def test_get_data__invalid_collection(
        self, mock_setup_nasa, connector_type, collection, start_date, bbox
    ):
        with pytest.raises(TerrakitValueError, match="Invalid collection"):
            dc = DataConnector(connector_type=connector_type)
            dc.connector.get_data(
                "collection_invalid", start_date, start_date, bbox=bbox
            )

    def test_get_data__invalid_start_date(
        self, mock_setup_nasa, connector_type, collection, end_date, bbox
    ):
        invalid_start_date = "not a date"
        with pytest.raises(
            TerrakitValueError, match=f"Invalid start date format: {invalid_start_date}"
        ):
            dc = DataConnector(connector_type=connector_type)
            dc.connector.get_data(collection, invalid_start_date, end_date, bbox=bbox)

    def test_get_data__invalid_end_date(
        self, mock_setup_nasa, connector_type, collection, start_date, bbox
    ):
        invalid_end_date = "not a date"
        with pytest.raises(
            TerrakitValueError, match=f"Invalid end date format: {invalid_end_date}"
        ):
            dc = DataConnector(connector_type=connector_type)
            dc.connector.get_data(collection, start_date, invalid_end_date, bbox=bbox)

    def test_get_data__end_date_before_start(
        self, mock_setup_nasa, connector_type, collection, start_date, end_date, bbox
    ):
        with pytest.raises(
            TerrakitValueError,
            match=f"Invalid date range: {end_date} to {start_date}. End date must be greater than start date.",
        ):
            dc = DataConnector(connector_type=connector_type)
            dc.connector.get_data(
                collection, date_start=end_date, date_end=start_date, bbox=bbox
            )

    def test_get_data__future_start_date(
        self, mock_setup_nasa, connector_type, collection, end_date, bbox
    ):
        start_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        with pytest.raises(TerrakitValueError, match="Date must be in the past."):
            dc = DataConnector(connector_type=connector_type)
            dc.connector.get_data(
                collection, date_start=start_date, date_end=end_date, bbox=bbox
            )

    def test_get_data__future_end_date(
        self, mock_setup_nasa, connector_type, collection, start_date, bbox
    ):
        end_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        with pytest.raises(TerrakitValueError, match="Date must be in the past."):
            dc = DataConnector(connector_type=connector_type)
            dc.connector.get_data(
                collection, date_start=start_date, date_end=end_date, bbox=bbox
            )

    def test_get_data__invalid_bbox(
        self, mock_setup_nasa, connector_type, collection, start_date, end_date
    ):
        invalid_bbox = "not a bbox"
        with pytest.raises(
            TerrakitValueError,
            match=f"Error: Issue finding data from {connector_type} with bbox '{invalid_bbox}'. Please specify 'bbox' as a list of floats.",
        ):
            dc = DataConnector(connector_type=connector_type)
            dc.connector.get_data(collection, start_date, end_date, bbox=invalid_bbox)

        invalid_bbox = [1, 2, 3]
        with pytest.raises(
            TerrakitValueError,
            match=re.escape(
                f"Error: Issue finding data from {connector_type} with bbox '{invalid_bbox}'. Please specify 'bbox' as a list of length 4."
            ),
        ):
            dc = DataConnector(connector_type=connector_type)
            dc.connector.get_data(collection, start_date, end_date, bbox=invalid_bbox)

        invalid_bbox = ["test", 0, 0, 0]
        with pytest.raises(
            TerrakitValueError,
            match=re.escape(
                f"Error: Issue finding data from {connector_type} with bbox '{invalid_bbox}'. Please specify 'bbox' as a list of floats. The entry 'test' is not a float."
            ),
        ):
            dc = DataConnector(connector_type=connector_type)
            dc.connector.get_data(collection, start_date, end_date, bbox=invalid_bbox)

        invalid_bbox = [["test", 0, 0, 0]]
        with pytest.raises(
            TerrakitValueError,
            match=f"Error: Issue finding data from {connector_type} with bbox",
        ):
            dc = DataConnector(connector_type=connector_type)
            dc.connector.get_data(collection, start_date, end_date, bbox=invalid_bbox)

    def test_get_data__invalid_bbox_out_of_bounds(
        self, mock_setup_nasa, connector_type, collection, start_date, end_date
    ):
        invalid_bbox = [0, 0, 0, 0]
        with pytest.raises(
            TerrakitValueError,
            match=re.escape(
                f"Error: Issue finding data from {connector_type} with bbox '{invalid_bbox}'. Cannot determine area from 'bbox'. Please specify a valid area."
            ),
        ):
            dc = DataConnector(connector_type=connector_type)
            dc.connector.get_data(collection, start_date, end_date, bbox=invalid_bbox)

    @pytest.mark.skip("WiP - Test plan")
    def test_get_data__invalid_maxcc(
        self, mock_setup_nasa, connector_type, collection, start_date, end_date, bbox
    ):
        invalid_maxcc = 101
        with pytest.raises(
            TerrakitValidationError,
            match="Invalid max cloud cover: maxcc={invalid_maxcc}",
        ):
            dc = DataConnector(connector_type=connector_type)
            dc.connector.get_data(
                collection,
                start_date,
                end_date,
                bbox=bbox,
                maxcc=invalid_maxcc,
            )

    @pytest.mark.skip("WiP - Test plan")
    def test_get_data__maxcc_no_results_found(
        self, mock_setup_nasa, connector_type, collection, start_date, end_date, bbox
    ):
        maxcc_no_results_found = 1  # Validate expected result when no items are found
        with pytest.raises(
            TerrakitValueError,
            match="No items found",
        ):
            dc = DataConnector(connector_type=connector_type)
            dc.connector.get_data(
                collection,
                start_date,
                end_date,
                bbox=bbox,
                maxcc=maxcc_no_results_found,
            )
