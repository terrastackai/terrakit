# © Copyright IBM Corporation 2026
# SPDX-License-Identifier: Apache-2.0


import pytest
from terrakit import DataConnector
from terrakit.general_utils.exceptions import (
    TerrakitValidationError,
)


class TestClimateDataStore:
    connector_type = "climate_data_store"

    def test_valid_data_connector(self):
        dc = DataConnector(connector_type=self.connector_type)
        assert dc.connector is not None

    def test_list_collections_climate_data_store(
        self,
        **kwargs,
    ):
        expected_collections = [
            "projections-cordex-domains-single-levels",
            "derived-era5-single-levels-daily-statistics",
        ]
        dc = DataConnector(connector_type=self.connector_type)
        collections = dc.connector.list_collections()
        assert collections == expected_collections

    def test__missing_credentials(
        self,
        unset_evn_vars,
        start_date,
        bbox,
        reset_dot_env,
    ):
        """
        Test that find_data only runs if credentials are provided.
        """
        collection = "derived-era5-single-levels-daily-statistics"
        with pytest.raises(TerrakitValidationError, match="Error: Missing credentials"):
            dc = DataConnector(connector_type=self.connector_type)
            dc.connector.find_data(collection, start_date, start_date, bbox=bbox)
