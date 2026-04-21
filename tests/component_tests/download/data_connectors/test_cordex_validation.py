# © Copyright IBM Corporation 2026
# SPDX-License-Identifier: Apache-2.0


import pytest

from terrakit import DataConnector
from terrakit.general_utils.exceptions import TerrakitValidationError


class TestCordexValidation:
    """Test CORDEX preflight validation using constraints_variables file."""

    connector_type = "climate_data_store"
    collection = "projections-cordex-domains-single-levels"

    @pytest.fixture
    def dc(self):
        """Create a DataConnector instance."""
        return DataConnector(connector_type=self.connector_type)

    @pytest.fixture
    def valid_cordex_params(self):
        """Valid CORDEX parameters that should pass validation."""
        return {
            "domain": "africa",
            "experiment": "historical",
            "horizontal_resolution": "0_44_degree_x_0_44_degree",
            "temporal_resolution": "daily_mean",
            "gcm_model": "ichec_ec_earth",
            "rcm_model": "knmi_racmo22t",
            "ensemble_member": "r1i1p1",
            "variable": "2m_air_temperature",
            "start_year": 1950,
            "end_year": 1950,
        }

    def test_valid_cordex_combination(self, dc, valid_cordex_params):
        """Test that a valid CORDEX combination passes validation."""
        # This should not raise an exception
        dc.connector._validate_cordex_constraints(
            collection_name=self.collection,
            domain=valid_cordex_params["domain"],
            experiment=valid_cordex_params["experiment"],
            horizontal_resolution=valid_cordex_params["horizontal_resolution"],
            temporal_resolution=valid_cordex_params["temporal_resolution"],
            gcm_model=valid_cordex_params["gcm_model"],
            rcm_model=valid_cordex_params["rcm_model"],
            ensemble_member=valid_cordex_params["ensemble_member"],
            variable=valid_cordex_params["variable"],
            start_year=valid_cordex_params["start_year"],
            end_year=valid_cordex_params["end_year"],
        )

    def test_invalid_gcm_rcm_combination(self, dc, valid_cordex_params):
        """Test that an invalid GCM-RCM combination raises validation error."""
        # ichec_ec_earth + mpi_csc_remo2009 is not a valid combination for africa
        with pytest.raises(TerrakitValidationError) as exc_info:
            dc.connector._validate_cordex_constraints(
                collection_name=self.collection,
                domain=valid_cordex_params["domain"],
                experiment=valid_cordex_params["experiment"],
                horizontal_resolution=valid_cordex_params["horizontal_resolution"],
                temporal_resolution=valid_cordex_params["temporal_resolution"],
                gcm_model="ichec_ec_earth",
                rcm_model="mpi_csc_remo2009",  # Invalid combination
                ensemble_member=valid_cordex_params["ensemble_member"],
                variable=valid_cordex_params["variable"],
                start_year=valid_cordex_params["start_year"],
                end_year=valid_cordex_params["end_year"],
            )

        error_msg = str(exc_info.value)
        assert "not available" in error_msg.lower()
        assert "valid alternatives" in error_msg.lower()

    def test_invalid_variable_for_combination(self, dc, valid_cordex_params):
        """Test that an invalid variable for a specific combination raises error."""
        # 2m_relative_humidity is not available for all combinations
        with pytest.raises(TerrakitValidationError) as exc_info:
            dc.connector._validate_cordex_constraints(
                collection_name=self.collection,
                domain=valid_cordex_params["domain"],
                experiment=valid_cordex_params["experiment"],
                horizontal_resolution=valid_cordex_params["horizontal_resolution"],
                temporal_resolution=valid_cordex_params["temporal_resolution"],
                gcm_model="cnrm_cerfacs_cm5",
                rcm_model="clmcom_clm_cclm4_8_17",
                ensemble_member="r1i1p1",
                variable="2m_relative_humidity",  # Not available for this combo
                start_year=1950,
                end_year=1950,
            )

        error_msg = str(exc_info.value)
        assert "not available" in error_msg.lower()
        assert "valid" in error_msg.lower()

    def test_invalid_year_range(self, dc, valid_cordex_params):
        """Test that an invalid year range raises validation error."""
        with pytest.raises(TerrakitValidationError) as exc_info:
            dc.connector._validate_cordex_constraints(
                collection_name=self.collection,
                domain=valid_cordex_params["domain"],
                experiment=valid_cordex_params["experiment"],
                horizontal_resolution=valid_cordex_params["horizontal_resolution"],
                temporal_resolution=valid_cordex_params["temporal_resolution"],
                gcm_model=valid_cordex_params["gcm_model"],
                rcm_model=valid_cordex_params["rcm_model"],
                ensemble_member=valid_cordex_params["ensemble_member"],
                variable=valid_cordex_params["variable"],
                start_year=1940,  # Too early
                end_year=1950,
            )

        error_msg = str(exc_info.value)
        assert "year range" in error_msg.lower()
        assert "not available" in error_msg.lower()

    def test_invalid_experiment_for_domain(self, dc, valid_cordex_params):
        """Test that an invalid experiment for a domain raises error."""
        with pytest.raises(TerrakitValidationError) as exc_info:
            dc.connector._validate_cordex_constraints(
                collection_name=self.collection,
                domain=valid_cordex_params["domain"],
                experiment="rcp_2_6",  # May not be available for all combinations
                horizontal_resolution=valid_cordex_params["horizontal_resolution"],
                temporal_resolution=valid_cordex_params["temporal_resolution"],
                gcm_model="cnrm_cerfacs_cm5",
                rcm_model="clmcom_clm_cclm4_8_17",
                ensemble_member="r1i1p1",
                variable=valid_cordex_params["variable"],
                start_year=2006,
                end_year=2010,
            )

        error_msg = str(exc_info.value)
        assert "not available" in error_msg.lower()

    def test_invalid_ensemble_member(self, dc, valid_cordex_params):
        """Test that an invalid ensemble member raises validation error."""
        with pytest.raises(TerrakitValidationError) as exc_info:
            dc.connector._validate_cordex_constraints(
                collection_name=self.collection,
                domain=valid_cordex_params["domain"],
                experiment=valid_cordex_params["experiment"],
                horizontal_resolution=valid_cordex_params["horizontal_resolution"],
                temporal_resolution=valid_cordex_params["temporal_resolution"],
                gcm_model=valid_cordex_params["gcm_model"],
                rcm_model=valid_cordex_params["rcm_model"],
                ensemble_member="r99i99p99",  # Invalid
                variable=valid_cordex_params["variable"],
                start_year=valid_cordex_params["start_year"],
                end_year=valid_cordex_params["end_year"],
            )

        error_msg = str(exc_info.value)
        assert "not available" in error_msg.lower()

    def test_error_message_includes_valid_alternatives(self, dc, valid_cordex_params):
        """Test that error messages include valid alternatives."""
        with pytest.raises(TerrakitValidationError) as exc_info:
            dc.connector._validate_cordex_constraints(
                collection_name=self.collection,
                domain=valid_cordex_params["domain"],
                experiment=valid_cordex_params["experiment"],
                horizontal_resolution=valid_cordex_params["horizontal_resolution"],
                temporal_resolution=valid_cordex_params["temporal_resolution"],
                gcm_model="invalid_gcm",
                rcm_model=valid_cordex_params["rcm_model"],
                ensemble_member=valid_cordex_params["ensemble_member"],
                variable=valid_cordex_params["variable"],
                start_year=valid_cordex_params["start_year"],
                end_year=valid_cordex_params["end_year"],
            )

        error_msg = str(exc_info.value)
        # Should suggest valid GCM models
        assert "valid" in error_msg.lower()
        assert "gcm" in error_msg.lower() or "model" in error_msg.lower()

    def test_validation_with_multiple_variables(self, dc, valid_cordex_params):
        """Test validation with multiple variables."""
        # Test with a list of variables
        variables = ["2m_air_temperature", "mean_precipitation_flux"]

        # Should validate each variable
        for var in variables:
            dc.connector._validate_cordex_constraints(
                collection_name=self.collection,
                domain=valid_cordex_params["domain"],
                experiment=valid_cordex_params["experiment"],
                horizontal_resolution=valid_cordex_params["horizontal_resolution"],
                temporal_resolution=valid_cordex_params["temporal_resolution"],
                gcm_model=valid_cordex_params["gcm_model"],
                rcm_model=valid_cordex_params["rcm_model"],
                ensemble_member=valid_cordex_params["ensemble_member"],
                variable=var,
                start_year=valid_cordex_params["start_year"],
                end_year=valid_cordex_params["end_year"],
            )

    def test_validation_called_before_download(self, dc, valid_cordex_params, bbox):
        """Test that validation is called before attempting download."""
        # This test verifies integration - validation should happen in get_data
        # before calling client.retrieve

        with pytest.raises(TerrakitValidationError):
            dc.connector.get_data(
                data_collection_name=self.collection,
                date_start="1950-01-01",
                date_end="1950-01-31",
                bbox=bbox,
                bands=["2m_air_temperature"],
                query_params={
                    "gcm_model": "invalid_model",  # This should fail validation
                    "rcm_model": "knmi_racmo22t",
                    "experiment": "historical",
                },
            )

    def test_fixed_temporal_resolution_no_year_validation(self, dc):
        """Test that fixed temporal resolution doesn't validate year ranges."""
        # For temporal_resolution='fixed', start_year and end_year are not in constraints
        dc.connector._validate_cordex_constraints(
            collection_name=self.collection,
            domain="africa",
            experiment="evaluation",
            horizontal_resolution="0_44_degree_x_0_44_degree",
            temporal_resolution="fixed",
            gcm_model="era_interim",
            rcm_model="clmcom_clm_cclm4_8_17",
            ensemble_member="r0i0p0",
            variable="land_area_fraction",
            start_year=None,  # Not applicable for fixed
            end_year=None,
        )


# Made with Bob
