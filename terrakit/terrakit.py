# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


# Assisted by watsonx Code Assistant
import logging

from pydantic import ValidationError

from terrakit.download.data_connectors.theweathercompany import TheWeatherCompany

from .download.connector import Connector
from .download.data_connectors.nasa_earthdata import NASA_EarthData
from .download.data_connectors.sentinel_aws import Sentinel_AWS
from .download.data_connectors.ibmresearch_stac import IBMResearchSTAC
from .download.data_connectors.sentinelhub import SentinelHub
from .general_utils.exceptions import TerrakitValidationError
from .validate.data_connector import ConnectorType

# Set up logging
logger = logging.getLogger(__name__)


# Define the factory class
class DataConnectorFactory:
    """
    A factory class for creating data connector objects.
    """

    @staticmethod
    def get_connector(connector_type: ConnectorType) -> Connector:
        """
        Create and return a data connector object based on the specified connector type.

        Parameters:
            connector_type (str): The type of data connector to create. Supported types are:
                - "sentinelhub"
                - "nasa_earthdata"
                - "sentinel_aws"

        Returns:
            object: An instance of the specified data connector class.

        Raises:
            ValueError: If an invalid connector type is provided.
        """
        if connector_type.connector_type == "sentinelhub":
            return SentinelHub()
        elif connector_type.connector_type == "nasa_earthdata":
            return NASA_EarthData()
        elif connector_type.connector_type == "sentinel_aws":
            return Sentinel_AWS()
        elif connector_type.connector_type == "IBMResearchSTAC":
            return IBMResearchSTAC()
        elif connector_type.connector_type == "TheWeatherCompany":
            return TheWeatherCompany()
        # -----> Include new connectors here < ------
        # elif connector_type == "<new_connector>"
        #   return NewConnectorClass()
        else:
            raise TerrakitValidationError(
                f"Invalid connector type: '{connector_type.connector_type}'"
            )


# Define the main class using the factory pattern
class DataConnector:
    """
    A class to manage data connectors.

    Attributes:
        connector (DataConnector): An instance of the connector class specified during initialization.
        connector_type (str): The type of data connector.

    Example:
        ```python
        # Example usage:
        from terrakit import DataConnector

        dc = DataConnector(connector_type="sentinel_aws")
        dc.connector.list_collections()
        ```

        or

        ```python
        dc = DataConnector("sentinel_aws")
        dc.connector.list_collections()
        ```
    """

    def __init__(self, connector_type: str):
        """
        Initialize DataConnector with the specified connector type.

        Parameters:
            connector_type (str): The type of data connector to initialize.

        """
        logger.info(f"Initializing DataConnector with connector type: {connector_type}")
        try:
            connector_type = ConnectorType(connector_type=connector_type)
            self.connector = DataConnectorFactory.get_connector(
                connector_type=connector_type
            )
        except ValidationError as e:
            raise TerrakitValidationError(
                message=f"Invalid connector type: '{connector_type}'"
            ) from e
        self.connector_type: str = connector_type
