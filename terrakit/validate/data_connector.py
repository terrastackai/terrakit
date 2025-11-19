# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


from pydantic import BaseModel
from typing import Literal


class ConnectorType(BaseModel):
    """
    Attributes:
        connector_type (Literal): The type of connector to be use to download data.

    Example:
        ```
        terrakit.DataConnector(connector_type="nasa_earthdata")
        ```
        or
        ```
        terrakit.DataConnector({"connector_type": "nasa_earthdata"})
        ```
    """

    connector_type: Literal[
        "nasa_earthdata",
        "sentinelhub",
        "sentinel_aws",
        "IBMResearchSTAC",
        "TheWeatherCompany",
    ]
    """The type of connector to be use to download data. nasa_earthdata, sentinelhub, sentinel_aws, IBMResearchSTAC or TheWeatherCompany"""
