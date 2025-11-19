# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


import requests  # type: ignore[import-untyped]
import logging

from typing import Any, Dict, Optional

from terrakit.general_utils.exceptions import TerrakitValueError

logger = logging.getLogger(__name__)


################################################################################################
def get(
    url: str, headers: Optional[Dict] = None, params: Optional[Dict[str, Any]] = None
) -> requests.Response:
    """Method to make a GET request.

    Args:
        url (str): The URL to make the GET request to.
        headers (Dict): The headers to include in the GET request.
        params (Dict, optional): The parameters to include in the GET request. Defaults to None.

    Returns:
        requests.Response: The response from the GET request.
    """
    adapter = requests.adapters.HTTPAdapter(max_retries=3)
    with requests.Session() as session:
        session.mount("https://", adapter)
        try:
            resp = session.get(url, headers=headers, params=params)
            if resp.status_code != 200:
                err_msg = f"GET request failed with status code {resp.status_code}\ntext={resp.text}\nurl={resp.url}\nheaders={headers}"
                logger.error(err_msg)
                raise TerrakitValueError(err_msg)
            return resp
        except requests.exceptions.RetryError as e:
            logger.error(e)
            raise (e)


def post(
    url: str, headers: Optional[Dict] = None, payload: Optional[Dict[str, Any]] = None
) -> requests.Response:
    """Method to make a POST request.

    Args:
        url (str): The URL to make the POST request to.
        headers (Dict): The headers to include in the POST request.
        payload (Dict, optional): The payload to include in the POST request. Defaults to None.

    Returns:
        requests.Response: The response from the POST request.
    """
    adapter = requests.adapters.HTTPAdapter(max_retries=3)
    with requests.Session() as session:
        session.mount("https://", adapter)
        try:
            resp = session.post(url, headers=headers, json=payload)
            if resp.status_code != 200:
                err_msg = f"POST request failed with status code {resp.status_code}\ntext={resp.text}\nurl={resp.url}\nheaders={headers}"
                logger.error(err_msg)
                raise TerrakitValueError(err_msg)
            return resp
        except requests.exceptions.RetryError as e:
            logger.error(e)
            raise (e)
