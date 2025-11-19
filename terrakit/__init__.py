# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


# Copyright 2018-2025 IBM

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

# http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import logging
import os

from .download.data_connectors.sentinelhub import SentinelHub  # noqa
from .download.data_connectors.nasa_earthdata import NASA_EarthData  # noqa
from .download.data_connectors.sentinel_aws import Sentinel_AWS  # noqa
from .terrakit import DataConnector  # noqa
from .download.download_data import download_data  # noqa
from .chip import tiling  # noqa
from .chip.tiling import chip_and_label_data  # noqa
from .transform import labels  # noqa
from .transform.labels import process_labels  # noqa
from .store.taco import taco_store_data, load_tortilla  # noqa
from .download.geodata_utils import *  # noqa

# Set up logging
LOGLEVEL = os.environ.get("LOGLEVEL", "INFO").upper()
logging.basicConfig(
    format="%(asctime)s [%(levelname)-8s] %(message)s (%(filename)s:%(lineno)s)",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=LOGLEVEL,
)
logger = logging.getLogger(__name__)
