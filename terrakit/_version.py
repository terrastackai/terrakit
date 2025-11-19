# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


import importlib.metadata

PACKAGE_NAME = "terrakit"
try:
    VERSION = importlib.metadata.version(PACKAGE_NAME)
except importlib.metadata.PackageNotFoundError:
    VERSION = "unknown (not installed)"
