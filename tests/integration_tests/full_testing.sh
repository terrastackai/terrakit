#!/bin/bash

# Run pytests
pytest

# Run end to end tests for download
python tests/integration_tests/dev.py

# Run end to end tests for pipelines
python tests/integration_tests/labels_to_data.py
python tests/integration_tests/test_ibmresearchstac.py

# Test CLI
terrakit --config docs/examples/config.yaml labels
terrakit --config docs/examples/config.yaml download
terrakit --config docs/examples/config.yaml chip
terrakit --config docs/examples/config.yaml store

rm -r ./tmp