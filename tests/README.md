# TerraKit Tests

## Unit tests
The TerraKit unit tests are run with each commit using the `pre-commit` library. At the time of writing, the tests mock out a number of remote function calls, ensuring they are quick to run. The tests are designed to provide assurance that the TerraKit APIs and functionality is as expected but more work is needed to improve code coverage by only mocking out the specific remote calls. These mocked functions should then be tested using the [Integration Tests](#running-integration-tests) run intermittently by each developer.

### Setup
Before running the unit tests, ensure that a `.env` file exists and has at least empty values for each of the required keys. Run the following to create `.env` file that can be used for testing.

```bash
cp .env .env-stash
echo "NASA_EARTH_BEARER_TOKEN='test'\nSH_CLIENT_ID='test'\nSH_CLIENT_SECRET='test'\nTHE_WEATHER_COMPANY_API_KEY='test'" > .env
```

Please also download the necessary label files by running the following code snippet from the project root directory:

```bash
python -c 'from terrakit.general_utils.labels_downloader import rapid_mapping_geojson_downloader;\
rapid_mapping_geojson_downloader(
    event_id="748",
    aoi="01",
    monitoring_number="05",
    version="v1",
    dest="docs/examples/test_wildfire_vector",
);\
rapid_mapping_geojson_downloader(
    event_id="801",
    aoi="01",
    monitoring_number="02",
    version="v1",
    dest=LABELS_FOLDER,
);'

```
This will use the `rapid_mapping_geojson_downloader` function to download two label files from the Copernicus Emergency Management Service. Two label files should now be found in `docs/examples/test_wildfire_vector`:

```bash
> ls -l docs/examples/test_wildfire_vector
-rw-r--r--@ 1 user  staff  896598 Sep 29 11:14 EMSR748_AOI01_DEL_MONIT05_observedEventA_v1_2024-08-26.json
-rw-r--r--@ 1 user  staff   57590 Sep 28 23:00 EMSR801_AOI01_DEL_MONIT02_observedEventA_v1_2025-04-23.json
```

### Running unit tests

To run all unit tests:
```bash
uv run pytest --slow
```

To exclude those tests marked as slow, use:
```bash
uv run pytest
```

To see which tests are running the slowest, use:
```bash
./.venv/bin/pytest --durations=0
```

To mark a test as running slowly, add the `@pytest.mark.slow` decorator to the test function.

To complete a pytest coverage report:
```bash
uv run pytest --cov=src/terrakit tests/
```

To run tests from the pre-commit pipeline:
```bash
pre-commit pytest
```

### Running integration tests
Alongside unit tests, integration test are used to validate TerraKit works with external components such as the data_connector APIs. At the time of writting, there is one integration test [tests/integration_tests/dev.py](../tests/integration_tests/dev.py). This python script runs `get_data` for each data connector. To run these tests, first populate the `.env` file at the root directory as follows:

```.env
SH_CLIENT_ID="<SentinelHub Client ID>"
SH_CLIENT_SECRET="<SentinelHub Client Secret>"
NASA_EARTH_BEARER_TOKEN="<NASA EarthData Bearer Token>"
```
To run the integration test:
```bash
uv run python tests/integration_tests/dev.py
```
The test runs four examples and should output the following `.tif` files to the root directory:

```bash
# ls -lht *.tif
-rw-r--r--   1 user  staff    37M Aug  5 14:07 sentinelhub_s2_l2a_2024-01-01.tif
-rw-r--r--   1 user  staff    37M Aug  5 14:07 sentinelhub_s2_l2a_2024-01-01_imputed.tif
-rw-r--r--   1 user  staff    24M Aug  5 14:07 s1_grd_2024-01-07.tif
-rw-r--r--   1 user  staff    24M Aug  5 14:07 s1_grd_2024-01-07_imputed.tif
-rw-r--r--   1 user  staff    73M Aug  5 14:07 sentinel_aws_sentinel-2-l2a_2024-01-01.tif
-rw-r--r--   1 user  staff    73M Aug  5 14:07 sentinel_aws_sentinel-2-l2a_2024-01-01_imputed.tif
-rw-r--r--   1 user  staff   2.1M Aug  5 14:08 nasa_earthdata_HLSL30_2.0_2024-01-04.tif
```

### Pytest coverage
To generate a test coverage report run:

```bash
uv run pytest --cov=src/terrakit tests
```