# TerraKit Data Connectors
Data connectors are classes which enable a user to search for data and query data from a particular data source using a common set of functions.  The TerraKit Pipeline makes use of the Data Connectors, but they can also be used independently to explore and retrieve EO data.

Each data connector has the following mandatory methods:

* list_collections()
* find_data()
* get_data()


## Available data connectors and collections
The following data connectors and associated collections are available:

| Connectors        | Collections |
| ----------------- | ----------- |
| sentinelhub       | s2_l1c, dem, s1_grd, hls_l30, s2_l2a, hls_s30 |
| nasa_earthdata    | HLSL30_2.0, HLSS30_2.0  |
| sentinel_aws      | sentinel-2-l2a  |
| climate_data_store| derived-era5-single-levels-daily-statistics, projections-cordex-domains-single-levels |
| IBMResearchSTAC | 'HLSS30', 'esa-sentinel-2A-msil1c', 'HLS_S30',, 'atmospheric-weather-era5', 'deforestation-umd', 'Radar-10min', 'tasmax-rcp85-land-cpm-uk-2.2km', 'vector-osm-power', 'ukcp18-land-cpm-uk-2.2km', 'treecovermaps-eudr', 'ch4' + more|
| TheWeatherCompany | weathercompany-daily-forecast |




## Try out
Data Connectors can be used outside the TerraKit Pipeline. Here is an example using the SentinelHub data connector.

```python
from terrakit import DataConnector
dc = DataConnector(connector_type='sentinelhub')
dc.connector.list_collections()
```

To list available bands:

```python
dc.connector.list_bands()
```

Take a look at the [TerraKit: Easy geospatial data search and query](examples/terrakit_download.ipynb) notebook for more help getting started with TerraKit Data Connectors. For access information, [see below](#data-connector-access).

## Data connector access
Each data connector has a different access requirements. For example, connecting to SentinelHub and NASA EarthData, you will need to obtain credentials from each provider. Once these have been obtained, they can be added to a `.env` file at the root directory level using the following syntax:

```.env
SH_CLIENT_ID="<SentinelHub Client ID>"
SH_CLIENT_SECRET="<SentinelHub Client Secret>"
NASA_EARTH_BEARER_TOKEN="<NASA EarthData Bearer Token>"
CDSAPI_KEY="<Climate Data Store API Key>"
```

### NASA Earthdata
To access NASA Earthdata, register for an Earthdata Login profile and requests a bearer token. [https://urs.earthdata.nasa.gov/profile](https://urs.earthdata.nasa.gov/profile)

### Sentinel Hub
To access sentinel hub, register for an account and requests an OAuth client using the Sentinel Hub dashboard [https://www.planet.com](https://www.planet.com)

### Sentinel AWS
Access sentinel AWS data is open and does not require any credentials.

### Climate Data Store
Create an account at [https://cds.climate.copernicus.eu/](https://cds.climate.copernicus.eu/). Once created, find your API  key under the `Profile` section. Each dataset may also require accepting the licence agreement. If this is the case, the first time a request is made, an error will be returned with the url to visit to accept the terms.

Available collections include:
 - [ERA5 post-processed daily statistics on single levels from 1940 to present](https://cds.climate.copernicus.eu/datasets/derived-era5-single-levels-daily-statistics?tab=overview)
 - [CORDEX regional climate model data on single levels](https://cds.climate.copernicus.eu/datasets/projections-cordex-domains-single-levels?tab=overview)

### The Weather Company
To access The Weather Company, register for an account and requests an API Key [https://www.weathercompany.com/weather-data-apis/](https://www.weathercompany.com/weather-data-apis/). Once you have an API key, set the following environment variable:

```
THE_WEATHER_COMPANY_API_KEY="<The Weather Company API key>"
```

### IBM Research STAC
Access IBM Research STAC is currently restricted to IBMers and partners. If you're elegible, you need to register for an IBM AppID account and set the following environment variables:

```
APPID_ISSUER=<issuer>
APPID_USERNAME=<user-email>
APPID_PASSWORD=<user-password>
CLIENT_ID=<client-id>
CLIENT_SECRET=<client-secret>
```

Please reach out the maintainers of this repo.

IBMers don't need credentials to access the internal instance of the STAC service.


## Bounding Box Constraints

**All TerraKit data connectors adhere to standard geographic bounding box constraints:**

Bounding boxes must be specified in the format: `bbox = [West, South, East, North] = [min_lon, min_lat, max_lon, max_lat]`

The following constraints are enforced:

* **Longitude (West/East)**: `-180 <= west < east <= 180`
* **Latitude (South/North)**: `-90 <= south < north <= 90`

These constraints ensure:

- Valid geographic coordinates within Earth's coordinate system
- Proper ordering (minimum < maximum for both longitude and latitude)
- Consistency across all data connectors regardless of the underlying data source

**Example of a valid bounding box:**
```python
# Valid: London area
bbox = [-0.5, 51.3, 0.3, 51.7]  # [West, South, East, North]

# Valid: Global extent
bbox = [-180, -90, 180, 90]

# Invalid: West >= East
bbox = [0.3, 51.3, -0.5, 51.7]  # ❌ West (0.3) must be < East (-0.5)

# Invalid: Longitude out of range
bbox = [-200, 51.3, 0.3, 51.7]  # ❌ West (-200) outside valid range [-180, 180]

# Invalid: South >= North
bbox = [-0.5, 51.7, 0.3, 51.3]  # ❌ South (51.7) must be < North (51.3)
```

**Note:** For regions crossing the antimeridian (180°/-180° longitude), split the query into two separate bounding boxes or use data connector-specific handling if available.


## Climate Data Store Data Connectors

### Parallel Downloads for Multi-Year Requests

The Climate Data Store connector automatically handles large multi-year requests by splitting them into smaller chunks and downloading them in parallel. The splitting strategy differs between ERA5 and CORDEX datasets:

#### ERA5 Multi-Year Downloads

For ERA5 datasets (e.g., `derived-era5-single-levels-daily-statistics`), TerraKit automatically splits requests into **monthly chunks** to handle CDS API constraints:

1. **Why monthly chunks?** The CDS API has two key limitations:
   - Separate year/month/day parameters create a Cartesian product, causing invalid date combinations across year boundaries
   - Large requests (e.g., full year) exceed cost limits with error "Your request is too large"

2. **Parallel processing:** By default, monthly chunks are downloaded in parallel using 4 workers, significantly speeding up large data requests.

**Example:**
```python
from terrakit import DataConnector

dc = DataConnector(connector_type="climate_data_store")

# Default parallel download (4 workers)
# Request spanning 4 years will be split into 48 monthly chunks
data = dc.connector.get_data(
    data_collection_name="derived-era5-single-levels-daily-statistics",
    date_start="2020-01-01",
    date_end="2023-12-31",
    bbox=[-10, 40, 5, 50],
    bands=["2m_temperature", "total_precipitation"]
)

# Faster download with more workers (8 workers)
data = dc.connector.get_data(
    data_collection_name="derived-era5-single-levels-daily-statistics",
    date_start="2020-01-01",
    date_end="2023-12-31",
    bbox=[-10, 40, 5, 50],
    bands=["2m_temperature", "total_precipitation"],
    query_params={"max_workers": 8}
)

# Sequential download (1 worker) - useful for debugging or rate limit issues
data = dc.connector.get_data(
    data_collection_name="derived-era5-single-levels-daily-statistics",
    date_start="2020-01-01",
    date_end="2023-12-31",
    bbox=[-10, 40, 5, 50],
    bands=["2m_temperature", "total_precipitation"],
    query_params={"max_workers": 1}
)
```

#### CORDEX Multi-Year Downloads

For CORDEX datasets (e.g., `projections-cordex-domains-single-levels`), TerraKit uses a different approach based on **year blocks** defined by the CDS constraints:

1. **Fixed vs. Flexible blocks:** CORDEX data is organized into year blocks that can be either:
   - **Fixed blocks:** Specific year ranges that must be requested exactly as defined (e.g., 1950-1955, 1956-1960 as separate blocks). You cannot request partial years from a fixed block.
   - **Flexible ranges:** Continuous ranges where any subset is valid (e.g., any years between 1950-2005)

2. **Automatic block detection:** TerraKit automatically detects which year blocks are needed to cover your requested date range and downloads them in parallel.

3. **Validation:** Before downloading, TerraKit validates that your requested combination of parameters (domain, experiment, models, variables, year range) is available in the CDS dataset.

**Example:**
```python
from terrakit import DataConnector

dc = DataConnector(connector_type="climate_data_store")

# CORDEX download with automatic year block splitting
# If the request spans multiple year blocks, they will be downloaded in parallel
data = dc.connector.get_data(
    data_collection_name="projections-cordex-domains-single-levels",
    date_start="1950-01-01",
    date_end="1960-12-31",
    bbox=[5, 45, 15, 55],  # Europe region
    bands=["2m_air_temperature"],
    query_params={
        "experiment": "historical",
        "horizontal_resolution": "0_44_degree_x_0_44_degree",
        "temporal_resolution": "daily_mean",
        "gcm_model": "ichec_ec_earth",
        "rcm_model": "knmi_racmo22t",
        "ensemble_member": "r1i1p1",
        "max_workers": 4  # Parallel download of year blocks
    }
)
```

**Important notes for CORDEX:**
- For fixed blocks, you must request the entire time period. Partial year requests (e.g., requesting 3 days from a year-long block) will raise a validation error.
- The CDS API returns the entire block regardless of the date range specified, so TerraKit enforces requesting complete blocks to avoid confusion.
- TerraKit performs preflight validation to check if your parameter combination is available before attempting download.

#### Performance Tips

**For both ERA5 and CORDEX:**
- **Default (4 workers):** Good balance for most use cases
- **Higher values (8-10 workers):** Faster for multi-year requests, but may hit API rate limits
- **Lower values (1-2 workers):** Use if experiencing rate limit issues or for debugging
- **Sequential (1 worker):** Useful for debugging or when you want to see each chunk download individually

### CDS Request Validation
#### CORDEX Data Validation

For CORDEX collections, TerraKit performs **preflight validation** to ensure that the requested combination of parameters is available before attempting to download data. This validation checks the joint combination of:

- Domain (derived from bounding box)
- Experiment (e.g., 'historical', 'rcp_8_5')
- Horizontal resolution (e.g., '0_44_degree_x_0_44_degree')
- Temporal resolution (e.g., 'daily_mean', 'fixed')
- GCM model (Global Climate Model)
- RCM model (Regional Climate Model)
- Ensemble member (e.g., 'r1i1p1')
- Variable (e.g., '2m_air_temperature')
- Year range (start_year and end_year)

If an invalid combination is requested, TerraKit will raise a `TerrakitValidationError` with helpful suggestions for valid alternatives, **before** making any API calls to the Climate Data Store. This saves time and helps users discover available data combinations.

#### ERA5 Data Validation

For ERA5 collections (e.g., `derived-era5-single-levels-daily-statistics`), TerraKit performs **preflight validation** to ensure that the requested parameters are valid before attempting to download data. This validation includes:

**Temporal Validation:**
- Verifies that `date_start` and `date_end` are in the correct format (`YYYY-MM-DD`)
- Ensures `date_start` is before or equal to `date_end`
- Checks that the requested date range falls within the collection's available temporal extent (e.g., 1940-01-01 to present for ERA5)
- Raises a `TerrakitValidationError` if dates are outside the allowed range

**Spatial Validation:**
- Validates the bounding box format and coordinates
- Ensures the bounding box meets the minimum size requirement for ERA5's 0.25° grid resolution
- Automatically expands bounding boxes smaller than 0.25° × 0.25° to meet the minimum resolution, preserving the center point
- Logs a warning when automatic expansion occurs, showing the original and adjusted dimensions

**Example validation errors:**
```python
# Date outside allowed range
TerrakitValidationError: "Start date 1939-01-01 is before allowed start date 1940-01-01"

# Bounding box too small (will be auto-expanded with warning)
# Original: [10.0, 20.0, 10.1, 20.1] (0.1° × 0.1°)
# Expanded: [9.925, 19.925, 10.175, 20.175] (0.25° × 0.25°)
```

These validations occur **before** any API calls to the Climate Data Store, saving time and providing immediate feedback on parameter issues.
