# Download data
Once labels have been processed, next up in the TerraKit pipeline is downloading the data.

Use the `download_data` function (or the `download` CLI subcommand) to download data from a set of data connectors for a time and location specified by the shapefiles output from the `process_labels` pipeline step.  

Here's an example of how to use the `download_data` step in the TerraKit pipeline:

```python
config = {
    "download": {
        "data_sources": [
            {
                "data_connector": "sentinel_aws",
                "collection_name": "sentinel-2-l2a",
                "bands": ["blue", "green", "red"],
                "save_file": "",
            },
        ],
        "date_allowance": {"pre_days": 0, "post_days": 21},
        "transform": {
            "scale_data_xarray": True,
            "impute_nans": True,
            "reproject": True,
        },
        "max_cloud_cover": 80,
    },
}

queried_data = download_data(
    data_sources=config["download"]["data_sources"],
    date_allowance=config["download"]["date_allowance"],
    transform=config["download"]["transform"],
    max_cloud_cover=config["download"]["max_cloud_cover"],
    dataset_name=DATASET_NAME,
    working_dir=WORKING_DIR,
    keep_files=False,
)
```

Write the same arguments in a config file that the TerraKit CLI can use:

```yaml
# ./docs/examples/config.yaml
download:
  data_sources:
  - data_connector: "sentinel_aws"
    collection_name: "sentinel-2-l2a"
    bands: ["blue", "green", "red"]
  date_allowance: 
    pre_days: 0
    post_days: 21
  transform:
    scale_data_xarray: True
    impute_nans: true
    reproject: True
```
```bash
#!/bin/bash
terrakit --config ./docs/examples/config.yaml download
```

Alternatively, use the TerraKit data_connectors directly by specify the collection, bbox, date and bands of interest.

```python
from terrakit import DataConnector

dc = DataConnector(connector_type="sentinel_aws")
dc.connector.list_collections()
```

## Configure the Download pipeline
Use the following parameters to configure the TerraKit Download pipeline.

### Active
`active`: Enables the labels pipeline to run. Set to `False` to skip the step. Default: `True`

### Data Allowance: `data_allowance`
Date range allowance for data query.

### Transform: `transform`
Transformation parameters for data.

### Data Sources: `data_sources`
List of data sources to query. The list should contain a valid `DataSource` object which specifies the `data_connector`, `collection_name` and `bands` to download. Optionally specify a unique filename to used for the save the downloaded files as using `save_file`. If not specified, the data will be downloaded as saved as `{working_dir}/{data_connector}_{collection_name}.tif`.

```python
# Example of a valid DataSource dictionary.
download_data(
    data_sources = [{
        "data_connector": "sentinel_aws",
        "collection_name": "sentinel-2-l2a",
        "bands": ["blue", "green", "red"],
    }]
)
```

Specify multiple data sources as follows:

```python
# Example of a valid multiple DataSource dictionaries passed as a list to the `data_sources` argument.
download_data(
    data_sources = [{
        "data_connector": "sentinel_aws",
        "collection_name": "sentinel-2-l2a",
        "bands": ["blue", "green", "red"],
    },
    {
        "data_connector": "sentinelhub",
        "collection_name": "s1_grd",
        "bands": ["B04", "B03", "B02"]
    }]
)
```
To specify multiple data sources with the CLI with the following config:

```yaml
# ./docs/examples/config.yaml
download:
  data_sources:
  - data_connector: "sentinel_aws"
    collection_name: "sentinel-2-l2a"
    bands: ["blue", "green", "red"]
  - data_connector: "sentinelhub"
    collection_name: "s1_grd"
    bands: ["B04", "B03", "B02"]
  date_allowance: 
    pre_days: 0
    post_days: 21
  transform:
    scale_data_xarray: True
    impute_nans: true
    reproject: True
```

### Max Cloud Cover: `max_cloud_cover`
Maximum cloud cover percentage for data selection.

### Datetime Bounding Box Shape File: `datetime_bbox_shp_file`
Path to a shapefile containing datetime and bounding box information. This shapefile will have been saved as `{working_dir}/{dataset_name}_all_bboxes.shp` if the `process_labels` set has already been run. If `datetime_bbox_shp_file` is not explicitly specified, TerraKit will first check for the default value (`./tmp/terrakit_curated_dataset_all_bboxes.shp`), followed by checking the working directory for `{dataset_name}_all_bboxes.shp`. 

The shapefile `{dataset_name}_all_bboxes.shp` must contain a `datetime` field and `geometry` field.

### Labels Shape File: `labels_shp_file`
Path to a shapefile containing datetime and label geometery information. This shapefile will have been saved as `{working_dir}/{dataset_name}_labels.shp` if the `process_labels` set has already been run. If `datetime_bbox_shp_file` is not explicitly specified, TerraKit will first check for the default value (`./tmp/terrakit_curated_dataset_labels.shp`), followed by checking the working directory for `{dataset_name}_labels.shp`. 

The shapefile `{dataset_name}_labels.shp` must contain a `datetime` field and `geometry` field.

### Keep files: `keep_files`
Flag to preserve shapefiles in the working directory once they have been used by the download data step. Downloaded files will not be removed. Set to `True` to ensure shapefiles remain in place.

### Set No Data: `set_no_data`
Controls how label rasterization handles the background (no-data) pixels. When set to `True`, background pixels are assigned a no-data value (-1), allowing label class 0 to be used for actual labels. When set to `False` (default), background pixels are assigned value 0, which means label classes must start from 1 to avoid conflicts.

**Important:** If your labels use class 0 and `set_no_data=False`, TerraKit will raise a `TerrakitValueError` because class 0 would conflict with the background class. In this case, you must either:
- Set `set_no_data=True` to use -1 for background pixels, or
- Ensure your label classes start from 1 instead of 0

Example with multi-class labels using class 0:
```python
queried_data = download_data(
    data_sources=config["download"]["data_sources"],
    date_allowance=config["download"]["date_allowance"],
    set_no_data=True,  # Required when using class 0
    transform=config["download"]["transform"],
)
```

## Data Connectors
Data connectors are classes which enable a user to search for data and query data from a particular data source using a common set of functions.  Each data connector has the following mandatory methods:

* list_collections()
* find_data()
* get_data()


## Available data connectors
The following data connectors and associated collections are available:

| Connectors        | Collections |
| ----------------- | ----------- |
| sentinelhub       | s2_l1c, dem, s1_grd, hls_l30, s2_l2a, hls_s30 |
| nasa_earthdata    | HLSL30_2.0, HLSS30_2.0  |
| sentinel_aws      | sentinel-2-l2a  |
| IBMResearchSTAC | 'HLSS30', 'esa-sentinel-2A-msil1c', 'HLS_S30',, 'atmospheric-weather-era5', 'deforestation-umd', 'Radar-10min', 'tasmax-rcp85-land-cpm-uk-2.2km', 'vector-osm-power', 'ukcp18-land-cpm-uk-2.2km', 'treecovermaps-eudr', 'ch4' + more|
| TheWeatherCompany | weathercompany-daily-forecast |

## Data connector access
Each data connector has a different access requirements. For example, connecting to SentinelHub and NASA EarthData, you will need to obtain credentials from each provider. Once these have been obtained, they can be added to a `.env` file at the root directory level using the following syntax:

```.env
SH_CLIENT_ID="<SentinelHub Client ID>"
SH_CLIENT_SECRET="<SentinelHub Client Secret>"
NASA_EARTH_BEARER_TOKEN="<NASA EarthData Bearer Token>"
```

### NASA Earthdata
To access NASA Earthdata, register for an Earthdata Login profile and requests a bearer token. https://urs.earthdata.nasa.gov/profile

### Sentinel Hub
To access sentinel hub, register for an account and requests an OAuth client using the Sentinel Hub dashboard https://www.planet.com

### Sentinel AWS
Access sentinel AWS data is open and does not require any credentials.

### The Weather Company
To access The Weather Company, register for an account and requests an API Key https://www.weathercompany.com/weather-data-apis/. Once you have an API key, set the following environment variable:

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

## Try out
Data Connectors can be used outside the TerraKit Pipeline. Take a look at the [TerraKit: Easy geospatial data search and query](examples/terrakit_download.ipynb) notebook for more help getting started with TerraKit Data Connectors.