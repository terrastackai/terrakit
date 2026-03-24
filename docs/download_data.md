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

## Data Connectors
Data connectors are classes which enable a user to search for data and query data from a particular data source using a common set of functions. Check out the [TerraKit Data Connectors](#data-connectors) section for more information.

## Try out
Try out the TerraKit data pipeline workflow using the [Terrakit: Labels to dataset pipeline](examples/labels_to_data.ipynb) notebook for more help getting started with TerraKit Data Connectors.