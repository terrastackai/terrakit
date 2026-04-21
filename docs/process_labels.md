# Process labels in preparation for curating a new dataset

<!-- Terrakit makes use of five pipeline steps to orchestrate the curation of a dataset from a set of geospatial labels.  -->
The first step in the TerraKit dataset curation pipeline is processing the labels. 

Use the `process_labels` function (or the CLI `labels` subcommand) to provide a directory containing geospatial labels in either vector or raster form. The function will return two DataFrame, the first containing bound box and temporal information for all of the geospatial locations identified in the labels files, the second containing the labels. Each DataFrame is also saved as a `.shp` file. 

The temporal information is expected in the label filename with `YYYY-MM-DD` format.

Here's an example of how to use the `process_labels` step in the TerraKit pipeline:

```python
from terrakit.transform.labels import process_labels
label_args = {
    "dataset_name": "MyDataset",
    "working_dir": "./tmp",
    "labels": {
        "labels_folder":  "./docs/example/test_wildfire_vector",
    },
}

labels_gdf, grouped_bbox_gdf = process_labels(
    dataset_name=label_args["dataset_name"],
    working_dir=label_args["working_dir"],
    labels_folder=label_args["labels"]["labels_folder"],
)
```
TerraKit will output `./tmp/MyDataset_all_bboxes.shp` and `./tmp/MyDataset_labels.shp`.

Alternatively, write the same arguments in a config file that the TerraKit CLI can use:

```yaml
# ./config.yaml
dataset_name: "MyDataset"
working_dir: "./tmp"
labels:
  labels_folder: "./docs/example/test_wildfire_vector"
```
```bash
#!/bin/bash
terrakit --config ./docs/examples/config.yaml labels
```

## Configure the Labels pipeline
Use the following parameters to configure the TerraKit Labels pipeline.

### Active
`active`: Enables the labels pipeline to run. Set to `False` to skip the step. Default: `True`

### Labels folder
`labels_folder`: Points to a directory containing geospatial label files to be processed. Required parameter.

### Datetime Information

`datetime_info`: Set to `filename` by default, TerraKit will look for temporal information in the label filename in the format `YYYY-MM-DD`. Alternatively set to `csv` to provide datetime information in an accompanying csv file in the format:

```csv
# metadata.csv
filename,date
EMSR748_AOI01_DEL_MONIT05_observedEventA_v1.json,2024-08-26
EMSR801_AOI01_DEL_MONIT02_observedEventA_v1.json,2025-04-23
```
TerraKit will look a file called `metadata.csv` in the `labels_folder`.

### label_type
`label_type`: Set to either `raster` or `vector`. TerraKit expects label data in either vector or raster format.

### Multi-class Labels
For multi-class label datasets, TerraKit supports automatic class detection through filename patterns. Include `_CLASS_<number>_` in your label filenames to specify the class:

```
EMSR801_AOI01_DEL_MONIT02_CLASS_0_observedEventA_v1_2025-04-23.json
EMSR801_AOI01_DEL_MONIT02_CLASS_1_observedEventA_v1_2025-04-23.json
```

The class number will be extracted from the filename and used during rasterization. If no `_CLASS_` pattern is found, the label defaults to class 1. This enables visualization with distinct colors for each class and proper handling of multi-class segmentation tasks.

## Download example labels
To download a set of example labels, use the `rapid_mapping_geojson_downloader` function to get started:

```python
from terrakit.general_utils.labels_downloader import rapid_mapping_geojson_downloader

example_label_1 = rapid_mapping_geojson_downloader(event_id="748", aoi="01", monitoring_number="05", version="v1", dest="./docs/examples/test_wildfire_vector")

example_label_2 = rapid_mapping_geojson_downloader(event_id="801", aoi="01", monitoring_number="02", version="v1", dest="./docs/examples/test_wildfire_vector")

```