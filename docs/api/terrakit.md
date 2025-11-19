# TerraKit API Documentation

TerraKit makes creating ML-ready EO datasets easy. Quickly finding, retrieving and processing geospatial information from a range of data connectors. To get started install terrakit:

```
uv add terrakit
```

Specify a directory containing a set of labels. Then run the TerraKit Pipeline steps to find, download, chip and store a set of labels and data from `sentinel AWS`.

```python
import terrakit

terrakit.process_labels(labels_folder="./docs/examples/test_wildfire_vector/")
terrakit.download_data()
terrakit.chip_and_label_data()
terrakit.taco_store_data()
```

To find out more, check out the Examples tab, or explore the API docs.