# Add a New Download Transformation
To add a new data connector, use the [download_transformation_template.py](https://github.com/terrastackai/terrakit/blob/main/terrakit/download/transformations/download_transformation_template.py) as a starting point. The new transformation should take an `xarray.DataArray` as an input and also return an `xarray.DataArray` as output. Update any data connectors that may need to use the new transformation by importing the function at the top of the file. For instance:

```python
from terrakit.download.transformations.impute_nans_xarray import impute_nans_xarray
```

Please also include tests for your new transformation function.

Make sure to also update the documentation. Each transformation function has a separate entry in [./adding_download_transformations.md](./adding_download_transformations.md) making it easy to add new docs.


## Transformation template function Documentation
::: terrakit.download.transformations.download_transformation_template
    handler: python
    options:
        show_docstring_only: false
        show_docstring_section: []
        show_root_heading: false
        show_source: true
        show_object_full_path: false
        show_signature: false

## Download Data Pipeline
To include a new transformation in the TerraKit Download Data Pipeline, a few code changes are needed. These are clearly sign posted, making in simple to extend the TerraKit library.

To enable the new transformation to be selected, update line 71 of `terrakit/validate/download_model.py`. Please set the default option to `False`.

```python
# terrakit/validate/download_model.py
""" >>> INCLUDE NEW TRANSFORMATIONS HERE <<< 
<new_transformation_option>: bool = False
"""
```

Next update `terrakit/download/download_data.py` to import the model into the download data pipeline code.

```python
# terrakit/download/download_data.py
""" >>> IMPORT NEW TRANSFORMATIONS HERE <<< 
from .transformations.<new_transformation> import <new_transformation>
"""
```

Finally update `terrakit/download/download_data.py` to implement the transformation after data has been downloaded. Consider limiting the useage to a given data connector, unless if can be safely applied to all data connector.
```python
# terrakit/download/download_data.py line 286
""" >>> INCLUDE NEW TRANSFORMATIONS HERE <<< 
if self.transform.<new_transformation_func>:
    dai = <new_tranformation_fnc(da)>
"""
```