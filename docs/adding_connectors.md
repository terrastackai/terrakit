# Add a New Data Connector
To add a new data connector, use the [connector_template.py](https://github.com/terrastackai/terrakit/blob/main/terrakit/download/connector_template.py) as a starting point. The new connector should implement the `list_collection`, `find_data` and `get_data` functions and extend the `Connector` class from the `terrakit.download.connector` module. Finally update [terrakit.py](https://github.com/terrastackai/terrakit/blob/main/terrakit/terrakit.py) to enable the new connector to be selected.

To also include new tests for the new connector, please make use of [test_connector_template.py](https://github.com/terrastackai/terrakit/blob/main/tests/component_tests/download/data_connectors/test_connector_template.py).

Make sure to also update the documentation. Each data connector has a separate markdown file making it easy to add new docs.


## Data Connector Template class Documentation
::: terrakit.download.data_connectors.connector_template
    handler: python
    options:
        members:
            - ConnectorTemplate
        show_docstring_only: false
        show_docstring_section: []
        show_root_heading: false
        show_source: true
        show_object_full_path: false
        show_signature: false


---
## Data Connector Abstract class Documentation
::: terrakit.download.connector
    handler: python
    options:
      members:
        - Connector
      show_root_heading: True
      show_source: true
      show_object_full_path: false
      show_signature: false

