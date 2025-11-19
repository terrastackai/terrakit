# Welcome to TerraKit's documentation!

TerraKit is a Python package for finding, retrieving and processing geospatial information from a range of data connectors. TerraKit makes creating ML-ready EO datasets easy.

![TerraKit Demo](img/demo.gif "TerraKit demo")
## Getting started
To install TerraKit, use pip

```bash
pip install terrakit
```
or uv:
```bash
uv pip install terrakit
```

Check TerraKit is working as expected by running:

```bash
python -c "import terrakit; data_source='nasa_earthdata'; dc = terrakit.DataConnector(connector_type=data_source)"
```

Take a look at the [example notebooks](examples/labels_to_data.ipynb) for more help getting started with TerraKit. 
<!-- <a target="_blank" rel="noopener noreferrer" href="https://colab.research.google.com/github/IBM/terrakit/blob/main/docs/examples/labels_to_data.ipynb"><img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/></a> -->


## TerraKit CLI
We can also run TerraKit using the CLI. Take a look at the [TerraKit CLI Notebook](examples/terrakit_cli.ipynb) for some examples of how to use this.
<!-- <a target="_blank" rel="noopener noreferrer" href="https://colab.research.google.com/github/IBM/terrakit/blob/main/docs/examples/terrakit_cli.ipynb"><img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/></a> -->

## TerraKit Pipelines
TerraKit provides tools for
For more information, checkout out the [Pipelines](process_labels.md) section.