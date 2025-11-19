# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


import inspect

from terrakit.download.download_data import DownloadCls
from terrakit.chip.tiling import ChipAndLabelCls
from terrakit.transform.labels import LabelsCls


def get_default_class_args_and_values(cls_name: type) -> dict:
    """
    Return a dictionary of class arguments and default values.

    Parameters:
        cls_name (type) : The class for which to retrieve default arguments.

    Returns:
        dict: A dictionary containing class arguments and their default values.
    """
    default_args = {}
    signature = inspect.signature(cls_name.__init__)  # type: ignore[misc]
    parameters = signature.parameters
    for name, param in parameters.items():
        if name == "self":
            continue  # Skip the 'self' parameter
        default_value = param.default
        if default_value is inspect.Parameter.empty:
            default_value = None
        default_args[name] = default_value
    return default_args


def get_pipeline_defaults() -> dict:
    """
    Return a dictionary of arguments and default values for all pipeline steps.

    Returns
        dict: A dictionary containing class arguments and their default values for pipeline steps.

    Example:
        ```python
        from terrakit.general_utils.defautls import get_pipeline_default

        options = get_pipeline_defaults()
        ```
    """
    onboarding_defaults = {}
    onboarding_defaults["labels"] = get_default_class_args_and_values(LabelsCls)
    onboarding_defaults["download"] = get_default_class_args_and_values(DownloadCls)
    onboarding_defaults["chip"] = get_default_class_args_and_values(ChipAndLabelCls)
    return onboarding_defaults


def update_pipeline_args(pipeline_options: dict) -> dict:
    """
    Update default values for any pipeline steps specified in pipelines_options.

    Parameters:
        pipeline_options (dict): Dictionary of all pipeline options.

    Returns:
        dict: Dictionary of class arguments and either default values or 'pipeline_options' values.

    Example:
        ```python
        from terrakit.general_utils.defautls import update_pipeline_args

        my_options = {
            "chip": {
                "sample_dim": 124,
            }
        }
        onboarding_options = update_pipeline_args(my_options)
        ```
    """
    default_onboarding_options = get_pipeline_defaults()
    for step in pipeline_options:
        if step in default_onboarding_options.keys():
            for parms in pipeline_options[step]:
                if parms in default_onboarding_options[step].keys():
                    default_onboarding_options[step][parms] = pipeline_options[step][
                        parms
                    ]
    return default_onboarding_options
