# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


import pytest
import os


def test_entrypoint():
    exit_status = os.system("terrakit --help")
    assert exit_status == 0


@pytest.mark.parametrize("action", ["labels"])
def test_cli(action, default_dir_clean_up):
    exit_status = os.system(f"terrakit --config docs/examples/config.yaml {action}")
    assert exit_status == 0


def test_cli_invaild_action(default_dir_clean_up):
    invalid_action = "nothing"
    exit_status = os.system(
        f"terrakit --config docs/examples/config.yaml {invalid_action}"
    )
    assert exit_status != 0
