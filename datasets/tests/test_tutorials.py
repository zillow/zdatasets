import os
import sys
from os.path import dirname, realpath
from subprocess import PIPE, STDOUT, run

import pytest


def test_hello_dataset_flow():
    run_flow("tutorials/0_hello_dataset_flow.py")


@pytest.mark.depends(on=["test_hello_dataset_flow"])
def test_input_output_flow():
    run_flow("tutorials/1_input_output_flow.py")


@pytest.mark.spark
@pytest.mark.depends(on=["test_input_output_flow"])
def test_dask_spark_flow():
    run_flow("tutorials/2_spark_dask_flow.py")


def test_foreach_flow():
    run_flow("tutorials/3_foreach_dataset_flow.py")


def run_flow(flow_py):
    os.environ["METAFLOW_COVERAGE_SOURCE"] = "tutorial,datasets"
    os.environ["METAFLOW_COVERAGE_OMIT"] = "metaflow"
    os.environ["METAFLOW_USER"] = "compile_only_user"
    base_dir = dirname(dirname(realpath(__file__)))
    file_name = os.path.join(base_dir, flow_py)
    cmd = [
        sys.executable,
        file_name,
        "--datastore=local",
        "--no-pylint",
        "run",
    ]
    process = run(cmd, stdout=PIPE, stderr=STDOUT, encoding="utf8")
    print(process.stdout)
    assert process.returncode == 0, process.stdout
