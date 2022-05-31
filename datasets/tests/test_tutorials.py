import os
import sys
from os.path import dirname, realpath
from subprocess import PIPE, STDOUT, run
from typing import Optional

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


@pytest.mark.depends(on=["test_foreach_flow"])
def test_hello_plugin_flow():
    run_flow("tutorials/4_hello_plugin_flow.py")


def test_consistent_flow():
    run_flow("tutorials/5_consistent_flow.py")
    run_flow(
        "tutorials/5_consistent_flow.py",
        [
            "--hello_ds",
            '{"name": "HelloDs", "mode": "WRITE", "columns": "value", "options":{"type":"OnlineOptions"}}',
        ],
    )
    run_flow("tutorials/5_consistent_flow.py", context="ONLINE")
    run_flow(
        "tutorials/5_consistent_flow.py",
        ["--hello_ds", '{"name": "HelloDs", "options":{"type":"OnlineOptions","keys":"first,second,third"}}'],
        context="ONLINE",
    )


def test_hive_flow():
    run_flow("tutorials/6_hive_dataset_flow.py")


def run_flow(flow_py, args: Optional[list] = None, context: Optional[str] = None) -> str:
    os.environ["METAFLOW_COVERAGE_SOURCE"] = "tutorial,datasets"
    os.environ["METAFLOW_COVERAGE_OMIT"] = "metaflow"
    os.environ["METAFLOW_USER"] = "compile_only_user"
    if context:
        os.environ["CONTEXT"] = context

    base_dir = dirname(dirname(realpath(__file__)))
    file_name = os.path.join(base_dir, flow_py)
    cmd = [
        sys.executable,
        file_name,
        "--datastore=local",
        "--no-pylint",
        "run",
    ]
    if args:
        cmd.extend(args)
    process = run(cmd, cwd=dirname(base_dir), stdout=PIPE, stderr=STDOUT, encoding="utf8")
    stdout = process.stdout
    if not process.returncode == 0:
        print(stdout)
    assert process.returncode == 0, stdout

    return stdout
