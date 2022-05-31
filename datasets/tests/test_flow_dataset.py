from metaflow import Flow, namespace

from datasets.plugins.batch.flow_dataset import _get_run_id
from datasets.tests.test_tutorials import run_flow


def test_get_run_id():
    flow_name = "HelloDatasetFlow"
    namespace(None)
    run_1_output = run_flow("tutorials/0_hello_dataset_flow.py")
    run_1_id = Flow(flow_name).latest_successful_run.id
    run_2_output = run_flow("tutorials/0_hello_dataset_flow.py")
    run, run_id = _get_run_id(flow_name, run_id=None)

    assert run_id is not None
    latest_successful_run = Flow(flow_name).latest_successful_run
    run, run_id = _get_run_id(flow_name, run_id=latest_successful_run.id)
    assert run_id == latest_successful_run.id
    assert run_id == run.id

    assert run_id in run_2_output

    run, run_id = _get_run_id(flow_name, run_id=run_1_id)
    assert run_id in run_1_output
