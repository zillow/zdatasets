import pytest

from datasets.tests.test_tutorials import run_flow


@pytest.mark.depends()
def test_input_output_flow():
    run_flow("tests/resources/txf_test_flow.py")

