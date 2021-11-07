import os

import pandas as pd
import pytest
from pandas._testing import assert_frame_equal

from datasets import Mode
from datasets.context import Context
from datasets.dataset_plugin import DatasetPlugin
from datasets.exceptions import InvalidOperationException
from datasets.plugins.batch.batch_dataset_plugin import BatchDatasetPlugin


ds1_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data/ds1")


@pytest.fixture
def path() -> str:
    return ds1_path


@pytest.fixture
def mode():
    return Mode.READ_WRITE


@pytest.fixture
def partition_by() -> str:
    return "col1,run_id"


@pytest.fixture
def name() -> str:
    return "Ds1"


@pytest.fixture
def dataset(name: str, path: str, partition_by: str, mode: Mode):
    return DatasetPlugin.from_keys(
        context=Context.BATCH,
        name=name,
        logical_key="my_key",
        path=path,
        partition_by=partition_by,
        mode=mode,
    )


@pytest.fixture
def df() -> pd.DataFrame:
    return pd.DataFrame({"col1": ["A", "A", "A", "B", "B", "B"], "col2": [1, 2, 3, 4, 5, 6]})


def test_from_keys_parquet_plugin(dataset: BatchDatasetPlugin):
    assert dataset.name == "Ds1"
    assert dataset.key == "my_key"
    assert dataset.path == ds1_path
    assert dataset.partition_by == "col1,run_id"


@pytest.mark.parametrize("mode", [Mode.WRITE])
def test_from_read_on_mode_write(dataset: BatchDatasetPlugin, df: pd.DataFrame):
    with pytest.raises(InvalidOperationException) as exec_info:
        dataset.read_pandas(columns="col1,col2")

    assert f"Cannot read because mode={Mode.WRITE}" in str(exec_info.value)


def test_from_write(dataset: BatchDatasetPlugin, df: pd.DataFrame):
    dataset.write(df.copy())
    read_df = dataset.read_pandas(columns="col1,col2")
    assert (df == read_df).all().all()


@pytest.mark.depends(on=["test_from_write"])
def test_read_columns(dataset: BatchDatasetPlugin):
    df = dataset.read_pandas(columns="col1")
    assert df.columns.to_list() == ["col1"]


@pytest.mark.parametrize("path", [None])
@pytest.mark.parametrize("partition_by", ["col1"])
def test_get_dataset_path(dataset: BatchDatasetPlugin, df: pd.DataFrame):
    dataset.write(df.copy())
    path = dataset._get_dataset_path()
    assert path.endswith("datasets/tests/data/datastore/my_program/ds1")


@pytest.mark.parametrize("mode", [Mode.READ])
def test_write_on_read_only(dataset: BatchDatasetPlugin):
    df = pd.DataFrame({"col1": ["A", "A", "A", "B", "B", "B"], "col2": [1, 2, 3, 4, 5, 6]})
    with pytest.raises(InvalidOperationException):
        dataset.write(df.copy())


@pytest.mark.parametrize("path", [None])
@pytest.mark.parametrize("name", ["MyTable"])
def test_zillow_dataset_path_plugin(dataset: BatchDatasetPlugin, df: pd.DataFrame):
    # import registers it!
    from datasets.plugins.batch.zillow_dataset_path import (
        _get_batch_dataset_path,
    )

    dataset.write(df.copy())

    assert dataset.name == "MyTable"
    assert dataset._table_name == "my_table"

    BatchDatasetPlugin._register_dataset_path_func(_get_batch_dataset_path)
    path = dataset._get_dataset_path()
    assert path.endswith("datasets/tests/data/datastore/my_program/my_table")

    os.environ["ZODIAC_SERVICE"] = "test_service"
    path = dataset._get_dataset_path()
    assert path.endswith("datasets/tests/data/datastore/test_service/my_program/my_table")


@pytest.mark.parametrize("path", [None])
@pytest.mark.parametrize("partition_by", ["col1"])
@pytest.mark.parametrize("name", ["DsDask"])
def test_parquet_dask_dataset(dataset: BatchDatasetPlugin, df):
    dataset.write(df.copy())
    df = dataset.read_dask(columns="col1").compute()
    assert df.columns.to_list() == ["col1"]


@pytest.mark.spark
def test_parquet_spark_dataset(dataset: BatchDatasetPlugin, df: pd.DataFrame, spark_session):
    dataset.write(df.copy())
    spark_df = dataset.read_spark(columns="col1")
    spark_df.show()
    assert spark_df.columns == ["col1", "run_id"]  # run_id is added


@pytest.mark.spark
def test_default_read(dataset: BatchDatasetPlugin, df: pd.DataFrame, spark_session):
    from pyspark import pandas as ps

    psdf: ps.DataFrame = ps.from_pandas(df)

    dataset.write(psdf, mode="overwrite")
    read_psdf: ps.DataFrame = dataset.read()

    assert isinstance(read_psdf, ps.DataFrame)
    read_df = read_psdf.to_pandas()
    assert_frame_equal(df, read_df, check_like=True)
