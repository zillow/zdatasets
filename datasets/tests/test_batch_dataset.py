import os
import shutil

import dask.dataframe as dd
import pandas as pd
import pytest
from pandas._testing import assert_frame_equal
from pyspark import pandas as ps
from pyspark.sql import DataFrame as SparkDataFrame

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
    data = {
        "col1": ["A", "A", "A", "B", "B", "B"],
        "col2": [1, 2, 3, 4, 5, 6],
        "col3": ["A1", "A1", "A2", "B1", "B2", "B2"],
    }
    return pd.DataFrame(data)


def test_from_keys_offline_plugin(dataset: BatchDatasetPlugin):
    assert dataset.name == "Ds1"
    assert dataset.key == "my_key"
    assert dataset.path == ds1_path
    assert dataset.partition_by == "col1,run_id"


@pytest.mark.parametrize("mode", [Mode.WRITE])
def test_from_read_on_mode_write(dataset: BatchDatasetPlugin, df: pd.DataFrame):
    with pytest.raises(InvalidOperationException) as exec_info:
        dataset.to_pandas(columns="col1,col2")

    assert f"Cannot read because mode={Mode.WRITE}" in str(exec_info.value)


def test_default_plugin_pandas(dataset: BatchDatasetPlugin, df: pd.DataFrame):
    dataset.write(df.copy())
    read_df = dataset.to_pandas(columns="col1,col2,col3")
    assert (df == read_df).all().all()


def test_default_plugin_pandas_csv(dataset: BatchDatasetPlugin, df: pd.DataFrame):
    shutil.rmtree(ds1_path, ignore_errors=True)
    df.to_csv(ds1_path)
    read_df = dataset.to_pandas(columns="col1,col2,col3", storage_format="csv")
    assert (df == read_df).all().all()
    shutil.rmtree(ds1_path, ignore_errors=True)


@pytest.mark.depends(on=["test_default_plugin_pandas"])
@pytest.mark.parametrize("mode", [Mode.READ, Mode.READ_WRITE])
def test_read_columns(dataset: BatchDatasetPlugin):
    df = dataset.to_pandas(columns="col1")
    assert df.columns.to_list() == ["col1"]


@pytest.mark.parametrize("path", [None])
@pytest.mark.parametrize("partition_by", ["col1,col3", ["col1", "col3"]])
@pytest.mark.parametrize("mode", [Mode.WRITE, Mode.READ_WRITE])
def test_get_dataset_path(dataset: BatchDatasetPlugin, df: pd.DataFrame):
    dataset.write(df.copy())
    path = dataset._get_dataset_path()
    assert path.endswith("datasets/tests/data/datastore/my_program/ds1")
    os.path.exists(f"{path}/col1=A/col3=A1")
    shutil.rmtree(path)


@pytest.mark.parametrize("mode", [Mode.READ])
def test_write_on_read_only_pandas(dataset: BatchDatasetPlugin):
    df = pd.DataFrame({"col1": ["A", "A", "A", "B", "B", "B"], "col2": [1, 2, 3, 4, 5, 6]})
    with pytest.raises(InvalidOperationException):
        dataset.write(df.copy())


@pytest.mark.parametrize("path", [None])
@pytest.mark.parametrize("partition_by", ["col1"])
@pytest.mark.parametrize("name", ["DsDask"])
def test_offline_plugin_dask(dataset: BatchDatasetPlugin, df):
    data = dd.from_pandas(df, npartitions=1)
    assert "dask.dataframe.core.DataFrame" in str(type(data))
    dataset.write(data)
    df = dataset.to_dask(columns="col1").compute()
    assert df.columns.to_list() == ["col1"]


@pytest.mark.parametrize("mode", [Mode.READ])
def test_write_on_read_only_dask(dataset: BatchDatasetPlugin, df):
    data = dd.from_pandas(df, npartitions=1)
    with pytest.raises(InvalidOperationException):
        dataset.write(data)


@pytest.mark.depends(on=["test_offline_plugin_dask"])
@pytest.mark.parametrize("mode", [Mode.WRITE])
def test_read_on_write_only_dask(dataset: BatchDatasetPlugin, df):
    with pytest.raises(InvalidOperationException):
        dataset.to_dask(columns="col1")


@pytest.mark.parametrize("path", [None])
@pytest.mark.parametrize("partition_by", ["col1"])
@pytest.mark.spark
def test_offline_plugin_spark(dataset: BatchDatasetPlugin, df: pd.DataFrame, spark_session):
    df: SparkDataFrame = ps.from_pandas(df).to_spark()
    assert isinstance(df, SparkDataFrame)
    dataset.write(df)
    spark_df = dataset.to_spark(columns="col1")
    spark_df.show()
    assert spark_df.columns == ["col1", "run_id"]  # run_id is added


@pytest.mark.spark
@pytest.mark.parametrize("mode", [Mode.READ])
def test_write_on_read_only_spark(dataset: BatchDatasetPlugin):
    df = pd.DataFrame({"col1": ["A", "A", "A", "B", "B", "B"], "col2": [1, 2, 3, 4, 5, 6]})
    spark_df: SparkDataFrame = ps.from_pandas(df).to_spark()
    with pytest.raises(InvalidOperationException):
        dataset.write(spark_df)


@pytest.mark.spark
@pytest.mark.parametrize("mode", [Mode.WRITE])
def test_read_on_write_only_spark(dataset: BatchDatasetPlugin, df):
    df: SparkDataFrame = ps.from_pandas(df).to_spark()
    dataset.write(df)
    with pytest.raises(InvalidOperationException):
        dataset.to_spark(columns="col1")


@pytest.mark.spark
def test_default_plugin_spark_pandas(dataset: BatchDatasetPlugin, df: pd.DataFrame, spark_session):
    dataset.write(ps.from_pandas(df))
    read_psdf: ps.DataFrame = dataset.to_spark_pandas()
    assert isinstance(read_psdf, ps.DataFrame)
    read_df = read_psdf.to_pandas()
    assert_frame_equal(df.set_index("col2"), read_df.set_index("col2"), check_like=True)


@pytest.mark.parametrize("mode", [Mode.READ])
def test_write_on_read_only_spark_pandas(dataset: BatchDatasetPlugin):
    df = pd.DataFrame({"col1": ["A", "A", "A", "B", "B", "B"], "col2": [1, 2, 3, 4, 5, 6]})
    with pytest.raises(InvalidOperationException):
        dataset.write(ps.from_pandas(df))


def test_write_unsupported(dataset: BatchDatasetPlugin):
    data = {}
    with pytest.raises(ValueError):
        dataset.write(data)


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
