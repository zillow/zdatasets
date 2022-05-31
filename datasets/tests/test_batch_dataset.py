import os
import shutil

import dask.dataframe as dd
import pandas as pd
import pytest
from pandas._testing import assert_frame_equal
from pyspark import pandas as ps
from pyspark.sql import DataFrame as SparkDataFrame

from datasets import Dataset, Mode
from datasets.exceptions import InvalidOperationException
from datasets.plugins.batch.batch_base_plugin import BatchOptions
from datasets.plugins.batch.batch_dataset import BatchDataset
from datasets.tests.conftest import TestExecutor


csv_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data/pandas.csv")


@pytest.fixture
def path(data_path: str) -> str:
    return os.path.join(data_path, "ds1")


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
    return Dataset(
        name=name,
        logical_key="my_key",
        mode=mode,
        options=BatchOptions(
            partition_by=partition_by,
            path=path,
        ),
    )


@pytest.fixture
def df() -> pd.DataFrame:
    data = {
        "col1": ["A", "A", "A", "B", "B", "B"],
        "col2": [1, 2, 3, 4, 5, 6],
        "col3": ["A1", "A1", "A2", "B1", "B2", "B2"],
    }
    return pd.DataFrame(data)


def test_dataset_factory_batch_plugin(dataset: BatchDataset, path: str):
    assert dataset.name == "Ds1"
    assert dataset.hive_table_name == "ds_1"
    assert dataset.key == "my_key"
    assert dataset.path == path
    assert dataset.partition_by == "col1,run_id"


@pytest.mark.parametrize("mode", [Mode.WRITE])
def test_from_read_on_mode_write(dataset: BatchDataset, df: pd.DataFrame):
    with pytest.raises(InvalidOperationException) as exec_info:
        dataset.to_pandas(columns="col1,col2")

    assert f"Cannot read because mode={Mode.WRITE}" in str(exec_info.value)


@pytest.mark.parametrize("path", [None])
def test_to_pandas(dataset: BatchDataset, df: pd.DataFrame):
    dataset.write(df.copy())
    read_df = dataset.to_pandas(columns="col1,col2,col3")
    assert (df == read_df).all().all()

    df = dataset.to_pandas(columns="col1")
    assert df.columns.to_list() == ["col1"]

    df1 = dataset.to_pandas(partitions=dict(col1="A", col3="A1"))
    assert df1["col1"].unique().tolist() == ["A"]
    assert df1["col3"].unique().tolist() == ["A1"]

    df2 = dataset.to_pandas(partitions=dict(col1="A"))
    assert df2["col1"].unique().tolist() == ["A"]
    assert df2["col3"].unique().tolist() == ["A1", "A2"]

    # write with a new run_time
    old_run_time = TestExecutor.test_run_time
    TestExecutor.test_run_time += 2
    dataset.write(read_df.copy())

    # read old_run_time
    df3 = dataset.to_pandas(
        columns="col1,col2,col3,run_time", partitions=dict(col1="A"), run_time=old_run_time
    )
    assert df3["run_time"].unique().tolist() == [old_run_time]


@pytest.mark.parametrize("path", [None])
@pytest.mark.parametrize("partition_by", ["col1,col3"])
@pytest.mark.parametrize("name", ["DsPartitioned"])
def test_to_pandas_partitioned(dataset: BatchDataset, df: pd.DataFrame):
    dataset.write(df.copy())
    read_df = dataset.to_pandas(columns="col1,col2,col3")
    assert (df == read_df).all().all()

    df = dataset.to_pandas(columns="col1")
    assert df.columns.to_list() == ["col1"]

    df1 = dataset.to_pandas(partitions=dict(col1="A", col3="A1"))
    assert df1["col1"].unique().tolist() == ["A"]
    assert df1["col3"].unique().tolist() == ["A1"]

    df2 = dataset.to_pandas(partitions=dict(col1="A"))
    assert df2["col1"].unique().tolist() == ["A"]
    assert df2["col3"].unique().tolist() == ["A1", "A2"]


@pytest.mark.parametrize("path", [csv_path])
@pytest.mark.parametrize("mode", ["READ_WRITE"])
def test_default_plugin_pandas_csv(dataset: BatchDataset, df: pd.DataFrame):
    shutil.rmtree(csv_path, ignore_errors=True)
    df.to_csv(csv_path)
    read_df = dataset.to_pandas(columns="col1,col2,col3", storage_format="csv")
    assert (df == read_df).all().all()
    shutil.rmtree(csv_path, ignore_errors=True)


def test_to_pandas_unsupported_format(dataset: BatchDataset):
    with pytest.raises(ValueError) as exec_info:
        dataset.to_pandas(storage_format="foo")

    assert "foo" in str(exec_info.value)


def test_to_pandas_mode_read(dataset: BatchDataset, df: pd.DataFrame):
    dataset.write(df)

    def test_read():
        assert dataset.to_pandas(columns="col1").columns.to_list() == ["col1"]

    test_read()
    dataset.mode = Mode.READ
    test_read()


@pytest.mark.parametrize("path", [None])
@pytest.mark.parametrize("partition_by", ["col1,col3", ["col1", "col3"]])
@pytest.mark.parametrize("mode", [Mode.WRITE, Mode.READ_WRITE])
def test_get_dataset_path(dataset: BatchDataset, df: pd.DataFrame):
    dataset.write(df.copy())
    path = dataset._get_dataset_path()
    assert path.endswith("datasets/tests/data/datastore/my_program/ds_1")
    os.path.exists(f"{path}/col1=A/col3=A1")
    shutil.rmtree(path)


@pytest.mark.parametrize("mode", [Mode.READ])
def test_write_on_read_only_pandas(dataset: BatchDataset):
    df = pd.DataFrame({"col1": ["A", "A", "A", "B", "B", "B"], "col2": [1, 2, 3, 4, 5, 6]})
    with pytest.raises(InvalidOperationException):
        dataset.write(df.copy())


@pytest.mark.parametrize("path", [None])
@pytest.mark.parametrize("partition_by", ["col1"])
@pytest.mark.parametrize("name", ["DsDask"])
def test_offline_plugin_dask(dataset: BatchDataset, df):
    data = dd.from_pandas(df, npartitions=1)
    assert "dask.dataframe.core.DataFrame" in str(type(data))
    dataset.write(data)
    df = dataset.to_dask(columns="col1").compute()
    assert df.columns.to_list() == ["col1"]


@pytest.mark.parametrize("mode", [Mode.READ])
def test_write_on_read_only_dask(dataset: BatchDataset, df):
    data = dd.from_pandas(df, npartitions=1)
    with pytest.raises(InvalidOperationException):
        dataset.write(data)


@pytest.mark.parametrize("mode", [Mode.WRITE])
def test_read_on_write_only_dask(dataset: BatchDataset, df):
    with pytest.raises(InvalidOperationException):
        dataset.to_dask(columns="col1")


@pytest.mark.parametrize("path", [None])
@pytest.mark.parametrize("partition_by", ["col1,col3"])
@pytest.mark.parametrize("name", ["DsSpark"])
@pytest.mark.spark
def test_to_spark(dataset: BatchDataset, df: pd.DataFrame, spark_session):
    sdf: SparkDataFrame = ps.from_pandas(df).to_spark()
    assert isinstance(sdf, SparkDataFrame)
    dataset.write(sdf)
    spark_df = dataset.to_spark(columns="col1")
    spark_df.show()
    assert spark_df.columns == ["col1", "run_id", "run_time"]  # run_id is added

    df1 = dataset.to_spark(partitions=dict(col1="A", col3="A1")).toPandas()
    assert df1["col1"].unique().tolist() == ["A"]
    assert df1["col3"].unique().tolist() == ["A1"]

    df2 = dataset.to_spark(partitions=dict(col1="A")).toPandas()
    assert df2["col1"].unique().tolist() == ["A"]
    assert df2["col3"].unique().tolist() == ["A1", "A2"]


@pytest.mark.spark
@pytest.mark.parametrize("mode", [Mode.READ])
def test_write_on_read_only_spark(dataset: BatchDataset):
    df = pd.DataFrame({"col1": ["A", "A", "A", "B", "B", "B"], "col2": [1, 2, 3, 4, 5, 6]})
    spark_df: SparkDataFrame = ps.from_pandas(df).to_spark()
    with pytest.raises(InvalidOperationException):
        dataset.write(spark_df)


@pytest.mark.spark
@pytest.mark.parametrize("mode", [Mode.WRITE])
def test_read_on_write_only_spark(dataset: BatchDataset, df):
    df: SparkDataFrame = ps.from_pandas(df).to_spark()
    dataset.write(df)
    with pytest.raises(InvalidOperationException):
        dataset.to_spark(columns="col1")


@pytest.mark.spark
def test_default_plugin_spark_pandas(dataset: BatchDataset, df: pd.DataFrame, spark_session):
    dataset.write(ps.from_pandas(df))
    read_psdf: ps.DataFrame = dataset.to_spark_pandas()
    assert isinstance(read_psdf, ps.DataFrame)
    read_df = read_psdf.to_pandas()
    assert_frame_equal(df.set_index("col2"), read_df.set_index("col2"), check_like=True)


@pytest.mark.parametrize("mode", [Mode.READ])
@pytest.mark.spark
def test_write_on_read_only_spark_pandas(dataset: BatchDataset):
    df = pd.DataFrame({"col1": ["A", "A", "A", "B", "B", "B"], "col2": [1, 2, 3, 4, 5, 6]})
    with pytest.raises(InvalidOperationException):
        dataset.write(ps.from_pandas(df))


def test_write_unsupported_data_type(dataset: BatchDataset):
    data = {}
    with pytest.raises(ValueError):
        dataset.write(data)


@pytest.mark.parametrize("path", [None])
@pytest.mark.parametrize("name", ["MyTable"])
def test_register_dataset_path_plugin(dataset: BatchDataset, df: pd.DataFrame):
    dataset.write(df.copy())

    assert dataset.name == "MyTable"
    path = dataset._get_dataset_path()
    assert path.endswith("datasets/tests/data/datastore/my_program/my_table")

    def test_dataset_path_func(passed_dataset: BatchDataset) -> str:
        return "fee_foo"

    BatchDataset.register_dataset_path_func(test_dataset_path_func)

    os.environ["ZODIAC_SERVICE"] = "test_service"
    dataset._path = None
    path = dataset._get_dataset_path()
    assert path.endswith("fee_foo")

    BatchDataset.register_dataset_path_func(None)
