from pathlib import Path

import dask.dataframe as dd
import pandas as pd
import pytest
from pandas._testing import assert_frame_equal
from pyspark import pandas as ps
from pyspark.sql import DataFrame as SparkDataFrame, SparkSession

from datasets import Dataset, Mode
from datasets.exceptions import InvalidOperationException
from datasets.plugins import HiveDataset
from datasets.plugins.batch.hive_dataset import (
    HiveOptions,
    _retry_with_backoff,
)
from datasets.tests.conftest import TestExecutor


@pytest.fixture
def mode():
    return Mode.READ_WRITE


@pytest.fixture
def partition_by() -> str:
    return "col1,col3"


@pytest.fixture
def columns() -> str:
    return "col1,col2,col3"


@pytest.fixture
def hive_table() -> str:
    return "my_hive_table"


@pytest.fixture
def dataset(hive_table: str, partition_by: str, mode: Mode, columns: str):
    return Dataset(
        name="Foo",
        logical_key="col1",
        columns=columns,
        mode=mode,
        options=HiveOptions(
            partition_by=partition_by,
            hive_table_name=hive_table,
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


def test_dataset_factory_hive_plugin(dataset: HiveDataset, hive_table: str):
    assert dataset.name == "Foo"
    assert dataset.hive_table_name == hive_table
    assert dataset.key == "col1"
    assert dataset.partition_by == "col1,col3"


def test_dataset_factory_is_hive_table(partition_by: str, mode: Mode, columns: str):
    dataset = Dataset(
        name="FooFoo",
        logical_key="col1",
        columns=columns,
        mode=mode,
        options=HiveOptions(partition_by=partition_by),
    )
    assert dataset.name == "FooFoo"
    assert dataset.hive_table_name == "foo_foo"
    assert dataset.key == "col1"
    assert dataset.partition_by == "col1,col3"


def test_dataset_factory_hive_not_pascal(partition_by: str, mode: Mode, columns: str):
    bad_name = "foofoo"
    with pytest.raises(ValueError) as exec_info:
        Dataset(
            name=bad_name,
            logical_key="col1",
            columns=columns,
            mode=mode,
            options=HiveOptions(partition_by=partition_by),
        )

    assert f"'{bad_name}' is not a valid Dataset name.  Please use Upper Pascal Case syntax:" in str(
        exec_info.value
    )


@pytest.mark.parametrize("mode", [Mode.WRITE])
def test_from_read_on_mode_write(dataset: HiveDataset):
    with pytest.raises(InvalidOperationException) as exec_info:
        dataset.to_spark(columns="col1,col2")

    assert f"Cannot read because mode={Mode.WRITE}" in str(exec_info.value)


@pytest.mark.parametrize("partition_by", ["col1,col3"])
@pytest.mark.spark
def test_hive_to_spark(dataset: HiveDataset, df: pd.DataFrame, spark_session: SparkSession):
    # spark_session.sql(f"DESCRIBE FORMATTED {dataset.hive_table_name}").show(truncate=False, n=100)
    # Create the Hive Table
    dataset.write(df)

    partition_col = (
        spark_session.sql(f"""show partitions {dataset.hive_table_name}""")
        .rdd.map(lambda x: x[0])
        .map(lambda x: [l.split("=")[0] for l in x.split("/")])  # noqa: E741
        .first()
    )
    assert partition_col == ["col1", "col3", "run_id", "run_time"]

    # add a new partition
    data = {"col1": ["C"], "col2": [7], "col3": ["C1"]}
    dataset.write(pd.DataFrame(data))

    # add a new row to an existing partition
    data = {"col1": ["C"], "col2": [8], "col3": ["C1"]}
    dataset.write(pd.DataFrame(data))
    read_spdf = dataset.to_spark_pandas()

    assert read_spdf.columns.to_list() == ["col1", "col2", "col3", "run_id", "run_time"]

    spark_df = dataset.to_spark(columns="col1")
    assert spark_df.columns == ["col1", "run_id", "run_time"]

    df1 = dataset.to_spark(partitions=dict(col1="A", col3="A1")).toPandas()
    assert df1["col1"].unique().tolist() == ["A"]
    assert df1["col3"].unique().tolist() == ["A1"]

    df2 = dataset.to_spark(partitions=dict(col1="A")).toPandas()
    assert df2["col1"].unique().tolist() == ["A"]
    assert df2["col3"].unique().tolist() == ["A1", "A2"]

    df3 = dataset.to_spark(partitions=dict(col1="C")).toPandas()
    assert df3["col1"].unique().tolist() == ["C"]
    assert sorted(df3["col2"].unique().tolist()) == [7, 8]
    assert df3["col3"].unique().tolist() == ["C1"]

    # write with a new run_time
    old_run_time = TestExecutor.test_run_time
    TestExecutor.test_run_time += 2
    print(f"{TestExecutor.test_run_time=}, {old_run_time=}")
    data = {"col1": ["D"], "col2": [42], "col3": ["D1"]}
    dataset.write(pd.DataFrame(data))

    def validate_latest(latest_df):
        assert latest_df["col1"].unique().tolist() == ["D"]
        assert latest_df["col2"].unique().tolist() == [42]
        assert latest_df["col3"].unique().tolist() == ["D1"]
        assert latest_df["run_time"].unique().tolist() == [TestExecutor.test_run_time]

    validate_latest(dataset.to_spark_pandas(columns="col1,col2,col3,run_time").to_pandas())
    validate_latest(spark_session.sql(f"SELECT * FROM {dataset.hive_table_name}_latest").toPandas())


@pytest.mark.parametrize("hive_table", ["test_hive_write_existing_table_run_id"])
@pytest.mark.parametrize("partition_by", ["col1,run_id"])
@pytest.mark.parametrize("columns", ["col1,col2,col3,run_id"])
@pytest.mark.spark
def test_hive_write_existing_table_run_id(
    dataset: HiveDataset, df: pd.DataFrame, spark_session: SparkSession, data_path: Path, run_id
):
    dataset.write(df)

    # Try a different path and partition_by
    data = {"col1": ["A", "A", "A"], "col2": [7, 8, 9], "col3": ["A11", "A11", "A12"]}
    df2 = pd.DataFrame(data)
    old_path = dataset._path
    dataset._path = str(data_path / "test_hive_write_existing_table")
    dataset.write(df2)
    dataset._path = old_path

    read_df = dataset.to_spark_pandas(partitions=dict(run_id=run_id)).to_pandas()
    assert read_df["col1"].sort_values().unique().tolist() == ["A", "B"]
    assert read_df["col2"].sort_values().unique().tolist() == list(range(1, 10))
    assert read_df["col3"].sort_values().unique().tolist() == ["A1", "A11", "A12", "A2", "B1", "B2"]
    assert read_df["run_id"].unique().tolist() == [run_id]


@pytest.mark.parametrize("hive_table", ["test_hive_write_existing_table"])
@pytest.mark.parametrize("partition_by", [None])
@pytest.mark.parametrize("columns", ["col1,col2,col3,test_run_id"])
@pytest.mark.spark
def test_hive_write_existing_table(
    dataset: HiveDataset, df: pd.DataFrame, spark_session: SparkSession, data_path: Path, run_id
):
    df["test_run_id"] = run_id
    dataset.write(df)

    # Try a different path and partition_by
    data = {"col1": ["A", "A", "A"], "col2": [7, 8, 9], "col3": ["A11", "A11", "A12"]}
    df2 = pd.DataFrame(data)
    df2["test_run_id"] = run_id
    old_path = dataset._path
    dataset._path = str(data_path / "test_hive_write_existing_table")
    dataset.write(df2)
    dataset._path = old_path

    read_df = dataset.to_spark_pandas(partitions=dict(test_run_id=run_id)).to_pandas()
    assert read_df["col1"].sort_values().unique().tolist() == ["A", "B"]
    assert read_df["col2"].sort_values().unique().tolist() == list(range(1, 10))
    assert read_df["col3"].sort_values().unique().tolist() == ["A1", "A11", "A12", "A2", "B1", "B2"]
    assert read_df["test_run_id"].unique().tolist() == [run_id]


@pytest.mark.parametrize("partition_by", ["col1,col3,run_id"])
@pytest.mark.parametrize("hive_table", ["test_db.test_hive_to_spark_run_id"])
@pytest.mark.parametrize("columns", ["col1,col2,col3,run_id"])
@pytest.mark.spark
def test_hive_to_spark_run_id(dataset: HiveDataset, df: pd.DataFrame, run_id: str, spark_session):
    spark_session.sql("create database if not exists test_db")

    dataset.write(df)

    spark_df = dataset.to_spark(columns="col1,run_id")
    spark_df.show()
    assert spark_df.columns == ["col1", "run_id", "run_time"]

    df1: pd.DataFrame = dataset.to_spark(partitions=dict(col1="A", col3="A1")).toPandas()
    assert df1["col1"].unique().tolist() == ["A"]
    assert df1["col2"].tolist() == list(range(1, 3))
    assert df1["col3"].unique().tolist() == ["A1"]
    assert df1["run_id"].unique().tolist() == [run_id]


def test_write_unsupported_data_frame(dataset: HiveDataset, df: pd.DataFrame):
    data = dd.from_pandas(df, npartitions=1)
    with pytest.raises(ValueError) as exec_info:
        dataset.write(data)

    assert "data is of unsupported type" in str(exec_info.value)


def test_write_invalid_column_name(dataset: HiveDataset, df: pd.DataFrame):
    column_name = "bad:name"
    df[column_name] = 1
    with pytest.raises(ValueError) as exec_info:
        dataset.write(df)

    assert f"{column_name} is not alphanum or underscore!" in str(exec_info.value)


@pytest.mark.spark
@pytest.mark.parametrize("mode", [Mode.READ])
def test_write_on_read_only_spark_data_frame(dataset: HiveDataset, df: pd.DataFrame):
    sdf: SparkDataFrame = ps.from_pandas(df).to_spark()
    with pytest.raises(InvalidOperationException):
        dataset.write(sdf)


@pytest.mark.spark
@pytest.mark.parametrize("mode", [Mode.WRITE])
def test_read_on_write_only_spark(dataset: HiveDataset, df):
    df: SparkDataFrame = ps.from_pandas(df).to_spark()
    dataset.write(df)
    with pytest.raises(InvalidOperationException):
        dataset.to_spark(columns="col1")


@pytest.mark.parametrize("partition_by", ["col1,col3,run_id"])
@pytest.mark.parametrize("hive_table", ["my_hive_table_spark_pandas"])
@pytest.mark.spark
def test_hive_default_plugin_spark_pandas(dataset: HiveDataset, df: pd.DataFrame, run_id: str, spark_session):
    dataset.write(ps.from_pandas(df))
    read_psdf: ps.DataFrame = dataset.to_spark_pandas(partitions=dict(run_id=run_id))
    assert isinstance(read_psdf, ps.DataFrame)
    read_df = read_psdf.to_pandas()
    del read_df["run_id"]
    del read_df["run_time"]
    assert_frame_equal(df.set_index("col2"), read_df.set_index("col2"), check_like=True)


@pytest.mark.parametrize("mode", [Mode.READ])
@pytest.mark.spark
def test_write_on_read_only_spark_pandas(dataset: HiveDataset):
    df = pd.DataFrame({"col1": ["A", "A", "A", "B", "B", "B"], "col2": [1, 2, 3, 4, 5, 6]})
    with pytest.raises(InvalidOperationException):
        dataset.write(ps.from_pandas(df))


i = 0


def test_retry_with_back_off():
    def happy_path():
        return ":)"

    assert _retry_with_backoff(happy_path) == ":)"

    def semi_happy_path():
        global i
        i += 1
        if i < 2:
            raise Exception(":|")
        else:
            print(f"...: {i=}")
            return ":|"

    assert _retry_with_backoff(semi_happy_path, backoff_in_seconds=0) == ":|"
    print(f"{i=}")
    assert i == 2

    def un_happy_path():
        raise Exception(":(")

    with pytest.raises(Exception) as exec_info:
        _retry_with_backoff(un_happy_path, retries=1, backoff_in_seconds=0)

    assert ":(" in str(exec_info.value)
