import os
import uuid

import pandas as pd
import pytest

from datasets import Mode
from datasets.dataset import Dataset
from datasets.plugins.offline.offline_dataset import (
    InvalidOperationException,
    OfflineDataset,
)
from datasets.program_executor import ProgramExecutor


_run_id = str(uuid.uuid1())


class TestExecutor(ProgramExecutor):
    @property
    def run_id(self) -> str:
        return _run_id

    @property
    def datastore_path(self) -> str:
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), "data")

    @property
    def program_name(self) -> str:
        return "my_program"

    @property
    def context(self) -> str:
        return "offline"


Dataset.register_executor(executor=TestExecutor())


ds1_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data/ds1")


@pytest.fixture
def path():
    return ds1_path


@pytest.fixture
def mode():
    return Mode.Write


@pytest.fixture
def partition_by():
    return "col1,run_id"


@pytest.fixture
def name():
    return "ds1"


@pytest.fixture
def dataset(name, path, partition_by, mode):
    return Dataset.from_keys(name=name, key="my_key", path=path, partition_by=partition_by, mode=mode)


@pytest.fixture
def df():
    return pd.DataFrame({"col1": ["A", "A", "A", "B", "B", "B"], "col2": [1, 2, 3, 4, 5, 6]})


def test_from_keys_parquet_plugin(dataset: OfflineDataset):
    assert dataset.name == "ds1"
    assert dataset.key == "my_key"
    assert dataset.path == ds1_path
    assert dataset.partition_by == "col1,run_id"


def test_from_write(dataset: OfflineDataset, df: pd.DataFrame):
    dataset.write(df.copy())
    read_df = dataset.read_pandas(columns="col1,col2")
    assert (df == read_df).all().all()


@pytest.mark.depends(on=["test_from_write"])
def test_read_columns(dataset: OfflineDataset):
    df = dataset.read_pandas(columns="col1")
    assert df.columns.to_list() == ["col1"]


@pytest.mark.parametrize("path", [None])
@pytest.mark.parametrize("partition_by", ["col1"])
def test_get_dataset_path(dataset: OfflineDataset, df: pd.DataFrame):
    dataset.write(df.copy())
    path = dataset._get_dataset_path()
    assert path.endswith("datasets/tests/data/datastore/my_program/ds1")


@pytest.mark.parametrize("mode", [Mode.Read])
def test_write_on_read_only(dataset: OfflineDataset):
    df = pd.DataFrame({"col1": ["A", "A", "A", "B", "B", "B"], "col2": [1, 2, 3, 4, 5, 6]})
    with pytest.raises(InvalidOperationException):
        dataset.write(df.copy())


@pytest.mark.parametrize("path", [None])
def test_zillow_dataset_path_plugin(dataset: OfflineDataset, df: pd.DataFrame):
    # import registers it!
    from datasets.plugins.zillow.zillow_dataset_path import _get_dataset_path

    dataset.write(df.copy())

    OfflineDataset._register_dataset_path_func(_get_dataset_path)
    path = dataset._get_dataset_path()
    assert path.endswith("datasets/tests/data/datastore/my_program/ds1")

    os.environ["ZODIAC_SERVICE"] = "test_service"
    path = dataset._get_dataset_path()
    assert path.endswith("datasets/tests/data/datastore/test_service/my_program/ds1")


@pytest.mark.parametrize("path", [None])
@pytest.mark.parametrize("partition_by", ["col1"])
@pytest.mark.parametrize("name", ["ds_dask"])
def test_parquet_dask_dataset(dataset: OfflineDataset, df):
    dataset.write(df.copy())
    df = dataset.read_dask(columns="col1").compute()
    assert df.columns.to_list() == ["col1"]


@pytest.mark.spark
def test_parquet_spark_dataset(dataset: OfflineDataset, df):
    dataset.write(df.copy())
    spark_df = dataset.read_spark(columns="col1")
    spark_df.show()
    df = spark_df.toPandas()
    assert df.columns.to_list() == ["col1", "run_id"]  # run_id is added
    print(f"{df=}")
