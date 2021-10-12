from __future__ import annotations

import datetime
import os
import time
from typing import TYPE_CHECKING, Any, List, Tuple

import dask.dataframe as dd
import pandas as pd
from metaflow import Flow
from metaflow.current import current
from metaflow.datastore import MetaflowDataStore

from .mode import Mode


if TYPE_CHECKING:
    import pyspark


_META_COLUMNS = ["run_id", "run_time"]


class Dataset:
    def __init__(
        self,
        name: str,
        flow_dataset: str = None,
        key: str = None,
        partition_by: str = None,
        path: str = None,
        mode: Mode = Mode.Read,
        columns=None,
    ):
        if flow_dataset:
            assert mode == mode.Read, "Cannot write to another Flow's dataset"
        self.name = name
        self.key = key
        self.partition_by = partition_by
        self.path = path
        self._run_id = self._run_time = None
        if flow_dataset:
            self.flow_name, dataset_name = flow_dataset.split(".")
            flow_dataset = getattr(Flow(self.flow_name).latest_successful_run.data, dataset_name)
            self.__dict__.update(flow_dataset.__dict__)
        else:
            self.flow_name = current.flow_name
        self.mode = mode
        self.columns = columns

    def read_dask(self, columns=None, type="parquet", **kwargs) -> dd.DataFrame:
        read_columns = self._get_read_columns(columns)
        path = self._get_dataset_path()
        filters: List[Tuple[str, str, Any]] = []
        if self._run_time:
            filters.append(("run_time", "=", self._run_time))
        if self._run_id:
            filters.append(("run_id", "=", self._run_id))
        print(f"read_dask {path=}")
        if type == "parquet":
            return dd.read_parquet(
                path,
                columns=read_columns,
                filters=filters if len(filters) else None,
                engine="pyarrow",
                **kwargs,
            )
        elif type == "csv":
            return dd.read_csv(
                path,
                columns=read_columns,
                filters=filters if len(filters) else None,
                engine="pyarrow",
                **kwargs,
            )

    def read_pandas(self, columns=None, type="parquet", **kwargs) -> pd.DataFrame:
        # pandas.io.parquet.read_parquet(
        #     self._get_dataset_path(),
        #     columns=columns,
        #     engine="pyarrow",
        #     filters=
        # )
        df: pd.DataFrame = self.read_dask(columns, type=type, **kwargs).compute()
        read_columns = self._get_read_columns(columns)
        if not (self._run_id is None or self._run_time is None):
            for meta_column in _META_COLUMNS:
                if meta_column in df and (read_columns is None or meta_column not in read_columns):
                    del df[meta_column]
        return df

    def read_spark(self, columns=None, conf=None, **kwargs) -> "pyspark.sql.DataFrame":
        from pyspark import SparkConf
        from pyspark.sql import DataFrame, SparkSession

        read_columns = self._get_read_columns(columns)
        read_columns = read_columns if read_columns else ["*"]
        if conf is None:
            conf = SparkConf()
        spark_session: SparkSession = SparkSession.builder.config(conf=conf).getOrCreate()

        df: DataFrame = spark_session.read.parquet(self._get_dataset_path()).select(*read_columns)
        if self._run_time:
            df = df.where(df["run_time"] == self._run_time)
        if self._run_id:
            df = df.where(df["run_id"] == self._run_id)
        return df

    def _get_read_columns(self, columns) -> List[str]:
        read_columns = columns if columns else self.columns
        if read_columns is not None and isinstance(read_columns, str):
            read_columns = read_columns.split(",")
        return read_columns

    def write(self, data, **kwargs):
        if not self.mode == Mode.Write:
            raise Exception(f"Cannot write because mode={self.mode}")

        partition_cols = self.partition_by.split(",") if self.partition_by else None

        if self.path is None:
            # Don't partition on run_id and run_time if @dataset(path="s3://..") is given
            partition_cols.extend(_META_COLUMNS)
            run_time = Dataset._get_run_time()
            self._run_id = current.run_id
            self._run_time = run_time

        if isinstance(data, pd.DataFrame):
            if self.path is None:
                # If a path is given, then we don't further partition by run_id and run_time.
                # The user may introduce such a partitioning themselves if they wish to.
                data["run_id"] = self._run_id
                data["run_time"] = self._run_time

            # data.to_parquet(
            #     self.dataset_path,
            #     engine="pyarrow",
            #     compression="snappy",
            #     index=False,
            #     partition_cols=partition_cols,
            # )

            dask_df = dd.from_pandas(data, sort=False, npartitions=1)
            dd.to_parquet(
                dask_df,
                self.dataset_path,
                engine="pyarrow",
                compression="snappy",
                partition_on=partition_cols,
                write_index=False,
                write_metadata_file=False,
                **kwargs,
            )

    @property
    def dataset_path(self):
        return self._get_dataset_path()

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return (
            f"Dataset(name={self.name}, key={self.key}, partition_by={self.partition_by}, "
            f"columns={self.columns}, dataset_path={self.dataset_path})"
        )

    @staticmethod
    def _get_run_time() -> str:
        return datetime.datetime.fromtimestamp(time.time()).strftime("%Y%m%d-%H%M%S")

    def _get_dataset_path(self) -> str:
        if self.path is not None:
            return self.path
        else:
            datastore: MetaflowDataStore = current.flow._datastore
            datastore_root: str = datastore.get_datastore_root_from_config(print)

            dataset_path = datastore_root
            dataset_path = os.path.join(dataset_path, "datastore")
            zodiac_service = os.environ.get("ZODIAC_SERVICE", None)
            if zodiac_service is not None:
                dataset_path = os.path.join(dataset_path, zodiac_service)
            flow_name = self.flow_name
            dataset_path = os.path.join(dataset_path, flow_name)
            dataset_path = os.path.join(dataset_path, self.name)
            return dataset_path
