from pathlib import Path
from typing import TYPE_CHECKING, Tuple

import pandas
import pandas as pd

from datasets import Mode
from datasets.dataset import Dataset


if TYPE_CHECKING:
    import dask.dataframe as dd
    import pyspark


class InvalidOperationException(Exception):
    pass


@Dataset.register_plugin(constructor_keys={"name"})
class OfflineDataset(Dataset):
    _dataset_path_func: callable = None

    def __init__(
        self,
        name: str,
        logical_key: str = None,
        columns=None,
        run_id=None,
        mode: Mode = Mode.Read,
        path: str = None,
        partition_by: str = None,
        attribute_name: str = None,
    ):
        self.path = path
        self.partition_by = partition_by
        super(OfflineDataset, self).__init__(
            name=name,
            logical_key=logical_key,
            columns=columns,
            run_id=run_id,
            mode=mode,
            attribute_name=attribute_name,
        )

    def _get_path_filters_columns(self, columns) -> Tuple[str, list, list[str]]:
        path = self._get_dataset_path()
        read_columns = self._get_read_columns(columns)
        filters = None
        if self.run_id:
            filters = [("run_id", "=", self.run_id)]
        return path, filters, read_columns

    def read_pandas(self, columns: str = None, **kwargs) -> pd.DataFrame:
        path, filters, read_columns = self._get_path_filters_columns(columns)
        df: pd.DataFrame = pandas.read_parquet(
            path, columns=read_columns, engine="pyarrow", filters=filters, **kwargs
        )

        for meta_column in self._META_COLUMNS:
            if meta_column in df and (read_columns is None or meta_column not in read_columns):
                del df[meta_column]
        return df

    def write(self, data: pd.DataFrame, **kwargs):
        if not self.mode == Mode.Write:
            raise InvalidOperationException(f"Cannot write because mode={self.mode}")

        if not isinstance(data, pd.DataFrame):
            assert ValueError("data is not a pandas DataFrame")

        if self.partition_by:
            partition_cols = self.partition_by.split(",")
        else:
            partition_cols = list()

        if self.path is None or "run_id" in partition_cols:
            # Only partition on run_id if @dataset(path="s3://..") is given
            # or run_id is in partition_cols
            if "run_id" not in partition_cols:
                partition_cols.append("run_id")
            if self.run_id is None:
                self.run_id = self._executor.current_run_id
            data["run_id"] = self.run_id

        if not self.program_name:
            self.program_name = self._executor.current_program_name

        data.to_parquet(
            self._get_dataset_path(),
            engine="pyarrow",
            compression="snappy",
            index=False,
            partition_cols=partition_cols,
            **kwargs,
        )

    def read_dask(self, columns=None, **kwargs) -> "dd.DataFrame":
        import dask.dataframe as dd

        path, filters, read_columns = self._get_path_filters_columns(columns)
        print(f"{filters=}")
        return dd.read_parquet(
            path,
            columns=read_columns,
            filters=filters if filters and len(filters) else None,
            engine="pyarrow",
            **kwargs,
        )

    def read_spark(self, columns=None, conf=None, **kwargs) -> "pyspark.sql.DataFrame":
        from pyspark import SparkConf
        from pyspark.sql import DataFrame, SparkSession

        path, _, read_columns = self._get_path_filters_columns(columns)

        read_columns = read_columns if read_columns else ["*"]
        if self.run_id:
            read_columns.append("run_id")

        if conf is None:
            conf = SparkConf()
        spark_session: SparkSession = SparkSession.builder.config(conf=conf).getOrCreate()

        df: DataFrame = spark_session.read.parquet(path).select(*read_columns)
        if self.run_id:
            df = df.where(df["run_id"] == self.run_id)
        return df

    @classmethod
    def _register_dataset_path_func(cls, func: callable):
        cls._dataset_path_func = func

    def _get_dataset_path(self) -> str:
        if self.path is not None:
            return self.path
        else:
            if OfflineDataset._dataset_path_func:
                return OfflineDataset._dataset_path_func(self)
            else:
                return str(Path(self._executor.datastore_path) / "datastore" / self.program_name / self.name)

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return (
            f"Dataset(name={self.name}, key={self.key}, partition_by={self.partition_by}, "
            f"columns={self.columns}, dataset_path={self._get_dataset_path()})"
        )
