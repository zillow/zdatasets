from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Callable,
    Iterable,
    List,
    Optional,
    Tuple,
    Union,
)

import pandas
import pandas as pd

from datasets import Mode
from datasets._typing import ColumnNames
from datasets.context import Context
from datasets.dataset_plugin import DatasetPlugin
from datasets.exceptions import InvalidOperationException
from datasets.utils import _pascal_to_snake_case


if TYPE_CHECKING:
    import dask.dataframe as dd
    from pyspark import SparkConf, pandas as ps
    from pyspark.sql import DataFrame as SparkDataFrame


@DatasetPlugin.register(constructor_keys={"name"}, context=Context.BATCH)
class BatchDatasetPlugin(DatasetPlugin):
    """
    The default plugin for the BATCH execution context.
    """

    _dataset_path_func: Callable = None

    def __init__(
        self,
        name: str,
        logical_key: str = None,
        columns: Optional[ColumnNames] = None,
        run_id: Optional[str] = None,
        mode: Mode = Mode.READ,
        partition_by: Optional[ColumnNames] = None,
        path: Optional[str] = None,
    ):
        self.path = path
        self.partition_by = partition_by
        self.program_name = self._executor.current_program_name
        super(BatchDatasetPlugin, self).__init__(
            name=name,
            logical_key=logical_key,
            columns=columns,
            run_id=run_id,
            mode=mode,
        )
        self._table_name = _pascal_to_snake_case(name)

    def _get_path_filters_columns(
        self, columns: Optional[ColumnNames] = None, run_id: Optional[str] = None
    ) -> Tuple[str, Optional[list], Optional[Iterable[str]]]:
        path = self._get_dataset_path()
        read_columns = self._get_read_columns(columns)
        filters = None
        query_run_id = run_id if run_id else self.run_id
        if query_run_id:
            filters = [("run_id", "=", query_run_id)]
        return path, filters, read_columns

    def to_spark_pandas(
        self,
        columns: Optional[str] = None,
        run_id: Optional[str] = None,
        conf: Optional["SparkConf"] = None,
        **kwargs,
    ) -> "ps.DataFrame":
        sdf: SparkDataFrame = self.to_spark(columns=columns, run_id=run_id, conf=conf, **kwargs)
        psdf: ps.DataFrame = sdf.to_pandas_on_spark(index_col=kwargs.get("index_col", None))
        return psdf

    def to_pandas(
        self,
        columns: Optional[str] = None,
        run_id: Optional[str] = None,
        storage_format: str = "parquet",
        **kwargs,
    ) -> pd.DataFrame:
        if not (self.mode & Mode.READ):
            raise InvalidOperationException(f"Cannot read because mode={self.mode}")

        path, filters, read_columns = self._get_path_filters_columns(columns, run_id=run_id)

        df: pd.DataFrame
        if storage_format == "parquet":
            df = pandas.read_parquet(
                path,
                columns=read_columns,
                filters=filters,
                engine=kwargs.get("engine", "pyarrow"),
                **kwargs,
            )
        elif storage_format == "csv":
            df = pandas.read_csv(path, usecols=read_columns, **kwargs)
        else:
            raise ValueError(f"{storage_format=} not supported.")

        for meta_column in self._META_COLUMNS:
            if meta_column in df and (read_columns is None or meta_column not in read_columns):
                del df[meta_column]
        return df

    def to_dask(
        self, columns: Optional[str] = None, run_id: Optional[str] = None, **kwargs
    ) -> "dd.DataFrame":
        if not (self.mode & Mode.READ):
            raise InvalidOperationException(f"Cannot read because mode={self.mode}")

        import dask.dataframe as dd

        path, filters, read_columns = self._get_path_filters_columns(columns, run_id)
        return dd.read_parquet(
            path,
            columns=read_columns,
            filters=filters,
            engine=kwargs.get("engine", "pyarrow"),
            **kwargs,
        )

    def to_spark(
        self,
        columns: Optional[str] = None,
        run_id: Optional[str] = None,
        conf: Optional["SparkConf"] = None,
        storage_format: str = "parquet",
        **kwargs,
    ) -> "SparkDataFrame":
        if not (self.mode & Mode.READ):
            raise InvalidOperationException(f"Cannot read because mode={self.mode}")

        from pyspark import SparkConf
        from pyspark.sql import DataFrame, SparkSession

        path, filters, read_columns = self._get_path_filters_columns(columns, run_id)

        read_columns = read_columns if read_columns else ["*"]
        if (self.run_id or run_id) and "*" not in read_columns and "run_id" not in read_columns:
            read_columns.append("run_id")

        if conf is None:
            conf = SparkConf()
        spark_session: SparkSession = SparkSession.builder.config(conf=conf).getOrCreate()

        df: DataFrame = spark_session.read.load(path, format=storage_format, **kwargs).select(*read_columns)
        for name, op, val in filters:
            df = df.where(df[name] == val)

        for meta_column in self._META_COLUMNS:
            if meta_column in df.columns and (read_columns is None or meta_column not in read_columns):
                df = df.drop(meta_column)
        return df

    def write(self, data: Union[pd.DataFrame, "ps.DataFrame", "SparkDataFrame", "dd.DataFrame"], **kwargs):
        if isinstance(data, pd.DataFrame):
            return self.write_pandas(data, **kwargs)
        elif "pyspark.pandas.frame.DataFrame" in str(type(data)):
            return self.write_spark_pandas(data, **kwargs)
        elif "pyspark.sql.dataframe.DataFrame" in str(type(data)):
            return self.write_spark(data, **kwargs)
        elif "dask.dataframe.core.DataFrame" in str(type(data)):
            return self.write_dask(data, **kwargs)
        else:
            raise ValueError(
                f"data is of unsupported type {type(data)=}." " Or PySpark or Dask is not installed."
            )

    def _partition_by_to_list(self, partition_by: Optional[ColumnNames] = None) -> List[str]:
        def to_list(partitions: Optional[ColumnNames]) -> List[str]:
            if partitions:
                if isinstance(partitions, str):
                    return partitions.split(",")
                else:
                    return partitions
            else:
                return list()

        return to_list(partition_by) if partition_by else to_list(self.partition_by)

    def _write_data_frame_prep(
        self, df: Union[pd.DataFrame, "dd.DataFrame"], partition_by: Optional[ColumnNames] = None
    ) -> List[str]:
        if not (self.mode & Mode.WRITE):
            raise InvalidOperationException(f"Cannot write because mode={self.mode}")

        partition_cols: List[str] = self._partition_by_to_list(partition_by)

        if self.path is None or "run_id" in partition_cols:
            # Only partition on run_id if @dataset(path="s3://..") is not given
            # or run_id is in partition_cols
            if "run_id" not in partition_cols:
                partition_cols.append("run_id")
            self.run_id = self._executor.current_run_id  # DO NOT ALLOW OVERWRITE OF ANOTHER RUN ID
            # TODO: should I add run_time for latest run query scenario?
            df["run_id"] = self.run_id
        return partition_cols

    def write_dask(self, df: "dd.DataFrame", partition_by: Optional[ColumnNames] = None, **kwargs):
        import dask.dataframe as dd

        partition_cols = self._write_data_frame_prep(df, partition_by=partition_by)
        print(f"{self._get_dataset_path()=}")
        print(f"{kwargs=}")
        dd.to_parquet(
            df,
            self._get_dataset_path(),
            engine=kwargs.get("engine", "pyarrow"),
            compression=kwargs.get("compression", "snappy"),
            partition_on=partition_cols,
            write_index=kwargs.get("write_index", False),
            write_metadata_file=kwargs.get("write_metadata_file", False),
            **kwargs,
        )

    def write_pandas(self, df: pd.DataFrame, partition_by: Optional[ColumnNames] = None, **kwargs):
        partition_cols = self._write_data_frame_prep(df, partition_by=partition_by)
        df.to_parquet(
            self._get_dataset_path(),
            engine=kwargs.get("engine", "pyarrow"),
            compression=kwargs.get("compression", "snappy"),
            partition_cols=partition_cols,
            index=kwargs.get("index", False),
            **kwargs,
        )

    def write_spark_pandas(self, df: "ps.DataFrame", partition_by: Optional[ColumnNames] = None, **kwargs):
        self.write_spark(
            df.to_spark(index_col=kwargs.get("index_col", None)),
            partition_by=partition_by,
            **kwargs,
        )

    def write_spark(self, df: "SparkDataFrame", partition_by: Optional[ColumnNames] = None, **kwargs):
        from pyspark.sql.functions import lit

        if not (self.mode & Mode.WRITE):
            raise InvalidOperationException(f"Cannot write because mode={self.mode}")

        partition_cols: List[str] = self._partition_by_to_list(partition_by)

        if self.path is None or "run_id" in partition_cols:
            # Only partition on run_id if @dataset(path="s3://..") is not given
            # or run_id is in partition_cols
            if "run_id" not in partition_cols:
                partition_cols.append("run_id")
            self.run_id = self._executor.current_run_id  # DO NOT ALLOW OVERWRITE OF ANOTHER RUN ID
            df = df.withColumn("run_id", lit(self.run_id))

        # TODO: should mode=overwrite be the default policy??
        df.write.options(**kwargs).mode(kwargs.get("mode", "overwrite")).parquet(
            path=self._get_dataset_path(),
            partitionBy=partition_cols,
            compression=kwargs.get("compression", "snappy"),
        )

    @classmethod
    def _register_dataset_path_func(cls, func: Callable):
        cls._dataset_path_func = func

    def _get_dataset_path(self) -> str:
        if self.path is not None:
            return self.path
        else:
            if BatchDatasetPlugin._dataset_path_func:
                return BatchDatasetPlugin._dataset_path_func(self)
            else:
                return str(
                    Path(self._executor.datastore_path)
                    / "datastore"
                    / (self.program_name if self.program_name else self._executor.current_program_name)
                    / self._table_name
                )

    def __repr__(self):
        return (
            f"BatchDatasetPlugin({self.name=},{self.key=},{self.partition_by=},"
            f"{self.run_id=},{self.columns=},"
            f"dataset_path={self._get_dataset_path()},{self._table_name=})"
        )
