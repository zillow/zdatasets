import logging
from typing import TYPE_CHECKING, List, Optional, Tuple, Union

import pandas as pd

from datasets import Mode
from datasets._typing import ColumnNames, DataFrameType
from datasets.context import Context
from datasets.dataset_plugin import DatasetPlugin
from datasets.exceptions import InvalidOperationException
from datasets.plugins.batch.batch_base_plugin import (
    BatchBasePlugin,
    BatchOptions,
)


_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)

if TYPE_CHECKING:
    import dask.dataframe as dd
    from pyspark import SparkConf, pandas as ps
    from pyspark.sql import DataFrame as SparkDataFrame


@DatasetPlugin.register(context=Context.BATCH, options_type=BatchOptions)
class BatchDataset(BatchBasePlugin):
    """
    The default plugin for the BATCH execution context.
    """

    def __init__(
        self,
        name: str,
        logical_key: str = None,
        columns: Optional[ColumnNames] = None,
        run_id: Optional[str] = None,
        run_time: Optional[int] = None,
        mode: Mode = Mode.READ,
        options: Optional[BatchOptions] = BatchOptions(),
    ):
        DatasetPlugin._dataset_name_validator(name)
        super(BatchDataset, self).__init__(
            name=name,
            logical_key=logical_key,
            columns=columns,
            run_id=run_id,
            run_time=run_time,
            mode=mode,
            options=options,
        )
        self.path = self._path = None
        if options and isinstance(options, BatchOptions):
            self.path = options.path
            self._path: Optional[str] = options.path
            if options.path:
                self["path"] = options.path

    def to_spark_pandas(
        self,
        columns: Optional[str] = None,
        run_id: Optional[str] = None,
        run_time: Optional[int] = None,
        conf: Optional["SparkConf"] = None,
        partitions: Optional[dict] = None,
        **kwargs,
    ) -> "ps.DataFrame":
        return self.to_spark(
            columns=columns, run_id=run_id, run_time=run_time, conf=conf, partitions=partitions, **kwargs
        ).to_pandas_on_spark(index_col=kwargs.get("index_col", None))

    def to_pandas(
        self,
        columns: Optional[str] = None,
        run_id: Optional[str] = None,
        run_time: Optional[int] = None,
        storage_format: str = "parquet",
        partitions: Optional[dict] = None,
        **kwargs,
    ) -> pd.DataFrame:
        if not (self.mode & Mode.READ):
            raise InvalidOperationException(f"Cannot read because mode={self.mode}")

        filters, read_columns = self._get_filters_columns(columns, run_id, run_time, partitions)
        self._path = self._get_dataset_path()
        _logger.info(
            f"to_pandas({self._path=},{read_columns=},{partitions=},{run_id=},{run_time=},{filters=})"
        )

        df: pd.DataFrame
        if storage_format == "parquet":
            df = pd.read_parquet(
                self._path,
                columns=read_columns,
                filters=filters,
                engine=kwargs.get("engine", "pyarrow"),
                **kwargs,
            )
        elif storage_format == "csv":
            df = pd.read_csv(self._path, usecols=read_columns, **kwargs)
        else:
            raise ValueError(f"{storage_format=} not supported.")

        for meta_column in self._META_COLUMNS:
            if meta_column in df and (read_columns is None or meta_column not in read_columns):
                del df[meta_column]
        return df

    def to_dask(
        self,
        columns: Optional[str] = None,
        run_id: Optional[str] = None,
        run_time: Optional[int] = None,
        partitions: Optional[dict] = None,
        **kwargs,
    ) -> "dd.DataFrame":
        if not (self.mode & Mode.READ):
            raise InvalidOperationException(f"Cannot read because mode={self.mode}")

        import dask.dataframe as dd

        filters, read_columns = self._get_filters_columns(columns, run_id, run_time, partitions)
        self._path = self._get_dataset_path()
        _logger.info(f"to_dask({self._path=},{read_columns=},{partitions=},{run_id=},{run_time=},{filters=})")
        return dd.read_parquet(
            self._path,
            columns=read_columns,
            filters=filters,
            engine=kwargs.get("engine", "pyarrow"),
            **kwargs,
        )

    def to_spark(
        self,
        columns: Optional[str] = None,
        run_id: Optional[str] = None,
        run_time: Optional[int] = None,
        conf: Optional["SparkConf"] = None,
        storage_format: str = "parquet",
        partitions: Optional[dict] = None,
        **kwargs,
    ) -> "SparkDataFrame":
        if not self.mode & Mode.READ:
            raise InvalidOperationException(f"Cannot read because mode={self.mode}")

        from pyspark.sql import DataFrame, SparkSession

        filters, read_columns = self._get_filters_columns(columns, run_id, run_time, partitions)
        self._path = self._get_dataset_path()

        read_columns = read_columns if read_columns else ["*"]
        if (self.run_id or run_id) and "*" not in read_columns and "run_id" not in read_columns:
            read_columns.append("run_id")

        if (self.run_time or run_time) and "*" not in read_columns and "run_time" not in read_columns:
            read_columns.append("run_time")

        spark_session: SparkSession = BatchBasePlugin._get_spark_builder(conf).getOrCreate()

        _logger.info(f"to_spark({self._path=}, {read_columns=}, {partitions=}, {run_id=}, {filters=})")
        df: DataFrame = spark_session.read.load(self._path, format=storage_format, **kwargs).select(
            *read_columns
        )

        if filters:
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
                f"data is of unsupported type {type(data)=}. Maybe PySpark/Dask is not installed?"
            )

    def _path_write_data_frame_prep(
        self,
        df: DataFrameType,
        partition_by: Optional[ColumnNames] = None,
    ) -> Tuple[DataFrameType, List[str]]:
        partition_cols: List[str] = self._partition_by_to_list(partition_by)
        if self.path is None:
            # partition on run_id if Dataset(path="s3://..") is not given
            if "run_id" not in partition_cols:
                partition_cols.append("run_id")

            if "run_time" not in partition_cols:
                partition_cols.append("run_time")

        return self._write_data_frame_prep(df, partition_cols)

    def write_dask(self, df: "dd.DataFrame", partition_by: Optional[ColumnNames] = None, **kwargs):
        import dask.dataframe as dd

        df, partition_cols = self._path_write_data_frame_prep(df, partition_by=partition_by)
        self._path = self._get_dataset_path()
        _logger.info(f"write_dask({self._path=}, {partition_cols=})")
        dd.to_parquet(
            df,
            self._path,
            engine=kwargs.get("engine", "pyarrow"),
            compression=kwargs.get("compression", "snappy"),
            partition_on=partition_cols,
            write_index=kwargs.get("write_index", False),
            write_metadata_file=kwargs.get("write_metadata_file", False),
            **kwargs,
        )

    def write_pandas(self, df: pd.DataFrame, partition_by: Optional[ColumnNames] = None, **kwargs):
        df, partition_cols = self._path_write_data_frame_prep(df, partition_by=partition_by)
        self._path = self._get_dataset_path()
        _logger.info(f"write_pandas({self._path=}, {partition_cols=})")
        df.to_parquet(
            self._path,
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
        df, partition_cols = self._path_write_data_frame_prep(df, partition_by=partition_by)
        self._path = self._get_dataset_path()
        _logger.info(f"write_spark({self._path=}, {partition_cols=})")

        # TODO: should mode=overwrite be the default policy??
        df.write.options(**kwargs).mode(kwargs.get("mode", "overwrite")).parquet(
            path=self._path,
            partitionBy=partition_cols,
            compression=kwargs.get("compression", "snappy"),
        )

    def __repr__(self):
        return (
            f"BatchDataset({self.name=},{self.key=},{self.partition_by=},"
            f"{self.run_id=},{self.columns=},{self.mode=},{self.path=},{self._path=})"
        )
