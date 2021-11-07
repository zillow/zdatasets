from pathlib import Path
from typing import TYPE_CHECKING, Callable, Iterable, List, Optional, Tuple, Union

import pandas
import pandas as pd

from datasets import Mode
from datasets._typing import ColumnNames
from datasets.context import Context
from datasets.data_container_type import DataContainerType
from datasets.dataset_plugin import DatasetPlugin
from datasets.exceptions import InvalidOperationException
from datasets.utils import _pascal_to_snake_case
from datasets.plugins.batch.config import BATCH_DEFAULT_CONTAINER


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

    def read(
        self, columns: Optional[str] = None, run_id: Optional[str] = None, **kwargs
    ) -> Union[pd.DataFrame, "ps.DataFrame", "dd.DataFrame"]:
        if DataContainerType.SPARK_PANDAS_OR_PANDAS == BATCH_DEFAULT_CONTAINER:
            try:
                return self.read_spark_pandas(colums=columns, run_id=run_id, **kwargs)
            except ImportError:
                return self.read_pandas(colums=columns, run_id=run_id, **kwargs)
        elif DataContainerType.SPARK_PANDAS == BATCH_DEFAULT_CONTAINER:
            return self.read_spark_pandas(colums=columns, run_id=run_id, **kwargs)
        elif DataContainerType.PANDAS == BATCH_DEFAULT_CONTAINER:
            return self.read_pandas(colums=columns, run_id=run_id, **kwargs)
        elif DataContainerType.DASK == BATCH_DEFAULT_CONTAINER:
            return self.read_dask(colums=columns, run_id=run_id, **kwargs)
        else:
            raise ValueError(f"{BATCH_DEFAULT_CONTAINER} does not exist")

    def read_spark_pandas(
        self,
        columns: Optional[str] = None,
        run_id: Optional[str] = None,
        conf: Optional["SparkConf"] = None,
        **kwargs,
    ) -> "ps.DataFrame":
        sdf: SparkDataFrame = self.read_spark(columns=columns, run_id=run_id, conf=conf, **kwargs)
        psdf: ps.DataFrame = sdf.to_pandas_on_spark(index_col=kwargs.get("index_col", None))
        return psdf

    def read_pandas(
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
            df = pandas.read_csv(path, columns=read_columns, filters=filters, **kwargs)
        else:
            raise ValueError(f"{storage_format=} not supported.")

        for meta_column in self._META_COLUMNS:
            if meta_column in df and (read_columns is None or meta_column not in read_columns):
                del df[meta_column]
        return df

    def read_dask(
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

    def read_spark(
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

        df: DataFrame = spark_session.read.load(path, format=storage_format, **kwargs).select(
            *read_columns
        )
        for name, op, val in filters:
            df = df.where(df[name] == val)

        for meta_column in self._META_COLUMNS:
            if meta_column in df.columns and (
                read_columns is None or meta_column not in read_columns
            ):
                df = df.drop(meta_column)
        return df

    def write(
        self, data: Union[pd.DataFrame, "ps.DataFrame", "SparkDataFrame", "dd.DataFrame"], **kwargs
    ):
        # TODO: what should we do if the type doesn't match with BATCH_DEFAULT_CONTAINER?
        #   -  Should we force writes through the BATCH_DEFAULT_CONTAINER??
        if isinstance(data, pd.DataFrame):
            if DataContainerType.SPARK_PANDAS == BATCH_DEFAULT_CONTAINER:
                from pyspark import pandas as ps

                print(
                    "<< Converting the Pandas DataFrame to Spark DataFrame "
                    "then write using Spark >>"
                )
                psdf: ps.DataFrame = ps.from_pandas(data)
                return self.write_spark_pandas(psdf, **kwargs)
            else:
                return self.write_pandas(data, **kwargs)
        elif "pyspark.pandas.frame.DataFrame" in str(type(data)):
            return self.write_spark_pandas(data, **kwargs)
        elif "pyspark.sql.dataframe.DataFrame" in str(type(data)):
            return self.write_spark(data, **kwargs)
        elif "dask.dataframe.core.DataFrame" in str(type(data)):
            # TODO: what do we do with Dask BATCH_DEFAULT_CONTAINER == SPARK_PANDAS?
            #   Is the onus on the Dask user?
            return self.write_dask(data, **kwargs)
        else:
            raise ValueError(
                f"data is of unsupported type {type(data)=}."
                " Or PySpark or Dask is not installed."
            )

    def write_dask(self, df: "dd.DataFrame", **kwargs):
        import dask.dataframe as dd

        partition_cols = self._write_data_frame_prep(df)
        dd.to_parquet(
            df,
            self.dataset_path,
            engine=kwargs.get("engine", "pyarrow"),
            compression=kwargs.get("compression", "snappy"),
            partition_on=partition_cols,
            write_index=kwargs.get("write_index", False),
            write_metadata_file=kwargs.get("write_metadata_file", False),
            **kwargs,
        )

    def write_pandas(self, df: pd.DataFrame, **kwargs):
        partition_cols = self._write_data_frame_prep(df)
        df.to_parquet(
            self._get_dataset_path(),
            engine=kwargs.get("engine", "pyarrow"),
            compression=kwargs.get("compression", "snappy"),
            partition_cols=partition_cols,
            index=kwargs.get("index", False),
            **kwargs,
        )

    def _write_data_frame_prep(self, df: Union[pd.DataFrame, "dd.DataFrame"]) -> List[str]:
        if not (self.mode & Mode.WRITE):
            raise InvalidOperationException(f"Cannot write because mode={self.mode}")

        partition_cols: List[str] = []
        if self.partition_by:
            if isinstance(self.partition_by, str):
                partition_cols = self.partition_by.split(",")
            else:
                partition_cols = self.partition_by
        else:
            partition_cols = list()

        if self.path is None or "run_id" in partition_cols:
            # Only partition on run_id if @dataset(path="s3://..") is not given
            # or run_id is in partition_cols
            if "run_id" not in partition_cols:
                partition_cols.append("run_id")
            self.run_id = self._executor.current_run_id  # DO NOT ALLOW OVERWRITE OF ANOTHER RUN ID
            # TODO: should I add run_time for latest run query scenario?
            try:
                from pyspark.sql import DataFrame as SparkDataFrame
                from pyspark.sql.functions import lit

                if isinstance(df, SparkDataFrame):
                    df = df.withColumn("run_id", lit(self.run_id))
                else:
                    df["run_id"] = self.run_id
            except ImportError:
                df["run_id"] = self.run_id
        return partition_cols

    def write_spark_pandas(self, df: "ps.DataFrame", **kwargs):
        self.write_spark(df.to_spark(index_col=kwargs.get("index_col", None)), **kwargs)

    def write_spark(self, df: "SparkDataFrame", **kwargs):
        from pyspark.sql.functions import lit

        if not (self.mode & Mode.WRITE):
            raise InvalidOperationException(f"Cannot write because mode={self.mode}")

        if self.partition_by:
            partition_cols = self.partition_by.split(",")
        else:
            partition_cols = list()

        if self.path is None or "run_id" in partition_cols:
            # Only partition on run_id if @dataset(path="s3://..") is not given
            # or run_id is in partition_cols
            if "run_id" not in partition_cols:
                partition_cols.append("run_id")
            self.run_id = self._executor.current_run_id  # DO NOT ALLOW OVERWRITE OF ANOTHER RUN ID
            df = df.withColumn("run_id", lit(self.run_id))

        df.write.options(**kwargs).parquet(
            path=self._get_dataset_path(),
            mode=kwargs.get("mode", "overwrite"),  # TODO: should this be the default policy??
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
