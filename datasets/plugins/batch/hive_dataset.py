import logging
from typing import TYPE_CHECKING, List, Optional, Union

import pandas as pd

from datasets import Mode
from datasets._typing import ColumnNames
from datasets.context import Context
from datasets.dataset_plugin import DatasetPlugin
from datasets.exceptions import InvalidOperationException
from datasets.plugins.batch.batch_base_plugin import BatchBasePlugin
from datasets.utils.case_utils import (
    is_upper_pascal_case,
    pascal_to_snake_case,
    snake_case_to_pascal,
)


_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)

if TYPE_CHECKING:
    from pyspark import SparkConf, pandas as ps
    from pyspark.sql import DataFrame as SparkDataFrame, SparkSession


@DatasetPlugin.register(constructor_keys={"is_hive_table", "hive_table"}, context=Context.BATCH)
class HiveDataset(BatchBasePlugin):
    def __init__(
        self,
        name: Optional[str] = None,
        is_hive_table: Optional[bool] = None,
        hive_table: Optional[str] = None,
        logical_key: Optional[str] = None,
        columns: Optional[ColumnNames] = None,
        run_id: Optional[str] = None,
        mode: Mode = Mode.READ,
        partition_by: Optional[ColumnNames] = None,
    ):
        if is_hive_table and hive_table is None and not is_upper_pascal_case(name):
            raise ValueError(f"{name=} is not upper pascal case.")

        self.hive_table = hive_table if hive_table else pascal_to_snake_case(name)

        super(HiveDataset, self).__init__(
            name=name if name else snake_case_to_pascal(self.hive_table),
            hive_table_name=self.hive_table,
            logical_key=logical_key,
            columns=columns,
            run_id=run_id,
            mode=mode,
            partition_by=partition_by,
        )

    def to_spark_pandas(
        self,
        columns: Optional[str] = None,
        run_id: Optional[str] = None,
        conf: Optional["SparkConf"] = None,
        partitions: Optional[dict] = None,
        **kwargs,
    ) -> "ps.DataFrame":
        return self.to_spark(
            columns=columns, run_id=run_id, conf=conf, partitions=partitions, **kwargs
        ).to_pandas_on_spark(index_col=kwargs.get("index_col", None))

    def to_spark(
        self,
        columns: Optional[str] = None,
        run_id: Optional[str] = None,
        conf: Optional["SparkConf"] = None,
        partitions: Optional[dict] = None,
        **kwargs,
    ) -> "SparkDataFrame":
        if not self.mode & Mode.READ:
            raise InvalidOperationException(f"Cannot read because mode={self.mode}")

        from pyspark.sql import DataFrame, SparkSession

        filters, read_columns = self._get_filters_columns(columns, run_id, partitions)

        read_columns = read_columns if read_columns else ["*"]
        if (self.run_id or run_id) and "*" not in read_columns and "run_id" not in read_columns:
            read_columns.append("run_id")

        spark_session: SparkSession = HiveDataset._get_or_create_spark_session(conf)

        _logger.info(f"to_spark({self.hive_table=}, {read_columns=}, {partitions=}, {run_id=}, {filters=})")
        df: DataFrame = spark_session.read.options(**kwargs).table(self.hive_table).select(*read_columns)

        if filters:
            for name, _, val in filters:
                df = df.filter(df[name] == val)

        for meta_column in self._META_COLUMNS:
            if meta_column in df.columns and (read_columns is None or meta_column not in read_columns):
                df = df.drop(meta_column)
        return df

    def write(self, data: Union[pd.DataFrame, "ps.DataFrame", "SparkDataFrame"], **kwargs):
        if isinstance(data, pd.DataFrame):
            from pyspark import pandas as ps

            return self.write_spark_pandas(ps.from_pandas(data), **kwargs)
        elif "pyspark.pandas.frame.DataFrame" in str(type(data)):
            return self.write_spark_pandas(data, **kwargs)
        elif "pyspark.sql.dataframe.DataFrame" in str(type(data)):
            return self.write_spark(data, **kwargs)
        else:
            raise ValueError(
                f"data is of unsupported type {type(data)=}. Maybe PySpark/Dask is not installed?"
            )

    def write_spark_pandas(self, df: "ps.DataFrame", partition_by: Optional[ColumnNames] = None, **kwargs):
        self.write_spark(
            df.to_spark(index_col=kwargs.get("index_col", None)),
            partition_by=partition_by,
            **kwargs,
        )

    @staticmethod
    def _get_or_create_spark_session(conf: "Optional[SparkConf]" = None) -> "SparkSession":
        from pyspark import SparkConf
        from pyspark.sql import SparkSession

        if conf is None:
            conf = SparkConf()
        return SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

    @staticmethod
    def _validate_columns(df: "SparkDataFrame"):
        # column names are alphanumeric and underscore
        #  HIVE-10120 Disallow create table with dot/colon in column name
        #  https://stackoverflow.com/a/55337025
        for column_name in df.columns:
            if not all(c.isalnum() or c == "_" for c in column_name):
                raise ValueError(f"{column_name} is not alphanum or underscore!")

    def write_spark(
        self,
        df: "SparkDataFrame",
        partition_by: Optional[ColumnNames] = None,
        conf: Optional["SparkConf"] = None,
        **kwargs,
    ):
        HiveDataset._validate_columns(df)

        spark: SparkSession = HiveDataset._get_or_create_spark_session(conf)
        table_exists: bool = spark.sql(f"SHOW TABLES LIKE '{self.hive_table}'").count() == 1
        if table_exists:
            # Query for partition information,
            # for self._write_data_frame_prep() to add run_id if needed
            from pyspark import Row

            desc_df: SparkDataFrame = spark.sql(f"desc {self.hive_table}")
            partition_list: List[Row] = desc_df.select(desc_df.col_name).collect()

            partition_index: Optional[int] = None
            for index, item in enumerate(partition_list):
                if item[0] == "# Partition Information":
                    partition_index = index + 2
                    break
            if partition_index:
                partition_by = [row[0] for row in partition_list[partition_index:]]
            else:
                partition_by = None
        else:
            # add run_id column by default
            partition_cols: List[str] = self._partition_by_to_list(partition_by)
            if partition_cols is None:
                partition_by = ["run_id"]
            elif "run_id" not in partition_cols:
                partition_by = partition_cols + ["run_id"]

        df, partition_cols = self._write_data_frame_prep(df, partition_by=partition_by)
        _logger.info(f"write_spark({self.hive_table=}, {partition_cols=})")

        # About <.mode("append")>:
        #  - SaveMode.Append	"append":
        #      When saving a DataFrame to a data source, if data/table already exists,
        #      contents of the DataFrame are expected to be appended to existing data.
        #  - SaveMode.Overwrite	"overwrite"
        #      Overwrite mode means that when saving a DataFrame to a data source,
        #      if data/table already exists, existing data is expected to be overwritten
        #      by the contents of the DataFrame.
        if table_exists:
            (
                df.select(spark.table(self.hive_table).columns)
                .write.options(**kwargs)
                .insertInto(self.hive_table)
            )
        else:
            path = self._get_dataset_path()
            _logger.info(f"{self.hive_table=} does not exist: creating {path=}!")
            (
                df.write.mode("append")
                .option("path", self._get_dataset_path())
                .partitionBy(partition_cols)
                .options(**kwargs)
                .saveAsTable(self.hive_table)
            )

    def __repr__(self):
        return (
            f"HiveDataset({self.hive_table=},{self.name=},{self.key=},{self.partition_by=},"
            f"{self.run_id=},{self.columns=},{self.mode=}"
        )
