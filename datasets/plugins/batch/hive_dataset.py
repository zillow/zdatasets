import logging
import random
import time
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Callable, List, Optional, Union

import pandas as pd

from datasets._typing import ColumnNames
from datasets.context import Context
from datasets.dataset_plugin import DatasetPlugin
from datasets.exceptions import InvalidOperationException
from datasets.mode import Mode
from datasets.plugins.batch.batch_base_plugin import (
    BatchBasePlugin,
    BatchOptions,
)
from datasets.utils.case_utils import (
    is_upper_pascal_case,
    snake_case_to_pascal,
)


_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)

if TYPE_CHECKING:
    from pyspark import SparkConf, pandas as ps
    from pyspark.sql import DataFrame as SparkDataFrame, SparkSession


@dataclass
class HiveOptions(BatchOptions):
    pass


@DatasetPlugin.register(context=Context.BATCH, options_type=HiveOptions, as_default_context_plugin=True)
class HiveDataset(BatchBasePlugin):
    def __init__(
        self,
        name: Optional[str] = None,
        logical_key: Optional[str] = None,
        columns: Optional[ColumnNames] = None,
        run_id: Optional[str] = None,
        run_time: Optional[int] = None,
        mode: Mode = Mode.READ,
        options: Optional[HiveOptions] = None,
    ):
        if options and options.hive_table_name is None and not is_upper_pascal_case(name):
            raise ValueError(f"{name=} is not upper pascal case.")

        super(HiveDataset, self).__init__(
            name=name if name else snake_case_to_pascal(options.hive_table_name),
            logical_key=logical_key,
            columns=columns,
            run_id=run_id,
            run_time=run_time,
            mode=mode,
            options=options,
        )

    def to_pandas(
        self,
        columns: Optional[str] = None,
        run_id: Optional[str] = None,
        run_time: Optional[int] = None,
        conf: Optional["SparkConf"] = None,
        partitions: Optional[dict] = None,
        **kwargs,
    ) -> "ps.DataFrame":
        return self.to_spark_pandas(
            columns=columns, run_id=run_id, run_time=run_time, conf=conf, partitions=partitions, **kwargs
        )

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

    def to_spark(
        self,
        columns: Optional[str] = None,
        run_id: Optional[str] = None,
        run_time: Optional[int] = None,
        conf: Optional["SparkConf"] = None,
        partitions: Optional[dict] = None,
        **kwargs,
    ) -> "SparkDataFrame":
        if not self.mode & Mode.READ:
            raise InvalidOperationException(f"Cannot read because mode={self.mode}")

        from pyspark.sql import DataFrame, SparkSession

        filters, read_columns = self._get_filters_columns(columns, run_id, run_time, partitions)

        read_columns = read_columns if read_columns else ["*"]
        if (self.run_id or run_id) and "*" not in read_columns and "run_id" not in read_columns:
            read_columns.append("run_id")

        if (self.run_time or run_time) and "*" not in read_columns and "run_time" not in read_columns:
            read_columns.append("run_time")

        spark: SparkSession = BatchBasePlugin._get_spark_builder(conf).enableHiveSupport().getOrCreate()

        _logger.info(
            f"to_spark({self.hive_table_name=},{read_columns=},{partitions=},{run_id=},{run_time=},{filters=})"
        )
        df: DataFrame = spark.read.options(**kwargs).table(self.hive_table_name).select(*read_columns)

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

        spark: SparkSession = BatchBasePlugin._get_spark_builder(conf).enableHiveSupport().getOrCreate()
        table_exists: bool = spark.sql(f"SHOW TABLES LIKE '{self.hive_table_name}'").count() == 1
        tmp_partition_by: Optional[ColumnNames] = None
        if table_exists:
            # Query for partition information,
            # for self._write_data_frame_prep() to add run_id,run_time if needed
            from pyspark import Row

            desc_df: SparkDataFrame = spark.sql(f"desc {self.hive_table_name}")
            partition_list: List[Row] = desc_df.select(desc_df.col_name).collect()

            partition_index: Optional[int] = None
            for index, item in enumerate(partition_list):
                if item[0] == "# Partition Information":
                    partition_index = index + 2
                    break
            if partition_index:
                tmp_partition_by = [row[0] for row in partition_list[partition_index:]]
            else:
                tmp_partition_by = None
        else:
            # add run_id column by default
            tmp_partition_by = self._partition_by_to_list(partition_by)
            if "run_id" not in tmp_partition_by:
                tmp_partition_by += ["run_id"]

            # add run_time column by default
            if "run_time" not in tmp_partition_by:
                tmp_partition_by += ["run_time"]

        df, partition_cols = self._write_data_frame_prep(df, partition_by=tmp_partition_by)

        _logger.info(f"write_spark({self.hive_table_name=}, {partition_cols=})")
        # TODO(talebz): This retry is especially for the race condition when another parallel SparkJob
        #   FileOutputCommitter removes the _temporary and this one gets /_temporary/0 not found.
        _retry_with_backoff(partial(self._write_spark_helper, spark, df, partition_cols, **kwargs))

    def _write_spark_helper(
        self, spark: "SparkSession", df: "SparkDataFrame", partition_cols: List[str], **kwargs
    ):
        # About <.mode("append")>:
        #  - SaveMode.Append	"append":
        #      When saving a DataFrame to a data source, if data/table already exists,
        #      contents of the DataFrame are expected to be appended to existing data.
        #  - SaveMode.Overwrite	"overwrite"
        #      Overwrite mode means that when saving a DataFrame to a data source,
        #      if data/table already exists, existing data is expected to be overwritten
        #      by the contents of the DataFrame.
        table_exists: bool = spark.sql(f"SHOW TABLES LIKE '{self.hive_table_name}'").count() == 1
        if table_exists:
            (
                df.select(spark.table(self.hive_table_name).columns)
                .write.options(**kwargs)
                .insertInto(self.hive_table_name)
            )
        else:
            path = self._get_dataset_path()
            _logger.info(f"{self.hive_table_name=} does not exist: creating {path=}!")
            (
                df.write.mode("append")
                .option("path", self._get_dataset_path())
                .partitionBy(partition_cols)
                .options(**kwargs)
                .saveAsTable(self.hive_table_name)
            )

            create_view_query = f"""
            CREATE OR REPLACE VIEW {self.hive_table_name}_latest AS
            SELECT * FROM (
                SELECT s.*, RANK() OVER(ORDER BY run_time DESC) rank FROM {self.hive_table_name} s
            ) s
            WHERE rank=1
            """
            spark.sql(create_view_query)

    def __repr__(self):
        return (
            f"HiveDataset({self.hive_table_name=},{self.name=},{self.key=},{self.partition_by=},"
            f"{self.run_id=},{self.run_time=},{self.columns=},{self.mode=}"
        )


def _retry_with_backoff(func: Callable, retries=5, backoff_in_seconds=1):
    i = 0
    while True:
        try:
            return func()
        except Exception as e:
            if i >= retries:
                print(f"Failed after {retries} retries:")
                raise
            else:
                i += 1
                sleep = backoff_in_seconds * 2**i + random.uniform(0, 5)
                print(e)
                print(f"  Retry after {sleep} seconds")
                time.sleep(sleep)
