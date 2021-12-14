import logging
from typing import TYPE_CHECKING, Optional, Union

import pandas as pd

from datasets import Mode
from datasets._typing import ColumnNames
from datasets.context import Context
from datasets.dataset_plugin import DatasetPlugin
from datasets.exceptions import InvalidOperationException
from datasets.plugins.batch.batch_base_plugin import BatchBasePlugin
from datasets.utils.case_utils import snake_case_to_pascal


_logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from pyspark import SparkConf, pandas as ps
    from pyspark.sql import DataFrame as SparkDataFrame, SparkSession


@DatasetPlugin.register(constructor_keys={"hive_table"}, context=Context.BATCH)
class HiveDataset(BatchBasePlugin):
    def __init__(
        self,
        hive_table: str,
        name: Optional[str] = None,
        logical_key: Optional[str] = None,
        columns: Optional[ColumnNames] = None,
        run_id: Optional[str] = None,
        mode: Mode = Mode.READ,
        partition_by: Optional[ColumnNames] = None,
    ):
        if name:
            ValueError(
                "You cannot specify the logical name of a Dataset already created and named. "
                "Maybe you are looking for 'field_name'?"
            )

        super(HiveDataset, self).__init__(
            name=name if name else snake_case_to_pascal(hive_table),
            logical_key=logical_key,
            columns=columns,
            run_id=run_id,
            mode=mode,
            partition_by=partition_by,
        )
        self.hive_table = hive_table
        self["hive_table"] = hive_table

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

        spark_session: SparkSession = self._get_or_create_spark_session(conf=kwargs.get("conf", None))

        print(f"to_spark({self.hive_table=}, {read_columns=}, {partitions=}, {run_id=}, {filters=})")
        df: DataFrame = spark_session.read.table(self.hive_table).select(*read_columns)

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

    def _get_or_create_spark_session(self, conf: "SparkConf" = None) -> "SparkSession":
        from pyspark import SparkConf
        from pyspark.sql import SparkSession

        if conf is None:
            conf = SparkConf()
        return SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

    def write_spark(self, df: "SparkDataFrame", partition_by: Optional[ColumnNames] = None, **kwargs):
        df, partition_cols = self._write_data_frame_prep(df, partition_by=partition_by)
        _logger.info(f"write_spark({self.hive_table=}, {partition_cols=})")
        print(f"write_spark({self.hive_table=}, {partition_cols=})")

        # About <.mode("append")>:
        #  - SaveMode.Append	"append":
        #      When saving a DataFrame to a data source, if data/table already exists,
        #      contents of the DataFrame are expected to be appended to existing data.
        #  - SaveMode.Overwrite	"overwrite"
        #      Overwrite mode means that when saving a DataFrame to a data source,
        #      if data/table already exists, existing data is expected to be overwritten
        #      by the contents of the DataFrame.

        # TODO: Policy question, if the table does not exist:
        #   Should "run_id" be added to partition_cols by default?
        self._get_or_create_spark_session(conf=kwargs.get("conf", None))
        (
            df.write.option("path", self._get_dataset_path())
            .options(**kwargs)
            .mode("append")
            .partitionBy(partition_cols)
            .saveAsTable(self.hive_table)
        )

    def __repr__(self):
        return (
            f"HiveDataset({self.hive_table=},{self.name=},{self.key=},{self.partition_by=},"
            f"{self.run_id=},{self.columns=},{self.mode=}"
        )
