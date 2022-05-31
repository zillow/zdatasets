import logging
import os
from dataclasses import dataclass
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

from datasets._typing import ColumnNames, DataFrameType
from datasets.dataset_plugin import DatasetPlugin, StorageOptions
from datasets.exceptions import InvalidOperationException
from datasets.mode import Mode
from datasets.utils.case_utils import pascal_to_snake_case


_logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from pyspark import SparkConf
    from pyspark.sql import SparkSession


@dataclass
class BatchOptions(StorageOptions):
    partition_by: Optional[ColumnNames] = None
    path: Optional[Union[str, Path]] = None
    hive_table_name: Optional[str] = None


class BatchBasePlugin(DatasetPlugin, dict):
    """
    The base plugin for the BATCH execution context.
    """

    _dataset_path_func: Callable = None

    def __init__(
        self,
        name: str,
        logical_key: str = None,
        columns: Optional[ColumnNames] = None,
        run_id: Optional[str] = None,
        run_time: Optional[int] = None,
        mode: Union[Mode, str] = Mode.READ,
        options: Optional[BatchOptions] = None,
    ):
        self.program_name: Optional[str] = None
        self.hive_table_name = None
        self.partition_by = None
        if options and isinstance(options, BatchOptions):
            self.hive_table_name = options.hive_table_name
            self.partition_by = options.partition_by

        if self.hive_table_name is None:
            self.hive_table_name = pascal_to_snake_case(name)

        self._path: Optional[str] = None
        super(BatchBasePlugin, self).__init__(
            name=name,
            logical_key=logical_key,
            columns=columns,
            run_id=run_id,
            run_time=run_time,
            mode=mode,
            options=options,
        )

        def set_name(key: str, value: Optional[object]):
            if value:
                self[key] = value

        dict.__init__(self, name=name, hive_table_name=self.hive_table_name, mode=self.mode.name)
        set_name("logical_key", logical_key)
        set_name("columns", columns)
        set_name("run_id", run_id)
        set_name("run_time", run_time)
        set_name("partition_by", self.partition_by)
        set_name("options", self.options)

    def _get_filters_columns(
        self,
        columns: Optional[ColumnNames] = None,
        run_id: Optional[str] = None,
        run_time: Optional[int] = None,
        partitions: Optional[dict] = None,
    ) -> Tuple[Optional[list], Optional[Iterable[str]]]:
        read_columns = self._get_read_columns(columns)
        filters: Optional[List[Tuple]] = None
        query_run_id = run_id if run_id else self.run_id
        if query_run_id:
            max_int = 0x7FFFFFFF
            if query_run_id.isnumeric() and int(query_run_id) < max_int:
                # pyarrow may save the run_id as int if isnumeric and < max_int
                query_run_id = int(query_run_id)
            filters = [("run_id", "=", query_run_id)]

        query_run_time = run_time if run_time else self.run_time
        if query_run_time:
            filters.append(("run_time", "=", query_run_time))

        if partitions:
            if filters is None:
                filters = []
            for key, value in partitions.items():
                filters.append((key, "=", value))

        return filters, read_columns

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
        self,
        df: DataFrameType,
        partition_by: Optional[ColumnNames] = None,
    ) -> Tuple[DataFrameType, List[str]]:
        if not (self.mode & Mode.WRITE):
            raise InvalidOperationException(f"Cannot write because mode={self.mode}")

        partition_cols: List[str] = self._partition_by_to_list(partition_by)

        def add_column(df: DataFrameType, name: str, value: Union[str, int]) -> DataFrameType:
            if "pyspark.sql.dataframe.DataFrame" in str(type(df)):
                from pyspark.sql.functions import lit

                df = df.withColumn(name, lit(value))
            else:
                df[name] = value

            return df

        if "run_id" in partition_cols:
            self.run_id = self._executor.current_run_id  # DO NOT ALLOW OVERWRITE OF ANOTHER RUN ID
            df = add_column(df, "run_id", self.run_id)

        if "run_time" in partition_cols:
            self.run_time = int(self._executor.run_time)
            df = add_column(df, "run_time", self.run_time)

        return df, partition_cols

    @staticmethod
    def _get_spark_builder(conf: "Optional[SparkConf]" = None) -> "SparkSession.Builder":
        from pyspark import SparkConf
        from pyspark.sql import SparkSession

        if conf is None:
            conf = SparkConf()
        return SparkSession.builder.config(conf=conf)

    @classmethod
    def register_dataset_path_func(cls, func: Optional[Callable]):
        BatchBasePlugin._dataset_path_func = func

    def _get_dataset_path(self) -> str:
        if self._path is not None:
            return self._path

        if BatchBasePlugin._dataset_path_func:
            return BatchBasePlugin._dataset_path_func(self)

        if self.program_name is None:
            self.program_name = self._executor.current_program_name

        path = os.path.join(
            self._executor.datastore_path,
            "datastore",
            self.program_name,
            self.hive_table_name,
        )
        return path
