import logging
from typing import TYPE_CHECKING, Iterable, List, Optional, Tuple, Union

import pandas as pd

from datasets import Mode
from datasets._typing import ColumnNames
from datasets.dataset_plugin import DatasetPlugin
from datasets.exceptions import InvalidOperationException


_logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import dask.dataframe as dd
    from pyspark.sql import DataFrame as SparkDataFrame


class BatchBasePlugin(DatasetPlugin, dict):
    """
    The base plugin for the BATCH execution context.
    """

    def __init__(
        self,
        name: str,
        logical_key: str = None,
        columns: Optional[ColumnNames] = None,
        run_id: Optional[str] = None,
        mode: Mode = Mode.READ,
        partition_by: Optional[ColumnNames] = None,
    ):
        self.partition_by = partition_by
        self.program_name = self._executor.current_program_name
        super(BatchBasePlugin, self).__init__(
            name=name,
            logical_key=logical_key,
            columns=columns,
            run_id=run_id,
            mode=mode,
        )

        def set_name(key: str, value: Optional[object]):
            if value:
                self[key] = value

        dict.__init__(self, name=name, mode=mode.name)
        set_name("logical_key", logical_key)
        set_name("columns", columns)
        set_name("run_id", run_id)
        set_name("partition_by", partition_by)

    def _get_filters_columns(
        self,
        columns: Optional[ColumnNames] = None,
        run_id: Optional[str] = None,
        partitions: Optional[dict] = None,
    ) -> Tuple[Optional[list], Optional[Iterable[str]]]:
        read_columns = self._get_read_columns(columns)
        filters: Optional[List[Tuple]] = None
        query_run_id = run_id if run_id else self.run_id
        if query_run_id:
            filters = [("run_id", "=", query_run_id)]

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
        df: Union[pd.DataFrame, "dd.DataFrame", "SparkDataFrame"],
        partition_by: Optional[ColumnNames] = None,
    ) -> Tuple[Union[pd.DataFrame, "dd.DataFrame", "SparkDataFrame"], List[str]]:
        if not (self.mode & Mode.WRITE):
            raise InvalidOperationException(f"Cannot write because mode={self.mode}")

        partition_cols: List[str] = self._partition_by_to_list(partition_by)

        if "run_id" in partition_cols:
            self.run_id = self._executor.current_run_id  # DO NOT ALLOW OVERWRITE OF ANOTHER RUN ID
            # TODO: should I add run_time for latest run query scenario?
            if "pyspark.sql.dataframe.DataFrame" in str(type(df)):
                from pyspark.sql.functions import lit

                df = df.withColumn("run_id", lit(self.run_id))
            else:
                df["run_id"] = self.run_id
        return df, partition_cols
