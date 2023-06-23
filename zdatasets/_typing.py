from typing import TYPE_CHECKING, List, Union

import pandas


if TYPE_CHECKING:
    # flake8: noqa: F401
    import dask.dataframe as dd
    from pyspark import pandas as ps
    from pyspark.sql import DataFrame as SparkDataFrame


ColumnNames = Union[str, List[str]]
DataFrameType = Union[
    pandas.DataFrame, "dd.DataFrame", "ps.DataFrame", "SparkDataFrame"
]  # flake8: noqa: F821
