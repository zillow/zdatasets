from typing import TYPE_CHECKING, Iterable, Union

import pandas as pd


if TYPE_CHECKING:
    # flake8: noqa: F401
    from pyspark import SparkConf, pandas as ps
    from pyspark.sql import DataFrame as SparkDataFrame, SparkSession


ColumnNames = Union[str, Iterable[str]]
DataFrameType = Union[pd.DataFrame, "dd.DataFrame", "SparkDataFrame"]  # flake8: noqa: F821
