# The default online plugin!
from typing import List, Optional, Union

import pandas as pd

from datasets import DatasetPlugin, Mode
from datasets.context import Context


@DatasetPlugin.register(constructor_keys={"name"}, context=Context.ONLINE)
class DefaultOnlineDatasetPlugin(DatasetPlugin):
    def __init__(self, keys: Optional[Union[List[str], str]] = None, **kwargs):
        if isinstance(keys, str):
            self.keys = keys.split(",")
        else:
            self.keys = keys

        self._db = pd.DataFrame(
            {"key": ["first", "second", "third", "fourth"], "value": [1, 2, 3, 4]},
        )
        self._db.set_index("key")

        super(DefaultOnlineDatasetPlugin, self).__init__(**kwargs)

    def to_pandas(self, keys: Optional[List[str]] = None, columns: Optional[str] = None) -> pd.DataFrame:
        read_columns = self._get_read_columns(columns)

        print(type(self.keys), f"{self.keys=}")
        if keys or self.keys:
            df = self._db[self._db.key.isin(keys if keys else self.keys)]
        else:
            df = self._db

        for meta_column in self._META_COLUMNS:
            if meta_column in df and (read_columns is None or meta_column not in read_columns):
                del df[meta_column]
        return df

    def write(self, data: pd.DataFrame):
        if not (self.mode & Mode.WRITE):
            raise ValueError(f"Cannot write because mode={self.mode}")
        new_db = pd.merge(self._db, data, on="key", how="outer")
        new_db["value"] = new_db["value_y"].fillna(new_db["value_x"]).astype("int")

        self._db = new_db[["key", "value"]]
