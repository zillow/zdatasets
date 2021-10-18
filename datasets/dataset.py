from __future__ import annotations

from abc import ABC
from typing import Callable, Dict, List, Optional

from .mode import Mode
from .program_executor import ProgramExecutor


class Dataset(ABC):
    _executor: ProgramExecutor
    _plugins: Dict[str, Dataset] = {}
    _META_COLUMNS = ["run_id"]

    def __init__(
        self,
        name: str = None,
        key: str = None,
        columns=None,
        run_id=None,
        mode: Mode = Mode.Read,
        attribute_name: str = None,
    ):
        self.name = name
        self.key = key
        self.mode = mode
        self.columns = columns
        self.run_id = run_id
        self.program_name: Optional[str] = None
        self._attribute_name = attribute_name
        if not self._attribute_name:
            self._attribute_name = name

    @classmethod
    def from_keys(cls, **kwargs) -> Dataset:
        constructor_keys = set(kwargs.keys())

        max_plugin_lookup = None
        max_fix = 0
        for plugin_lookup in cls._plugins.keys():
            plugin_keys = set(plugin_lookup.split(","))

            if cls._executor.context in plugin_keys:
                fit = len(constructor_keys.intersection(plugin_keys))

                if fit >= max_fix:
                    max_plugin_lookup = plugin_lookup
                max_fix = fit

        return cls._plugins[max_plugin_lookup](**kwargs)

    @classmethod
    def register_plugin(
        cls, plugin: Dataset, constructor_keys: set[str] = None, context: str = "offline"
    ) -> Callable:
        if constructor_keys is None:
            raise ValueError("constructor_keys cannot be None!")

        plugin_lookup = ",".join({context}.union(constructor_keys))
        if plugin_lookup in cls._plugins:
            raise ValueError(f"{constructor_keys} already registered as a dataset plugin!")
        cls._plugins[plugin_lookup] = plugin

    @classmethod
    def register_executor(cls, executor: ProgramExecutor = None):
        cls._executor = executor

    def _get_read_columns(self, columns: str = None) -> List[str]:
        read_columns = columns if columns else self.columns
        if read_columns is not None and isinstance(read_columns, str):
            read_columns = read_columns.split(",")
        return read_columns

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f"Dataset(name={self.name}, key={self.key}, columns={self.columns})"
