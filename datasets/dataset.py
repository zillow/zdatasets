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
        logical_key: str = None,
        columns=None,
        run_id: str = None,
        mode: Mode = Mode.Read,
        attribute_name: str = None,
    ):
        self.name = name
        self.key = logical_key
        self.mode = mode
        self.columns = columns
        self.run_id = run_id
        self.program_name: Optional[str] = None
        self._attribute_name = attribute_name
        if not self._attribute_name:
            self._attribute_name = name

    @classmethod
    def from_keys(cls, **kwargs) -> Dataset:
        """
        This is the factory method for datasets.
        :param kwargs: dataset constructor args
        :return: found Dataset
        """
        dataset_args = set(kwargs.keys())

        default_name_plugin = None
        for plugin_lookup in (p for p in cls._plugins.keys() if cls._executor.context in p):
            plugin_keys: set[str] = set(plugin_lookup.split(","))

            plugin_keys_only = plugin_keys.difference({cls._executor.context})
            if plugin_keys_only.issubset(dataset_args):
                if plugin_keys_only == {"name"}:
                    default_name_plugin = plugin_lookup
                else:
                    return cls._plugins[plugin_lookup](**kwargs)

        if default_name_plugin:
            return cls._plugins[default_name_plugin](**kwargs)
        else:
            raise ValueError(f"f{kwargs} not found in {cls._plugins}")

    @classmethod
    def register_plugin(cls, constructor_keys: set[str], context: str = "offline") -> Callable:
        """
        Registration method for a dataset plugin.
        Plugins area looked up by (constructor_keys, context), so no two can be registered at the same time.

        Plugins are constructed by from_keys(), by ensuring that the current
        ProgramExecutor.context == plugin.context
        and that plugin.constructor_keys.issubset(dataset_arguments)

        constructor_keys="name" is a special case and is loaded last if no other plugins are found

        :param constructor_keys: set of dataset constructor keys
        :param context: defaults to offline, but is the context this plugin supports
        :return: decorated class
        """
        if constructor_keys is None:
            raise ValueError("constructor_keys cannot be None!")

        def inner_wrapper(wrapped_class: Dataset) -> Dataset:
            plugin_lookup = ",".join(constructor_keys.union({context}))  # KEY for plugin factory
            if plugin_lookup in cls._plugins:
                raise ValueError(f"{constructor_keys} already registered as a dataset plugin!")
            cls._plugins[plugin_lookup] = wrapped_class
            return wrapped_class

        return inner_wrapper

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
