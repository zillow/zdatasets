from __future__ import annotations

from abc import ABC
from typing import Callable, Dict, List, Optional

from datasets.context import Context

from .mode import Mode
from .program_executor import ProgramExecutor


class DatasetPlugin(ABC):
    """
    All dataset plugins derive from this class.
    To register as a dataset they must decorate themselves with or call DatasetPlugin.register_plugin()
    """

    _executor: ProgramExecutor
    # Context -> constructor_keys -> dataset plugin
    _plugins: Dict[Context, Dict[set[str], DatasetPlugin]] = {}
    _META_COLUMNS = ["run_id"]

    def __init__(
        self,
        name: str,
        logical_key: Optional[str] = None,
        columns=None,
        run_id: Optional[str] = None,
        mode: Mode = Mode.Read,
        attribute_name: Optional[str] = None,
    ):
        self.name = name
        self.key = logical_key
        self.mode = mode if isinstance(mode, Mode) else Mode[mode]
        self.columns = columns
        self.run_id = run_id
        self._attribute_name = attribute_name
        if not self._attribute_name:
            self._attribute_name = name

    @classmethod
    def from_keys(cls, context: Optional[Context] = None, **kwargs) -> DatasetPlugin:
        """
        This is the factory method for datasets. Not directly used by the user.
        For example usage please see test_from_keys()

        :param context: If not specified it uses the current executor context.
        :param kwargs: dataset constructor args
        :return: found DatasetPlugin
        """
        dataset_args = set(kwargs.keys())
        context_lookup: Context = context if context else cls._executor.context

        default_plugin: Optional[DatasetPlugin] = None
        max_intersect_count = 0
        ret_plugin = None

        for plugin_context in (
            plugin_context for plugin_context in cls._plugins.keys() if context_lookup & plugin_context
        ):
            for plugin_constructor_keys, plugin in cls._plugins[plugin_context].items():
                if plugin_constructor_keys.issubset(dataset_args):
                    if plugin_constructor_keys == {"name"}:
                        default_plugin = plugin
                    else:
                        match_count = len(plugin_constructor_keys.intersection(dataset_args))
                        if match_count > max_intersect_count:
                            max_intersect_count = match_count
                            ret_plugin = plugin

        if ret_plugin:
            return ret_plugin(**kwargs)
        elif default_plugin:
            return default_plugin(**kwargs)
        else:
            raise ValueError(f"f{kwargs} and {context_lookup=} not found in {cls._plugins}")

    @classmethod
    def register_plugin(cls, constructor_keys: set[str], context: Context) -> Callable:
        """
        Registration method for a dataset plugin.
        Plugins area looked up by (constructor_keys, context), so no two can be registered at the same time.

        Plugins are constructed by from_keys(), by ensuring that the current
        ProgramExecutor.context == plugin.context
        and that plugin.constructor_keys.issubset(dataset_arguments)

        constructor_keys="name" is a special case and is loaded last if no other plugins are found

        :param constructor_keys: set of dataset constructor keys
        :param context: defaults to batch, but is the context this plugin supports
        :return: decorated class
        """
        if constructor_keys is None:
            raise ValueError("constructor_keys cannot be None!")

        if context is None:
            raise ValueError("context cannot be None!")

        if not isinstance(context, Context):
            raise ValueError(f"{context=} is not of type(Context)!")

        def inner_wrapper(wrapped_class: DatasetPlugin) -> DatasetPlugin:
            if context not in cls._plugins:
                cls._plugins[context] = {}

            keys = frozenset(constructor_keys)

            if keys in cls._plugins[context] and wrapped_class != cls._plugins[context][keys]:
                raise ValueError(
                    f"{constructor_keys} already registered as a " f"dataset plugin as {context}!"
                )

            cls._plugins[context][keys] = wrapped_class
            return wrapped_class

        return inner_wrapper

    @classmethod
    def register_executor(cls, executor: ProgramExecutor):
        cls._executor = executor

    def _get_read_columns(self, columns: Optional[str] = None) -> List[str]:
        read_columns = columns if columns else self.columns
        if read_columns is not None and isinstance(read_columns, str):
            read_columns = read_columns.split(",")
        return read_columns

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f"DatasetPlugin(name={self.name}, key={self.key}, columns={self.columns})"