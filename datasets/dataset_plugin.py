from __future__ import annotations

import dataclasses
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, Optional, Tuple, Type, Union

from datasets._typing import ColumnNames, DataFrameType
from datasets.context import Context
from datasets.utils.case_utils import is_upper_pascal_case

from .mode import Mode
from .program_executor import ProgramExecutor


_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)


@dataclass
class StorageOptions:
    def to_json(self) -> dict:
        ret = dataclasses.asdict(self, dict_factory=lambda x: {k: v for (k, v) in x if v is not None})
        ret["type"] = type(self).__name__
        return ret


DatasetPluginFactory = Callable[
    [Dict[StorageOptions, "DatasetPlugin"], Context, Optional[StorageOptions]],
    Tuple["DatasetPlugin", Optional[StorageOptions]],
]


class DatasetPlugin(ABC):
    """
    All dataset plugins derive from this class.
    To register as a dataset they must decorate themselves with or call Dataset.register()
    """

    _executor: ProgramExecutor
    _plugins: Dict[Type[StorageOptions], DatasetPlugin] = {}
    _META_COLUMNS = ["run_id", "run_time"]

    _dataset_name_validator: Callable[[str], None]
    _dataset_plugin_factory: DatasetPluginFactory
    _default_context_plugins: Dict[Context, DatasetPlugin] = {}

    @abstractmethod
    def __init__(
        self,
        name: str,
        logical_key: Optional[str] = None,
        columns: Optional[ColumnNames] = None,
        run_id: Optional[str] = None,
        run_time: Optional[int] = None,
        mode: Union[Mode, str] = Mode.READ,
        options: Optional[StorageOptions] = None,
    ):
        """
        :param name: The dataset logical name.
        :param logical_key:
            The logical primary key, strongly suggested, and can later be
            used when creating Hive/Dynamo tables or registering with a Catalog.
        :param columns: Fetch columns
        :param run_id: The program run_id partition to select from.
        :param run_time: The program run_time in UTC epochs
        :param mode: The data access read/write mode
        """
        DatasetPlugin._dataset_name_validator(name)
        self.name = name
        self.key = logical_key  # TODO: validate this too!
        self.mode: Mode = mode if isinstance(mode, Mode) else Mode[mode]
        self.columns = columns
        self.run_id = run_id
        self.run_time = run_time
        self.options = options

    @abstractmethod
    def write(self, data: DataFrameType, **kwargs):
        pass

    @abstractmethod
    def to_pandas(
        self,
        columns: Optional[str] = None,
        run_id: Optional[str] = None,
        run_time: Optional[int] = None,
        **kwargs,
    ) -> DataFrameType:
        pass

    @classmethod
    def factory(
        cls,
        name: Optional[str] = None,
        logical_key: Optional[str] = None,
        columns: Optional[ColumnNames] = None,
        run_id: Optional[str] = None,
        run_time: Optional[int] = None,
        mode: Union[Mode, str] = Mode.READ,
        options: Optional[StorageOptions] = None,
        options_by_context: Optional[Dict[Context, StorageOptions]] = None,
        context: Optional[Union[Context, str]] = None,
        *args,
        **kwargs,
    ) -> DatasetPlugin:
        if name:
            # name is None in the FlowDataset case
            cls._dataset_name_validator(name)

        # Use InitialCaps for class names (or for factory functions that return classes).
        plugin: DatasetPlugin
        options: Optional[StorageOptions]
        plugin, options = cls._dataset_plugin_factory(
            context=context, options=options, options_by_context=options_by_context
        )
        return plugin(
            name=name,
            logical_key=logical_key,
            columns=columns,
            run_id=run_id,
            run_time=run_time,
            mode=mode,
            options=options,
            *args,
            **kwargs,
        )

    @classmethod
    def _get_context(cls, context: Optional[Union[Context, str]] = None) -> Context:
        if context:
            return context if isinstance(context, Context) else Context[context]
        else:
            return cls._executor.context

    @classmethod
    def _validate_register_parameters(
        cls,
        context=Context.BATCH,
        options_type: Optional[Type[StorageOptions]] = None,
        as_default_context_plugin=False,
    ):
        if context is None:
            raise ValueError("context cannot be None!")

        if not isinstance(context, Context):
            raise ValueError(f"{context=} is not of type(Context)!")

        if as_default_context_plugin and context in cls._default_context_plugins:
            raise ValueError(f"{context=} already registered in {cls._default_context_plugins=}")

        if options_type in cls._plugins:
            raise ValueError(f"{options_type=} already registered in {cls._plugins=}")

    @classmethod
    def register(
        cls,
        context=Context.BATCH,
        options_type: Optional[Type[StorageOptions]] = None,
        as_default_context_plugin: bool = False,
    ) -> Callable:
        cls._validate_register_parameters(context, options_type, as_default_context_plugin)

        def inner_wrapper(wrapped_class: DatasetPlugin) -> DatasetPlugin:
            if as_default_context_plugin:
                cls._default_context_plugins[context] = wrapped_class

            if options_type:
                cls._plugins[options_type] = wrapped_class

            return wrapped_class

        return inner_wrapper

    @classmethod
    def register_executor(cls, executor: ProgramExecutor):
        cls._executor = executor

    @classmethod
    def default_plugin_factory(
        cls,
        context: Optional[Union[Context, str]] = None,
        options: Optional[StorageOptions] = None,
        options_by_context: Optional[Dict[Context, StorageOptions]] = None,
    ) -> Tuple[DatasetPlugin, Optional[StorageOptions]]:
        context_lookup: Context = DatasetPlugin._get_context(context)

        if options is None and options_by_context is None:
            return (cls._default_context_plugins[context_lookup], None)
        elif options and options_by_context:
            raise ValueError("Please set one of options or options_by_context, not both.")
        elif options:
            if type(options) not in cls._plugins:
                raise ValueError(f"{type(options)=} not in {cls._plugins=}")
            return (cls._plugins[type(options)], options)
        elif options_by_context:
            if context_lookup not in options_by_context:
                raise ValueError(f"{context_lookup=} not in {options_by_context=}")
            context_options = options_by_context[context_lookup]
            plugin = cls._plugins[type(context_options)]
            return (plugin, context_options)
        else:
            raise ValueError("Either options or options_by_context must be set")

    @classmethod
    def register_plugin_factory(cls, func: DatasetPluginFactory):
        cls._dataset_plugin_factory = func

    @classmethod
    def register_dataset_name_validator(cls, dataset_name_validator: Callable[[str]]):
        cls._dataset_name_validator = dataset_name_validator

    @staticmethod
    def validate_dataset_name(name: str):
        if not is_upper_pascal_case(name):
            raise ValueError(
                f"'{name}' is not a valid Dataset name.  "
                f"Please use Upper Pascal Case syntax: https://en.wikipedia.org/wiki/Camel_case"
            )
        else:
            pass

    def _get_read_columns(self, columns: Optional[ColumnNames] = None) -> Optional[Iterable[str]]:
        read_columns = columns if columns else self.columns
        if read_columns is not None and isinstance(read_columns, str):
            read_columns = read_columns.split(",")
        return read_columns

    def __repr__(self):
        return f"Dataset({self.name=},{self.mode=},{self.key=},{self.columns=})"
