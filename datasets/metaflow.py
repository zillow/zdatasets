import functools
import json
from typing import Dict, List, Optional, Type, Union

import pydantic
from metaflow._vendor.click import ParamType
from metaflow.parameters import Parameter
from datasets import DataFrameType

from datasets._typing import ColumnNames
from datasets.context import Context
from datasets.dataset_plugin import DatasetPlugin, StorageOptions
from datasets.mode import Mode


class _DatasetTypeClass(ParamType):
    name = "Dataset"

    def convert(self, value, param, ctx) -> DatasetPlugin:
        if isinstance(value, str):
            params: _PydanticDatasetParameters = _PydanticDatasetParameters.parse_raw(value)
            params_dict = params.__dict__.copy()
            if "context" in params_dict:
                del params_dict["context"]
            return DatasetPlugin.factory(
                context=params.context if params.context else DatasetPlugin._executor.context, **params_dict
            )
        else:
            return value

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "Dataset"


class _OptionsDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(self, object_hook=self.object_hook, *args, **kwargs)

    @staticmethod
    def get_subclasses(cls: object) -> list[Type[StorageOptions]]:
        all_subclasses: List[Type] = []

        for subclass in cls.__subclasses__():
            all_subclasses.append(subclass)
            all_subclasses.extend(_OptionsDecoder.get_subclasses(subclass))

        return all_subclasses

    def object_hook(self, obj):
        type = obj.get("type")
        if type is None:
            return obj

        del obj["type"]  # Delete the `type` key as it isn't used in the models

        mapping: Dict[str, Type[StorageOptions]] = {
            f.__name__.lower(): f for f in _OptionsDecoder.get_subclasses(StorageOptions)
        }

        return mapping[type.lower()].parse_obj(obj)


class _PydanticDatasetParameters(pydantic.BaseModel):
    name: Optional[str] = None
    logical_key: Optional[str] = None
    columns: Optional[ColumnNames] = None
    run_id: Optional[str] = None
    run_time: Optional[int] = None
    mode: Union[Mode, str] = Mode.READ
    options: Optional[StorageOptions] = None
    options_by_context: Optional[Dict[Union[Context, str], StorageOptions]] = None
    context: Optional[Union[Context, str]] = None

    class Config:
        def options_encoder(obj):
            return dict(type=type(obj).__name__, **obj.dict())

        json_encoders = {StorageOptions: options_encoder}
        json_loads = functools.partial(json.loads, cls=_OptionsDecoder)


class DatasetParameter(Parameter, DatasetPlugin):
    def __init__(
        self,
        name: str,
        default: Optional[DatasetPlugin] = None,
        required: Optional[bool] = False,
        help: Optional[str] = None,
        **kwargs,
    ):
        super(DatasetParameter, self).__init__(
            name, required=required, help=help, default=default, type=_DatasetTypeClass(), **kwargs
        )

    def write(self, data: DataFrameType, **kwargs):
        raise NotImplementedError()

    def to_pandas(
        self,
        columns: Optional[str] = None,
        run_id: Optional[str] = None,
        run_time: Optional[int] = None,
        **kwargs,
    ) -> DataFrameType:
        raise NotImplementedError()
