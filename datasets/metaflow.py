from typing import Optional

from metaflow._vendor.click import ParamType
from metaflow.parameters import Parameter

from datasets.dataset_plugin import DatasetPlugin


class DatasetTypeClass(ParamType):
    name = "Dataset"

    def convert(self, value, param, ctx):
        if isinstance(value, str):
            import json

            params = json.loads(value)
            return DatasetPlugin.Dataset(context=DatasetPlugin._executor.context, **params)
        else:
            return value

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "Dataset"


class DatasetParameter(Parameter):
    def __init__(
        self,
        name: str,
        default: Optional[DatasetPlugin] = None,
        required: Optional[bool] = False,
        help: Optional[str] = None,
        **kwargs
    ):
        super(DatasetParameter, self).__init__(
            name, required=required, help=help, default=default, type=DatasetTypeClass(), **kwargs
        )
