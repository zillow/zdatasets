# isort: skip_file
# flake8: noqa: F401
from metaflow._vendor.click import ParamType

from datasets.dataset_plugin import DatasetPlugin
from datasets.datasets_decorator import dataset

from datasets.mode import Mode

from datasets import plugins


class DatasetTypeClass(ParamType):
    name = "Dataset"

    def convert(self, value, param, ctx):
        if isinstance(value, str):
            import json

            params = json.loads(value)
            return DatasetPlugin.from_keys(context=DatasetPlugin._executor.context, **params)
        elif isinstance(value, dict):
            return DatasetPlugin.from_keys(context=DatasetPlugin._executor.context, **value)
        else:
            return value

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "Dataset"


DatasetType = DatasetTypeClass()
