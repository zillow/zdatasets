# isort: skip_file
# flake8: noqa: F401
from metaflow._vendor.click import ParamType
from metaflow.parameters import Parameter

from datasets.mode import Mode
from datasets.dataset_plugin import DatasetPlugin

from datasets.context import Context
from datasets.datasets_decorator import dataset


from datasets import plugins
from datasets.plugins.batch.hive_dataset import HiveDataset

Dataset = DatasetPlugin.Dataset


class DatasetTypeClass(ParamType):
    name = "Dataset"

    def convert(self, value, param, ctx):
        if isinstance(value, str):
            import json

            params = json.loads(value)
            return Dataset(context=DatasetPlugin._executor.context, **params)
        else:
            return value

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "Dataset"


DatasetType = DatasetTypeClass()

# class DatasetParameter(Parameter):
#     def __init__(
#         self,
#         name,
#         required=False,
#         help=None,
#         **kwargs
#     ):
#         # Defaults are DeployTimeField
#         v = kwargs.get("default")
#         if v is not None:
#             _, file_type, _ = LocalFile.is_file_handled(v)
#             # Ignore error because we may never use the default
#             if file_type is None:
#                 o = {"type": "self", "is_text": is_text, "encoding": encoding, "url": v}
#                 kwargs["default"] = DeployTimeField(
#                     name,
#                     str,
#                     "default",
#                     lambda ctx, full_evaluation, o=o: LocalFile(
#                         o["is_text"], o["encoding"], o["url"]
#                     )(ctx)
#                     if full_evaluation
#                     else json.dumps(o),
#                     print_representation=v,
#                 )
#             else:
#                 kwargs["default"] = DeployTimeField(
#                     name,
#                     str,
#                     "default",
#                     lambda _, __, is_text=is_text, encoding=encoding, v=v: Uploader.encode_url(
#                         "external-default", v, is_text=is_text, encoding=encoding
#                     ),
#                     print_representation=v,
#                 )

#         super(IncludeFile, self).__init__(
#             name,
#             required=required,
#             help=help,
#             type=FilePathClass(is_text, encoding),
#             **kwargs
#         )

#     def load_parameter(self, val):
#         if val is None:
#             return val
#         ok, file_type, err = LocalFile.is_file_handled(val)
#         if not ok:
#             raise MetaflowException(
#                 "Parameter '%s' could not be loaded: %s" % (self.name, err)
#             )
#         if file_type is None or isinstance(file_type, LocalFile):
#             raise MetaflowException(
#                 "Parameter '%s' was not properly converted" % self.name
#             )
#         return file_type.load(val)
