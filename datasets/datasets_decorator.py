import functools
from typing import Optional

from datasets.context import Context

from .dataset_plugin import DatasetPlugin


def dataset(context: Optional[Context] = None, **dataset_kwargs):
    def step_decorator(func):
        @functools.wraps(func)
        def step_wrapper(*args, **kwargs):
            self = args[0]
            dataset = DatasetPlugin.from_keys(context=context, **dataset_kwargs)

            if not hasattr(self, dataset._class_field_name):
                setattr(self, dataset._class_field_name, dataset)

            func(*args, **kwargs)

        return step_wrapper

    return step_decorator
