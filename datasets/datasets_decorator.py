import functools
from typing import Optional

from datasets import DatasetPlugin
from datasets.context import Context


def dataset(context: Optional[Context] = None, **dataset_kwargs):
    def step_decorator(func: callable):
        @functools.wraps(func)
        def step_wrapper(*args, **kwargs):
            self = args[0]
            dataset = DatasetPlugin.from_keys(context=context, **dataset_kwargs)

            setattr(self, dataset._class_field_name, dataset)

            func(*args, **kwargs)

        return step_wrapper

    return step_decorator
