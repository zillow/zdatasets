import functools
import keyword
from typing import Optional

from datasets import DatasetPlugin
from datasets.context import Context


def dataset(name: str, field_name: Optional[str] = None, context: Optional[Context] = None, **dataset_kwargs):
    def step_decorator(func: callable):
        @functools.wraps(func)
        def step_wrapper(*args, **kwargs):
            self = args[0]
            dataset = DatasetPlugin.from_keys(name=name, context=context, **dataset_kwargs)

            if field_name:
                if not field_name.isidentifier() or keyword.iskeyword(field_name):
                    raise ValueError(f"{field_name} is not a valid Python identifier")
                setattr(self, field_name, dataset)
            else:
                setattr(self, name, dataset)

            func(*args, **kwargs)

        return step_wrapper

    return step_decorator
