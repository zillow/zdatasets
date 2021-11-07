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
            dataset = DatasetPlugin.from_keys(
                name=name, class_field_name=field_name, context=context, **dataset_kwargs
            )

            if not dataset._class_field_name.isidentifier() or keyword.iskeyword(dataset._class_field_name):
                if field_name is None:
                    raise ValueError(
                        f"{name} is not a valid Python identifier, "
                        "please use 'field_name' for class field name"
                    )
                else:
                    raise ValueError(f"{field_name} is not a valid Python identifier")

            setattr(self, dataset._class_field_name, dataset)

            func(*args, **kwargs)

        return step_wrapper

    return step_decorator
