import functools
import keyword
from typing import Callable, Optional

from datasets import DatasetPlugin
from datasets.context import Context
from datasets.utils.case_utils import pascal_to_snake_case


def dataset(
    name: str = None,
    field_name: Optional[str] = None,
    context: Optional[Context] = None,
    **dataset_kwargs,
):
    def step_decorator(func: Callable):
        @functools.wraps(func)
        def step_wrapper(*args, **kwargs):
            self = args[0]
            dataset = DatasetPlugin.from_keys(name=name, context=context, **dataset_kwargs)

            if field_name:
                if not field_name.isidentifier() or keyword.iskeyword(field_name):
                    raise ValueError(f"{field_name} is not a valid Python identifier")
                setattr(self, field_name, dataset)
            else:
                _snake_name = pascal_to_snake_case(dataset.name)
                setattr(self, _snake_name, dataset)

            func(*args, **kwargs)

        return step_wrapper

    return step_decorator
