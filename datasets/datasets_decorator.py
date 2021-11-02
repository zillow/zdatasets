import functools
import keyword
from typing import Callable, Optional

from datasets.context import Context
from datasets.utils import _pascal_to_snake_case
from datasets.txf_integration.txf_utils import add_txf_attributes
from .dataset_plugin import DatasetPlugin


# flake8: noqa: C901
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
                _snake_name = _pascal_to_snake_case(dataset.name)
                setattr(self, _snake_name, dataset)

            # Transformation integration
            if hasattr(self, "name"):
                flow_name = self.name
            else:
                flow_name = type(self).__name__

            add_txf_attributes(self)
            if not self._txf_registered_datasets.get(flow_name, None):
                self._txf_registered_datasets[flow_name] = [dataset]
            else:
                self._txf_registered_datasets[flow_name].append(dataset)

            if len(self._txf_callbacks):
                for _, callback in self._txf_callbacks.items():
                    callback()
            # End Transformation integration

            func(*args, **kwargs)

        return step_wrapper

    return step_decorator
