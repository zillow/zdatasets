import functools
from typing import Optional

from datasets.context import Context
from datasets.txf_integration.txf_utils import add_txf_attributes
from .dataset_plugin import DatasetPlugin


def dataset(context: Optional[Context] = None, **dataset_kwargs):
    def step_decorator(func: callable):
        @functools.wraps(func)
        def step_wrapper(*args, **kwargs):
            self = args[0]
            dataset = DatasetPlugin.from_keys(context=context, **dataset_kwargs)

            setattr(self, dataset._class_field_name, dataset)

            # Transformation integration
            add_txf_attributes(self)
            if not self._txf_registered_datasets.get(self.name, None):
                self._txf_registered_datasets[self.name] = [dataset]
            else:
                self._txf_registered_datasets[self.name].append(dataset)

            if len(self._txf_callbacks):
                for _, callback in self._txf_callbacks.items():
                    callback()
            # End Transformation integration

            func(*args, **kwargs)

        return step_wrapper

    return step_decorator
